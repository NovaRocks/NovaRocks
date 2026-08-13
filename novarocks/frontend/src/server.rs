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

use std::future::Future;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use novarocks::common::app_config::NovaRocksConfig;
use novarocks::engine::frontend_capabilities as core_capabilities;
use novarocks::engine::table_maintenance::BackgroundMaintenanceAttemptFactory;
use novarocks::mv::storage_observation::MvStorageObservationPort;
use novarocks::query_execution::backend::CoordinatorReportEndpointSink;
use novarocks::query_execution::session::QuerySessionFactory;
use novarocks_spi::connector::ConnectorControlFactory;
use novarocks_state_store::StateStoreHostConfig;

use crate::mv::{maintenance::MaintenanceCoordinatorConfig, scheduler::FrontendMvSchedulerConfig};
use crate::native::report_server::FrontendReportServerHandle;
use crate::{
    ClusterBackendOpenConfig, FrontendApplicationError, FrontendApplicationErrorKind,
    FrontendApplicationHost, FrontendExecutionConfig, FrontendQueryControlTimeouts,
};

type ShutdownSignal = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Clone)]
struct FrontendBackgroundMaintenanceAttemptFactory {
    role: novarocks::common::app_config::ClusterRole,
    topology: novarocks::query_execution::backend::BackendTopologyService,
}

impl BackgroundMaintenanceAttemptFactory for FrontendBackgroundMaintenanceAttemptFactory {
    fn begin_automatic_maintenance_attempt(
        &self,
    ) -> Result<novarocks::engine::table_maintenance::BackgroundMaintenanceAttempt, String> {
        core_capabilities::background_maintenance_attempt(self.role, self.topology.clone())
    }
}

#[derive(Clone)]
pub struct FrontendServerConfig {
    pub config: NovaRocksConfig,
    pub config_path: Option<PathBuf>,
    pub port_override: Option<u16>,
    /// Provider-owned FE control factories composed by the server root.
    pub connector_control_factories: Vec<Arc<dyn ConnectorControlFactory>>,
    /// Application-owned storage observation composed by the server role.
    /// Frontend and Core never decode provider table handles directly.
    pub mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    /// Typed StateStore host input. The FE remains the owner of opening and
    /// shutting down this host; the server only supplies the composition data.
    pub state_store_host_config: Option<StateStoreHostConfig>,
}

#[cfg(test)]
fn standalone_open_services(
    system_catalog: Arc<dyn novarocks::engine::system_catalog::SystemCatalog>,
    host: &FrontendApplicationHost,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
) -> novarocks::engine::StandaloneOpenServices {
    novarocks::engine::StandaloneOpenServices::new(
        host.execution_role(),
        system_catalog,
        host.view_service(),
        host.statistics_service(),
        host.table_maintenance_service(),
        host.mv_repository(),
        host.mv_application_service(),
        host.query_execution_service(),
        host.backend_query_event_sink(),
        host.backend_topology_port(),
        host.coordinator_report_endpoint_sink(),
        host.query_control_service(),
        host.connector_control_registry(),
        host.connector_control_factory_resolver(),
        0,
    )
    .with_catalog_application(
        host.catalog_application_port(),
        host.catalog_runtime_projection(),
    )
    .with_statistics_application(host.statistics_application_port())
    .with_statistics_target_resolver_sink(host.statistics_application_port())
    .with_statistics_table_reader_sink(host.statistics_application_port())
    .with_statistics_attempt_executor_sink(host.statistics_application_port())
    .with_mv_refresh_provider_activation_sink(host.mv_refresh_provider_activation_sink())
    .with_mv_background_engine_sink(host.mv_background_engine_sink())
    .with_mv_storage_observation(mv_storage_observation.clone())
    // The frontend owns when MV state is restored at startup. The engine keeps
    // its own implementation as the fallback for a composition without a
    // frontend; both run through the same runner, so the step ordering is
    // identical either way.
    .with_mv_startup_restore(Arc::new(
        crate::mv::startup_restore::FrontendMvStartupRestore::new(
            host.connector_control_registry(),
            host.catalog_runtime_projection(),
            host.catalog_application_port(),
            mv_storage_observation,
            host.mv_repository(),
            {
                let application = host.mv_application_service();
                Box::new(move || {
                    application
                        .recover_startup_mv_refreshes()
                        .map_err(|error| format!("frontend MV startup recovery failed: {error}"))
                })
            },
        ),
    ))
}

/// Opens the frontend services once for an externally composed server. The
/// all-in-one composition root uses the returned host both to run MySQL and
/// to provide the terminal fallback ingress installed on the native backend endpoint.
pub async fn open_frontend_application_for_server(
    config: &FrontendServerConfig,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    let execution = resolve_frontend_execution_config(config)?;
    let backend = cluster_backend_open_config(&config.config)?;
    FrontendApplicationHost::open_with_factories(
        resolved_state_store_host_config(config),
        execution,
        backend,
        config.connector_control_factories.clone(),
    )
    .await
}

/// Complete the one Frontend-owned startup graph and return a ready SQL
/// session factory.  Every Core value constructed here is a closed domain
/// capability; this function never creates an application aggregate or lets a
/// request resolve services from the lifecycle host.
pub fn build_frontend_query_session_factory(
    host: &FrontendApplicationHost,
    system_catalog: Arc<dyn novarocks::engine::system_catalog::SystemCatalog>,
    exchange_port: u16,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
) -> Result<Arc<dyn QuerySessionFactory>, FrontendApplicationError> {
    let catalog_service = Arc::new(novarocks::engine::new_query_catalog_service());
    let unified_statistics = Arc::new(novarocks::engine::UnifiedStatisticsResolver::default());
    let catalog_application = host.catalog_application_port();
    let catalog_projection = host.catalog_runtime_projection();
    let connector_control = host.connector_control_registry();
    let query_execution = host.query_execution_service();
    let topology = host.backend_topology_port();
    let role = host.execution_role();
    let mv_repository = host.mv_repository();
    let mv_application = host.mv_application_service();
    let view_service = host.view_service();
    let statistics_service = host.statistics_service();
    let statistics_application = host.statistics_application_port();
    let maintenance_service = host.table_maintenance_service();

    core_capabilities::bind_catalog_runtime_projection(
        catalog_projection.as_ref(),
        Arc::clone(&catalog_service),
        Arc::clone(&connector_control),
    )
    .map_err(FrontendApplicationError::server)?;

    let iceberg_mv_ports = novarocks::engine::IcebergMvCorePorts::new(
        Arc::clone(&catalog_service),
        Some(Arc::clone(&catalog_application)),
        Arc::clone(&connector_control),
        Arc::clone(&mv_repository),
        Arc::clone(&mv_storage_observation),
    );
    if let Some(sink) = host.mv_refresh_provider_activation_sink() {
        core_capabilities::bind_mv_refresh_provider_activation(
            sink.as_ref(),
            core_capabilities::MvRefreshProviderActivationPorts::new(
                Arc::clone(&catalog_service),
                Some(Arc::clone(&catalog_application)),
                Arc::clone(&connector_control),
                Arc::clone(&unified_statistics),
                query_execution.clone(),
                topology.clone(),
                exchange_port,
                Arc::clone(&mv_repository),
                Arc::clone(&mv_storage_observation),
            ),
        )
        .map_err(FrontendApplicationError::server)?;
    }

    let startup_restore = crate::mv::startup_restore::FrontendMvStartupRestore::new(
        Arc::clone(&connector_control),
        Arc::clone(&catalog_projection),
        Arc::clone(&catalog_application),
        Arc::clone(&mv_storage_observation),
        Arc::clone(&mv_repository),
        {
            let application = Arc::clone(&mv_application);
            Box::new(move || {
                application
                    .recover_startup_mv_refreshes()
                    .map_err(|error| format!("frontend MV startup recovery failed: {error}"))
            })
        },
    );
    novarocks::mv::startup_restore::run_mv_startup_restore(&startup_restore)
        .map_err(FrontendApplicationError::server)?;

    core_capabilities::bind_statistics_target_resolver(
        statistics_application.as_ref(),
        Arc::clone(&connector_control),
    )
    .map_err(FrontendApplicationError::server)?;
    core_capabilities::bind_statistics_table_reader(
        statistics_application.as_ref(),
        Arc::clone(&connector_control),
    )
    .map_err(FrontendApplicationError::server)?;
    core_capabilities::bind_statistics_attempt_executor(
        statistics_application.as_ref(),
        core_capabilities::StatisticsAttemptExecutorPorts::new(
            role,
            Arc::clone(&connector_control),
            topology.clone(),
            query_execution.clone(),
            iceberg_mv_ports.clone(),
        ),
    )
    .map_err(FrontendApplicationError::server)?;

    let maintenance_ports = core_capabilities::MaintenanceCommandPorts::new(
        Arc::clone(&catalog_service),
        Some(Arc::clone(&catalog_application)),
        Arc::clone(&connector_control),
        Arc::clone(&mv_storage_observation),
        query_execution.clone(),
        Arc::clone(&maintenance_service),
    );
    let maintenance_engine = core_capabilities::background_maintenance_engine(
        maintenance_ports.clone(),
        Arc::new(FrontendBackgroundMaintenanceAttemptFactory {
            role,
            topology: topology.clone(),
        }),
    );
    if let Err(error) = maintenance_service.start(Arc::clone(&maintenance_engine)) {
        let primary = FrontendApplicationError::server(format!(
            "start table maintenance service failed: {error}"
        ));
        return match maintenance_service.shutdown() {
            Ok(()) => Err(primary),
            Err(cleanup_error) => Err(primary.with_cleanup_context(format!(
                "shutdown table maintenance service after startup failure: {cleanup_error}"
            ))),
        };
    }
    if let Some(sink) = host.mv_background_engine_sink() {
        if let Err(error) = core_capabilities::bind_mv_background_engine(
            sink.as_ref(),
            core_capabilities::MvBackgroundPorts::new(
                Arc::clone(&catalog_service),
                Some(Arc::clone(&catalog_application)),
                Arc::clone(&connector_control),
                Arc::clone(&mv_repository),
                Arc::clone(&mv_storage_observation),
            ),
            Arc::clone(&maintenance_engine),
        ) {
            let primary = FrontendApplicationError::server(format!(
                "bind frontend MV background engine failed: {error}"
            ));
            return match maintenance_service.shutdown() {
                Ok(()) => Err(primary),
                Err(cleanup_error) => Err(primary.with_cleanup_context(format!(
                    "shutdown table maintenance service after MV background bind failure: {cleanup_error}"
                ))),
            };
        }
    }

    let query_compiler =
        core_capabilities::query_compiler(core_capabilities::QueryCompilerPorts::new(
            Arc::clone(&catalog_service),
            Some(Arc::clone(&catalog_application)),
            Arc::clone(&connector_control),
            Arc::clone(&unified_statistics),
            query_execution.clone(),
            topology.clone(),
            exchange_port,
            view_service.clone(),
            system_catalog,
            Arc::clone(&mv_repository),
            Arc::clone(&mv_storage_observation),
        ));
    let session_catalog_resolver =
        core_capabilities::session_catalog_resolver(core_capabilities::SessionCatalogPorts::new(
            Arc::clone(&catalog_service),
            Some(Arc::clone(&catalog_application)),
            Arc::clone(&connector_control),
        ));
    let catalog_command_executor =
        core_capabilities::catalog_command_executor(core_capabilities::CatalogCommandPorts::new(
            Arc::clone(&catalog_service),
            Some(Arc::clone(&catalog_application)),
            Arc::clone(&connector_control),
            Arc::clone(&mv_repository),
            Arc::clone(&mv_storage_observation),
            view_service,
        ));
    let statistics_command_executor = core_capabilities::statistics_command_executor(
        core_capabilities::StatisticsCommandPorts::new(
            Arc::clone(&catalog_service),
            Arc::clone(&connector_control),
            Arc::clone(&unified_statistics),
            statistics_service,
            statistics_application,
            query_execution.clone(),
        ),
    );
    let backend_command_executor = core_capabilities::backend_command_executor(
        core_capabilities::BackendCommandPorts::new(topology.clone()),
    );
    let view_command_executor =
        core_capabilities::view_command_executor(core_capabilities::ViewCommandPorts::new(
            Arc::clone(&catalog_service),
            Some(Arc::clone(&catalog_application)),
            Arc::clone(&connector_control),
            host.view_service(),
        ));
    let iceberg_ref_command_executor = core_capabilities::iceberg_ref_command_executor(
        core_capabilities::IcebergRefCommandPorts::new(
            Arc::clone(&connector_control),
            Arc::clone(&mv_storage_observation),
        ),
    );
    let mv_command_executor =
        core_capabilities::mv_command_executor(core_capabilities::MvCommandPorts::new(
            Arc::clone(&catalog_service),
            Some(Arc::clone(&catalog_application)),
            Arc::clone(&connector_control),
            Arc::clone(&unified_statistics),
            Arc::clone(&mv_repository),
            mv_application,
            Arc::clone(&mv_storage_observation),
            query_execution.clone(),
        ));
    let maintenance_command_executor =
        core_capabilities::maintenance_command_executor(maintenance_ports);
    let maintenance_read_command_executor =
        core_capabilities::maintenance_read_command_executor(maintenance_service);
    let dml_engines = core_capabilities::dml_engines(core_capabilities::DmlEnginePorts::new(
        Arc::clone(&catalog_service),
        Some(catalog_application),
        connector_control,
        unified_statistics,
        mv_storage_observation,
        query_execution.clone(),
    ));
    host.ctas_recovery_binding()
        .install_ctas_engine(Arc::clone(&dml_engines.ctas))
        .map_err(|error| {
            FrontendApplicationError::server(format!(
                "bind CTAS recovery before controller start: {error}"
            ))
        })?;

    Ok(Arc::new(
        crate::query::FrontendQueryService::new_with_recovery_bound(
            session_catalog_resolver,
            query_compiler,
            catalog_command_executor,
            statistics_command_executor,
            backend_command_executor,
            view_command_executor,
            iceberg_ref_command_executor,
            mv_command_executor,
            maintenance_command_executor,
            maintenance_read_command_executor,
            host.query_control_service(),
            query_execution,
            role,
            topology,
            host.dml_service(),
            dml_engines.insert,
            dml_engines.delete,
            dml_engines.mutation,
            dml_engines.add_files,
            dml_engines.ctas,
            dml_engines.truncate,
            host.optimizer_query_mem_limit_bytes(),
        ),
    ))
}

pub fn run_frontend_server(config: FrontendServerConfig) -> Result<(), FrontendApplicationError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| {
            FrontendApplicationError::server(format!(
                "build frontend Tokio runtime failed: {error}"
            ))
        })?;

    runtime.block_on(run_frontend_server_with_signal(
        config,
        tokio::signal::ctrl_c(),
    ))
}

pub async fn run_frontend_server_until_shutdown<F>(
    config: FrontendServerConfig,
    shutdown: F,
) -> Result<(), FrontendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    let mv_storage_observation = Arc::clone(&config.mv_storage_observation);
    let host = open_frontend_application_for_server(&config).await?;
    let server_result =
        serve_ready_frontend_session_factory(config, &host, mv_storage_observation, shutdown).await;
    let shutdown_result = host.shutdown().await;
    combine_server_and_shutdown(server_result, shutdown_result)
}

async fn run_frontend_server_with_signal<S, E>(
    config: FrontendServerConfig,
    signal: S,
) -> Result<(), FrontendApplicationError>
where
    S: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let mv_storage_observation = Arc::clone(&config.mv_storage_observation);
    let host = open_frontend_application_for_server(&config).await?;
    let server_result = run_server_until_signal(config, (), signal, |config, (), shutdown| {
        serve_ready_frontend_session_factory(config, &host, mv_storage_observation, shutdown)
    })
    .await;
    let shutdown_result = host.shutdown().await;
    combine_server_and_shutdown(server_result, shutdown_result)
}

async fn serve_ready_frontend_session_factory<F>(
    config: FrontendServerConfig,
    host: &FrontendApplicationHost,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    shutdown: F,
) -> Result<(), FrontendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    let mut report_server = FrontendReportServerHandle::start(
        &config.config.server.host,
        config.config.server.grpc_port,
        host.terminal_ingress(),
    )
    .map_err(FrontendApplicationError::server)?;
    let exchange_port = report_server.bound_addr().port();
    host.coordinator_report_endpoint_sink()
        .set_bound_port(exchange_port);
    let system_catalog: Arc<dyn novarocks::engine::system_catalog::SystemCatalog> =
        Arc::new(crate::system_catalog::SystemCatalogService::with_defaults());
    let session_factory = match build_frontend_query_session_factory(
        host,
        system_catalog,
        exchange_port,
        mv_storage_observation,
    ) {
        Ok(factory) => factory,
        Err(error) => {
            let stop_result = report_server
                .stop()
                .map_err(FrontendApplicationError::server);
            return combine_server_and_shutdown(Err(error), stop_result);
        }
    };
    let listener =
        novarocks::server::resolve_mysql_listener_settings(&config.config, config.port_override)
            .map_err(FrontendApplicationError::server)?;
    let server_result =
        novarocks::server::run_mysql_server_until_shutdown(listener, session_factory, shutdown)
            .await
            .map_err(FrontendApplicationError::server);
    let stop_result = report_server
        .stop()
        .map_err(FrontendApplicationError::server);
    combine_server_and_shutdown(server_result, stop_result)
}

fn resolve_frontend_execution_config(
    server: &FrontendServerConfig,
) -> Result<FrontendExecutionConfig, FrontendApplicationError> {
    let advertised =
        novarocks::common::network::standalone_advertise_endpoint_for_config(&server.config)
            .map_err(FrontendApplicationError::server)?;
    let runtime_filter_worker_count =
        NonZeroUsize::new(server.config.runtime.actual_exec_threads()).ok_or_else(|| {
            FrontendApplicationError::server("frontend runtime-filter worker count must be nonzero")
        })?;
    let mut execution = FrontendExecutionConfig::new(
        advertised.host,
        advertised.port,
        runtime_filter_worker_count,
    )
    .with_optimizer_query_mem_limit_bytes(server.config.runtime.optimizer_query_mem_limit_bytes)
    .with_query_control_timeouts(FrontendQueryControlTimeouts {
        heartbeat_interval_ms: server.config.runtime.query_control_heartbeat_interval_ms,
        heartbeat_timeout_ms: server.config.runtime.query_control_heartbeat_timeout_ms,
        init_rpc_timeout_ms: server.config.runtime.query_control_init_rpc_timeout_ms,
        attach_timeout_ms: server.config.runtime.query_control_attach_timeout_ms,
        stage_rpc_timeout_ms: server.config.runtime.query_control_stage_rpc_timeout_ms,
        start_rpc_timeout_ms: server.config.runtime.query_control_start_rpc_timeout_ms,
        terminal_drain_timeout_ms: server
            .config
            .runtime
            .query_control_terminal_drain_timeout_ms,
        terminal_ack_timeout_ms: server.config.runtime.query_control_terminal_ack_timeout_ms,
        pre_start_timeout_ms: server.config.runtime.query_control_pre_start_timeout_ms,
    });
    if let Some(standalone) = server.config.standalone_server.as_ref() {
        let failure_backoff_ms = standalone.mv_refresh_scheduler_failure_backoff_ms.max(1);
        execution = execution.with_mv_scheduler_config(FrontendMvSchedulerConfig {
            enabled: standalone.mv_refresh_scheduler_enabled,
            tick_interval_ms: standalone.mv_refresh_scheduler_interval_ms.max(1),
            max_concurrent_refreshes: standalone.mv_refresh_scheduler_max_concurrent.max(1),
            failure_backoff_ms,
            max_failure_backoff_ms: standalone
                .mv_refresh_scheduler_max_failure_backoff_ms
                .max(failure_backoff_ms),
        });
        execution = execution.with_mv_maintenance_config(MaintenanceCoordinatorConfig {
            enabled: standalone.iceberg_maintenance_enabled,
            tick_interval_ms: standalone.iceberg_maintenance_tick_interval_ms.max(1),
            max_concurrent: standalone.iceberg_maintenance_max_concurrent.max(1),
            compaction_min_data_files: standalone
                .iceberg_maintenance_compaction_min_data_files
                .try_into()
                .unwrap_or(i64::MAX),
            dv_min_delete_files: standalone
                .iceberg_maintenance_dv_min_delete_files
                .try_into()
                .unwrap_or(i64::MAX),
            action_cooldown_ms: standalone.iceberg_maintenance_action_cooldown_ms,
            max_consecutive_failures: standalone.iceberg_maintenance_max_consecutive_failures,
        });
    }
    Ok(execution)
}

#[cfg(test)]
async fn run_frontend_server_until_shutdown_with_ports<
    F,
    Host,
    OpenHost,
    OpenHostFuture,
    ExtractService,
    Service,
    Serve,
    ServeFuture,
    ShutdownHost,
    ShutdownHostFuture,
>(
    config: FrontendServerConfig,
    shutdown: F,
    open_host: OpenHost,
    extract_service: ExtractService,
    serve: Serve,
    shutdown_host: ShutdownHost,
) -> Result<(), FrontendApplicationError>
where
    F: Future<Output = ()> + Send,
    OpenHost: FnOnce(Option<StateStoreHostConfig>) -> OpenHostFuture,
    OpenHostFuture: Future<Output = Result<Host, FrontendApplicationError>>,
    ExtractService: FnOnce(&Host) -> Service,
    Serve: FnOnce(FrontendServerConfig, Service, F) -> ServeFuture,
    ServeFuture: Future<Output = Result<(), FrontendApplicationError>>,
    ShutdownHost: FnOnce(Host) -> ShutdownHostFuture,
    ShutdownHostFuture: Future<Output = Result<(), FrontendApplicationError>>,
{
    let state_store_host_config = resolved_state_store_host_config(&config);
    let host = open_host(state_store_host_config).await?;
    let service = extract_service(&host);
    let server_result = serve(config, service, shutdown).await;
    let shutdown_result = shutdown_host(host).await;

    combine_server_and_shutdown(server_result, shutdown_result)
}

#[cfg(test)]
async fn run_frontend_server_with_signal_and_ports<
    S,
    E,
    Host,
    OpenHost,
    OpenHostFuture,
    ExtractService,
    Service,
    Serve,
    ServeFuture,
    ShutdownHost,
    ShutdownHostFuture,
>(
    config: FrontendServerConfig,
    signal: S,
    open_host: OpenHost,
    extract_service: ExtractService,
    serve: Serve,
    shutdown_host: ShutdownHost,
) -> Result<(), FrontendApplicationError>
where
    S: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
    OpenHost: FnOnce(Option<StateStoreHostConfig>) -> OpenHostFuture,
    OpenHostFuture: Future<Output = Result<Host, FrontendApplicationError>>,
    ExtractService: FnOnce(&Host) -> Service,
    Serve: FnOnce(FrontendServerConfig, Service, ShutdownSignal) -> ServeFuture,
    ServeFuture: Future<Output = Result<(), FrontendApplicationError>>,
    ShutdownHost: FnOnce(Host) -> ShutdownHostFuture,
    ShutdownHostFuture: Future<Output = Result<(), FrontendApplicationError>>,
{
    let state_store_host_config = resolved_state_store_host_config(&config);
    let host = open_host(state_store_host_config).await?;
    let service = extract_service(&host);
    let server_result = run_server_until_signal(config, service, signal, serve).await;
    let shutdown_result = shutdown_host(host).await;

    combine_server_and_shutdown(server_result, shutdown_result)
}

fn resolved_state_store_host_config(config: &FrontendServerConfig) -> Option<StateStoreHostConfig> {
    config.state_store_host_config.clone().or_else(|| {
        config
            .config
            .state_store
            .clone()
            .map(|state_store| StateStoreHostConfig {
                state_store,
                foundationdb_client: config.config.foundationdb_client.clone(),
            })
    })
}

fn cluster_backend_open_config(
    config: &NovaRocksConfig,
) -> Result<ClusterBackendOpenConfig, FrontendApplicationError> {
    let seeds = config
        .cluster
        .backends
        .iter()
        .map(|endpoint| {
            endpoint.parse().map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::ClusterBackendOpen,
                    format!("parse configured backend endpoint '{endpoint}' failed: {error}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    ClusterBackendOpenConfig::new(
        config.cluster.role,
        seeds,
        Duration::from_millis(config.cluster.heartbeat_interval_ms),
        config.cluster.heartbeat_timeout_retries,
        Duration::from_secs(config.cluster.decommission_timeout_secs),
    )
    .map_err(|error| {
        FrontendApplicationError::new(FrontendApplicationErrorKind::ClusterBackendOpen, error)
    })
}

fn combine_server_and_shutdown(
    server_result: Result<(), FrontendApplicationError>,
    shutdown_result: Result<(), FrontendApplicationError>,
) -> Result<(), FrontendApplicationError> {
    match (server_result, shutdown_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(server_error), Ok(())) => Err(server_error),
        (Ok(()), Err(shutdown_error)) => Err(shutdown_error),
        (Err(server_error), Err(shutdown_error)) => {
            Err(server_error.with_cleanup_context(shutdown_error))
        }
    }
}

async fn run_server_until_signal<S, E, Service, Serve, ServeFuture>(
    config: FrontendServerConfig,
    service: Service,
    signal: S,
    serve: Serve,
) -> Result<(), FrontendApplicationError>
where
    S: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
    Serve: FnOnce(FrontendServerConfig, Service, ShutdownSignal) -> ServeFuture,
    ServeFuture: Future<Output = Result<(), FrontendApplicationError>>,
{
    let mut signal = Box::pin(signal);
    let initial_signal = std::future::poll_fn(|context| match signal.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(None),
        Poll::Ready(result) => Poll::Ready(Some(result)),
    })
    .await;

    match initial_signal {
        Some(Ok(())) => return Ok(()),
        Some(Err(error)) => {
            return Err(FrontendApplicationError::server(format!(
                "Ctrl-C listener initialization failed: {error}"
            )));
        }
        None => {}
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let signal_result = Arc::new(Mutex::new(None));
    let signal_result_for_task = Arc::clone(&signal_result);
    let signal_task = tokio::spawn(async move {
        let result = signal.await.map_err(|error| error.to_string());
        *signal_result_for_task.lock().expect("signal result lock") = Some(result);
        let _ = shutdown_tx.send(());
    });

    let server_result = serve(
        config,
        service,
        Box::pin(async move {
            let _ = shutdown_rx.await;
        }),
    )
    .await;

    let completed_signal = signal_result.lock().expect("signal result lock").take();
    let Some(signal_result) = completed_signal else {
        signal_task.abort();
        let _ = signal_task.await;
        return server_result;
    };

    if let Err(error) = signal_task.await {
        return match server_result {
            Ok(()) => Err(FrontendApplicationError::server(format!(
                "Ctrl-C listener task failed: {error}"
            ))),
            Err(server_error) => Err(server_error),
        };
    }

    match (server_result, signal_result) {
        (Err(server_error), _) => Err(server_error),
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) => Err(FrontendApplicationError::server(format!(
            "Ctrl-C listener failed: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::{
        FrontendServerConfig, run_frontend_server, run_frontend_server_until_shutdown,
        run_frontend_server_until_shutdown_with_ports, run_frontend_server_with_signal_and_ports,
        standalone_open_services,
    };
    use crate::{
        FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
        FrontendExecutionConfig,
    };
    use novarocks_state_store::{
        FoundationDbClientConfig, StateStoreAppConfig, StateStoreConfig, StateStoreHostConfig,
        StateStoreLimitOverrides, StateStoreProviderConfig,
    };
    use uuid::Uuid;

    #[derive(Debug)]
    struct RecordingHostPort;

    #[derive(Clone, Debug)]
    struct RecordingServerPort {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl RecordingServerPort {
        fn new(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self { events }
        }

        fn record(&self, event: &'static str) {
            self.events.lock().expect("events lock").push(event);
        }
    }

    fn frontend_config() -> FrontendServerConfig {
        FrontendServerConfig {
            config: novarocks::common::app_config::NovaRocksConfig::default(),
            config_path: None,
            port_override: None,
            connector_control_factories: Vec::new(),
            mv_storage_observation: Arc::new(
                novarocks::mv::storage_observation::UnavailableMvStorageObservationPort,
            ),
            state_store_host_config: None,
        }
    }

    /// Answers whichever catalog instance the factory request carries, so the
    /// cutover test exercises the real create path without an object store.
    struct EchoingControlFactory;

    impl novarocks_spi::connector::ConnectorControlFactory for EchoingControlFactory {
        fn provider_id(&self) -> &novarocks_spi::connector::ConnectorProviderId {
            static PROVIDER: std::sync::OnceLock<novarocks_spi::connector::ConnectorProviderId> =
                std::sync::OnceLock::new();
            PROVIDER.get_or_init(|| {
                novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .expect("provider ID")
            })
        }

        fn create_control(
            &self,
            request: novarocks_spi::connector::ConnectorControlFactoryRequest,
        ) -> Result<
            novarocks_spi::connector::ConnectorControlCreation,
            novarocks_spi::connector::ConnectorError,
        > {
            let binding = crate::connector::control_host::tests::test_control_binding_for(
                request.instance_id().clone(),
                1,
            );
            novarocks_spi::connector::ConnectorControlCreation::try_new(
                &request,
                binding,
                Vec::new(),
            )
        }
    }

    /// CP-2 cutover gate: the StateStore attachment is the only catalog
    /// authority the production composition installs, and Core reaches it only
    /// through the frontend application port.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cp2_production_composition_owns_catalog_ddl_through_the_state_store_attachment() {
        let temp = tempfile::tempdir().expect("temporary cutover directory");
        let mut config = novarocks::common::app_config::NovaRocksConfig::default();
        config.cluster.role = novarocks::common::app_config::ClusterRole::AllInOne;
        let state_store = StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id: "cp2-cutover".to_string(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Sqlite {
                        path: temp.path().join("state-store.sqlite"),
                        deployment_owner: "cp2-cutover".to_string(),
                    },
                },
                mysql_client: None,
            },
            foundationdb_client: None,
        };
        let host = FrontendApplicationHost::open_with_factories(
            Some(state_store),
            FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                std::num::NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
            ),
            super::cluster_backend_open_config(&config).expect("valid all-in-one backend config"),
            vec![Arc::new(EchoingControlFactory)],
        )
        .await
        .expect("open frontend application host");
        let store = host.state_store().expect("frontend StateStore");
        let attachments =
            crate::catalog_attachment::CatalogAttachmentRepository::open(Arc::clone(&store))
                .await
                .expect("open catalog attachment repository");

        let services = standalone_open_services(
            Arc::new(crate::system_catalog::SystemCatalogService::with_defaults()),
            &host,
            Arc::new(novarocks::mv::storage_observation::UnavailableMvStorageObservationPort),
        );
        assert!(
            services.catalog_application.is_some(),
            "production composition must install the frontend catalog application"
        );
        assert!(
            services.catalog_runtime_projection.is_some(),
            "production composition must install the catalog runtime projection"
        );
        let engine = novarocks::engine::StandaloneNovaRocks::open_with_config(
            novarocks::engine::StandaloneOptions::default(),
            config,
            services,
        )
        .expect("open engine with the frontend catalog authority");

        let cancellation = novarocks::query_execution::cancellation::QueryCancellationSource::new();
        let context = novarocks::query_execution::request_context::RequestContext::admit(
            novarocks::query_execution::request_context::RequestAdmission::new(
                None,
                "db1".to_string(),
                novarocks::common::app_config::ClusterRole::AllInOne,
                novarocks::query_execution::backend::BackendTopologySnapshot::empty(1),
                None,
                cancellation.view(),
                novarocks::query_execution::request_context::SessionOptimizerSettings::default(),
            ),
        );
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse("warehouse").expect("instance ID");

        engine
            .command_executor()
            .execute(
                r#"CREATE EXTERNAL CATALOG warehouse PROPERTIES("type"="iceberg")"#,
                &context,
                None,
            )
            .expect("CREATE CATALOG commits a durable StateStore attachment");
        let created = attachments
            .get(&instance_id)
            .await
            .expect("read attachment")
            .expect("CREATE CATALOG must commit to the StateStore attachment keyspace");
        assert_eq!(created.attachment.provider_id.as_str(), "iceberg");
        assert_eq!(created.attachment.display_name, "warehouse");
        engine
            .require_external_catalog_ready("warehouse")
            .expect("the committed attachment is admitted by this frontend");

        engine
            .command_executor()
            .execute("DROP CATALOG warehouse", &context, None)
            .expect("DROP CATALOG deletes the durable StateStore attachment");
        assert!(
            attachments
                .get(&instance_id)
                .await
                .expect("read attachment")
                .is_none(),
            "DROP CATALOG must remove the durable attachment"
        );
        assert_eq!(
            engine
                .require_external_catalog_ready("warehouse")
                .expect_err("a dropped catalog stops being admitted")
                .kind(),
            novarocks::catalog_application::CatalogApplicationErrorKind::NotFound
        );

        // The engine and this test's probe both hold StateStore references; the
        // host owns closing the deployment lock, so release them first.
        drop(attachments);
        drop(store);
        drop(engine);
        host.shutdown().await.expect("host shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frontend_report_endpoint_binds_loopback_without_core_transport_facade() {
        let mut config = novarocks::common::app_config::NovaRocksConfig::default();
        config.cluster.role = novarocks::common::app_config::ClusterRole::AllInOne;
        config.cluster.advertise_host = "127.0.0.1".to_string();
        config.server.host = "127.0.0.1".to_string();
        config.server.grpc_port = 0;
        let host = FrontendApplicationHost::open(
            None,
            FrontendExecutionConfig::new("127.0.0.1", 0, std::num::NonZeroUsize::new(1).unwrap()),
            super::cluster_backend_open_config(&config).expect("valid all-in-one backend config"),
        )
        .await
        .expect("open frontend application host");
        let report_endpoint = host.coordinator_report_endpoint_sink();
        let mut report_server = crate::native::report_server::FrontendReportServerHandle::start(
            &config.server.host,
            config.server.grpc_port,
            host.terminal_ingress(),
        )
        .expect("start frontend-owned report endpoint");
        let grpc_port = report_server.bound_addr().port();
        report_endpoint.set_bound_port(grpc_port);
        assert_ne!(
            grpc_port, 0,
            "ephemeral report listener selects a real port"
        );
        assert_eq!(
            report_server.poll_failure().expect("poll report listener"),
            None,
            "report listener remains live after bind"
        );
        report_server.stop().expect("stop frontend report endpoint");
        host.shutdown()
            .await
            .expect("shutdown frontend application host");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlx2_application_frontend_services_inject_statistics_application_port() {
        let mut config = novarocks::common::app_config::NovaRocksConfig::default();
        config.cluster.role = novarocks::common::app_config::ClusterRole::AllInOne;
        let host = FrontendApplicationHost::open(
            None,
            FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                std::num::NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
            ),
            super::cluster_backend_open_config(&config).expect("valid all-in-one backend config"),
        )
        .await
        .expect("open frontend application host");
        let engine = novarocks::engine::StandaloneNovaRocks::open_with_config(
            novarocks::engine::StandaloneOptions::default(),
            config,
            standalone_open_services(
                Arc::new(crate::system_catalog::SystemCatalogService::with_defaults()),
                &host,
                Arc::new(novarocks::mv::storage_observation::UnavailableMvStorageObservationPort),
            ),
        )
        .expect("open engine with frontend-owned application services");

        let cancellation = novarocks::query_execution::cancellation::QueryCancellationSource::new();
        let context = novarocks::query_execution::request_context::RequestContext::admit(
            novarocks::query_execution::request_context::RequestAdmission::new(
                None,
                "db1".to_string(),
                novarocks::common::app_config::ClusterRole::AllInOne,
                novarocks::query_execution::backend::BackendTopologySnapshot::empty(1),
                None,
                cancellation.view(),
                novarocks::query_execution::request_context::SessionOptimizerSettings::default(),
            ),
        );
        let error = engine
            .command_executor()
            .execute("SHOW ANALYZE JOBS", &context, None)
            .expect_err("a host without StateStore must reach the frontend statistics service");
        assert!(
            error.contains("statistics job commands require a configured frontend StateStore"),
            "statistics application port was not injected: {error}"
        );
        assert!(
            !error.contains("statistics application service is unavailable"),
            "Core default statistics application port must not be used: {error}"
        );

        drop(engine);
        host.shutdown()
            .await
            .expect("shutdown frontend application host");
    }

    #[test]
    fn runner_exports_typed_application_errors() {
        fn accepts_sync_runner(
            _: fn(FrontendServerConfig) -> Result<(), FrontendApplicationError>,
        ) {
        }
        fn accepts_async_runner<F>(_: F)
        where
            F: Future<Output = Result<(), FrontendApplicationError>>,
        {
        }

        accepts_sync_runner(run_frontend_server);
        accepts_async_runner(run_frontend_server_until_shutdown(
            frontend_config(),
            async {},
        ));
    }

    #[tokio::test]
    async fn host_opens_before_server_bind() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let host_port = RecordingServerPort::new(Arc::clone(&events));
        let server_port = RecordingServerPort::new(Arc::clone(&events));

        run_frontend_server_until_shutdown_with_ports(
            frontend_config(),
            async {},
            move |_| {
                host_port.record("host_open");
                async { Ok(RecordingHostPort) }
            },
            |_| (),
            move |_, (), shutdown| async move {
                server_port.record("server_bind");
                shutdown.await;
                Ok(())
            },
            |_| async { Ok(()) },
        )
        .await
        .expect("frontend orchestration should succeed");

        assert_eq!(
            events.lock().expect("events lock").as_slice(),
            ["host_open", "server_bind"]
        );
    }

    #[tokio::test]
    async fn normal_shutdown_drains_server_before_store() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let server_port = RecordingServerPort::new(Arc::clone(&events));
        let shutdown_port = RecordingServerPort::new(Arc::clone(&events));

        run_frontend_server_until_shutdown_with_ports(
            frontend_config(),
            async {},
            |_| async { Ok(RecordingHostPort) },
            |_| (),
            move |_, (), shutdown| async move {
                server_port.record("server_started");
                shutdown.await;
                server_port.record("server_drained");
                Ok(())
            },
            move |_| async move {
                shutdown_port.record("store_shutdown");
                Ok(())
            },
        )
        .await
        .expect("frontend orchestration should succeed");

        assert_eq!(
            events.lock().expect("events lock").as_slice(),
            ["server_started", "server_drained", "store_shutdown"]
        );
    }

    #[tokio::test]
    async fn startup_failure_still_shuts_host() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let shutdown_port = RecordingServerPort::new(Arc::clone(&events));

        let error = run_frontend_server_until_shutdown_with_ports(
            frontend_config(),
            std::future::pending::<()>(),
            |_| async { Ok(RecordingHostPort) },
            |_| (),
            |_, (), _| async { Err(FrontendApplicationError::server("core startup failed")) },
            move |_| async move {
                shutdown_port.record("store_shutdown");
                Ok(())
            },
        )
        .await
        .expect_err("core startup failure should be returned");

        assert_eq!(error.kind(), FrontendApplicationErrorKind::Server);
        assert!(error.to_string().contains("core startup failed"));
        assert_eq!(
            events.lock().expect("events lock").as_slice(),
            ["store_shutdown"]
        );
    }

    #[tokio::test]
    async fn server_and_shutdown_failure_preserve_server_error() {
        let error = run_frontend_server_until_shutdown_with_ports(
            frontend_config(),
            std::future::pending::<()>(),
            |_| async { Ok(RecordingHostPort) },
            |_| (),
            |_, (), _| async { Err(FrontendApplicationError::server("core server failed")) },
            |_| async { Err(FrontendApplicationError::server("store shutdown failed")) },
        )
        .await
        .expect_err("both failures should be returned");

        assert_eq!(error.kind(), FrontendApplicationErrorKind::Server);
        assert!(error.to_string().contains("core server failed"));
        assert!(
            error
                .to_string()
                .contains("cleanup failed: Server: store shutdown failed")
        );
    }

    #[tokio::test]
    async fn preloaded_config_is_not_reread() {
        let temp = tempfile::tempdir().expect("create tempdir");
        let unreadable_config_path: PathBuf = temp.path().join("missing.toml");
        let mut config = frontend_config();
        config.config.log_level = "sentinel-preloaded".to_string();
        config.config_path = Some(unreadable_config_path.clone());
        let server_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server_called_in_port = Arc::clone(&server_called);

        run_frontend_server_until_shutdown_with_ports(
            config,
            async {},
            |_| async { Ok(RecordingHostPort) },
            |_| (),
            move |config, (), shutdown| async move {
                assert_eq!(config.config.log_level, "sentinel-preloaded");
                assert_eq!(config.config_path, Some(unreadable_config_path));
                server_called_in_port.store(true, std::sync::atomic::Ordering::SeqCst);
                shutdown.await;
                Ok(())
            },
            |_| async { Ok(()) },
        )
        .await
        .expect("preloaded config should reach the core port without a disk read");

        assert!(server_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn ctrl_c_listener_failure_shuts_host_without_server_bind() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let host_port = RecordingServerPort::new(Arc::clone(&events));
        let server_port = RecordingServerPort::new(Arc::clone(&events));
        let shutdown_port = RecordingServerPort::new(Arc::clone(&events));

        let error = run_frontend_server_with_signal_and_ports(
            frontend_config(),
            async { Err::<(), _>("Ctrl-C registration failed") },
            move |_| {
                host_port.record("host_open");
                async { Ok(RecordingHostPort) }
            },
            |_| (),
            move |_, (), _| async move {
                server_port.record("server_bind");
                Ok(())
            },
            move |_| async move {
                shutdown_port.record("store_shutdown");
                Ok(())
            },
        )
        .await
        .expect_err("Ctrl-C listener failure must be returned");

        assert_eq!(error.kind(), FrontendApplicationErrorKind::Server);
        assert!(error.to_string().contains("Ctrl-C registration failed"));
        assert_eq!(
            events.lock().expect("events lock").as_slice(),
            ["host_open", "store_shutdown"]
        );
    }

    #[tokio::test]
    async fn host_open_failure_does_not_bind_server() {
        let server_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server_called_in_port = Arc::clone(&server_called);

        let error = run_frontend_server_until_shutdown_with_ports(
            frontend_config(),
            async {},
            |_| async {
                Err::<RecordingHostPort, _>(FrontendApplicationError::new(
                    FrontendApplicationErrorKind::ViewServiceOpen,
                    "corrupt frontend view record",
                ))
            },
            |_| (),
            move |_, (), _| async move {
                server_called_in_port.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
            |_| async { Ok(()) },
        )
        .await
        .expect_err("host open failure must abort before server bind");

        assert_eq!(error.kind(), FrontendApplicationErrorKind::ViewServiceOpen);
        assert!(!server_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn full_process_config_pairs_foundationdb_client_with_state_store_for_host() {
        let cluster_file = tempfile::NamedTempFile::new().expect("FoundationDB cluster file");
        let mut config = frontend_config();
        config.config.state_store = Some(StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "frontend-cluster".to_owned(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Foundationdb {
                    cluster_file: cluster_file.path().to_path_buf(),
                    keyspace_id: Uuid::nil(),
                },
            },
            mysql_client: None,
        });
        let foundationdb_client = FoundationDbClientConfig {
            disable_multi_version_client: true,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            tls_verify_peers: None,
            tls_password_env: None,
        };
        config.config.foundationdb_client = Some(foundationdb_client.clone());
        let captured = Arc::new(Mutex::new(None::<StateStoreHostConfig>));
        let captured_in_port = Arc::clone(&captured);

        run_frontend_server_until_shutdown_with_ports(
            config,
            async {},
            move |host_config| {
                *captured_in_port.lock().expect("captured config lock") = host_config;
                async { Ok(RecordingHostPort) }
            },
            |_| (),
            |_, (), shutdown| async move {
                shutdown.await;
                Ok(())
            },
            |_| async { Ok(()) },
        )
        .await
        .expect("frontend orchestration should succeed");

        let captured = captured
            .lock()
            .expect("captured config lock")
            .clone()
            .expect("state store host config");
        assert!(matches!(
            captured.state_store.store.provider,
            StateStoreProviderConfig::Foundationdb { cluster_file: ref path, keyspace_id }
                if path == cluster_file.path() && keyspace_id == Uuid::nil()
        ));
        assert_eq!(captured.foundationdb_client, Some(foundationdb_client));
    }
}
