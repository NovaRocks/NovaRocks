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

use novarocks_native_trust::NativeTrust;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::runtime::Handle;

use crate::capabilities as core_capabilities;
use crate::common::query_cancellation::QueryCancellationReason;
use crate::native::transport::FrontendNativeTransport;
use crate::state_store::{StateStoreHostInput, StateStoreProviderRegistry};
use crate::workload_lifecycle::{
    FrontendServingSnapshotReader, LateBoundFrontendServingSnapshotReader,
};
use crate::{
    ClientConnectionControlPort, ClientConnectionTerminationReason, MysqlClientConnectionRegistry,
    QuerySessionFactory, ResolvedMysqlListenerSettings,
};
use novarocks_spi::connector::ConnectorControlRoleBindingFactory;
use novarocks_spi::connector::MvStorageObservationPort;

use crate::query_execution::maintenance::{
    BackgroundMaintenanceAttempt, BackgroundMaintenanceAttemptFactory,
};
use crate::{
    ClusterBackendOpenConfig, FrontendApplicationError, FrontendApplicationHost,
    FrontendExecutionConfig,
};

type ShutdownSignal = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Clone)]
struct FrontendBackgroundMaintenanceAttemptFactory {
    role: novarocks_types::ClusterRole,
    topology: crate::common::backend_topology::BackendTopologyService,
    runtime_policy: crate::common::admitted_query_context::LakePublicationRuntimePolicy,
}

impl BackgroundMaintenanceAttemptFactory for FrontendBackgroundMaintenanceAttemptFactory {
    fn begin_automatic_maintenance_attempt(&self) -> Result<BackgroundMaintenanceAttempt, String> {
        core_capabilities::background_maintenance_attempt(
            self.role,
            self.topology.clone(),
            self.runtime_policy.max_attempt_duration(),
        )
    }
}

#[derive(Clone)]
pub struct FrontendServerConfig {
    pub execution: FrontendExecutionConfig,
    pub backend_open: ClusterBackendOpenConfig,
    pub report_bind_host: String,
    pub report_grpc_port: u16,
    /// Dedicated role=fe management HTTP endpoint.
    pub metrics_http_port: u16,
    /// Maximum time admitted FE workload leases may continue after drain starts.
    pub frontend_drain_timeout: Duration,
    /// Upper bound for terminal resource cleanup after graceful/deadline drain.
    pub frontend_cleanup_timeout: Duration,
    pub mysql_listener: ResolvedMysqlListenerSettings,
    /// Provider-owned FE control role factories composed by the server root.
    pub connector_control_role_factories: Vec<Arc<dyn ConnectorControlRoleBindingFactory>>,
    /// Application-owned storage observation composed by the server role.
    /// Frontend and Core never decode provider table handles directly.
    pub mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    /// Typed StateStore host input. The FE remains the owner of opening and
    /// shutting down this host; the server only supplies the composition data.
    pub state_store_input: Option<StateStoreHostInput>,
    /// Concrete provider registrations supplied only by Server composition.
    pub state_store_provider_registry: StateStoreProviderRegistry,
    /// Server-owned deployment trust capability. It is mandatory for every
    /// production FE Native channel and report listener.
    pub native_trust: Arc<NativeTrust>,
    /// Server-materialized plaintext/TLS transport capability paired with the
    /// deployment trust above.
    pub native_transport: FrontendNativeTransport,
}

/// Opens the frontend services once for an externally composed server.
pub async fn open_frontend_application_for_server(
    config: &FrontendServerConfig,
    data_runtime: Handle,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
        config.state_store_input.clone(),
        &config.state_store_provider_registry,
        config.execution.clone(),
        config.backend_open.clone(),
        config.connector_control_role_factories.clone(),
        data_runtime,
        Arc::clone(&config.native_trust),
        config.native_transport.clone(),
    )
    .await
}

/// Complete the one Frontend-owned startup graph and return a ready SQL
/// session factory.  Every Core value constructed here is a closed domain
/// capability; this function never creates an application aggregate or lets a
/// request resolve services from the lifecycle host.
pub fn build_frontend_query_session_factory(
    host: &FrontendApplicationHost,
    system_catalog: Arc<dyn crate::catalog_application::system_catalog::SystemCatalog>,
    exchange_port: u16,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    client_connection_control: Arc<dyn ClientConnectionControlPort>,
) -> Result<Arc<dyn QuerySessionFactory>, FrontendApplicationError> {
    let catalog_service =
        Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service());
    let unified_statistics = Arc::new(crate::connector::UnifiedStatisticsResolver::default());
    let catalog_application = host.catalog_application_port();
    let function_catalog = host.function_catalog();
    let catalog_projection = host.catalog_runtime_projection();
    let connector_control = host.connector_control_registry();
    // Constructor-supplied, exactly once: query preparation receives the
    // registry here and never resolves it from the host at request time.
    let typed_connector_control = host.typed_connector_control();
    let query_execution = host.query_execution_service();
    let topology = host.backend_topology_port();
    let role = host.execution_role();
    let mv_repository = host.mv_repository();
    let mv_application = host.mv_application_service();
    let mv_service = host.mv_service();
    let mv_readiness = mv_service.readiness_port();
    let view_service = host.view_service();
    let statistics_application = host.statistics_application_port();
    let maintenance_service = host.table_maintenance_service();

    core_capabilities::bind_catalog_runtime_projection(
        catalog_projection.as_ref(),
        Arc::clone(&catalog_service),
        Arc::clone(&connector_control),
    )
    .map_err(FrontendApplicationError::server)?;

    if let Some(sink) = host.mv_refresh_provider_activation_sink() {
        core_capabilities::bind_mv_refresh_provider_activation(
            sink.as_ref(),
            core_capabilities::MvRefreshProviderActivationPorts::new(
                Arc::clone(&function_catalog),
                Arc::clone(&catalog_service),
                Some(Arc::clone(&catalog_application)),
                Arc::clone(&connector_control),
                Arc::clone(&typed_connector_control),
                Arc::clone(&unified_statistics),
                query_execution.clone(),
                topology.clone(),
                exchange_port,
                Arc::clone(&mv_repository),
                Arc::clone(&mv_readiness),
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
        mv_service.readiness_port(),
    );
    crate::mv::domain::startup_restore::run_mv_startup_restore(&startup_restore)
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
            Arc::clone(&typed_connector_control),
            topology.clone(),
            query_execution.clone(),
            Arc::clone(&function_catalog),
            host.lake_publication_runtime_policy()
                .max_attempt_duration(),
        ),
    )
    .map_err(FrontendApplicationError::server)?;

    let maintenance_ports = core_capabilities::MaintenanceCommandPorts::new(
        Arc::clone(&function_catalog),
        Arc::clone(&catalog_service),
        Some(Arc::clone(&catalog_application)),
        Arc::clone(&connector_control),
        Arc::clone(&typed_connector_control),
        Arc::clone(&mv_storage_observation),
        query_execution.clone(),
        Arc::clone(&maintenance_service),
    );
    let maintenance_engine = core_capabilities::background_maintenance_engine(
        maintenance_ports.clone(),
        Arc::new(FrontendBackgroundMaintenanceAttemptFactory {
            role,
            topology: topology.clone(),
            runtime_policy: host.lake_publication_runtime_policy(),
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
    if let Some(sink) = host.mv_background_engine_sink()
        && let Err(error) = core_capabilities::bind_mv_background_engine(
            sink.as_ref(),
            core_capabilities::MvBackgroundPorts::new(
                Arc::clone(&function_catalog),
                Arc::clone(&catalog_service),
                Some(Arc::clone(&catalog_application)),
                Arc::clone(&connector_control),
                Arc::clone(&mv_repository),
                Arc::clone(&mv_readiness),
                Arc::clone(&mv_storage_observation),
            ),
            Arc::clone(&maintenance_engine),
        )
    {
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

    let query_compiler =
        core_capabilities::query_compiler(core_capabilities::QueryCompilerPorts::new(
            Arc::clone(&function_catalog),
            Arc::clone(&catalog_service),
            Some(Arc::clone(&catalog_application)),
            Arc::clone(&connector_control),
            Arc::clone(&typed_connector_control),
            Arc::clone(&unified_statistics),
            query_execution.clone(),
            topology.clone(),
            exchange_port,
            view_service.clone(),
            system_catalog,
            Arc::clone(&mv_readiness),
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
            Arc::clone(&mv_readiness),
            Arc::clone(&mv_storage_observation),
            view_service,
        ));
    let statistics_command_executor =
        core_capabilities::statistics_command_executor(statistics_application);
    let backend_command_executor = core_capabilities::backend_command_executor(
        core_capabilities::BackendCommandPorts::new(topology.clone()),
    );
    let view_command_executor =
        core_capabilities::view_command_executor(core_capabilities::ViewCommandPorts::new(
            Arc::clone(&function_catalog),
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
            Arc::clone(&function_catalog),
            Arc::clone(&catalog_service),
            Some(Arc::clone(&catalog_application)),
            Arc::clone(&connector_control),
            Arc::clone(&mv_repository),
            mv_application,
            mv_service,
            Arc::clone(&mv_storage_observation),
            query_execution.clone(),
        ));
    let maintenance_command_executor =
        core_capabilities::maintenance_command_executor(maintenance_ports);
    let maintenance_read_command_executor =
        core_capabilities::maintenance_read_command_executor(maintenance_service);
    let dml_engines = core_capabilities::dml_engines(core_capabilities::DmlEnginePorts::new(
        function_catalog,
        Arc::clone(&catalog_service),
        Some(catalog_application),
        connector_control,
        typed_connector_control,
        unified_statistics,
        mv_storage_observation,
        query_execution.clone(),
        host.lake_publication_runtime_policy(),
    ));
    host.dml_service()
        .install_local_catalog(Arc::clone(&catalog_service));

    let query_service = Arc::new(
        crate::query::FrontendQueryService::new(
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
            client_connection_control,
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
            host.lake_publication_runtime_policy(),
        )
        .with_serving_lifecycle((*host.serving_lifecycle()).clone()),
    );
    host.serving_lifecycle().mark_ready().map_err(|error| {
        FrontendApplicationError::server(format!(
            "mark frontend serving lifecycle ready after bootstrap: {error:?}"
        ))
    })?;
    Ok(query_service)
}

pub fn run_frontend_server(config: FrontendServerConfig) -> Result<(), FrontendApplicationError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
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

// Design: ADR-0121 (docs/adr/ADR-0121-frontend-serving-lifecycle-and-admission-drain.md)
pub async fn run_frontend_server_until_shutdown<F>(
    config: FrontendServerConfig,
    data_runtime: Handle,
    shutdown: F,
) -> Result<(), FrontendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    let mv_storage_observation = Arc::clone(&config.mv_storage_observation);
    let cleanup_timeout = config.frontend_cleanup_timeout;
    let (serving_reader, island_reader, convergence_reader, mut metrics_http_server) =
        start_early_management_server(&config)?;
    let host = match open_frontend_application_for_server(&config, data_runtime).await {
        Ok(host) => host,
        Err(error) => {
            let cleanup = metrics_http_server
                .stop()
                .map_err(FrontendApplicationError::server);
            return combine_server_and_shutdown(Err(error), cleanup);
        }
    };
    if let Err(error) = serving_reader.install(host.serving_lifecycle()) {
        let shutdown = host
            .shutdown_until(std::time::Instant::now() + cleanup_timeout)
            .await;
        let cleanup = metrics_http_server
            .stop()
            .map_err(FrontendApplicationError::server);
        return combine_server_and_shutdown(
            Err(FrontendApplicationError::server(format!(
                "install frontend serving reader after application open: {error}"
            ))),
            combine_server_and_shutdown(shutdown, cleanup),
        );
    }
    if let Err(error) = island_reader.install(host.backend_island_snapshot_reader()) {
        let shutdown = host
            .shutdown_until(std::time::Instant::now() + cleanup_timeout)
            .await;
        let cleanup = metrics_http_server
            .stop()
            .map_err(FrontendApplicationError::server);
        return combine_server_and_shutdown(
            Err(FrontendApplicationError::server(format!(
                "install frontend island reader after application open: {error}"
            ))),
            combine_server_and_shutdown(shutdown, cleanup),
        );
    }
    if let Err(error) = convergence_reader.install(host.lifecycle_convergence_reader()) {
        let shutdown = host
            .shutdown_until(std::time::Instant::now() + cleanup_timeout)
            .await;
        let cleanup = metrics_http_server
            .stop()
            .map_err(FrontendApplicationError::server);
        return combine_server_and_shutdown(
            Err(FrontendApplicationError::server(format!(
                "install frontend lifecycle convergence reader after application open: {error}"
            ))),
            combine_server_and_shutdown(shutdown, cleanup),
        );
    }
    let server_result = serve_ready_frontend_session_factory(
        config,
        &host,
        mv_storage_observation,
        shutdown,
        &mut metrics_http_server,
    )
    .await;
    let shutdown_result = host
        .shutdown_until(std::time::Instant::now() + cleanup_timeout)
        .await;
    let metrics_stop = metrics_http_server
        .stop()
        .map_err(FrontendApplicationError::server);
    combine_server_and_shutdown(
        combine_server_and_shutdown(server_result, shutdown_result),
        metrics_stop,
    )
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
    let cleanup_timeout = config.frontend_cleanup_timeout;
    let (serving_reader, island_reader, convergence_reader, mut metrics_http_server) =
        start_early_management_server(&config)?;
    let host = match open_frontend_application_for_server(&config, Handle::current()).await {
        Ok(host) => host,
        Err(error) => {
            let cleanup = metrics_http_server
                .stop()
                .map_err(FrontendApplicationError::server);
            return combine_server_and_shutdown(Err(error), cleanup);
        }
    };
    if let Err(error) = serving_reader.install(host.serving_lifecycle()) {
        let shutdown = host
            .shutdown_until(std::time::Instant::now() + cleanup_timeout)
            .await;
        let cleanup = metrics_http_server
            .stop()
            .map_err(FrontendApplicationError::server);
        return combine_server_and_shutdown(
            Err(FrontendApplicationError::server(format!(
                "install frontend serving reader after application open: {error}"
            ))),
            combine_server_and_shutdown(shutdown, cleanup),
        );
    }
    if let Err(error) = island_reader.install(host.backend_island_snapshot_reader()) {
        let shutdown = host
            .shutdown_until(std::time::Instant::now() + cleanup_timeout)
            .await;
        let cleanup = metrics_http_server
            .stop()
            .map_err(FrontendApplicationError::server);
        return combine_server_and_shutdown(
            Err(FrontendApplicationError::server(format!(
                "install frontend island reader after application open: {error}"
            ))),
            combine_server_and_shutdown(shutdown, cleanup),
        );
    }
    if let Err(error) = convergence_reader.install(host.lifecycle_convergence_reader()) {
        let shutdown = host
            .shutdown_until(std::time::Instant::now() + cleanup_timeout)
            .await;
        let cleanup = metrics_http_server
            .stop()
            .map_err(FrontendApplicationError::server);
        return combine_server_and_shutdown(
            Err(FrontendApplicationError::server(format!(
                "install frontend lifecycle convergence reader after application open: {error}"
            ))),
            combine_server_and_shutdown(shutdown, cleanup),
        );
    }
    let server_result = run_server_until_signal(config, (), signal, |config, (), shutdown| {
        serve_ready_frontend_session_factory(
            config,
            &host,
            mv_storage_observation,
            shutdown,
            &mut metrics_http_server,
        )
    })
    .await;
    let shutdown_result = host
        .shutdown_until(std::time::Instant::now() + cleanup_timeout)
        .await;
    let metrics_stop = metrics_http_server
        .stop()
        .map_err(FrontendApplicationError::server);
    combine_server_and_shutdown(
        combine_server_and_shutdown(server_result, shutdown_result),
        metrics_stop,
    )
}

fn start_early_management_server(
    config: &FrontendServerConfig,
) -> Result<
    (
        Arc<LateBoundFrontendServingSnapshotReader>,
        Arc<crate::topology::LateBoundBackendIslandSnapshotReader>,
        Arc<crate::metrics::LateBoundQueryLifecycleConvergenceReader>,
        crate::metrics::MetricsHttpServer,
    ),
    FrontendApplicationError,
> {
    let metrics_registry =
        crate::metrics::FrontendMetricsRegistry::new().map_err(FrontendApplicationError::server)?;
    let serving_reader = Arc::new(LateBoundFrontendServingSnapshotReader::default());
    let island_reader = Arc::new(crate::topology::LateBoundBackendIslandSnapshotReader::new(
        config.backend_open.native_compatibility_id(),
    ));
    let convergence_reader =
        Arc::new(crate::metrics::LateBoundQueryLifecycleConvergenceReader::default());
    let management_reader: Arc<dyn FrontendServingSnapshotReader> = serving_reader.clone();
    let management_island_reader: Arc<dyn crate::topology::BackendIslandSnapshotReader> =
        island_reader.clone();
    let management_convergence_reader: Arc<
        dyn crate::coordinator::QueryLifecycleConvergenceReader,
    > = convergence_reader.clone();
    let metrics_http_server = crate::metrics::MetricsHttpServer::start(
        &config.report_bind_host,
        config.metrics_http_port,
        Arc::clone(&metrics_registry),
        management_reader,
        management_island_reader,
        Some(management_convergence_reader),
    )
    .map_err(FrontendApplicationError::server)?;
    Ok((
        serving_reader,
        island_reader,
        convergence_reader,
        metrics_http_server,
    ))
}

async fn serve_ready_frontend_session_factory<F>(
    config: FrontendServerConfig,
    host: &FrontendApplicationHost,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    shutdown: F,
    metrics_http_server: &mut crate::metrics::MetricsHttpServer,
) -> Result<(), FrontendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    let mut report_server = host.start_report_server_from_host(
        &config.report_bind_host,
        config.report_grpc_port,
        Arc::clone(&config.native_trust),
        config.native_transport.clone(),
    )?;
    let exchange_port = report_server.bound_addr().port();
    let system_catalog: Arc<dyn crate::catalog_application::system_catalog::SystemCatalog> =
        Arc::new(crate::system_catalog::SystemCatalogService::with_defaults());
    let client_connections = Arc::new(MysqlClientConnectionRegistry::new());
    let client_connection_control: Arc<dyn ClientConnectionControlPort> =
        client_connections.clone();
    let session_factory = match build_frontend_query_session_factory(
        host,
        system_catalog,
        exchange_port,
        mv_storage_observation,
        client_connection_control,
    ) {
        Ok(factory) => factory,
        Err(error) => {
            let stop_result = report_server
                .stop()
                .map_err(FrontendApplicationError::server);
            return combine_server_and_shutdown(
                combine_server_and_shutdown(Err(error), stop_result),
                Ok(()),
            );
        }
    };
    let server_result = run_mysql_with_listener_supervision(
        config.mysql_listener,
        session_factory,
        client_connections,
        shutdown,
        &mut report_server,
        metrics_http_server,
        host.serving_lifecycle(),
        config.frontend_drain_timeout,
        config.frontend_cleanup_timeout,
    )
    .await;
    let stop_result = report_server
        .stop()
        .map_err(FrontendApplicationError::server);
    combine_server_and_shutdown(server_result, stop_result)
}

async fn run_mysql_with_listener_supervision<F>(
    mysql_listener: ResolvedMysqlListenerSettings,
    session_factory: Arc<dyn QuerySessionFactory>,
    client_connections: Arc<MysqlClientConnectionRegistry>,
    shutdown: F,
    report_server: &mut crate::native::report_server::FrontendReportServerHandle,
    management_server: &mut crate::metrics::MetricsHttpServer,
    lifecycle: Arc<crate::workload_lifecycle::FrontendServingLifecycle>,
    drain_timeout: Duration,
    cleanup_timeout: Duration,
) -> Result<(), FrontendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    let (drain_tx, drain_rx) = tokio::sync::watch::channel(false);
    let (finalize_tx, finalize_rx) = tokio::sync::watch::channel(false);
    let wait_for_signal = |mut receiver: tokio::sync::watch::Receiver<bool>| async move {
        while !*receiver.borrow() {
            if receiver.changed().await.is_err() {
                break;
            }
        }
    };
    let mysql_server = crate::mysql::run_mysql_server_until_drain_then_shutdown(
        mysql_listener,
        Arc::clone(&session_factory),
        Arc::clone(&client_connections),
        wait_for_signal(drain_rx),
        wait_for_signal(finalize_rx),
        cleanup_timeout,
    );
    tokio::pin!(mysql_server);

    tokio::select! {
        result = &mut mysql_server => result.map_err(FrontendApplicationError::server),
        _ = shutdown => {
            lifecycle.begin_drain(drain_timeout);
            let _ = drain_tx.send(true);
            let graceful = tokio::time::timeout(drain_timeout, lifecycle.wait_for_no_active_work()).await;
            if graceful.is_err() {
                lifecycle.cancel_active_at_drain_deadline(drain_timeout.as_millis().min(u64::MAX as u128) as u64);
                // Keep the admitted protocol tasks alive long enough to
                // observe the first-wins deadline cancellation and return
                // its typed error. Final connection termination remains the
                // fallback when a cancelled attempt does not converge inside
                // the configured bounded cleanup window.
                let _ = tokio::time::timeout(cleanup_timeout, lifecycle.wait_for_no_active_work()).await;
            }
            session_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            client_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
            lifecycle.mark_stopping();
            let _ = finalize_tx.send(true);
            mysql_server.await.map_err(FrontendApplicationError::server)
        }
        error = wait_for_frontend_listener_failure(report_server, management_server) => {
            lifecycle.begin_drain(drain_timeout);
            let _ = drain_tx.send(true);
            lifecycle.cancel_active_at_drain_deadline(drain_timeout.as_millis().min(u64::MAX as u128) as u64);
            session_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            client_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
            lifecycle.mark_stopping();
            let _ = finalize_tx.send(true);
            let mysql_result = mysql_server.await.map_err(FrontendApplicationError::server);
            match mysql_result {
                Ok(()) => Err(FrontendApplicationError::server(error)),
                Err(mysql_error) => Err(FrontendApplicationError::server(error)
                    .with_cleanup_context(format!("shutdown MySQL listener after Frontend listener failure: {mysql_error}"))),
            }
        }
    }
}

async fn wait_for_frontend_listener_failure(
    report_server: &mut crate::native::report_server::FrontendReportServerHandle,
    management_server: &mut crate::metrics::MetricsHttpServer,
) -> String {
    loop {
        match report_server.poll_failure() {
            Ok(Some(error)) => return format!("frontend report listener failed: {error}"),
            Ok(None) => {}
            Err(error) => return format!("poll frontend report listener failed: {error}"),
        }
        match management_server.poll_failure() {
            Ok(Some(error)) => return format!("frontend management listener failed: {error}"),
            Ok(None) => {}
            Err(error) => return format!("poll frontend management listener failed: {error}"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
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
    OpenHost: FnOnce(Option<StateStoreHostInput>) -> OpenHostFuture,
    OpenHostFuture: Future<Output = Result<Host, FrontendApplicationError>>,
    ExtractService: FnOnce(&Host) -> Service,
    Serve: FnOnce(FrontendServerConfig, Service, F) -> ServeFuture,
    ServeFuture: Future<Output = Result<(), FrontendApplicationError>>,
    ShutdownHost: FnOnce(Host) -> ShutdownHostFuture,
    ShutdownHostFuture: Future<Output = Result<(), FrontendApplicationError>>,
{
    let state_store_input = config.state_store_input.clone();
    let host = open_host(state_store_input).await?;
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
    OpenHost: FnOnce(Option<StateStoreHostInput>) -> OpenHostFuture,
    OpenHostFuture: Future<Output = Result<Host, FrontendApplicationError>>,
    ExtractService: FnOnce(&Host) -> Service,
    Serve: FnOnce(FrontendServerConfig, Service, ShutdownSignal) -> ServeFuture,
    ServeFuture: Future<Output = Result<(), FrontendApplicationError>>,
    ShutdownHost: FnOnce(Host) -> ShutdownHostFuture,
    ShutdownHostFuture: Future<Output = Result<(), FrontendApplicationError>>,
{
    let state_store_input = config.state_store_input.clone();
    let host = open_host(state_store_input).await?;
    let service = extract_service(&host);
    let server_result = run_server_until_signal(config, service, signal, serve).await;
    let shutdown_result = shutdown_host(host).await;

    combine_server_and_shutdown(server_result, shutdown_result)
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
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::UnavailableMvStorageObservationPort;

    use super::{
        FrontendServerConfig, build_frontend_query_session_factory, run_frontend_server,
        run_frontend_server_until_shutdown, run_frontend_server_until_shutdown_with_ports,
        run_frontend_server_with_signal_and_ports,
    };
    use crate::catalog_application::{CatalogAdmission, CatalogDesiredStateSourceInput};
    use crate::native::transport::FrontendNativeTransport;
    use crate::state_store::{
        StateStoreProviderRegistry,
        testing::{input as test_state_store_input, registry as test_state_store_registry},
    };
    use crate::{
        ClusterBackendOpenConfig, FrontendApplicationError, FrontendApplicationErrorKind,
        FrontendApplicationHost, FrontendExecutionConfig, MysqlClientConnectionRegistry,
    };
    use crate::{QueryServiceErrorKind, QuerySessionOpenRequest, ResolvedMysqlListenerSettings};

    fn test_native_trust() -> Arc<NativeTrust> {
        Arc::new(NativeTrust::new(
            DeploymentId::parse("frontend-server-test").expect("deployment"),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .expect("secret"),
            NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("subject"),
            NativeTransportMode::Disabled,
        ))
    }

    fn builtin_function_catalog() -> Arc<novarocks_functions::EngineFunctionCatalog> {
        Arc::new(
            novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin function catalog"),
        )
    }

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
            execution: FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
            ),
            backend_open: frontend_backend_open_config(),
            report_bind_host: "127.0.0.1".to_string(),
            report_grpc_port: 0,
            metrics_http_port: 0,
            frontend_drain_timeout: Duration::from_secs(1),
            frontend_cleanup_timeout: Duration::from_secs(1),
            mysql_listener: ResolvedMysqlListenerSettings::new(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                "root",
            ),
            connector_control_role_factories: Vec::new(),
            mv_storage_observation: Arc::new(UnavailableMvStorageObservationPort),
            state_store_input: None,
            state_store_provider_registry: StateStoreProviderRegistry::new(),
            native_trust: test_native_trust(),
            native_transport: FrontendNativeTransport::plaintext(),
        }
    }

    fn frontend_backend_open_config() -> ClusterBackendOpenConfig {
        ClusterBackendOpenConfig::new(
            novarocks_types::ClusterRole::Fe,
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            Duration::from_secs(1),
            3,
            Duration::from_secs(1),
        )
        .expect("valid frontend backend config")
    }

    /// Answers whichever catalog instance the role binding carries, so the
    /// cutover test exercises the real scheduler-backed CREATE path without an
    /// object store.
    struct EchoingControlRoleFactory;

    impl novarocks_spi::connector::ConnectorControlRoleBindingFactory for EchoingControlRoleFactory {
        fn provider_id(&self) -> novarocks_spi::connector::ConnectorProviderId {
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static provider ID")
        }

        fn normalize_and_validate(
            &self,
            properties: novarocks_spi::connector::CatalogProperties,
        ) -> Result<
            novarocks_spi::connector::NormalizedCatalogProperties,
            novarocks_spi::connector::ConnectorMaterializationError,
        > {
            novarocks_spi::connector::NormalizedCatalogProperties::try_new(properties).map_err(
                |detail| novarocks_spi::connector::ConnectorMaterializationError::new(
                    novarocks_spi::connector::ConnectorMaterializationErrorClass::InvalidDefinition,
                    novarocks_spi::connector::ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    detail,
                ),
            )
        }

        fn materialize(
            &self,
            properties: novarocks_spi::connector::NormalizedCatalogProperties,
            _context: novarocks_spi::connector::MaterializationContext,
        ) -> futures::future::BoxFuture<
            'static,
            Result<
                novarocks_spi::connector::ConnectorControlRoleBinding,
                novarocks_spi::connector::ConnectorMaterializationError,
            >,
        > {
            use futures::FutureExt;

            async move {
                let control = crate::connector::control_host::tests::test_control_binding_for(
                    properties.handle().catalog_name().clone(),
                    1,
                )
                .with_catalog_properties(properties.as_catalog_properties().clone())
                .map_err(novarocks_spi::connector::ConnectorMaterializationError::from)?;
                novarocks_spi::connector::ConnectorControlRoleBinding::try_new(
                    properties,
                    Arc::new(control),
                    None,
                    None,
                )
                .map_err(novarocks_spi::connector::ConnectorMaterializationError::from)
            }
            .boxed()
        }
    }

    /// CP-2 cutover gate: the StateStore attachment is the only catalog
    /// authority the production composition installs, and Core reaches it only
    /// through the frontend application port.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cp2_production_composition_owns_catalog_ddl_through_the_state_store_attachment() {
        let state_store = test_state_store_input("cp2-cutover");
        let registry = test_state_store_registry();
        let host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                std::num::NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
            )
            .with_catalog_desired_state_source(CatalogDesiredStateSourceInput::DynamicStateStore),
            frontend_backend_open_config(),
            vec![Arc::new(EchoingControlRoleFactory)],
            tokio::runtime::Handle::current(),
            test_native_trust(),
            FrontendNativeTransport::plaintext(),
        )
        .await
        .expect("open frontend application host");
        let store = host.state_store().expect("frontend StateStore");
        let attachments = crate::catalog_attachment::CatalogAttachmentRepository::open(
            Arc::clone(&store),
            host.run_policy(),
        )
        .await
        .expect("open catalog attachment repository");

        let session_factory = build_frontend_query_session_factory(
            &host,
            Arc::new(crate::system_catalog::SystemCatalogService::with_defaults()),
            0,
            Arc::new(UnavailableMvStorageObservationPort),
            Arc::new(MysqlClientConnectionRegistry::new()),
        )
        .expect("build ready frontend session factory");
        let session = session_factory
            .open_session(QuerySessionOpenRequest::new(
                crate::ClientConnectionToken::new(1, 1).expect("valid connection token"),
                "cp2-cutover",
            ))
            .expect("open frontend query session");
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse("warehouse").expect("instance ID");

        session
            .execute_batch(r#"CREATE EXTERNAL CATALOG warehouse PROPERTIES("type"="iceberg")"#)
            .await
            .expect("CREATE CATALOG commits a durable StateStore attachment");
        let created = attachments
            .get(&instance_id)
            .await
            .expect("read attachment")
            .expect("CREATE CATALOG must commit to the StateStore attachment keyspace");
        assert_eq!(created.attachment.provider_id.as_str(), "iceberg");
        assert_eq!(created.attachment.display_name, "warehouse");
        assert!(matches!(
            host.catalog_application_port().admit_catalog(&instance_id),
            CatalogAdmission::Ready(_)
        ));
        session
            .execute_batch("SET CATALOG warehouse")
            .await
            .expect("the committed attachment is admitted by this frontend session");

        session
            .execute_batch("DROP CATALOG warehouse")
            .await
            .expect("DROP CATALOG deletes the durable StateStore attachment");
        assert!(
            attachments
                .get(&instance_id)
                .await
                .expect("read attachment")
                .is_none(),
            "DROP CATALOG must remove the durable attachment"
        );
        assert!(matches!(
            host.catalog_application_port().admit_catalog(&instance_id),
            CatalogAdmission::Absent
        ));
        assert_eq!(
            session
                .execute_batch("SET CATALOG warehouse")
                .await
                .expect_err("a dropped catalog stops being admitted")
                .kind(),
            QueryServiceErrorKind::BadDatabase
        );

        // The ready session factory and this test's probe both hold StateStore references; the
        // host owns closing the deployment lock, so release them first.
        drop(attachments);
        drop(store);
        session.close();
        drop(session);
        drop(session_factory);
        host.shutdown().await.expect("host shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frontend_report_endpoint_binds_loopback_without_core_transport_facade() {
        let state_store = test_state_store_input("frontend-report-listener");
        let registry = test_state_store_registry();
        let host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                std::num::NonZeroUsize::new(1).unwrap(),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
            ),
            frontend_backend_open_config(),
            Vec::new(),
            tokio::runtime::Handle::current(),
            test_native_trust(),
            FrontendNativeTransport::plaintext(),
        )
        .await
        .expect("open frontend application host");
        for bind_addr in ["127.0.0.1:0".parse().unwrap(), "[::1]:0".parse().unwrap()] {
            let mut report_server = host
                .start_report_server(
                    bind_addr,
                    test_native_trust(),
                    FrontendNativeTransport::plaintext(),
                )
                .expect("start frontend-owned report endpoint");
            let bound_addr = report_server.bound_addr();
            assert_ne!(
                bound_addr.port(),
                0,
                "ephemeral report listener selects a real port"
            );
            assert_eq!(bound_addr.is_ipv6(), bind_addr.is_ipv6());
            assert_eq!(
                report_server.poll_failure().expect("poll report listener"),
                None,
                "report listener remains live after bind"
            );
            report_server.stop().expect("stop frontend report endpoint");
        }
        host.shutdown()
            .await
            .expect("shutdown frontend application host");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlx2_application_frontend_services_inject_statistics_application_port() {
        let state_store = test_state_store_input("statistics-application-port");
        let registry = test_state_store_registry();
        let host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                std::num::NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
            ),
            frontend_backend_open_config(),
            Vec::new(),
            tokio::runtime::Handle::current(),
            test_native_trust(),
            FrontendNativeTransport::plaintext(),
        )
        .await
        .expect("open frontend application host");
        let session_factory = build_frontend_query_session_factory(
            &host,
            Arc::new(crate::system_catalog::SystemCatalogService::with_defaults()),
            0,
            Arc::new(UnavailableMvStorageObservationPort),
            Arc::new(MysqlClientConnectionRegistry::new()),
        )
        .expect("build ready frontend session factory");
        let session = session_factory
            .open_session(QuerySessionOpenRequest::new(
                crate::ClientConnectionToken::new(2, 1).expect("valid connection token"),
                "statistics-binding",
            ))
            .expect("open frontend query session");
        session
            .execute_batch("SHOW ANALYZE JOBS")
            .await
            .expect("configured Frontend statistics application port handles SHOW ANALYZE JOBS");

        session.close();
        drop(session);
        drop(session_factory);
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
        let data_runtime = tokio::runtime::Runtime::new().expect("data runtime");
        accepts_async_runner(run_frontend_server_until_shutdown(
            frontend_config(),
            data_runtime.handle().clone(),
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
    async fn full_process_config_passes_provider_neutral_state_store_input_to_host() {
        let mut config = frontend_config();
        let input = test_state_store_input("frontend-cluster");
        config.state_store_input = Some(input.clone());
        let captured = Arc::new(Mutex::new(None));
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
            .expect("state store input");
        assert_eq!(captured, input);
    }
}
