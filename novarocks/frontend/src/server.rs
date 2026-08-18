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
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use crate::capabilities as core_capabilities;
use novarocks::mv::storage_observation::MvStorageObservationPort;
use novarocks::server::ResolvedMysqlListenerSettings;
use novarocks::server::session::QuerySessionFactory;
use novarocks_spi::connector::ConnectorControlFactory;
use novarocks_state_store::StateStoreHostConfig;

use crate::native::report_server::FrontendReportServerHandle;
use crate::query_execution::maintenance::{
    BackgroundMaintenanceAttempt, BackgroundMaintenanceAttemptFactory,
};
use crate::{
    ClusterBackendOpenConfig, FrontendApplicationError, FrontendApplicationErrorKind,
    FrontendApplicationHost, FrontendExecutionConfig, FrontendQueryControlTimeouts,
};

type ShutdownSignal = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Clone)]
struct FrontendBackgroundMaintenanceAttemptFactory {
    role: novarocks_types::ClusterRole,
    topology: crate::common::backend_topology::BackendTopologyService,
}

impl BackgroundMaintenanceAttemptFactory for FrontendBackgroundMaintenanceAttemptFactory {
    fn begin_automatic_maintenance_attempt(&self) -> Result<BackgroundMaintenanceAttempt, String> {
        core_capabilities::background_maintenance_attempt(self.role, self.topology.clone())
    }
}

#[derive(Clone)]
pub struct FrontendServerConfig {
    pub execution: FrontendExecutionConfig,
    pub backend_open: ClusterBackendOpenConfig,
    pub report_bind_host: String,
    pub report_grpc_port: u16,
    pub mysql_listener: ResolvedMysqlListenerSettings,
    /// Provider-owned FE control factories composed by the server root.
    pub connector_control_factories: Vec<Arc<dyn ConnectorControlFactory>>,
    /// Application-owned storage observation composed by the server role.
    /// Frontend and Core never decode provider table handles directly.
    pub mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    /// Typed StateStore host input. The FE remains the owner of opening and
    /// shutting down this host; the server only supplies the composition data.
    pub state_store_host_config: Option<StateStoreHostConfig>,
}

/// Opens the frontend services once for an externally composed server. The
/// all-in-one composition root uses the returned host both to run MySQL and
/// to provide the terminal fallback ingress installed on the native backend endpoint.
pub async fn open_frontend_application_for_server(
    config: &FrontendServerConfig,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    FrontendApplicationHost::open_with_factories(
        config.state_store_host_config.clone(),
        config.execution.clone(),
        config.backend_open.clone(),
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
    system_catalog: Arc<dyn novarocks::catalog_application::system_catalog::SystemCatalog>,
    exchange_port: u16,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
) -> Result<Arc<dyn QuerySessionFactory>, FrontendApplicationError> {
    let catalog_service =
        Arc::new(novarocks::catalog_application::query_catalog::new_query_catalog_service());
    let unified_statistics = Arc::new(novarocks::connector::UnifiedStatisticsResolver::default());
    let catalog_application = host.catalog_application_port();
    let catalog_projection = host.catalog_runtime_projection();
    let connector_control = host.connector_control_registry();
    let query_execution = host.query_execution_service();
    let topology = host.backend_topology_port();
    let role = host.execution_role();
    let mv_repository = host.mv_repository();
    let mv_application = host.mv_application_service();
    let mv_service = host.mv_service();
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
            let service = Arc::clone(&mv_service);
            Box::new(move || {
                service
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
    let statistics_command_executor =
        core_capabilities::statistics_command_executor(statistics_application);
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
        Arc::clone(&catalog_service),
        Some(catalog_application),
        connector_control,
        unified_statistics,
        mv_storage_observation,
        query_execution.clone(),
    ));
    host.dml_service()
        .install_local_catalog(Arc::clone(&catalog_service));
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
        .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
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
        &config.report_bind_host,
        config.report_grpc_port,
        host.terminal_ingress(),
        host.lifecycle_convergence_reader(),
    )
    .map_err(FrontendApplicationError::server)?;
    let exchange_port = report_server.bound_addr().port();
    host.coordinator_report_endpoint_sink()
        .set_bound_port(exchange_port);
    let system_catalog: Arc<dyn novarocks::catalog_application::system_catalog::SystemCatalog> =
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
    let server_result = novarocks::server::run_mysql_server_until_shutdown(
        config.mysql_listener,
        session_factory,
        shutdown,
    )
    .await
    .map_err(FrontendApplicationError::server);
    let stop_result = report_server
        .stop()
        .map_err(FrontendApplicationError::server);
    combine_server_and_shutdown(server_result, stop_result)
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
    let state_store_host_config = config.state_store_host_config.clone();
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
    let state_store_host_config = config.state_store_host_config.clone();
    let host = open_host(state_store_host_config).await?;
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

    use super::{
        FrontendServerConfig, build_frontend_query_session_factory, run_frontend_server,
        run_frontend_server_until_shutdown, run_frontend_server_until_shutdown_with_ports,
        run_frontend_server_with_signal_and_ports,
    };
    use crate::{
        ClusterBackendOpenConfig, FrontendApplicationError, FrontendApplicationErrorKind,
        FrontendApplicationHost, FrontendExecutionConfig,
    };
    use novarocks::{
        catalog_application::CatalogAdmission, server::ResolvedMysqlListenerSettings,
        server::session::QuerySessionOpenRequest,
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
            execution: FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
            ),
            backend_open: frontend_backend_open_config(),
            report_bind_host: "127.0.0.1".to_string(),
            report_grpc_port: 0,
            mysql_listener: ResolvedMysqlListenerSettings::new(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                "root",
            ),
            connector_control_factories: Vec::new(),
            mv_storage_observation: Arc::new(
                novarocks::mv::storage_observation::UnavailableMvStorageObservationPort,
            ),
            state_store_host_config: None,
        }
    }

    fn frontend_backend_open_config() -> ClusterBackendOpenConfig {
        ClusterBackendOpenConfig::new(
            novarocks_types::ClusterRole::AllInOne,
            Vec::new(),
            Duration::from_secs(1),
            3,
            Duration::from_secs(1),
        )
        .expect("valid all-in-one backend config")
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
            frontend_backend_open_config(),
            vec![Arc::new(EchoingControlFactory)],
        )
        .await
        .expect("open frontend application host");
        let store = host.state_store().expect("frontend StateStore");
        let attachments =
            crate::catalog_attachment::CatalogAttachmentRepository::open(Arc::clone(&store))
                .await
                .expect("open catalog attachment repository");

        let session_factory = build_frontend_query_session_factory(
            &host,
            Arc::new(crate::system_catalog::SystemCatalogService::with_defaults()),
            0,
            Arc::new(novarocks::mv::storage_observation::UnavailableMvStorageObservationPort),
        )
        .expect("build ready frontend session factory");
        let session = session_factory
            .open_session(QuerySessionOpenRequest::new(1, "cp2-cutover"))
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
            novarocks::server::session::QueryServiceErrorKind::BadDatabase
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
        let host = FrontendApplicationHost::open(
            None,
            FrontendExecutionConfig::new("127.0.0.1", 0, std::num::NonZeroUsize::new(1).unwrap()),
            frontend_backend_open_config(),
        )
        .await
        .expect("open frontend application host");
        let report_endpoint = host.coordinator_report_endpoint_sink();
        let mut report_server = crate::native::report_server::FrontendReportServerHandle::start(
            "127.0.0.1",
            0,
            host.terminal_ingress(),
            host.lifecycle_convergence_reader(),
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
        let host = FrontendApplicationHost::open(
            None,
            FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                std::num::NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
            ),
            frontend_backend_open_config(),
        )
        .await
        .expect("open frontend application host");
        let session_factory = build_frontend_query_session_factory(
            &host,
            Arc::new(crate::system_catalog::SystemCatalogService::with_defaults()),
            0,
            Arc::new(novarocks::mv::storage_observation::UnavailableMvStorageObservationPort),
        )
        .expect("build ready frontend session factory");
        let session = session_factory
            .open_session(QuerySessionOpenRequest::new(2, "statistics-binding"))
            .expect("open frontend query session");
        let error = session
            .execute_batch("SHOW ANALYZE JOBS")
            .await
            .expect_err("a host without StateStore must reach the frontend statistics service");
        assert!(
            error
                .message()
                .contains("statistics job commands require a configured frontend StateStore"),
            "statistics application port was not injected: {error}"
        );
        assert!(
            !error
                .message()
                .contains("statistics application service is unavailable"),
            "Core default statistics application port must not be used: {error}"
        );

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
        let foundationdb_client = FoundationDbClientConfig {
            disable_multi_version_client: true,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            tls_verify_peers: None,
            tls_password_env: None,
        };
        config.state_store_host_config = Some(StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id: "frontend-cluster".to_owned(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Foundationdb {
                        cluster_file: cluster_file.path().to_path_buf(),
                        keyspace_id: Uuid::nil(),
                    },
                },
                mysql_client: None,
            },
            foundationdb_client: Some(foundationdb_client.clone()),
        });
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
