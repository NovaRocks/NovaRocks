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
use novarocks::mv::storage_observation::MvStorageObservationPort;
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
    .with_statistics_application(host.statistics_application_port())
    .with_statistics_target_resolver_sink(host.statistics_application_port())
    .with_statistics_table_reader_sink(host.statistics_application_port())
    .with_statistics_attempt_executor_sink(host.statistics_application_port())
    .with_mv_refresh_provider_activation_sink(host.mv_refresh_provider_activation_sink())
    .with_mv_background_engine_sink(host.mv_background_engine_sink())
    .with_mv_storage_observation(mv_storage_observation)
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

/// Builds standalone services from a previously opened frontend host.
pub fn standalone_open_services_for_server(
    host: &FrontendApplicationHost,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
) -> novarocks::engine::StandaloneOpenServices {
    standalone_open_services(
        Arc::new(crate::system_catalog::SystemCatalogService::with_defaults()),
        host,
        mv_storage_observation,
    )
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
    let system_catalog: Arc<dyn novarocks::engine::system_catalog::SystemCatalog> =
        Arc::new(crate::system_catalog::SystemCatalogService::with_defaults());
    let execution = resolve_frontend_execution_config(&config)?;
    let optimizer_query_mem_limit_bytes = execution.optimizer_query_mem_limit_bytes();
    let backend = cluster_backend_open_config(&config.config)?;
    let connector_factories = config.connector_control_factories.clone();
    let mv_storage_observation = Arc::clone(&config.mv_storage_observation);
    run_frontend_server_until_shutdown_with_ports(
        config,
        shutdown,
        move |state_store| {
            let connector_factories = connector_factories.clone();
            async move {
                FrontendApplicationHost::open_with_factories(
                    state_store,
                    execution,
                    backend,
                    connector_factories,
                )
                .await
            }
        },
        move |host| {
            (
                standalone_open_services(
                    system_catalog,
                    host,
                    Arc::clone(&mv_storage_observation),
                ),
                host.dml_service(),
                host.terminal_ingress(),
            )
        },
        move |config, (mut services, dml, terminal_ingress), shutdown| async move {
            let mut report_server = FrontendReportServerHandle::start(
                &config.config.server.host,
                config.config.server.grpc_port,
                terminal_ingress,
            )
            .map_err(FrontendApplicationError::server)?;
            services.exchange_port = report_server.bound_addr().port();
            let query_control = services.query_control.clone();
            let query_execution = services.query_execution.clone();
            let topology = services.backend_topology.clone();
            let role = services.execution_role;
            let server_result = novarocks::server::run_standalone_server_with_config_until_shutdown_with_session_factory(
                config.config,
                config.config_path,
                config.port_override,
                services,
                move |engine| {
                    let insert_engine = engine.insert_engine();
                    let delete_engine = engine.delete_engine();
                    let mutation_engine = engine.mutation_engine();
                    let ctas_engine = engine.ctas_engine();
                    let truncate_engine = engine.truncate_engine();
                    let add_files_engine = engine.add_files_engine();
                    Ok(Arc::new(crate::query::FrontendQueryService::new(
                        engine,
                        query_control,
                        query_execution,
                        role,
                        topology,
                        dml,
                        insert_engine,
                        delete_engine,
                        mutation_engine,
                        add_files_engine,
                        ctas_engine,
                        truncate_engine,
                        optimizer_query_mem_limit_bytes,
                    )))
                },
                shutdown,
            )
            .await
            .map_err(|error| {
                FrontendApplicationError::server(format!("standalone server failed: {error}"))
            });
            let stop_result = report_server.stop().map_err(FrontendApplicationError::server);
            combine_server_and_shutdown(server_result, stop_result)
        },
        |host| async move { host.shutdown().await },
    )
    .await
}

async fn run_frontend_server_with_signal<S, E>(
    config: FrontendServerConfig,
    signal: S,
) -> Result<(), FrontendApplicationError>
where
    S: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let system_catalog: Arc<dyn novarocks::engine::system_catalog::SystemCatalog> =
        Arc::new(crate::system_catalog::SystemCatalogService::with_defaults());
    let execution = resolve_frontend_execution_config(&config)?;
    let optimizer_query_mem_limit_bytes = execution.optimizer_query_mem_limit_bytes();
    let backend = cluster_backend_open_config(&config.config)?;
    let connector_factories = config.connector_control_factories.clone();
    let mv_storage_observation = Arc::clone(&config.mv_storage_observation);
    run_frontend_server_with_signal_and_ports(
        config,
        signal,
        move |state_store| {
            let connector_factories = connector_factories.clone();
            async move {
                FrontendApplicationHost::open_with_factories(
                    state_store,
                    execution,
                    backend,
                    connector_factories,
                )
                .await
            }
        },
        move |host| {
            (
                standalone_open_services(
                    system_catalog,
                    host,
                    Arc::clone(&mv_storage_observation),
                ),
                host.dml_service(),
                host.terminal_ingress(),
            )
        },
        move |config, (mut services, dml, terminal_ingress), shutdown| async move {
            let mut report_server = FrontendReportServerHandle::start(
                &config.config.server.host,
                config.config.server.grpc_port,
                terminal_ingress,
            )
            .map_err(FrontendApplicationError::server)?;
            services.exchange_port = report_server.bound_addr().port();
            let query_control = services.query_control.clone();
            let query_execution = services.query_execution.clone();
            let topology = services.backend_topology.clone();
            let role = services.execution_role;
            let server_result = novarocks::server::run_standalone_server_with_config_until_shutdown_with_session_factory(
                config.config,
                config.config_path,
                config.port_override,
                services,
                move |engine| {
                    let insert_engine = engine.insert_engine();
                    let delete_engine = engine.delete_engine();
                    let mutation_engine = engine.mutation_engine();
                    let ctas_engine = engine.ctas_engine();
                    let truncate_engine = engine.truncate_engine();
                    let add_files_engine = engine.add_files_engine();
                    Ok(Arc::new(crate::query::FrontendQueryService::new(
                        engine,
                        query_control,
                        query_execution,
                        role,
                        topology,
                        dml,
                        insert_engine,
                        delete_engine,
                        mutation_engine,
                        add_files_engine,
                        ctas_engine,
                        truncate_engine,
                        optimizer_query_mem_limit_bytes,
                    )))
                },
                shutdown,
            )
            .await
            .map_err(|error| {
                FrontendApplicationError::server(format!("standalone server failed: {error}"))
            });
            let stop_result = report_server.stop().map_err(FrontendApplicationError::server);
            combine_server_and_shutdown(server_result, stop_result)
        },
        |host| async move { host.shutdown().await },
    )
    .await
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
