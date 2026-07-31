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
use novarocks_state_store::StateStoreHostConfig;

use crate::{
    ClusterBackendOpenConfig, FrontendApplicationError, FrontendApplicationErrorKind,
    FrontendApplicationHost, FrontendExecutionConfig,
};

type ShutdownSignal = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendGrpcEndpointOwnership {
    HostedReportOnly,
    ExternallyHosted,
}

impl FrontendGrpcEndpointOwnership {
    pub const fn hosts_report_endpoint(self) -> bool {
        matches!(self, Self::HostedReportOnly)
    }

    const fn core_ownership(self) -> novarocks::server::StandaloneGrpcEndpointOwnership {
        match self {
            Self::HostedReportOnly => {
                novarocks::server::StandaloneGrpcEndpointOwnership::HostedReportOnly
            }
            Self::ExternallyHosted => {
                novarocks::server::StandaloneGrpcEndpointOwnership::ExternallyHosted
            }
        }
    }
}

#[derive(Clone)]
pub struct FrontendServerConfig {
    pub config: NovaRocksConfig,
    pub config_path: Option<PathBuf>,
    pub port_override: Option<u16>,
    pub grpc_endpoint: FrontendGrpcEndpointOwnership,
}

fn standalone_open_services(
    system_catalog: Arc<dyn novarocks::engine::system_catalog::SystemCatalog>,
    host: &FrontendApplicationHost,
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
        host.native_report_handler(),
        host.query_control_service(),
        host.connector_control_registry(),
        0,
    )
    .with_terminal_ingress(host.terminal_ingress())
}

/// Opens the frontend services once for an externally composed server. The
/// all-in-one composition root uses the returned host both to run MySQL and
/// to provide the report handler installed on the native backend endpoint.
pub async fn open_frontend_application_for_server(
    config: &FrontendServerConfig,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    let execution = resolve_frontend_execution_config(config)?;
    let backend = cluster_backend_open_config(&config.config)?;
    FrontendApplicationHost::open(state_store_host_config(&config.config), execution, backend).await
}

/// Builds standalone services from a previously opened frontend host.
pub fn standalone_open_services_for_server(
    host: &FrontendApplicationHost,
) -> novarocks::engine::StandaloneOpenServices {
    standalone_open_services(
        Arc::new(crate::system_catalog::SystemCatalogService::with_defaults()),
        host,
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
    let backend = cluster_backend_open_config(&config.config)?;
    run_frontend_server_until_shutdown_with_ports(
        config,
        shutdown,
        |state_store| async move { FrontendApplicationHost::open(state_store, execution, backend).await },
        move |host| {
            (
                standalone_open_services(system_catalog, host),
                host.dml_service(),
            )
        },
        move |config, (services, dml), shutdown| async move {
            let query_control = services.query_control.clone();
            let query_execution = services.query_execution.clone();
            let topology = services.backend_topology.clone();
            let role = services.execution_role;
            novarocks::server::run_standalone_server_with_config_until_shutdown_with_session_factory(
                config.config,
                config.config_path,
                config.port_override,
                config.grpc_endpoint.core_ownership(),
                services,
                move |engine| {
                    let insert_engine = engine.insert_engine();
                    Ok(Arc::new(crate::query::FrontendQueryService::new(
                        engine,
                        query_control,
                        query_execution,
                        role,
                        topology,
                        dml,
                        insert_engine,
                    )))
                },
                shutdown,
            )
            .await
            .map_err(|error| {
                FrontendApplicationError::server(format!("standalone server failed: {error}"))
            })
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
    let backend = cluster_backend_open_config(&config.config)?;
    run_frontend_server_with_signal_and_ports(
        config,
        signal,
        |state_store| async move { FrontendApplicationHost::open(state_store, execution, backend).await },
        move |host| {
            (
                standalone_open_services(system_catalog, host),
                host.dml_service(),
            )
        },
        move |config, (services, dml), shutdown| async move {
            let query_control = services.query_control.clone();
            let query_execution = services.query_execution.clone();
            let topology = services.backend_topology.clone();
            let role = services.execution_role;
            novarocks::server::run_standalone_server_with_config_until_shutdown_with_session_factory(
                config.config,
                config.config_path,
                config.port_override,
                config.grpc_endpoint.core_ownership(),
                services,
                move |engine| {
                    let insert_engine = engine.insert_engine();
                    Ok(Arc::new(crate::query::FrontendQueryService::new(
                        engine,
                        query_control,
                        query_execution,
                        role,
                        topology,
                        dml,
                        insert_engine,
                    )))
                },
                shutdown,
            )
            .await
            .map_err(|error| {
                FrontendApplicationError::server(format!("standalone server failed: {error}"))
            })
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
    Ok(FrontendExecutionConfig::new(
        advertised.host,
        advertised.port,
        runtime_filter_worker_count,
    ))
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
    let state_store_host_config = state_store_host_config(&config.config);
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
    let state_store_host_config = state_store_host_config(&config.config);
    let host = open_host(state_store_host_config).await?;
    let service = extract_service(&host);
    let server_result = run_server_until_signal(config, service, signal, serve).await;
    let shutdown_result = shutdown_host(host).await;

    combine_server_and_shutdown(server_result, shutdown_result)
}

fn state_store_host_config(config: &NovaRocksConfig) -> Option<StateStoreHostConfig> {
    config
        .state_store
        .clone()
        .map(|state_store| StateStoreHostConfig {
            state_store,
            foundationdb_client: config.foundationdb_client.clone(),
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
        FrontendGrpcEndpointOwnership, FrontendServerConfig, run_frontend_server,
        run_frontend_server_until_shutdown, run_frontend_server_until_shutdown_with_ports,
        run_frontend_server_with_signal_and_ports, standalone_open_services,
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
            grpc_endpoint: FrontendGrpcEndpointOwnership::HostedReportOnly,
        }
    }

    struct GrpcServerTestGuard;

    impl Drop for GrpcServerTestGuard {
        fn drop(&mut self) {
            let _ = novarocks::service::grpc_server::stop_grpc_server();
        }
    }

    struct RejectingNativeFragmentIngress;

    struct ReadyQueryLifecycleIngress;

    struct ReadyQueryControl {
        events: tokio::sync::mpsc::Sender<novarocks::query_execution::lifecycle::QueryControlEvent>,
    }

    impl novarocks::query_execution::lifecycle::BackendQueryControl for ReadyQueryControl {
        fn heartbeat(
            &self,
            sequence: u64,
        ) -> Result<(), novarocks::query_execution::lifecycle::QueryLifecycleError> {
            self.events
                .try_send(
                    novarocks::query_execution::lifecycle::QueryControlEvent::HeartbeatAck {
                        sequence,
                    },
                )
                .map_err(|error| {
                    novarocks::query_execution::lifecycle::QueryLifecycleError::new(
                        novarocks::query_execution::lifecycle::QueryLifecycleErrorCode::Internal,
                        error.to_string(),
                    )
                })
        }

        fn abort(
            &self,
            _reason: String,
        ) -> Result<(), novarocks::query_execution::lifecycle::QueryLifecycleError> {
            self.events
                .try_send(
                    novarocks::query_execution::lifecycle::QueryControlEvent::TerminationAccepted {
                        reason: novarocks::query_execution::lifecycle::QueryTerminationReason::CoordinatorAbort,
                    },
                )
                .map_err(|error| {
                    novarocks::query_execution::lifecycle::QueryLifecycleError::new(
                        novarocks::query_execution::lifecycle::QueryLifecycleErrorCode::Internal,
                        error.to_string(),
                    )
                })
        }

        fn finalize(
            &self,
        ) -> Result<(), novarocks::query_execution::lifecycle::QueryLifecycleError> {
            self.events
                .try_send(
                    novarocks::query_execution::lifecycle::QueryControlEvent::TerminationAccepted {
                        reason: novarocks::query_execution::lifecycle::QueryTerminationReason::CoordinatorFinalize,
                    },
                )
                .map_err(|error| {
                    novarocks::query_execution::lifecycle::QueryLifecycleError::new(
                        novarocks::query_execution::lifecycle::QueryLifecycleErrorCode::Internal,
                        error.to_string(),
                    )
                })
        }

        fn coordinator_lost(
            &self,
            _reason: novarocks::query_execution::lifecycle::QueryTerminationReason,
        ) -> Result<(), novarocks::query_execution::lifecycle::QueryLifecycleError> {
            Ok(())
        }
    }

    impl novarocks::service::native_fragment_ingress::NativeFragmentIngress
        for RejectingNativeFragmentIngress
    {
        fn cancel(
            &self,
            _request: novarocks::service::native_fragment_ingress::NativeFragmentCancelRequest,
        ) -> Result<(), novarocks::service::native_fragment_ingress::NativeFragmentIngressError>
        {
            Ok(())
        }
    }

    impl novarocks::query_execution::lifecycle::QueryLifecycleIngress for ReadyQueryLifecycleIngress {
        fn bind_backend_identity(
            &self,
            _backend_id: u64,
        ) -> Result<(), novarocks::query_execution::lifecycle::QueryLifecycleError> {
            Ok(())
        }

        fn init_query(
            &self,
            request: novarocks::query_execution::lifecycle::QueryInitRequest,
        ) -> novarocks::query_execution::lifecycle::QueryInitAck {
            novarocks::query_execution::lifecycle::QueryInitAck::new(
                request.manifest().execution_id(),
                request.digest(),
                novarocks::query_execution::lifecycle::QueryInitOutcome::Applied,
            )
        }

        fn abort_query(
            &self,
            request: novarocks::query_execution::lifecycle::QueryAbortRequest,
        ) -> Result<
            novarocks::query_execution::lifecycle::QueryTerminationAck,
            novarocks::query_execution::lifecycle::QueryLifecycleError,
        > {
            Ok(
                novarocks::query_execution::lifecycle::QueryTerminationAck::new(
                    request.execution_id(),
                    novarocks::query_execution::lifecycle::QueryTerminationReason::CoordinatorAbort,
                ),
            )
        }

        fn attach_control(
            &self,
            _attach: novarocks::query_execution::lifecycle::QueryControlAttach,
        ) -> Result<
            novarocks::query_execution::lifecycle::QueryControlAttachment,
            novarocks::query_execution::lifecycle::QueryLifecycleError,
        > {
            let (events, receiver) = tokio::sync::mpsc::channel(32);
            events
                .try_send(novarocks::query_execution::lifecycle::QueryControlEvent::ControlReady)
                .expect("publish test ControlReady");
            Ok(
                novarocks::query_execution::lifecycle::QueryControlAttachment {
                    control: Arc::new(ReadyQueryControl { events }),
                    events: receiver,
                },
            )
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_in_one_production_composition_uses_frontend_reports_and_loopback_grpc() {
        let _grpc_guard = GrpcServerTestGuard;
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
        let mut services = standalone_open_services(
            Arc::new(crate::system_catalog::SystemCatalogService::with_defaults()),
            &host,
        );
        novarocks::service::grpc_server::start_grpc_exchange_server_with_terminal_ingress(
            &config.server.host,
            config.server.grpc_port,
            Arc::new(RejectingNativeFragmentIngress),
            Arc::new(ReadyQueryLifecycleIngress),
            Arc::clone(&services.native_report_handler),
            Some(host.terminal_ingress()),
        )
        .expect("start production-composed all-in-one gRPC endpoint");
        let grpc_port = novarocks::service::grpc_server::grpc_server_bound_port()
            .expect("all-in-one combined gRPC endpoint bound port");
        services.exchange_port = grpc_port;
        let loopback_endpoint = format!("127.0.0.1:{grpc_port}")
            .parse()
            .expect("all-in-one loopback endpoint");
        services
            .backend_topology
            .add_backend(loopback_endpoint)
            .expect("register all-in-one loopback backend");
        let live_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        while !services
            .backend_topology
            .snapshot()
            .expect("all-in-one topology snapshot")
            .targets()
            .iter()
            .any(|backend| backend.endpoint() == loopback_endpoint)
        {
            assert!(
                tokio::time::Instant::now() < live_deadline,
                "all-in-one loopback backend did not become Live"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let backend_topology = Arc::clone(&services.backend_topology);
        let engine = novarocks::engine::StandaloneNovaRocks::open_with_config(
            novarocks::engine::StandaloneOptions::default(),
            config,
            services,
        )
        .expect("open production-composed all-in-one engine");
        report_endpoint.set_bound_port(grpc_port);

        assert_eq!(
            backend_topology
                .snapshot()
                .expect("all-in-one topology snapshot")
                .targets()
                .len(),
            1,
            "all-in-one publishes its loopback backend"
        );

        let client = novarocks::service::grpc_client::NovaRocksGrpcRemoteClient::new(
            format!("127.0.0.1:{grpc_port}")
                .parse()
                .expect("all-in-one loopback address"),
        )
        .expect("all-in-one loopback client");
        let report_response = client
            .blocking_report_exec_status(novarocks_protocol::novarocks::ReportExecStatusRequest {
                report: Some(novarocks_protocol::novarocks::ExecStatusReport {
                    query_id: Some(novarocks_protocol::common::UniqueId { hi: 61, lo: 71 }),
                    fragment_instance_id: Some(novarocks_protocol::common::UniqueId {
                        hi: 61,
                        lo: 72,
                    }),
                    status: Some(novarocks_protocol::common::Status::default()),
                    done: true,
                    ..Default::default()
                }),
            })
            .expect("all-in-one report RPC returns a business response");
        assert_eq!(report_response.status_code, 2);
        assert_eq!(report_response.error_code, "WriteCoordinatorGone");
        assert_eq!(
            report_response.message,
            "frontend query 61/71 is not active"
        );

        let fixture = novarocks::query_execution::contract_test_support::non_empty_result_contract_fixture_with_topology(
            backend_topology
                .snapshot()
                .expect("capture loopback topology for the contract fixture"),
        );
        let error = match host.execute_distributed_query_for_test(fixture.into_request()) {
            Ok(_) => panic!("contract fixture must reach backend ingress over loopback gRPC"),
            Err(error) => error,
        };
        assert!(error.message().contains("StageFragments"), "{error}");

        drop(engine);
        novarocks::service::grpc_server::stop_grpc_server()
            .expect("stop all-in-one combined gRPC endpoint");
        host.shutdown()
            .await
            .expect("shutdown frontend application host");
    }

    #[test]
    fn frontend_endpoint_ownership_has_no_full_execution_state() {
        assert!(FrontendGrpcEndpointOwnership::HostedReportOnly.hosts_report_endpoint());
        assert!(!FrontendGrpcEndpointOwnership::ExternallyHosted.hosts_report_endpoint());
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
