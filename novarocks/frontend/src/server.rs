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
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(test)]
use std::{sync::Mutex, task::Poll};
use tokio::runtime::Handle;
use tracing::info;

use crate::capabilities as core_capabilities;
use crate::workload_lifecycle::{
    FrontendServingSnapshotReader, LateBoundFrontendServingSnapshotReader,
};
use novarocks_mv_application::service::MvProductService;
use novarocks_mysql_adapter::{MysqlClientConnectionRegistry, ResolvedMysqlListenerSettings};
use novarocks_native_adapter::FrontendNativeTransport;
use novarocks_query_application::cancellation::QueryCancellationReason;
use novarocks_query_application::client_connection::{
    ClientConnectionControlPort, ClientConnectionTerminationReason,
};
use novarocks_query_application::session::QuerySessionFactory;
use novarocks_spi::connector::ConnectorControlRoleBindingFactory;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_state_store_runtime::{StateStoreHostInput, StateStoreProviderRegistry};
use novarocks_types::naming::DEFAULT_DATABASE;
use novarocks_version as version;

use crate::query_execution::maintenance::{
    BackgroundMaintenanceAttempt, BackgroundMaintenanceAttemptFactory,
};
use crate::{
    application::{
        FrontendApplicationError, FrontendApplicationHost, FrontendCatalogRoleRuntime,
        FrontendExecutionConfig,
    },
    topology::ClusterBackendOpenConfig,
};

#[cfg(test)]
type ShutdownSignal = std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Clone)]
struct FrontendBackgroundMaintenanceAttemptFactory {
    role: novarocks_types::ClusterRole,
    topology: novarocks_query_application::api::BackendTopologyService,
    runtime_policy: novarocks_query_application::publication::LakePublicationRuntimePolicy,
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

/// Inputs required to open the Frontend application owner.
///
/// The Server role composes these inputs; this type deliberately excludes
/// listeners and process supervision policy.
#[derive(Clone)]
pub struct FrontendApplicationOpenConfig {
    pub execution: FrontendExecutionConfig,
    pub backend_open: ClusterBackendOpenConfig,
    /// Provider-owned FE control role factories composed by the server root.
    pub connector_control_role_factories: Vec<Arc<dyn ConnectorControlRoleBindingFactory>>,
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

/// Inputs for the Frontend-owned management listener.
#[derive(Clone)]
pub struct FrontendManagementConfig {
    pub bind_host: String,
    pub http_port: u16,
    pub native_compatibility_id: novarocks_types::NativeCompatibilityId,
    /// The one memory capacity authority this OS process was given, so the
    /// management surface can report its facts without owning any of them.
    pub memory_authority: Arc<novarocks_memory::MemoryAuthority>,
}

/// Inputs for serving one ready Frontend application through native and MySQL
/// listeners. The Server role owns their process supervision.
#[derive(Clone)]
pub struct FrontendServingConfig {
    pub report_bind_host: String,
    pub report_grpc_port: u16,
    /// Maximum time admitted FE workload leases may continue after drain starts.
    pub frontend_drain_timeout: Duration,
    /// Upper bound for terminal resource cleanup after graceful/deadline drain.
    pub frontend_cleanup_timeout: Duration,
    pub mysql_listener: ResolvedMysqlListenerSettings,
    pub native_trust: Arc<NativeTrust>,
    pub native_transport: FrontendNativeTransport,
}

/// Immutable Frontend role products constructed before SQL session assembly.
///
/// The Server composition creates this graph after the Native report endpoint
/// has a concrete port and before it opens MySQL admission.  Query-session
/// assembly consumes these products; it does not start maintenance or MV
/// workers as a side effect of creating a client-facing factory.
struct FrontendRoleProducts {
    /// The complete catalog lifecycle moves here only after all fallible
    /// product construction has succeeded, so Host retains it for startup
    /// rollback and role products retain it for serving shutdown.
    catalog_runtime: FrontendCatalogRoleRuntime,
    catalog_service: Arc<crate::catalog_application::query_catalog::QueryCatalogService>,
    unified_statistics: Arc<crate::connector::UnifiedStatisticsResolver>,
    catalog_application: Arc<dyn novarocks_catalog_application::CatalogApplicationPort>,
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    connector_control: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    query_control: novarocks_query_application::session_control::QueryControlService,
    query_execution: crate::query_execution::service::QueryExecutionService,
    logical_read_launcher: Arc<dyn crate::query_execution::logical_read::LogicalReadLauncher>,
    topology: novarocks_query_application::api::BackendTopologyService,
    role: novarocks_types::ClusterRole,
    mv_repository: Arc<dyn novarocks_mv_application::repository::MvRepository>,
    view_service: Arc<dyn novarocks_query_application::view::ViewService>,
    dml_service: Arc<crate::dml::DmlService>,
    statistics_application: Arc<crate::statistics_jobs::service::FrontendStatisticsApplicationPort>,
    maintenance_service: Arc<dyn crate::query_execution::maintenance::TableMaintenanceService>,
    maintenance_engine: Arc<dyn crate::query_execution::maintenance::TableMaintenanceEngine>,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    mv_candidate_reader: crate::mv::domain::readiness::MvCandidateReader,
    /// The role graph owns the sole MV process product. The frontend adapter
    /// receives only a clone of this exact product for port translation.
    mv_product_service: Arc<MvProductService>,
    mv_service: Arc<crate::mv::FrontendMvProductAdapter>,
    maintenance_ports: core_capabilities::MaintenanceCommandPorts,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    exchange_port: u16,
}

impl FrontendRoleProducts {
    /// Starts the two product-owned background domains only after the complete
    /// immutable role graph exists. SQL/session assembly never starts either
    /// worker as a side effect.
    fn start_background_workers(&self) -> Result<(), FrontendApplicationError> {
        self.maintenance_service
            .start(Arc::clone(&self.maintenance_engine))
            .map_err(|error| {
                FrontendApplicationError::server(format!(
                    "start table maintenance service failed: {error}"
                ))
            })?;
        self.mv_service
            .start_background_workers(core_capabilities::mv_background_bindings(
                core_capabilities::MvBackgroundPorts::new(
                    Arc::clone(&self.function_catalog),
                    Arc::clone(&self.catalog_service),
                    Some(Arc::clone(&self.catalog_application)),
                    Arc::clone(&self.connector_control),
                    Arc::clone(&self.mv_repository),
                    Arc::clone(&self.mv_readiness),
                    Arc::clone(&self.mv_storage_observation),
                ),
                Arc::clone(&self.maintenance_engine),
            ))
            .map_err(|error| {
                FrontendApplicationError::server(format!(
                    "start frontend MV background workers failed: {error}"
                ))
            })
    }

    async fn shutdown_background_workers_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), FrontendApplicationError> {
        let mut first_error = None;
        self.mv_product_service.begin_stopping();
        if let Err(error) = self
            .statistics_application
            .shutdown_worker_until(deadline)
            .await
        {
            first_error = Some(FrontendApplicationError::server(format!(
                "shutdown statistics analyze worker failed: {error}"
            )));
        }
        if let Err(error) = self
            .mv_service
            .shutdown_background_workers_until(deadline)
            .await
        {
            first_error.get_or_insert_with(|| {
                FrontendApplicationError::server(format!(
                    "shutdown frontend MV background workers failed: {error}"
                ))
            });
        }
        if let Err(error) = self.maintenance_service.shutdown_until(deadline).await {
            first_error.get_or_insert_with(|| {
                FrontendApplicationError::server(format!(
                    "shutdown frontend table-maintenance service failed: {error}"
                ))
            });
        }
        if let Err(error) = self.catalog_runtime.shutdown_until(deadline).await {
            first_error.get_or_insert_with(|| {
                FrontendApplicationError::server(format!(
                    "shutdown frontend catalog role runtime failed: {error}"
                ))
            });
        }
        first_error.map_or(Ok(()), Err)
    }

    fn request_background_stop_for_process_exit(&self) {
        self.statistics_application
            .request_worker_stop_for_process_exit();
        self.mv_product_service.begin_stopping();
        self.mv_service.request_background_stop_for_process_exit();
        self.maintenance_service.request_shutdown_for_process_exit();
        self.catalog_runtime.request_stop_for_process_exit();
    }
}

/// Opens the frontend services once for an externally composed server.
pub async fn open_frontend_application_for_server(
    config: &FrontendApplicationOpenConfig,
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
async fn build_frontend_role_products(
    host: &mut FrontendApplicationHost,
    exchange_port: u16,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
) -> Result<FrontendRoleProducts, FrontendApplicationError> {
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
    let query_control =
        novarocks_query_application::query_control::QueryApplicationControl::service();
    let query_execution = host.build_query_execution_service()?;
    let logical_read_launcher = host.build_logical_read_launcher();
    let topology = host.backend_topology_port();
    let role = host.execution_role();
    let mv_repository = host.mv_repository_for_role_product_construction();
    let view_service: Arc<dyn novarocks_query_application::view::ViewService> =
        Arc::new(novarocks_query_application::view::QueryViewService::new());
    let dml_service = Arc::new(crate::dml::DmlService::new());
    let maintenance_service: Arc<dyn crate::query_execution::maintenance::TableMaintenanceService> =
        Arc::new(
            crate::table_maintenance::FrontendTableMaintenanceService::open(
                host.durable(),
                Handle::current(),
                host.workload_root_admission(),
            )
            .await
            .map_err(|error| {
                FrontendApplicationError::new(
                    crate::application::FrontendApplicationErrorKind::TableMaintenanceServiceOpen,
                    error,
                )
            })?
            .with_lake_publication_runtime_policy(host.lake_publication_runtime_policy()),
        );

    core_capabilities::bind_catalog_runtime_projection(
        catalog_projection.as_ref(),
        Arc::clone(&catalog_service),
        Arc::clone(&connector_control),
    )
    .map_err(FrontendApplicationError::server)?;

    // The role composition root owns construction of the MV process product.
    // Frontend adapters receive only its already-created readiness service.
    let mv_product_service = Arc::new(MvProductService::new_with_readiness_runtime(
        host.mv_scheduler_config(),
        Arc::clone(&mv_repository),
        Arc::new(novarocks_mv_application::process_runtime::ProcessRuntime::default()),
    ));
    let mv_readiness = Arc::new(crate::mv::domain::readiness::MvReadinessPort::from_product(
        mv_product_service
            .readiness_service()
            .expect("serving MV product must own readiness state"),
        tokio::runtime::Handle::current(),
    ));
    let mv_candidate_reader = mv_readiness.candidate_reader();
    let mv_activation = core_capabilities::mv_refresh_provider_activation(
        core_capabilities::MvRefreshProviderActivationPorts::new(
            Arc::clone(&function_catalog),
            Arc::clone(&catalog_service),
            Arc::clone(&catalog_application),
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
    );
    let mv_service = Arc::new(
        crate::mv::FrontendMvProductAdapter::with_refresh_dependencies(
            Arc::clone(&mv_readiness),
            query_execution.clone(),
            Arc::clone(&connector_control),
            mv_activation,
            role,
            topology.clone(),
            Arc::clone(&mv_product_service),
            host.mv_maintenance_config(),
            Arc::clone(&maintenance_service),
            host.optimizer_query_mem_limit_bytes(),
            host.lake_publication_runtime_policy()
                .max_attempt_duration(),
            host.workload_root_admission(),
        ),
    );
    let startup_restore = crate::mv::startup_restore::FrontendMvStartupRestore::new(
        Arc::clone(&connector_control),
        Arc::clone(&catalog_projection),
        Arc::clone(&catalog_application),
        Arc::clone(&mv_storage_observation),
        Arc::clone(&mv_readiness),
    );
    crate::mv::domain::startup_restore::run_mv_startup_restore(&startup_restore)
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
        Handle::current(),
    );
    let maintenance_engine = core_capabilities::background_maintenance_engine(
        maintenance_ports.clone(),
        Arc::new(FrontendBackgroundMaintenanceAttemptFactory {
            role,
            topology: topology.clone(),
            runtime_policy: host.lake_publication_runtime_policy(),
        }),
    );
    let statistics_connector_control = Arc::clone(&connector_control)
        as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>;
    let statistics_application = Arc::new(
        crate::statistics_jobs::service::FrontendStatisticsApplicationPort::new(
            novarocks_statistics_application::StatisticsJobService::new(),
            Arc::new(
                crate::statistics_jobs::application::ConnectorStatisticsTargetResolver::new(
                    Arc::clone(&statistics_connector_control),
                ),
            ),
            Arc::new(
                crate::statistics_jobs::service::RootAdmissionStatisticsJobSource::new(
                    host.workload_root_admission(),
                ),
            ),
            crate::statistics_jobs::service::table_statistics_reader_for_role(Arc::clone(
                &statistics_connector_control,
            )),
            core_capabilities::statistics_three_phase_attempt_executor(
                core_capabilities::StatisticsAttemptExecutorPorts::new(
                    role,
                    statistics_connector_control,
                    Arc::clone(&typed_connector_control),
                    topology.clone(),
                    query_execution.clone(),
                    Arc::clone(&function_catalog),
                    host.lake_publication_runtime_policy()
                        .max_attempt_duration(),
                ),
            ),
            Handle::current(),
        ),
    );
    // No fallible product construction follows these transfers. Keeping them
    // at the tail means an earlier failure still reaches Host's exact reverse
    // cleanup path, while a serving role owns the catalog lifecycle and its
    // durable MV repository.
    let catalog_runtime = host.take_catalog_role_runtime()?;
    let mv_repository = host.take_mv_repository()?;
    Ok(FrontendRoleProducts {
        catalog_runtime,
        catalog_service,
        unified_statistics,
        catalog_application,
        function_catalog,
        connector_control,
        typed_connector_control,
        query_control,
        query_execution,
        logical_read_launcher,
        topology,
        role,
        mv_repository,
        view_service,
        dml_service,
        statistics_application,
        maintenance_service,
        maintenance_engine,
        mv_readiness,
        mv_candidate_reader,
        mv_product_service,
        mv_service,
        maintenance_ports,
        mv_storage_observation,
        exchange_port,
    })
}

/// Assemble SQL/session adapters from an already-started, immutable role
/// product graph. This intentionally has no worker-start or product-lifecycle
/// side effect.
fn build_frontend_query_session_factory_from_role_products(
    host: &FrontendApplicationHost,
    products: &FrontendRoleProducts,
    system_catalog: Arc<dyn novarocks_query_application::system_catalog::SystemCatalog>,
    client_connection_control: Arc<dyn ClientConnectionControlPort>,
) -> Result<Arc<dyn QuerySessionFactory>, FrontendApplicationError> {
    let catalog_service = Arc::clone(&products.catalog_service);
    let unified_statistics = Arc::clone(&products.unified_statistics);
    let catalog_application = Arc::clone(&products.catalog_application);
    let function_catalog = Arc::clone(&products.function_catalog);
    let connector_control = Arc::clone(&products.connector_control);
    let typed_connector_control = Arc::clone(&products.typed_connector_control);
    let query_execution = products.query_execution.clone();
    let topology = products.topology.clone();
    let role = products.role;
    let mv_repository = Arc::clone(&products.mv_repository);
    let view_service = Arc::clone(&products.view_service);
    let statistics_application = Arc::clone(&products.statistics_application);
    let maintenance_service = Arc::clone(&products.maintenance_service);
    let mv_readiness = Arc::clone(&products.mv_readiness);
    let mv_candidate_reader = products.mv_candidate_reader.clone();
    let mv_service = Arc::clone(&products.mv_service);
    let maintenance_ports = products.maintenance_ports.clone();
    let mv_storage_observation = Arc::clone(&products.mv_storage_observation);
    let exchange_port = products.exchange_port;

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
            mv_candidate_reader,
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
        core_capabilities::BackendCommandPorts::new(host.backend_topology_command_port()),
    );
    let view_command_executor =
        core_capabilities::view_command_executor(core_capabilities::ViewCommandPorts::new(
            Arc::clone(&function_catalog),
            Arc::clone(&catalog_service),
            Some(Arc::clone(&catalog_application)),
            Arc::clone(&connector_control),
            Arc::clone(&products.view_service),
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
            mv_service,
            Arc::clone(&mv_storage_observation),
            query_execution.clone(),
        ));
    let mv_command_consumer: Arc<
        dyn novarocks_query_application::api::MaterializedViewCommandConsumer,
    > = Arc::new(crate::mv::command::FrontendMvCommandConsumer::new(
        mv_command_executor.clone(),
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
    let query_service = Arc::new(crate::query::FrontendQueryService::new(
        session_catalog_resolver,
        query_compiler,
        catalog_command_executor,
        statistics_command_executor,
        backend_command_executor,
        view_command_executor,
        iceberg_ref_command_executor,
        mv_command_consumer,
        mv_command_executor,
        maintenance_command_executor,
        maintenance_read_command_executor,
        products.query_control.clone(),
        client_connection_control,
        query_execution,
        Arc::clone(&products.logical_read_launcher),
        host.workload_root_admission(),
        host.workload_resources(),
        role,
        topology,
        Arc::clone(&products.dml_service),
        dml_engines.insert,
        dml_engines.delete,
        dml_engines.mutation,
        dml_engines.add_files,
        dml_engines.ctas,
        dml_engines.truncate,
        host.query_cpu_executor(),
        host.query_blocking_executor(),
        host.connector_blocking_io_supervisor(),
        host.optimizer_query_mem_limit_bytes(),
        host.lake_publication_runtime_policy(),
        host.serving_lifecycle().admission(),
    ));
    host.mark_ready()?;
    Ok(query_service)
}

#[cfg(test)]
async fn build_frontend_query_session_factory(
    host: &mut FrontendApplicationHost,
    system_catalog: Arc<dyn novarocks_query_application::system_catalog::SystemCatalog>,
    exchange_port: u16,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    client_connection_control: Arc<dyn ClientConnectionControlPort>,
) -> Result<(Arc<dyn QuerySessionFactory>, FrontendRoleProducts), FrontendApplicationError> {
    let products =
        build_frontend_role_products(host, exchange_port, mv_storage_observation).await?;
    products.start_background_workers()?;
    let session_factory = build_frontend_query_session_factory_from_role_products(
        host,
        &products,
        system_catalog,
        client_connection_control,
    )?;
    Ok((session_factory, products))
}

/// Drives the product-owned runtimes to convergence before the Host releases
/// the coordinator, topology, or StateStore they reach through immutable ports.
async fn shutdown_frontend_role_products_to_convergence(
    products: &mut FrontendRoleProducts,
    attempt_timeout: Duration,
) -> Result<(), FrontendApplicationError> {
    let first_error = match products
        .shutdown_background_workers_until(Instant::now() + attempt_timeout)
        .await
    {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    tracing::warn!(
        error = %first_error,
        ?attempt_timeout,
        "frontend role-product shutdown did not converge; retaining the exact MV owner for one final pass"
    );
    match products
        .shutdown_background_workers_until(Instant::now() + attempt_timeout)
        .await
    {
        Ok(()) => Err(first_error),
        Err(final_error) => {
            tracing::error!(
                error = %final_error,
                ?attempt_timeout,
                "frontend role products remained unconverged; committing the runner to process exit"
            );
            products.request_background_stop_for_process_exit();
            Err(first_error.with_cleanup_context(final_error))
        }
    }
}

/// Drives the exact Frontend owner graph to convergence before the production
/// runner is allowed to drop it.
///
/// `frontend_cleanup_timeout` bounds each of two graceful convergence passes.
/// The first error remains visible to the caller. If the same owner graph still
/// cannot converge on the second pass, the runner explicitly commits to
/// process exit and releases process-local joins through the Host's final-exit
/// boundary. That boundary is never available to a reusable application Host.
pub async fn shutdown_frontend_application_to_convergence(
    host: &mut FrontendApplicationHost,
    attempt_timeout: Duration,
) -> Result<(), FrontendApplicationError> {
    let first_error = match host
        .shutdown_until(std::time::Instant::now() + attempt_timeout)
        .await
    {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    tracing::warn!(
        error = %first_error,
        ?attempt_timeout,
        "frontend bounded shutdown did not converge; retaining the exact Host owner graph for one final pass"
    );

    match host
        .shutdown_until(std::time::Instant::now() + attempt_timeout)
        .await
    {
        Ok(()) => Err(first_error),
        Err(final_error) => {
            tracing::error!(
                error = %final_error,
                ?attempt_timeout,
                "frontend Host remained unconverged; committing the runner to process exit"
            );
            host.abandon_for_process_exit();
            Err(first_error.with_cleanup_context(final_error))
        }
    }
}

/// Frontend-owned management listener construction. The Server role runner
/// owns when this listener starts, receives failures, and is torn down.
pub struct FrontendManagementServer {
    serving_reader: Arc<LateBoundFrontendServingSnapshotReader>,
    island_reader: Arc<crate::topology::LateBoundBackendIslandSnapshotReader>,
    convergence_reader: Arc<crate::metrics::LateBoundQueryLifecycleConvergenceReader>,
    metrics_http_server: crate::metrics::MetricsHttpServer,
}

pub fn start_frontend_management_server(
    config: &FrontendManagementConfig,
) -> Result<FrontendManagementServer, FrontendApplicationError> {
    let metrics_registry =
        crate::metrics::FrontendMetricsRegistry::new().map_err(FrontendApplicationError::server)?;
    let serving_reader = Arc::new(LateBoundFrontendServingSnapshotReader::default());
    let island_reader = Arc::new(crate::topology::LateBoundBackendIslandSnapshotReader::new(
        config.native_compatibility_id,
    ));
    let convergence_reader =
        Arc::new(crate::metrics::LateBoundQueryLifecycleConvergenceReader::default());
    let management_reader: Arc<dyn FrontendServingSnapshotReader> = serving_reader.clone();
    let management_island_reader: Arc<dyn crate::topology::BackendIslandSnapshotReader> =
        island_reader.clone();
    let management_convergence_reader: Arc<
        dyn crate::query_execution::lifecycle_diagnostics::QueryLifecycleConvergenceReader,
    > = convergence_reader.clone();
    let metrics_http_server = crate::metrics::MetricsHttpServer::start(
        &config.bind_host,
        config.http_port,
        Arc::clone(&metrics_registry),
        management_reader,
        management_island_reader,
        Some(management_convergence_reader),
        Arc::clone(&config.memory_authority),
    )
    .map_err(FrontendApplicationError::server)?;
    Ok(FrontendManagementServer {
        serving_reader,
        island_reader,
        convergence_reader,
        metrics_http_server,
    })
}

impl FrontendManagementServer {
    pub fn install(&self, host: &FrontendApplicationHost) -> Result<(), FrontendApplicationError> {
        self.serving_reader
            .install(host.serving_snapshot_reader())
            .map_err(|error| {
                FrontendApplicationError::server(format!(
                    "install frontend serving reader after application open: {error}"
                ))
            })?;
        self.island_reader
            .install(host.backend_island_snapshot_reader())
            .map_err(|error| {
                FrontendApplicationError::server(format!(
                    "install frontend island reader after application open: {error}"
                ))
            })?;
        self.convergence_reader
            .install(host.lifecycle_convergence_reader())
            .map_err(|error| {
                FrontendApplicationError::server(format!(
                    "install frontend lifecycle convergence reader after application open: {error}"
                ))
            })
    }

    pub fn poll_failure(&mut self) -> Result<Option<String>, FrontendApplicationError> {
        self.metrics_http_server
            .poll_failure()
            .map_err(FrontendApplicationError::server)
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.metrics_http_server.stop()
    }
}

pub async fn serve_ready_frontend_session_factory<F>(
    config: FrontendServingConfig,
    host: &mut FrontendApplicationHost,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    shutdown: F,
    management_server: &mut FrontendManagementServer,
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
    let system_catalog: Arc<dyn novarocks_query_application::system_catalog::SystemCatalog> =
        Arc::new(
            novarocks_query_application::system_catalog::SystemCatalogService::with_defaults(),
        );
    let client_connections = Arc::new(MysqlClientConnectionRegistry::new());
    let client_connection_control: Arc<dyn ClientConnectionControlPort> =
        client_connections.clone();
    let mut products =
        match build_frontend_role_products(host, exchange_port, mv_storage_observation).await {
            Ok(products) => products,
            Err(error) => {
                let stop_result = report_server
                    .stop()
                    .map_err(FrontendApplicationError::server);
                return combine_server_and_shutdown(Err(error), stop_result);
            }
        };
    if let Err(error) = products.start_background_workers() {
        let product_shutdown = shutdown_frontend_role_products_to_convergence(
            &mut products,
            config.frontend_cleanup_timeout,
        )
        .await;
        let stop_result = report_server
            .stop()
            .map_err(FrontendApplicationError::server);
        return combine_server_and_shutdown(
            combine_server_and_shutdown(Err(error), product_shutdown),
            stop_result,
        );
    }
    let session_factory = match build_frontend_query_session_factory_from_role_products(
        host,
        &products,
        system_catalog,
        client_connection_control,
    ) {
        Ok(factory) => factory,
        Err(error) => {
            let product_shutdown = shutdown_frontend_role_products_to_convergence(
                &mut products,
                config.frontend_cleanup_timeout,
            )
            .await;
            let stop_result = report_server
                .stop()
                .map_err(FrontendApplicationError::server);
            return combine_server_and_shutdown(
                combine_server_and_shutdown(Err(error), product_shutdown),
                stop_result,
            );
        }
    };
    let server_result = run_mysql_with_listener_supervision(
        config.mysql_listener,
        session_factory,
        client_connections,
        shutdown,
        &mut report_server,
        management_server,
        host,
        config.frontend_drain_timeout,
        config.frontend_cleanup_timeout,
    )
    .await;
    let product_shutdown = shutdown_frontend_role_products_to_convergence(
        &mut products,
        config.frontend_cleanup_timeout,
    )
    .await;
    let stop_result = report_server
        .stop()
        .map_err(FrontendApplicationError::server);
    combine_server_and_shutdown(
        combine_server_and_shutdown(server_result, product_shutdown),
        stop_result,
    )
}

async fn run_mysql_with_listener_supervision<F>(
    mysql_listener: ResolvedMysqlListenerSettings,
    session_factory: Arc<dyn QuerySessionFactory>,
    client_connections: Arc<MysqlClientConnectionRegistry>,
    shutdown: F,
    report_server: &mut crate::native::report_server::FrontendReportServerHandle,
    management_server: &mut FrontendManagementServer,
    host: &FrontendApplicationHost,
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
    let ready_user = mysql_listener.user().to_string();
    let mysql_server =
        novarocks_mysql_adapter::serve_query_application_mysql_until_drain_then_shutdown(
            mysql_listener,
            version::short_version().to_string(),
            Arc::clone(&session_factory),
            Arc::clone(&client_connections),
            wait_for_signal(drain_rx),
            wait_for_signal(finalize_rx),
            cleanup_timeout,
            move |bound_addr| emit_frontend_mysql_ready(bound_addr, &ready_user),
        );
    tokio::pin!(mysql_server);

    tokio::select! {
        result = &mut mysql_server => result.map_err(FrontendApplicationError::server),
        _ = shutdown => {
            host.begin_serving_drain(drain_timeout);
            let _ = drain_tx.send(true);
            let graceful = tokio::time::timeout(
                drain_timeout,
                host.workload_observation().wait_until_no_root_responsibilities(),
            )
            .await;
            if graceful.is_err() {
                host.cancel_governed_work_at_drain_deadline();
                // Keep the admitted protocol tasks alive long enough to
                // observe the first-wins deadline cancellation and return
                // its typed error. Final connection termination remains the
                // fallback when a cancelled attempt does not converge inside
                // the configured bounded cleanup window.
                let _ = tokio::time::timeout(
                    cleanup_timeout,
                    host.workload_observation().wait_until_no_root_responsibilities(),
                )
                .await;
            }
            session_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            client_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
            host.serving_lifecycle().mark_stopping();
            let _ = finalize_tx.send(true);
            mysql_server.await.map_err(FrontendApplicationError::server)
        }
        error = wait_for_frontend_listener_failure(report_server, management_server) => {
            host.begin_serving_drain(drain_timeout);
            let _ = drain_tx.send(true);
            host.cancel_governed_work_at_drain_deadline();
            session_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            client_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
            host.serving_lifecycle().mark_stopping();
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

fn emit_frontend_mysql_ready(bind_addr: std::net::SocketAddr, user: &str) {
    info!(
        "standalone mysql server listening on {} (user={}, db={})",
        bind_addr, user, DEFAULT_DATABASE
    );
    // Emit a parser-friendly readiness marker on stdout. Orchestration
    // scripts must wait for this exact line before connecting; probing the
    // mysql port alone cannot distinguish a freshly-bound server from a
    // pre-existing process that already owned the port.
    println!(
        "NOVAROCKS_READY mysql_port={} pid={}",
        bind_addr.port(),
        std::process::id()
    );
}

async fn wait_for_frontend_listener_failure(
    report_server: &mut crate::native::report_server::FrontendReportServerHandle,
    management_server: &mut FrontendManagementServer,
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
#[derive(Clone)]
struct FrontendTestServerConfig {
    state_store_input: Option<StateStoreHostInput>,
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
    config: FrontendTestServerConfig,
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
    Serve: FnOnce(FrontendTestServerConfig, Service, F) -> ServeFuture,
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
    config: FrontendTestServerConfig,
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
    Serve: FnOnce(FrontendTestServerConfig, Service, ShutdownSignal) -> ServeFuture,
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

#[cfg(test)]
async fn run_server_until_signal<S, E, Service, Serve, ServeFuture>(
    config: FrontendTestServerConfig,
    service: Service,
    signal: S,
    serve: Serve,
) -> Result<(), FrontendApplicationError>
where
    S: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
    Serve: FnOnce(FrontendTestServerConfig, Service, ShutdownSignal) -> ServeFuture,
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
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    use novarocks_query_application::client_connection::ClientConnectionToken;
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::UnavailableMvStorageObservationPort;
    use novarocks_workload_control::{WorkClass, WorkRequest};

    use super::{
        FrontendTestServerConfig, build_frontend_query_session_factory,
        run_frontend_server_until_shutdown_with_ports, run_frontend_server_with_signal_and_ports,
        shutdown_frontend_application_to_convergence,
        shutdown_frontend_role_products_to_convergence,
    };
    use crate::state_store::testing::{
        input as test_state_store_input, registry as test_state_store_registry,
    };
    use crate::{
        application::{
            FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
            FrontendExecutionConfig,
        },
        topology::ClusterBackendOpenConfig,
    };
    use novarocks_catalog_application::{CatalogAdmission, CatalogDesiredStateSourceInput};
    use novarocks_mysql_adapter::MysqlClientConnectionRegistry;
    use novarocks_native_adapter::FrontendNativeTransport;
    use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
    use novarocks_query_application::session::{QuerySessionOpenRequest, QuerySessionStatement};
    use novarocks_query_application::session_error::QueryServiceErrorKind;

    fn settle_governed_completion(statement: QuerySessionStatement) {
        let (result, terminal) = statement.into_parts();
        let StatementResult::GovernedCompletion(result) = result else {
            panic!("successful statement must retain its owner through terminal OK");
        };
        let mut protocol = result.into_protocol();
        let _ = protocol.seal_success_visibility();
        let _ = protocol.complete();
        terminal.complete();
    }

    fn settle_governed_query(statement: QuerySessionStatement) {
        let (result, terminal) = statement.into_parts();
        let StatementResult::GovernedQuery(result) = result else {
            panic!("query result must retain its owner through terminal EOF");
        };
        let (_result, mut protocol) = result.into_parts();
        let _ = protocol.seal_success_visibility();
        let _ = protocol.complete();
        terminal.complete();
    }

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

    fn frontend_config() -> FrontendTestServerConfig {
        FrontendTestServerConfig {
            state_store_input: None,
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
                let control =
                    novarocks_catalog_application::test_support::test_control_binding_for(
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
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
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
        let attachments = novarocks_catalog_application::CatalogAttachmentRepository::open(
            Arc::clone(&store),
            host.run_policy(),
        )
        .await
        .expect("open catalog attachment repository");

        let (session_factory, mut products) = build_frontend_query_session_factory(
            &mut host,
            Arc::new(
                novarocks_query_application::system_catalog::SystemCatalogService::with_defaults(),
            ),
            0,
            Arc::new(UnavailableMvStorageObservationPort),
            Arc::new(MysqlClientConnectionRegistry::new()),
        )
        .await
        .expect("build ready frontend session factory");
        let session = session_factory
            .open_session(QuerySessionOpenRequest::new(
                ClientConnectionToken::new(1, 1).expect("valid connection token"),
                "cp2-cutover",
            ))
            .expect("open frontend query session");
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse("warehouse").expect("instance ID");

        let result = session
            .execute_batch(r#"CREATE EXTERNAL CATALOG warehouse PROPERTIES("type"="iceberg")"#)
            .await
            .expect("CREATE CATALOG commits a durable StateStore attachment");
        settle_governed_completion(result);
        let created = attachments
            .get(&instance_id)
            .await
            .expect("read attachment")
            .expect("CREATE CATALOG must commit to the StateStore attachment keyspace");
        assert_eq!(created.attachment.provider_id.as_str(), "iceberg");
        assert_eq!(created.attachment.display_name, "warehouse");
        assert!(matches!(
            products.catalog_application.admit_catalog(&instance_id),
            CatalogAdmission::Ready(_)
        ));
        let result = session
            .execute_batch("SET CATALOG warehouse")
            .await
            .expect("the committed attachment is admitted by this frontend session");
        settle_governed_completion(result);

        let result = session
            .execute_batch("DROP CATALOG warehouse")
            .await
            .expect("DROP CATALOG deletes the durable StateStore attachment");
        settle_governed_completion(result);
        assert!(
            attachments
                .get(&instance_id)
                .await
                .expect("read attachment")
                .is_none(),
            "DROP CATALOG must remove the durable attachment"
        );
        assert!(matches!(
            products.catalog_application.admit_catalog(&instance_id),
            CatalogAdmission::Absent
        ));
        let result = session
            .execute_batch("SET CATALOG warehouse")
            .await
            .expect("admission error is retained until the protocol terminal error");
        let (result, terminal) = result.into_parts();
        let StatementResult::GovernedError(result) = result else {
            panic!("a dropped catalog must produce a governed terminal error");
        };
        let (error, mut protocol) = result.into_parts();
        assert_eq!(error.kind(), QueryServiceErrorKind::BadDatabase);
        let _ = protocol.fail();
        terminal.complete();

        // The ready session factory and this test's probe both hold StateStore references; the
        // host owns closing the deployment lock, so release them first.
        drop(attachments);
        drop(store);
        session.close();
        drop(session);
        drop(session_factory);
        shutdown_frontend_role_products_to_convergence(&mut products, Duration::from_secs(1))
            .await
            .expect("shutdown frontend role products");
        host.shutdown().await.expect("host shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frontend_report_endpoint_binds_loopback_without_core_transport_facade() {
        let state_store = test_state_store_input("frontend-report-listener");
        let registry = test_state_store_registry();
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
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
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
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
        let (session_factory, mut products) = build_frontend_query_session_factory(
            &mut host,
            Arc::new(
                novarocks_query_application::system_catalog::SystemCatalogService::with_defaults(),
            ),
            0,
            Arc::new(UnavailableMvStorageObservationPort),
            Arc::new(MysqlClientConnectionRegistry::new()),
        )
        .await
        .expect("build ready frontend session factory");
        let session = session_factory
            .open_session(QuerySessionOpenRequest::new(
                ClientConnectionToken::new(2, 1).expect("valid connection token"),
                "statistics-binding",
            ))
            .expect("open frontend query session");
        let result = session
            .execute_batch("SHOW ANALYZE JOBS")
            .await
            .expect("configured Frontend statistics application port handles SHOW ANALYZE JOBS");
        settle_governed_query(result);

        session.close();
        drop(session);
        drop(session_factory);
        shutdown_frontend_role_products_to_convergence(&mut products, Duration::from_secs(1))
            .await
            .expect("shutdown frontend role products");
        host.shutdown()
            .await
            .expect("shutdown frontend application host");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn role_products_inject_their_unique_mv_product_into_the_frontend_adapter() {
        let state_store = test_state_store_input("mv-product-role-composition");
        let registry = test_state_store_registry();
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
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
        let (session_factory, mut products) = build_frontend_query_session_factory(
            &mut host,
            Arc::new(
                novarocks_query_application::system_catalog::SystemCatalogService::with_defaults(),
            ),
            0,
            Arc::new(UnavailableMvStorageObservationPort),
            Arc::new(MysqlClientConnectionRegistry::new()),
        )
        .await
        .expect("build ready frontend session factory");

        assert!(std::ptr::eq(
            products.mv_product_service.as_ref(),
            products.mv_service.product_service(),
        ));
        assert!(
            products.mv_product_service.readiness_service().is_some(),
            "serving role product must retain the readiness state injected into its adapters"
        );

        drop(session_factory);
        shutdown_frontend_role_products_to_convergence(&mut products, Duration::from_secs(1))
            .await
            .expect("shutdown frontend role products");
        host.shutdown()
            .await
            .expect("shutdown frontend application host");
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn production_shutdown_has_a_finite_process_exit_boundary() {
        let registry = test_state_store_registry();
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(test_state_store_input("server-bounded-shutdown-owner")),
            &registry,
            FrontendExecutionConfig::new_for_test(
                "127.0.0.1",
                0,
                NonZeroUsize::new(1).unwrap(),
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
        host.mark_ready().expect("mark frontend host ready");
        let work = host
            .workload_root_admission()
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit one governed root");
        let started = Instant::now();
        let shutdown_error = tokio::time::timeout(
            Duration::from_millis(100),
            shutdown_frontend_application_to_convergence(&mut host, Duration::from_millis(1)),
        )
        .await
        .expect("production shutdown must reach its finite process-exit boundary")
        .expect_err("the first bounded deadline remains observable");
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(
            shutdown_error.kind(),
            FrontendApplicationErrorKind::Shutdown
        );
        assert!(
            shutdown_error
                .to_string()
                .contains("shutdown deadline exceeded")
        );
        drop(work);
        // The explicit process-exit boundary makes every fail-closed local
        // owner drop-safe without waiting forever for the held responsibility.
        drop(host);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serving_drain_refuses_new_governed_roots() {
        let registry = test_state_store_registry();
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(test_state_store_input("server-drain-closes-root-admission")),
            &registry,
            FrontendExecutionConfig::new_for_test(
                "127.0.0.1",
                0,
                NonZeroUsize::new(1).unwrap(),
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
        host.mark_ready().expect("mark frontend host ready");

        let active = host
            .workload_root_admission()
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit one root before drain");
        host.begin_serving_drain(Duration::from_secs(1));
        assert!(
            host.workload_root_admission()
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .is_err(),
            "serving drain must close the one governed root-admission authority"
        );
        drop(active.business);
        active.owner.complete();

        host.shutdown().await.expect("shutdown drained host");
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
