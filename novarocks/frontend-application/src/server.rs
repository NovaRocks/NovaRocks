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

#[cfg(feature = "mem-1-m07-closing-pressure")]
mod closing_pressure_fixture;
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
mod exact_mysql_write_fixture;
#[cfg(feature = "mem-1-m07-root-observation")]
mod root_observation_identity;

use crate::capabilities as core_capabilities;
use crate::workload_lifecycle::{
    FrontendServingSnapshotReader, LateBoundFrontendServingSnapshotReader,
};
use novarocks_mv_application::service::MvKeyedProductService;
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
    runtime: Handle,
}

impl BackgroundMaintenanceAttemptFactory for FrontendBackgroundMaintenanceAttemptFactory {
    fn begin_original_root_maintenance_attempt(
        &self,
        owner: &novarocks_workload_control::WorkOwner,
    ) -> Result<BackgroundMaintenanceAttempt, String> {
        core_capabilities::original_background_maintenance_attempt(
            self.role,
            self.topology.clone(),
            self.runtime_policy.max_attempt_duration(),
            &self.runtime,
            owner,
        )
    }

    fn begin_automatic_maintenance_attempt(&self) -> Result<BackgroundMaintenanceAttempt, String> {
        core_capabilities::background_maintenance_attempt(
            self.role,
            self.topology.clone(),
            self.runtime_policy.max_attempt_duration(),
            &self.runtime,
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

#[cfg(feature = "mem-1-m07-hms-listing-observe")]
#[path = "server/hms_admission_observation.rs"]
pub(crate) mod hms_admission_observation;
#[cfg(feature = "mem-1-m07-hms-listing-observe")]
pub use hms_admission_observation::HmsListingObservationSetup;

/// Feature-only observer supplied by Server; it owns no provider capability.
#[cfg(feature = "mem-1-m07-hms-listing-observe")]
pub type HmsListingObservationHandler =
    Arc<dyn Fn(&[u8]) -> Result<Vec<u8>, &'static str> + Send + Sync>;

/// Inputs for the Frontend-owned management listener.
#[derive(Clone)]
pub struct FrontendManagementConfig {
    pub bind_host: String,
    pub http_port: u16,
    pub native_compatibility_id: novarocks_types::NativeCompatibilityId,
    /// The one memory capacity authority this OS process was given, so the
    /// management surface can report its facts without owning any of them.
    pub memory_authority: Arc<novarocks_memory::MemoryAuthority>,
    /// This process's allocator and physical memory readings for `/metrics`;
    /// `None` exports no process memory series.
    pub process_memory: Option<crate::metrics::FrontendProcessMemoryObservation>,
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub hms_listing_observation: Option<HmsListingObservationSetup>,
}

/// Inputs for serving one ready Frontend application through native and MySQL
/// listeners. The Server role owns their process supervision.
#[derive(Clone)]
pub struct FrontendServingConfig {
    /// Executable-frozen version projected to the MySQL greeting.
    pub server_version: String,
    pub report_bind_host: String,
    pub report_grpc_port: u16,
    /// Maximum time admitted FE workload leases may continue after drain starts.
    pub frontend_drain_timeout: Duration,
    /// Upper bound for terminal resource cleanup after graceful/deadline drain.
    pub frontend_cleanup_timeout: Duration,
    pub mysql_listener: ResolvedMysqlListenerSettings,
    pub trino_listener: novarocks_trino_adapter::TrinoListenerSettings,
    pub native_trust: Arc<NativeTrust>,
    pub native_transport: FrontendNativeTransport,
    pub native_ingress: novarocks_native_adapter::native_server::NativeIngressConfig,
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
    constant_policy: novarocks_functions::ConstantPolicy,
    connector_control: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    query_control: novarocks_query_application::session_control::QueryControlService,
    logical_execution_launcher:
        Arc<dyn crate::query_execution::logical_execution::LogicalExecutionLauncher>,
    mv_execution_services: crate::query::mv_action::MvExecutionServices,
    topology: novarocks_query_application::api::BackendTopologyService,
    role: novarocks_types::ClusterRole,
    view_service: Arc<dyn novarocks_query_application::view::ViewService>,
    dml_service: Arc<crate::dml::DmlService>,
    statistics_application: Arc<crate::statistics_jobs::service::FrontendStatisticsApplicationPort>,
    maintenance_service: Arc<dyn crate::query_execution::maintenance::TableMaintenanceService>,
    maintenance_engine: Arc<dyn crate::query_execution::maintenance::TableMaintenanceEngine>,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    mv_candidate_reader: crate::mv::domain::readiness::MvCandidateReader,
    /// The role graph owns the sole MV process product.
    mv_product_service: Arc<MvKeyedProductService>,
    mv_service: Arc<crate::mv::FrontendMvProductAdapter>,
    maintenance_ports: core_capabilities::MaintenanceCommandPorts,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    /// Retained because the catalog publication set observes it weakly: this
    /// is the reference that decides how long MV rediscovery reacts to newly
    /// admitted catalogs, and it ends with the role graph.
    _mv_catalog_admission: Arc<dyn crate::catalog_application::CatalogAdmissionObserver>,
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
                    Arc::clone(&self.mv_readiness),
                    Arc::clone(&self.mv_storage_observation),
                    self.mv_product_service
                        .management_entrance()
                        .expect("serving MV product owns document-management authority"),
                    self.constant_policy,
                ),
                self.mv_execution_services.clone(),
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

/// Synchronously installs the original process owner before borrowed bootstrap.
pub fn prepare_frontend_application_for_server(
    config: &FrontendApplicationOpenConfig,
    data_runtime: Handle,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    FrontendApplicationHost::prepare_with_role_factories_and_state_store_registry(
        config.state_store_input.clone(),
        &config.state_store_provider_registry,
        config.execution.clone(),
        config.backend_open.clone(),
        config.connector_control_role_factories.clone(),
        data_runtime,
        Arc::clone(&config.native_trust),
        config.native_transport.clone(),
    )
}

/// Convenience borrower; an error leaves the same Host in receiving.
pub async fn open_frontend_application_for_server(
    receiving: &mut Option<FrontendApplicationHost>,
    config: &FrontendApplicationOpenConfig,
    data_runtime: Handle,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    if receiving.is_some() {
        return Err(FrontendApplicationError::server(
            "frontend Host receiving slot is occupied",
        ));
    }
    *receiving = Some(prepare_frontend_application_for_server(
        config,
        data_runtime,
    )?);
    receiving.as_mut().unwrap().open_prepared().await?;
    Ok(receiving.take().expect("normal original Host handoff"))
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
    let query_application_control = Arc::new(
        novarocks_query_application::query_control::QueryApplicationControl::new(
            host.frontend_process_id(),
        ),
    );
    let query_control = novarocks_query_application::session_control::QueryControlService::new(
        query_application_control.clone(),
    );
    host.install_statement_control(query_control.clone())?;
    #[cfg(novarocks_dev_controls)]
    if let Some(owner) = host
        .test_session
        .as_ref()
        .and_then(|session| session.statement.as_ref())
    {
        // Retain the same actual session owner used by QueryControlService.
        // Each arm snapshots its token once; replacement never refreshes it.
        owner
            .install_session_resolver(Arc::new(move |connection_id| {
                query_application_control.current_session_for_test_controls(connection_id)
            }))
            .map_err(FrontendApplicationError::server)?;
    }
    let logical_execution_launcher = host.build_logical_execution_launcher();
    let topology = host.backend_topology_port();
    let role = host.execution_role();
    let mv_repository = host.take_mv_repository_for_role_product_construction()?;
    let view_service: Arc<dyn novarocks_query_application::view::ViewService> =
        Arc::new(novarocks_query_application::view::QueryViewService::new());
    let dml_service = Arc::new(crate::dml::DmlService::new());
    let maintenance_service = crate::table_maintenance::FrontendTableMaintenanceService::open(
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
    .with_lake_publication_runtime_policy(host.lake_publication_runtime_policy());
    #[cfg(novarocks_dev_controls)]
    let maintenance_service = maintenance_service.with_test_controls(
        host.test_session
            .as_ref()
            .and_then(|session| session.maintenance.clone()),
    );
    let maintenance_service: Arc<dyn crate::query_execution::maintenance::TableMaintenanceService> =
        Arc::new(maintenance_service);

    core_capabilities::bind_catalog_runtime_projection(
        catalog_projection.as_ref(),
        Arc::clone(&catalog_service),
        Arc::clone(&connector_control),
    )
    .map_err(FrontendApplicationError::server)?;

    // The role composition root owns construction of the MV process product.
    // Frontend adapters receive only its already-created readiness service.
    let mv_management_entrance = Arc::new(
        novarocks_mv_application::management::ManagementEntrance::new(
            novarocks_mv_application::management::DeploymentOwner::parse(
                host.native_deployment_id(),
            )
            .map_err(FrontendApplicationError::server)?,
            novarocks_mv_application::management::ProcessIncarnation::parse(
                uuid::Uuid::now_v7().to_string(),
            )
            .expect("UUIDv7 process incarnation is a valid management identity"),
        ),
    );
    let mv_product_service = Arc::new(
        MvKeyedProductService::new_with_keyed_management_continuation(
            host.mv_scheduler_config(),
            mv_repository,
            Arc::new(novarocks_mv_application::process_runtime::ProcessRuntime::default()),
            mv_management_entrance,
            host.mv_remote_effect_policy(),
        ),
    );
    let mv_readiness = crate::mv::domain::readiness::MvReadinessPort::from_product(
        mv_product_service
            .keyed_readiness_service()
            .expect("serving MV product must own readiness state"),
        tokio::runtime::Handle::current(),
        mv_product_service
            .management_entrance()
            .expect("serving MV product owns management entrance"),
    );
    #[cfg(novarocks_dev_controls)]
    let mv_readiness = match &host.test_session {
        Some(session) => match &session.mv {
            Some(mv) => mv_readiness.with_test_controls(mv.clone()),
            None => mv_readiness,
        },
        None => mv_readiness,
    };
    let mv_readiness = Arc::new(mv_readiness);
    let mv_candidate_reader = mv_readiness.candidate_reader();
    let mv_activation = core_capabilities::mv_refresh_provider_activation(
        core_capabilities::MvRefreshProviderActivationPorts::new(
            Arc::clone(&function_catalog),
            Arc::clone(&catalog_service),
            Arc::clone(&catalog_application),
            Arc::clone(&connector_control),
            Arc::clone(&typed_connector_control),
            Arc::clone(&unified_statistics),
            topology.clone(),
            exchange_port,
            Arc::clone(&mv_readiness),
            Arc::clone(&mv_storage_observation),
            mv_product_service
                .management_entrance()
                .expect("serving MV product owns document-management authority"),
            host.constant_policy(),
            host.static_plan_carrier(),
        ),
    );
    let mv_service = Arc::new(
        crate::mv::FrontendMvProductAdapter::with_refresh_dependencies(
            Arc::clone(&mv_readiness),
            mv_activation,
            role,
            topology.clone(),
            Arc::clone(&mv_product_service),
            host.mv_maintenance_config(),
            host.optimizer_query_mem_limit_bytes(),
            host.lake_publication_runtime_policy()
                .max_attempt_duration(),
            host.workload_root_admission(),
            host.mv_startup_isolation(),
        ),
    );
    let mv_management_entrance_for_restore = mv_product_service
        .management_entrance()
        .expect("serving MV product owns document-management authority");
    // Observe admission before sweeping what is already admitted, so a catalog
    // converging between the two is rebuilt by the observer rather than missed
    // by both. Rebuilding one twice is a re-observation, not a second effect.
    let mv_catalog_admission: Arc<dyn crate::catalog_application::CatalogAdmissionObserver> =
        Arc::new(crate::mv::startup_restore::FrontendMvCatalogAdmission::new(
            Arc::clone(&connector_control),
            Arc::clone(&catalog_application),
            Arc::clone(&mv_readiness),
            Arc::clone(&mv_management_entrance_for_restore),
        ));
    catalog_projection
        .bind_admission_observer(&mv_catalog_admission)
        .map_err(|error| FrontendApplicationError::server(error.to_string()))?;
    let startup_restore = crate::mv::startup_restore::FrontendMvStartupRestore::new(
        Arc::clone(&connector_control),
        Arc::clone(&catalog_projection),
        Arc::clone(&catalog_application),
        Arc::clone(&mv_readiness),
        mv_management_entrance_for_restore,
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
        Arc::clone(&mv_readiness),
        Arc::clone(&maintenance_service),
        Handle::current(),
        host.constant_policy(),
        host.static_plan_carrier(),
    )
    .with_state_store_original(host.state_store_original_binding())
    .with_rewrite_runtime(crate::query_execution::kernels::MaintenanceRewriteRuntime {
        client: host.logical_execution_client(),
        launcher: logical_execution_launcher.clone(),
        completion: crate::query_execution::completion_facts::CompletionFactOwners::new(
            catalog_service.clone(),
            Some(catalog_application.clone()),
            typed_connector_control.clone(),
            unified_statistics.clone(),
            mv_candidate_reader.clone(),
            mv_storage_observation.clone(),
            host.connector_blocking_io_supervisor(),
        ),
        execution: (
            host.connector_blocking_io_supervisor(),
            host.write_data_runtime(),
        ),
    });

    let maintenance_engine = core_capabilities::background_maintenance_engine(
        maintenance_ports.clone(),
        Arc::new(FrontendBackgroundMaintenanceAttemptFactory {
            role,
            topology: topology.clone(),
            runtime_policy: host.lake_publication_runtime_policy(),
            runtime: Handle::current(),
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
            core_capabilities::statistics_attempt_executor(
                core_capabilities::StatisticsAttemptExecutorPorts::new(
                    role,
                    statistics_connector_control,
                    Arc::clone(&typed_connector_control),
                    topology.clone(),
                    host.logical_execution_client(),
                    logical_execution_launcher.clone(),
                    crate::query_execution::completion_facts::CompletionFactOwners::new(
                        catalog_service.clone(),
                        Some(catalog_application.clone()),
                        typed_connector_control.clone(),
                        unified_statistics.clone(),
                        mv_candidate_reader.clone(),
                        mv_storage_observation.clone(),
                        host.connector_blocking_io_supervisor(),
                    ),
                    (
                        host.connector_blocking_io_supervisor(),
                        host.write_data_runtime(),
                    ),
                    Arc::clone(&function_catalog),
                    host.lake_publication_runtime_policy(),
                    Handle::current(),
                    host.constant_policy(),
                    host.static_plan_carrier(),
                ),
            ),
            Handle::current(),
            host.connector_blocking_io_supervisor(),
        ),
    );
    // No fallible product construction follows these transfers. Keeping them
    // at the tail means an earlier failure still reaches Host's exact reverse
    // cleanup path, while a serving role owns the catalog lifecycle and its
    // MV product.
    let catalog_runtime = host.take_catalog_role_runtime()?;
    let mv_execution_services = crate::query::mv_action::MvExecutionServices {
        client: host.logical_execution_client(),
        launcher: logical_execution_launcher.clone(),
        completion: crate::query_execution::completion_facts::CompletionFactOwners::new(
            catalog_service.clone(),
            Some(catalog_application.clone()),
            typed_connector_control.clone(),
            unified_statistics.clone(),
            mv_candidate_reader.clone(),
            mv_storage_observation.clone(),
            host.connector_blocking_io_supervisor(),
        ),
        connector_control: typed_connector_control.clone(),
        blocking: host.connector_blocking_io_supervisor(),
        data: host.write_data_runtime(),
        state_store: host.state_store_original_binding(),
        #[cfg(novarocks_dev_controls)]
        controls: mv_readiness.test_controls_owner(),
    };
    Ok(FrontendRoleProducts {
        mv_execution_services,
        catalog_runtime,
        catalog_service,
        unified_statistics,
        catalog_application,
        function_catalog,
        constant_policy: host.constant_policy(),
        connector_control,
        typed_connector_control,
        query_control,
        logical_execution_launcher,
        topology,
        role,
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
        _mv_catalog_admission: mv_catalog_admission,
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
    let topology = products.topology.clone();
    let role = products.role;
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
            topology.clone(),
            exchange_port,
            view_service.clone(),
            system_catalog,
            Arc::clone(&mv_readiness),
            mv_candidate_reader,
            Arc::clone(&mv_storage_observation),
            host.connector_blocking_io_supervisor(),
            host.constant_policy(),
            host.static_plan_carrier(),
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
            host.constant_policy(),
        ));
    let iceberg_ref_command_executor = core_capabilities::iceberg_ref_command_executor(
        core_capabilities::IcebergRefCommandPorts::new(
            Arc::clone(&connector_control),
            Arc::clone(&mv_storage_observation),
            Arc::clone(&mv_readiness),
        ),
    );
    let mv_command_executor =
        core_capabilities::mv_command_executor(core_capabilities::MvCommandPorts::new(
            Arc::clone(&function_catalog),
            Arc::clone(&catalog_service),
            Some(Arc::clone(&catalog_application)),
            Arc::clone(&connector_control),
            Arc::clone(&products.typed_connector_control),
            mv_service,
            Arc::clone(&mv_storage_observation),
            products
                .mv_product_service
                .management_entrance()
                .expect("serving MV product owns document-management authority"),
            products.mv_product_service.management_continuation(),
            host.mv_management_audit_sink(),
            host.constant_policy(),
        ))
        .with_execution_services(products.mv_execution_services.clone());
    let mv_command_consumer: Arc<
        dyn novarocks_query_application::api::MaterializedViewCommandConsumer,
    > = Arc::new(crate::mv::command::FrontendMvCommandConsumer::new(
        mv_command_executor.clone(),
    ));
    let maintenance_command_executor =
        core_capabilities::maintenance_command_executor(maintenance_ports);
    let maintenance_read_command_executor =
        core_capabilities::maintenance_read_command_executor(maintenance_service);
    let product_command_router = crate::query::product_command_router(
        catalog_command_executor,
        view_command_executor.clone(),
        iceberg_ref_command_executor.clone(),
        statistics_command_executor,
        maintenance_command_executor,
        maintenance_read_command_executor,
        host.query_blocking_executor(),
        host.state_store_original_binding(),
        #[cfg(novarocks_dev_controls)]
        host.test_session
            .as_ref()
            .and_then(|session| session.statement.clone()),
    );
    let dml_engines = core_capabilities::dml_engines(
        core_capabilities::DmlEnginePorts::new(
            function_catalog,
            Arc::clone(&catalog_service),
            Some(catalog_application),
            connector_control,
            typed_connector_control,
            unified_statistics,
            mv_storage_observation,
            Arc::clone(&mv_readiness),
            Handle::current(),
            host.lake_publication_runtime_policy(),
            host.constant_policy(),
            host.static_plan_carrier(),
        )
        .with_write_runtime(
            host.connector_blocking_io_supervisor(),
            host.write_data_runtime(),
        ),
    );
    let query_service = crate::query::FrontendQueryService::new(
        session_catalog_resolver,
        query_compiler,
        product_command_router,
        backend_command_executor,
        view_command_executor,
        mv_command_consumer,
        mv_command_executor,
        products.query_control.clone(),
        client_connection_control,
        Arc::clone(&products.logical_execution_launcher),
        host.workload_root_admission(),
        role,
        topology,
        Arc::clone(&products.dml_service),
        dml_engines.insert,
        dml_engines.delete,
        dml_engines.mutation,
        dml_engines.add_files,
        dml_engines.ctas,
        dml_engines.truncate,
        host.query_blocking_executor(),
        host.optimizer_query_mem_limit_bytes(),
        host.lake_publication_runtime_policy(),
        host.serving_lifecycle().admission(),
    )
    .with_state_store_original(host.state_store_original_binding());
    #[cfg(novarocks_dev_controls)]
    let query_service = query_service.with_statement_observation(
        host.test_session
            .as_ref()
            .and_then(|session| session.statement.clone()),
    );
    #[cfg(novarocks_dev_controls)]
    let query_service = query_service.with_result_handoff(
        host.test_session
            .as_ref()
            .and_then(|session| session.result_handoff.clone()),
    );
    let query_service = Arc::new(query_service);
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
    metrics_registry: Arc<crate::metrics::FrontendMetricsRegistry>,
    serving_reader: Arc<LateBoundFrontendServingSnapshotReader>,
    island_reader: Arc<crate::topology::LateBoundBackendIslandSnapshotReader>,
    convergence_reader: Arc<crate::metrics::LateBoundQueryLifecycleConvergenceReader>,
    metrics_http_server: crate::metrics::MetricsHttpServer,
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    hms_admission_observation: Option<Arc<hms_admission_observation::HmsAdmissionObservation>>,
}

pub fn start_frontend_management_server(
    config: &FrontendManagementConfig,
    host: &FrontendApplicationHost,
) -> Result<FrontendManagementServer, FrontendApplicationError> {
    let metrics_registry =
        crate::metrics::FrontendMetricsRegistry::with_process_memory(config.process_memory.clone())
            .map_err(FrontendApplicationError::server)?;
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
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    let hms_admission_observation = host
        .test_session
        .as_ref()
        .and_then(|session| session.hms_admission_owner.clone());
    #[cfg(novarocks_dev_controls)]
    let statement_observation = host
        .test_session
        .as_ref()
        .and_then(|session| session.statement.clone());
    #[cfg(not(novarocks_dev_controls))]
    let _ = host;
    let metrics_http_server = crate::metrics::MetricsHttpServer::start(
        &config.bind_host,
        config.http_port,
        Arc::clone(&metrics_registry),
        management_reader,
        management_island_reader,
        Some(management_convergence_reader),
        Arc::clone(&config.memory_authority),
        Some(host.catalog_activity_observer()),
        #[cfg(novarocks_dev_controls)]
        statement_observation,
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        hms_admission_observation
            .as_ref()
            .map(|owner| owner.handler()),
    )
    .map_err(FrontendApplicationError::server)?;
    Ok(FrontendManagementServer {
        metrics_registry,
        serving_reader,
        island_reader,
        convergence_reader,
        metrics_http_server,
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        hms_admission_observation,
    })
}

impl FrontendManagementServer {
    pub fn install(&self, host: &FrontendApplicationHost) -> Result<(), FrontendApplicationError> {
        // The original role runner calls this after opening its original Host
        // and before creating role products/admission observers/SQL readiness.
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        if let Some(owner) = self.hms_admission_observation.as_ref() {
            owner
                .require_installed(&host.catalog_runtime_projection())
                .map_err(FrontendApplicationError::server)?;
        }
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

    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    fn require_hms_admission_projection(
        &self,
        host: &FrontendApplicationHost,
    ) -> Result<(), FrontendApplicationError> {
        if let Some(owner) = self.hms_admission_observation.as_ref() {
            owner
                .require_installed(&host.catalog_runtime_projection())
                .map_err(FrontendApplicationError::server)?;
        }
        Ok(())
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
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    management_server.require_hms_admission_projection(host)?;
    let mut report_server = host.start_report_server_from_host(
        &config.report_bind_host,
        config.report_grpc_port,
        Arc::clone(&config.native_trust),
        config.native_transport.clone(),
        config.native_ingress,
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
    let server_result = run_client_listeners_with_supervision(
        #[cfg(any(
            feature = "mem-1-m07-exact-mysql-write",
            feature = "mem-1-m07-root-observation",
            feature = "mem-1-m07-closing-pressure"
        ))]
        Arc::clone(&config.native_trust),
        ClientListenerInputs {
            mysql_listener: config.mysql_listener,
            trino_listener: config.trino_listener,
            server_version: config.server_version,
            session_factory,
            query_control: products.query_control.clone(),
            client_connections,
        },
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

struct ClientListenerInputs {
    mysql_listener: ResolvedMysqlListenerSettings,
    trino_listener: novarocks_trino_adapter::TrinoListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    query_control: novarocks_query_application::session_control::QueryControlService,
    client_connections: Arc<MysqlClientConnectionRegistry>,
}

async fn run_client_listeners_with_supervision<F>(
    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-root-observation",
        feature = "mem-1-m07-closing-pressure"
    ))]
    native_trust: Arc<NativeTrust>,
    listeners: ClientListenerInputs,
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
    let ClientListenerInputs {
        mysql_listener,
        trino_listener,
        server_version,
        session_factory,
        query_control,
        client_connections,
    } = listeners;
    let mut trino_server = novarocks_trino_adapter::TrinoServerHandle::bind(
        &trino_listener,
        server_version.clone(),
        host.serving_lifecycle().admission(),
        Arc::clone(&session_factory),
        query_control,
    )
    .await
    .map_err(FrontendApplicationError::server)?;
    let trino_port = trino_server.bound_addr().port();
    let result = async {
    management_server.metrics_registry.install_trino(&trino_server.observation())
        .map_err(FrontendApplicationError::server)?;
    #[cfg(feature = "mem-1-m07-root-observation")]
    let _root_observation_claim = root_observation_identity::emit(
        &native_trust,
        host.test_session.as_ref().map_or_else(Vec::new, |session| {
            session
                .scope()
                .grants_for(root_observation_identity::DOMAIN)
        }),
    )?;
    #[cfg(any(
        feature = "mem-1-m07-closing-pressure",
        feature = "mem-1-m07-exact-mysql-write"
    ))]
    let authorization = host
        .test_session
        .as_ref()
        .map(|session| session.take_mysql_fixture())
        .transpose()
        .map_err(FrontendApplicationError::server)?
        .flatten();
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    let authorization = match authorization {
        Some(authorization) if authorization.is_closing_pressure() => {
            let fixture = closing_pressure_fixture::bind_authorized(
                &native_trust,
                authorization,
                host.workload_observation(),
            )?;
            return closing_pressure_fixture::serve(
                fixture,
                mysql_listener,
                server_version,
                session_factory,
                client_connections,
                shutdown,
                report_server,
                management_server,
                host,
                drain_timeout,
                cleanup_timeout,
            &mut trino_server,
            )
            .await;

        }
        other => other,
    };
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    if let Some(authorization) = authorization {
        let fixture = exact_mysql_write_fixture::bind_authorized(&native_trust, authorization)?;
        return exact_mysql_write_fixture::serve(
            fixture,
            mysql_listener,
            server_version,
            session_factory,
            client_connections,
            shutdown,
            report_server,
            management_server,
            host,
            drain_timeout,
            cleanup_timeout,
            &mut trino_server,
        )
        .await;

    }
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
            server_version,
            Arc::clone(&session_factory),
            Arc::clone(&client_connections),
            wait_for_signal(drain_rx),
            wait_for_signal(finalize_rx),
            cleanup_timeout,
            move |bound_addr| emit_frontend_mysql_ready(bound_addr, &ready_user, trino_port),
        );
    tokio::pin!(mysql_server);

    tokio::select! {
        result = &mut mysql_server => result.map_err(FrontendApplicationError::server),
        _ = shutdown => {
            host.begin_serving_drain(drain_timeout);
            trino_server.begin_drain();
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
        error = wait_for_frontend_listener_failure(report_server, management_server, &mut trino_server, host) => {
            host.begin_serving_drain(drain_timeout);
            trino_server.begin_drain();
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
    }.await;
    trino_server.finalize();
    combine_server_and_shutdown(
        result,
        trino_server
            .wait_stopped()
            .await
            .map_err(FrontendApplicationError::server),
    )
}

fn emit_frontend_mysql_ready(bind_addr: std::net::SocketAddr, user: &str, trino_port: u16) {
    info!(
        "standalone mysql server listening on {} (user={}, db={})",
        bind_addr, user, DEFAULT_DATABASE
    );
    // Emit a parser-friendly readiness marker on stdout. Orchestration
    // scripts must wait for this exact line before connecting; probing the
    // mysql port alone cannot distinguish a freshly-bound server from a
    // pre-existing process that already owned the port.
    println!(
        "NOVAROCKS_READY mysql_port={} pid={} trino_port={}",
        bind_addr.port(),
        std::process::id(),
        trino_port
    );
}

async fn wait_for_frontend_listener_failure(
    report_server: &mut crate::native::report_server::FrontendReportServerHandle,
    management_server: &mut FrontendManagementServer,
    trino_server: &mut novarocks_trino_adapter::TrinoServerHandle,
    host: &FrontendApplicationHost,
) -> String {
    loop {
        if let Some(error) = trino_server.poll_failure() {
            return error;
        }
        if host.workload_progress_has_failed() {
            return "frontend governed role service failed or exited unexpectedly".to_string();
        }
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
            #[cfg(any(
                feature = "mem-1-m07-exact-mysql-write",
                feature = "mem-1-m07-root-observation",
                feature = "mem-1-m07-closing-pressure"
            ))]
            return Err(server_error.with_role_cleanup(shutdown_error));
            #[cfg(not(any(
                feature = "mem-1-m07-exact-mysql-write",
                feature = "mem-1-m07-root-observation",
                feature = "mem-1-m07-closing-pressure"
            )))]
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
        if let StatementResult::GovernedError(result) = result {
            let (error, _protocol) = result.into_parts();
            panic!("statement returned an error before terminal OK: {error:?}");
        }
        let StatementResult::GovernedCompletion(result) = result else {
            panic!("successful statement must retain its owner through terminal OK");
        };
        let (completion, mut protocol) = result.into_parts();
        assert_eq!(completion.affected_rows(), 0);
        let _ = protocol.seal_success_visibility();
        let _ = protocol.complete();
        terminal.complete();
    }

    fn settle_governed_query(statement: QuerySessionStatement) {
        let (result, terminal) = statement.into_parts();
        let StatementResult::GovernedQuery(result) = result else {
            panic!("query result must retain its owner through terminal EOF");
        };
        let (result, mut protocol) = result.into_parts();
        drop(result);
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

        fn open_catalog(
            &self,
            identity: novarocks_spi::connector::CatalogScopeIdentity,
        ) -> Arc<dyn novarocks_spi::connector::ProviderCatalogControlFactory> {
            Arc::new(EchoingCatalogFactory { identity })
        }
    }

    struct EchoingCatalogFactory {
        identity: novarocks_spi::connector::CatalogScopeIdentity,
    }

    impl novarocks_spi::connector::ProviderCatalogControlFactory for EchoingCatalogFactory {
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

            assert_eq!(
                properties.as_catalog_properties().provider_id(),
                self.identity.provider_id(),
            );
            assert_eq!(
                properties.handle().catalog_name(),
                self.identity.instance_id(),
            );

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
        let mut host_receiving_9 = None;
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            &mut host_receiving_9,
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
                std::num::NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
                crate::application::test_constant_policy(),
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
        let mut host_receiving_8 = None;
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            &mut host_receiving_8,
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
                std::num::NonZeroUsize::new(1).unwrap(),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
                crate::application::test_constant_policy(),
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
                    novarocks_native_adapter::native_server::NativeIngressConfig::default(),
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
        let mut host_receiving_7 = None;
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            &mut host_receiving_7,
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
                std::num::NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
                crate::application::test_constant_policy(),
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
        let mut host_receiving_6 = None;
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            &mut host_receiving_6,
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
                std::num::NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
                crate::application::test_constant_policy(),
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
            products
                .mv_product_service
                .keyed_readiness_service()
                .is_some(),
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
        let mut host_receiving_5 = None;
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            &mut host_receiving_5,
            Some(test_state_store_input("server-bounded-shutdown-owner")),
            &registry,
            FrontendExecutionConfig::new_for_test(
                NonZeroUsize::new(1).unwrap(),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
                crate::application::test_constant_policy(),
            ),
            frontend_backend_open_config(),
            Vec::new(),
            tokio::runtime::Handle::current(),
            test_native_trust(),
            FrontendNativeTransport::plaintext(),
        )
        .await
        .expect("open frontend application host");
        host.install_statement_control_for_test();
        let serving = host.serving_snapshot_reader();
        assert!(!serving.frontend_serving_snapshot().base_ready());
        host.mark_workload_ready()
            .expect("open internal governed startup work");
        let snapshot = serving.frontend_serving_snapshot();
        assert_eq!(
            snapshot.serving_state,
            novarocks_query_application::serving_admission::FrontendServingState::Starting
        );
        assert!(
            !snapshot.base_ready(),
            "internal authority cannot expose public readiness"
        );
        let management = host
            .workload_root_admission()
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .expect("mandatory startup may use a real Management Root");
        drop(management.business);
        management.owner.complete_after_terminal_cancel_settled();
        host.mark_ready().expect("mark frontend host ready");
        assert!(serving.frontend_serving_snapshot().base_ready());
        drop(serving);
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
        let mut host_receiving_4 = None;
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            &mut host_receiving_4,
            Some(test_state_store_input("server-drain-closes-root-admission")),
            &registry,
            FrontendExecutionConfig::new_for_test(
                NonZeroUsize::new(1).unwrap(),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
                crate::application::test_constant_policy(),
            ),
            frontend_backend_open_config(),
            Vec::new(),
            tokio::runtime::Handle::current(),
            test_native_trust(),
            FrontendNativeTransport::plaintext(),
        )
        .await
        .expect("open frontend application host");
        host.install_statement_control_for_test();
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

    #[cfg(all(unix, novarocks_dev_controls))]
    mod real_statement_entry_tests {
        use super::*;
        use anyhow::{Result, anyhow, ensure};
        use novarocks_failpoint::liveness::{PreparedRoleSession, RunnerSession};
        use novarocks_failpoint::session::{DomainGrant, SessionDeclaration, SessionRole};
        use novarocks_query_application::session::QuerySession;
        use novarocks_query_application::session_control::GovernedStatementFinishOutcome;
        use novarocks_query_application::session_error::QueryServiceError;
        use sha2::{Digest, Sha256};
        use std::os::unix::fs::PermissionsExt;

        fn complete_empty_provider_execution_config() -> Result<FrontendExecutionConfig> {
            // Same frozen ingress author used by the actual session/control
            // service; no test-owned listener quota or session registry.
            let mysql = novarocks_mysql_adapter::frozen_mysql_startup_parameters()
                .map_err(anyhow::Error::msg)?;
            let ingress = crate::native_workload::FrontendStatementIngress {
                ordinary_connections: NonZeroUsize::new(mysql.ordinary_positions)
                    .ok_or_else(|| anyhow!("ordinary MySQL positions must be nonzero"))?,
                control_connections: NonZeroUsize::new(mysql.control_positions)
                    .ok_or_else(|| anyhow!("control MySQL positions must be nonzero"))?,
                ordinary_command_bytes: NonZeroUsize::new(mysql.ordinary.limits.command_bytes)
                    .ok_or_else(|| anyhow!("ordinary MySQL command bound must be nonzero"))?,
                control_command_bytes: NonZeroUsize::new(mysql.control.limits.command_bytes)
                    .ok_or_else(|| anyhow!("control MySQL command bound must be nonzero"))?,
            };
            FrontendExecutionConfig::new_for_test(
                NonZeroUsize::new(1).unwrap(),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                builtin_function_catalog(),
                crate::application::test_constant_policy(),
            )
            .try_with_empty_provider_read_profile_for_test(ingress)
            .map_err(anyhow::Error::new)
        }

        fn arm_real_entry(
            owner: &crate::preparation_diagnostics::StatementObservationOwner,
            token: &str,
            connection_id: u32,
            sql: &str,
        ) -> Result<()> {
            owner
                .arm(
                    crate::preparation_diagnostics::StatementObservationArmRequest {
                        run_token: token.to_owned(),
                        connection_id,
                        sql_sha256: hex::encode(Sha256::digest(sql.as_bytes())),
                    },
                )
                .map_err(anyhow::Error::msg)
        }

        async fn actual_entry_error(
            session: &dyn QuerySession,
            sql: &str,
        ) -> Result<QueryServiceError> {
            match session.execute_batch(sql).await {
                Err(error) => Ok(error),
                Ok(statement) => {
                    let (output, terminal) = statement.into_parts();
                    match output {
                        StatementResult::GovernedError(result) => {
                            let (error, mut protocol) = result.into_parts();
                            let finish = protocol.fail();
                            terminal.complete();
                            if matches!(finish, GovernedStatementFinishOutcome::Stale) {
                                return Err(anyhow::Error::new(error).context(
                                    "real error protocol failed to finish its statement",
                                ));
                            }
                            Ok(error)
                        }
                        output => {
                            drop(output);
                            terminal.complete();
                            Err(anyhow!(
                                "selected entry unexpectedly returned a successful result"
                            ))
                        }
                    }
                }
            }
        }

        async fn configure_real_entry(session: &dyn QuerySession) -> Result<()> {
            let statement = session.execute_batch("SET query_timeout = 1").await?;
            let (output, terminal) = statement.into_parts();
            let StatementResult::GovernedCompletion(result) = output else {
                drop(output);
                terminal.complete();
                return Err(anyhow!(
                    "query timeout SET did not return its governed completion"
                ));
            };
            let (completion, mut protocol) = result.into_parts();
            ensure!(completion.affected_rows() == 0, "SET has no affected rows");
            let seal = protocol.seal_success_visibility();
            let finish = protocol.complete();
            terminal.complete();
            ensure!(matches!(seal,
                novarocks_query_application::session_control::GovernedStatementVisibilitySealOutcome::Sealed),
                "query timeout SET visibility did not seal: {seal:?}");
            ensure!(
                matches!(finish, GovernedStatementFinishOutcome::Completed),
                "query timeout SET did not actually finish: {finish:?}"
            );
            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn real_unbound_arm_cannot_move_to_replacement_connection() -> Result<()> {
            run_unbound_entry_case(false).await
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn real_unbound_native_permission_cannot_retarget_sql_through_drain_and_rearm()
        -> Result<()> {
            run_unbound_entry_case(true).await
        }

        async fn run_unbound_entry_case(rearm: bool) -> Result<()> {
            let execution_config = complete_empty_provider_execution_config()?;
            let directory = tempfile::Builder::new()
                .prefix("g2-real-entry-")
                .tempdir_in("/tmp")?;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
            let mut grants = vec![DomainGrant {
                domain: "statement-observation".into(),
                grant_id: "observe-real-entry".into(),
                parameters: serde_json::json!({"action":"observe", "bearer_secret":"0123456789abcdef0123456789abcdef"}),
            }];
            // These bind grants freeze statement ownership without inventing a BE
            // receipt or a QueryExecutionId. This test fails before placement.
            {
                let token = "first-entry";
                grants.push(DomainGrant {
                    domain: "native".into(),
                    grant_id: format!("bind-{token}"),
                    parameters: serde_json::json!({
                        "action":"bind-create-task-ack-drop", "root":directory.path(),
                        "backend_index":0, "statement_run_token":token,
                        "target_session_id":"unpublished-backend-entry",
                        "target_process_receipt":directory.path().join("unpublished-be.receipt.json"),
                    }),
                });
            }
            let declaration = SessionDeclaration {
                schema_version: 1,
                session_id: "real-entry-fe".into(),
                launch_id: "real-entry-launch".into(),
                role: SessionRole::Fe,
                lifetime_millis: 30_000,
                liveness_socket: directory.path().join("live.sock"),
                handshake_secret: "0123456789abcdef0123456789abcdef".into(),
                domain_grants: grants,
            };
            let path = directory.path().join("declaration.json");
            declaration.write_private(&path)?;
            let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)?;
            let mut runner = RunnerSession::start(declaration)?;
            let registry = test_state_store_registry();
            let mut host_receiving_3 = None;
            let opened =
                FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
                    &mut host_receiving_3,
                    Some(test_state_store_input("g2-real-entry")),
                    &registry,
                    execution_config.with_test_session(
                        crate::test_session::FrontendTestSessionConfig::new(prepared),
                    ),
                    frontend_backend_open_config(),
                    Vec::new(),
                    tokio::runtime::Handle::current(),
                    test_native_trust(),
                    FrontendNativeTransport::plaintext(),
                )
                .await;
            let mut host = match opened {
                Ok(host) => host,
                Err(primary) => {
                    let cleanup = runner.end_and_join();
                    return Err(anyhow!("{primary}; runner cleanup: {cleanup:?}"));
                }
            };
            let built = build_frontend_query_session_factory(
                &mut host,
                Arc::new(
                    novarocks_query_application::system_catalog::SystemCatalogService::with_defaults(),
                ),
                0,
                Arc::new(UnavailableMvStorageObservationPort),
                Arc::new(MysqlClientConnectionRegistry::new()),
            )
            .await;
            let (session_factory, mut products) = match built {
                Ok(products) => products,
                Err(primary) => {
                    let host_cleanup = host.shutdown().await;
                    let runner_cleanup = runner.end_and_join();
                    return Err(anyhow!(
                        "{primary}; host cleanup: {host_cleanup:?}; runner cleanup: {runner_cleanup:?}"
                    ));
                }
            };
            let mut sessions = Vec::<Arc<dyn QuerySession>>::new();
            let primary: Result<()> = async {
                ensure!(host.backend_topology_port().snapshot()?.targets().is_empty(),
                    "the preallocation check must use the actual empty topology");
                let controls = host.test_session.as_ref().ok_or_else(|| anyhow!("missing actual role session"))?;
                let owner = controls.statement.as_ref().ok_or_else(|| anyhow!("missing actual observation owner"))?;
                let scope = controls.scope().clone();
                let original = session_factory.open_session(QuerySessionOpenRequest::new(
                    ClientConnectionToken::new(41, 1).unwrap(), "unbound-original"))?;
                sessions.push(original.clone());
                configure_real_entry(original.as_ref()).await?;
                let selected_sql = if rearm { "SELECT" } else { "SELECT 1" };
                arm_real_entry(owner, "first-entry", 41, selected_sql)?;
                ensure!(owner.peek("first-entry").map_err(anyhow::Error::msg)?.work_id.is_none(),
                    "the selected arm must still be unbound before its first matching statement");
                let replacement = if rearm {
                    // Parser failure occurs before QueryControl allocates any
                    // statement for this selected request. Keep the exact same
                    // live client while testing a forbidden SQL retarget.
                    let parse = actual_entry_error(original.as_ref(), selected_sql).await?;
                    ensure!(parse.kind() == QueryServiceErrorKind::Parse, "{parse}");
                    ensure!(owner.peek("first-entry").map_err(anyhow::Error::msg)?.work_id.is_none(),
                        "the parser failure unexpectedly allocated a statement");
                    original.clone()
                } else {
                    original.close();
                    let replacement = session_factory.open_session(QuerySessionOpenRequest::new(
                        ClientConnectionToken::new(41, 2).unwrap(), "unbound-replacement"))?;
                    sessions.push(replacement.clone());
                    configure_real_entry(replacement.as_ref()).await?;
                    replacement
                };
                let rearmed = if rearm {
                    owner.drain("first-entry").map_err(anyhow::Error::msg)?;
                    Some(arm_real_entry(owner, "first-entry", 41, "SELECT 1"))
                } else {
                    None
                };

                // Poll the actual entry to its returned error before checking the
                // nomination predicate. No direct bind or invented execution is
                // used, and failure assertions cannot abandon a query future.
                let error = actual_entry_error(replacement.as_ref(), "SELECT 1").await?;
                ensure!(error.kind() == QueryServiceErrorKind::Timeout, "{error}");
                ensure!(products.query_control.list_processes().iter().all(|process| process.statement.is_none()),
                    "the returned query error retained an active statement");
                if let Some(rearmed) = rearmed {
                    ensure!(rearmed.is_err(),
                        "the same predeclared Native permission retargeted SQL through drain/rearm after parser failure");
                } else {
                    let old = owner.peek("first-entry").map_err(anyhow::Error::msg)?;
                    ensure!(old.work_id.is_none(),
                        "the original unbound arm moved to the replacement client: work_id={:?}", old.work_id);
                    ensure!(old.execution_id.is_none() && old.logical_execution_id.is_none()
                        && old.attempt_id.is_none() && old.snapshot.is_none(),
                        "an abandoned arm acquired replacement execution evidence");
                }
                ensure!(scope.observations().iter().all(|event| event.domain != "native"),
                    "replacement observation caused a Native authority claim/hit");
                ensure!(!directory.path().join("unpublished-be.receipt.json").exists(),
                    "the entry test fabricated a backend receipt");
                Ok(())
            }.await;
            for session in &sessions {
                session.close();
            }
            drop(sessions);
            drop(session_factory);
            let products_cleanup = shutdown_frontend_role_products_to_convergence(
                &mut products,
                Duration::from_secs(1),
            )
            .await;
            let host_cleanup = host.shutdown().await;
            let runner_cleanup = runner.end_and_join();
            let failures = [
                products_cleanup.map_err(|error| error.to_string()),
                host_cleanup.map_err(|error| error.to_string()),
                runner_cleanup.map_err(|error| error.to_string()),
            ]
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
            match (primary, failures.is_empty()) {
                (Ok(()), true) => Ok(()),
                (Err(primary), true) => Err(primary),
                (Ok(()), false) => Err(anyhow!(
                    "real entry cleanup failed: {}",
                    failures.join("; ")
                )),
                (Err(primary), false) => {
                    Err(primary.context(format!("real entry cleanup: {}", failures.join("; "))))
                }
            }
        }

        fn actual_statement_identity(work_id: Option<&str>) -> Result<(u32, u64, u64)> {
            let fields = work_id
                .ok_or_else(|| anyhow!("real statement entry did not bind"))?
                .split(':')
                .collect::<Vec<_>>();
            ensure!(
                fields.len() == 4 && fields[0] == "statement",
                "invalid statement work identity"
            );
            let connection = fields[1].parse()?;
            let epoch = fields[2].parse()?;
            let generation = fields[3].parse()?;
            ensure!(
                epoch > 0 && generation > 0,
                "statement identity must be owner allocated"
            );
            Ok((connection, epoch, generation))
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn real_entry_finite_input_survives_unbound_drain_and_bound_successors() -> Result<()>
        {
            let execution_config = complete_empty_provider_execution_config()?;
            let directory = tempfile::Builder::new()
                .prefix("g2-real-entry-")
                .tempdir_in("/tmp")?;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
            let mut grants = vec![DomainGrant {
                domain: "statement-observation".into(),
                grant_id: "observe-real-entry".into(),
                parameters: serde_json::json!({"action":"observe", "bearer_secret":"0123456789abcdef0123456789abcdef"}),
            }];
            // These bind grants freeze statement ownership without inventing a BE
            // receipt or a QueryExecutionId. This test fails before placement.
            for token in ["first-entry", "independent-entry"] {
                grants.push(DomainGrant {
                    domain: "native".into(),
                    grant_id: format!("bind-{token}"),
                    parameters: serde_json::json!({
                        "action":"bind-create-task-ack-drop", "root":directory.path(),
                        "backend_index":0, "statement_run_token":token,
                        "target_session_id":"unpublished-backend-entry",
                        "target_process_receipt":directory.path().join("unpublished-be.receipt.json"),
                    }),
                });
            }
            let declaration = SessionDeclaration {
                schema_version: 1,
                session_id: "real-entry-fe".into(),
                launch_id: "real-entry-launch".into(),
                role: SessionRole::Fe,
                lifetime_millis: 30_000,
                liveness_socket: directory.path().join("live.sock"),
                handshake_secret: "0123456789abcdef0123456789abcdef".into(),
                domain_grants: grants,
            };
            let path = directory.path().join("declaration.json");
            declaration.write_private(&path)?;
            let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)?;
            let mut runner = RunnerSession::start(declaration)?;
            let registry = test_state_store_registry();
            let mut host_receiving_2 = None;
            let opened =
                FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
                    &mut host_receiving_2,
                    Some(test_state_store_input("g2-real-entry")),
                    &registry,
                    execution_config.with_test_session(
                        crate::test_session::FrontendTestSessionConfig::new(prepared),
                    ),
                    frontend_backend_open_config(),
                    Vec::new(),
                    tokio::runtime::Handle::current(),
                    test_native_trust(),
                    FrontendNativeTransport::plaintext(),
                )
                .await;
            let mut host = match opened {
                Ok(host) => host,
                Err(primary) => {
                    let cleanup = runner.end_and_join();
                    return Err(
                        anyhow::Error::new(primary).context(format!("runner cleanup: {cleanup:?}"))
                    );
                }
            };
            let built = build_frontend_query_session_factory(
                &mut host,
                Arc::new(
                    novarocks_query_application::system_catalog::SystemCatalogService::with_defaults(),
                ),
                0,
                Arc::new(UnavailableMvStorageObservationPort),
                Arc::new(MysqlClientConnectionRegistry::new()),
            )
            .await;
            let (session_factory, mut products) = match built {
                Ok(products) => products,
                Err(primary) => {
                    let host_cleanup = host.shutdown().await;
                    let runner_cleanup = runner.end_and_join();
                    return Err(anyhow::Error::new(primary).context(format!(
                        "host cleanup: {host_cleanup:?}; runner cleanup: {runner_cleanup:?}"
                    )));
                }
            };
            let mut sessions = Vec::<Arc<dyn QuerySession>>::new();
            let primary: Result<()> = async {
                ensure!(
                    host.backend_topology_port()
                        .snapshot()?
                        .targets()
                        .is_empty(),
                    "the preallocation check must use the actual empty topology"
                );
                let controls = host
                    .test_session
                    .as_ref()
                    .ok_or_else(|| anyhow!("missing actual role session"))?;
                let owner = controls
                    .statement
                    .as_ref()
                    .ok_or_else(|| anyhow!("missing actual observation owner"))?;
                let scope = controls.scope().clone();
                let original = session_factory.open_session(QuerySessionOpenRequest::new(
                    ClientConnectionToken::new(41, 1).unwrap(),
                    "complement-original",
                ))?;
                sessions.push(original.clone());
                let unrelated = session_factory.open_session(QuerySessionOpenRequest::new(
                    ClientConnectionToken::new(42, 1).unwrap(),
                    "complement-unrelated",
                ))?;
                sessions.push(unrelated.clone());
                configure_real_entry(original.as_ref()).await?;
                configure_real_entry(unrelated.as_ref()).await?;
                arm_real_entry(owner, "first-entry", 41, "SELECT 1")?;

                // Neither an unrelated client nor another SQL on the selected client
                // can acquire the unbound first permission. Await both real entries.
                let foreign = actual_entry_error(unrelated.as_ref(), "SELECT 1").await?;
                ensure!(
                    foreign.kind() == QueryServiceErrorKind::Timeout,
                    "{foreign}"
                );
                ensure!(
                    owner
                        .peek("first-entry")
                        .map_err(anyhow::Error::msg)?
                        .work_id
                        .is_none(),
                    "unrelated connection stole the finite nomination"
                );
                let different_sql = actual_entry_error(original.as_ref(), "SELECT 2").await?;
                ensure!(
                    different_sql.kind() == QueryServiceErrorKind::Timeout,
                    "{different_sql}"
                );
                let unbound = owner.peek("first-entry").map_err(anyhow::Error::msg)?;
                ensure!(
                    unbound.work_id.is_none()
                        && unbound.execution_id.is_none()
                        && unbound.logical_execution_id.is_none()
                        && unbound.attempt_id.is_none()
                        && unbound.snapshot.is_none(),
                    "an unmatched statement acquired selected evidence"
                );
                let drained = owner.drain("first-entry").map_err(anyhow::Error::msg)?;
                ensure!(
                    drained.work_id.is_none(),
                    "the first arm unexpectedly bound before drain"
                );
                original.close();
                let replacement = session_factory.open_session(QuerySessionOpenRequest::new(
                    ClientConnectionToken::new(41, 2).unwrap(),
                    "complement-replacement",
                ))?;
                sessions.push(replacement.clone());
                configure_real_entry(replacement.as_ref()).await?;
                let old_rearm = arm_real_entry(owner, "first-entry", 41, "SELECT 1");
                // Even a faulty rearm must not abandon its actual query future.
                let replacement_error = actual_entry_error(replacement.as_ref(), "SELECT 1").await?;
                ensure!(
                    replacement_error.kind() == QueryServiceErrorKind::Timeout,
                    "{replacement_error}"
                );
                let refusal = old_rearm.err().ok_or_else(|| {
                    anyhow!(
                        "unbound original permission followed the replacement client through drain/rearm"
                    )
                })?;
                ensure!(
                    refusal
                        .to_string()
                        .contains("Native original statement input replacement refused"),
                    "unexpected unbound input refusal: {refusal}"
                );
                ensure!(
                    owner.peek("first-entry").is_err(),
                    "failed finite input rearm retained a replacement journal"
                );

                // The separately declared permission remains usable after that refusal.
                arm_real_entry(owner, "independent-entry", 41, "SELECT 1")?;
                let first = actual_entry_error(replacement.as_ref(), "SELECT 1").await?;
                ensure!(first.kind() == QueryServiceErrorKind::Timeout, "{first}");
                let bound = owner
                    .peek("independent-entry")
                    .map_err(anyhow::Error::msg)?;
                let first_id = actual_statement_identity(bound.work_id.as_deref())?;
                ensure!(
                    first_id.0 == 41,
                    "nomination differs from the actual replacement connection"
                );
                ensure!(
                    bound.execution_id.is_none()
                        && bound.logical_execution_id.is_none()
                        && bound.attempt_id.is_none()
                        && bound.snapshot.is_none(),
                    "preallocation failure invented an execution or completion"
                );

                // With its journal retained, a later real generation cannot move the
                // existing binding. Drain may retire only that observer journal.
                let later = actual_entry_error(replacement.as_ref(), "SELECT 1").await?;
                ensure!(later.kind() == QueryServiceErrorKind::Timeout, "{later}");
                ensure!(
                    owner
                        .peek("independent-entry")
                        .map_err(anyhow::Error::msg)?
                        .work_id
                        == bound.work_id,
                    "a bound journal rolled forward to a later generation"
                );
                owner
                    .drain("independent-entry")
                    .map_err(anyhow::Error::msg)?;
                arm_real_entry(owner, "independent-entry", 41, "SELECT 1")?;
                let next = actual_entry_error(replacement.as_ref(), "SELECT 1").await?;
                ensure!(
                    next.kind() == QueryServiceErrorKind::Internal
                        && next
                            .message()
                            .contains("Native first statement nomination replacement refused"),
                    "drain/rearm rolled an existing permission into the next generation: {next}"
                );
                let next_bound = owner
                    .peek("independent-entry")
                    .map_err(anyhow::Error::msg)?;
                let next_id = actual_statement_identity(next_bound.work_id.as_deref())?;
                ensure!(
                    next_id.0 == first_id.0 && next_id.1 == first_id.1 && next_id.2 > first_id.2,
                    "real QueryControl did not advance the same client generation"
                );
                ensure!(
                    next_bound.execution_id.is_none()
                        && next_bound.logical_execution_id.is_none()
                        && next_bound.attempt_id.is_none()
                        && next_bound.snapshot.is_none(),
                    "refused generation acquired execution evidence"
                );
                owner
                    .drain("independent-entry")
                    .map_err(anyhow::Error::msg)?;
                replacement.close();

                let successor = session_factory.open_session(QuerySessionOpenRequest::new(
                    ClientConnectionToken::new(41, 3).unwrap(),
                    "complement-successor",
                ))?;
                sessions.push(successor.clone());
                configure_real_entry(successor.as_ref()).await?;
                let bound_rearm = arm_real_entry(owner, "independent-entry", 41, "SELECT 1");
                let successor_error = actual_entry_error(successor.as_ref(), "SELECT 1").await?;
                ensure!(
                    successor_error.kind() == QueryServiceErrorKind::Timeout,
                    "{successor_error}"
                );
                let refusal = bound_rearm
                    .err()
                    .ok_or_else(|| anyhow!("bound original permission followed a replacement client"))?;
                ensure!(
                    refusal
                        .to_string()
                        .contains("Native original statement input replacement refused"),
                    "unexpected bound input refusal: {refusal}"
                );
                ensure!(
                    owner.peek("independent-entry").is_err(),
                    "failed bound-input rearm retained a replacement journal"
                );
                ensure!(
                    products
                        .query_control
                        .list_processes()
                        .iter()
                        .all(|process| process.statement.is_none()),
                    "returned real queries retained an active statement"
                );
                ensure!(
                    scope
                        .observations()
                        .iter()
                        .all(|event| event.domain != "native"),
                    "nomination or observation consumed Native authority"
                );
                ensure!(
                    !directory
                        .path()
                        .join("unpublished-be.receipt.json")
                        .exists(),
                    "the entry test fabricated a backend receipt"
                );
                Ok(())
            }
            .await;
            for session in &sessions {
                session.close();
            }
            drop(sessions);
            drop(session_factory);
            let products_cleanup = shutdown_frontend_role_products_to_convergence(
                &mut products,
                Duration::from_secs(1),
            )
            .await;
            let host_cleanup = host.shutdown().await;
            let runner_cleanup = runner.end_and_join();
            let failures = [
                products_cleanup.map_err(|error| error.to_string()),
                host_cleanup.map_err(|error| error.to_string()),
                runner_cleanup.map_err(|error| error.to_string()),
            ]
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
            match (primary, failures.is_empty()) {
                (Ok(()), true) => Ok(()),
                (Err(primary), true) => Err(primary),
                (Ok(()), false) => Err(anyhow!(
                    "real entry cleanup failed: {}",
                    failures.join("; ")
                )),
                (Err(primary), false) => {
                    Err(primary.context(format!("real entry cleanup: {}", failures.join("; "))))
                }
            }
        }
    }
}
