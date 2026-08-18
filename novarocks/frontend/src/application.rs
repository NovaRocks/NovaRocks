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

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::query_execution::dml::ctas::CtasEngine;
use crate::query_execution::service::QueryExecutionService;
use novarocks_spi::connector::ConnectorControlFactory;
use novarocks_spi::state_store::{StateStore, StateStoreProviderId};
use novarocks_state_store::{
    StateStoreHost, StateStoreHostConfig, builtin_state_store_provider_registry,
};

use crate::catalog_application::FrontendCatalogApplicationPort;
use crate::catalog_attachment::CatalogAttachmentRepository;
use crate::catalog_controller::{CatalogProjectionConfig, FrontendCatalogController};
use crate::connector::ConnectorControlHost;
use crate::coordination::FrontendCoordinationRuntime;
use crate::coordinator::{
    BackendQueryActivity, FrontendDistributedQueryCoordinator, QueryLifecycleConvergenceReader,
};
use crate::deployment::{FeDeploymentViewSource, SqliteSingleFeDeploymentViewSource};
use crate::dml::recovery::DmlRecoveryController;
use crate::dml::{DmlService, StateStoreOperationJournal};
use crate::mv::maintenance::MaintenanceCoordinatorConfig;
use crate::mv::scheduler::FrontendMvSchedulerConfig;
use crate::mv::{
    FrontendMvRefreshProviderActivationPort, FrontendMvService, repository::StateStoreMvRepository,
};
use crate::query_control::FrontendQueryControl;
use crate::query_execution::maintenance::TableMaintenanceService;
use crate::statistics::FrontendStatisticsService;
use crate::statistics_jobs::repository::StatisticsJobRepository;
use crate::statistics_jobs::service::{
    FrontendStatisticsApplicationPort, StatisticsApplicationService,
};
use crate::table_maintenance::FrontendTableMaintenanceService;
use crate::table_maintenance::coordination::MaintenanceCoordination;
use crate::topology::{ClusterBackendOpenConfig, ClusterBackendService};
use crate::view::FrontendViewService;

const STATE_STORE_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const STATE_STORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendApplicationErrorKind {
    DeploymentSource,
    StateStoreHost,
    CoordinationOpen,
    DmlServiceOpen,
    ViewServiceOpen,
    TableMaintenanceServiceOpen,
    MvServiceOpen,
    StatisticsApplicationServiceOpen,
    CatalogApplicationServiceOpen,
    CatalogControllerOpen,
    ConnectorControlHost,
    ClusterBackendOpen,
    CoordinatorOpen,
    Server,
    Shutdown,
}

#[derive(Debug)]
pub struct FrontendApplicationError {
    kind: FrontendApplicationErrorKind,
    message: String,
}

impl FrontendApplicationError {
    pub(crate) fn new(kind: FrontendApplicationErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
        }
    }

    pub(crate) fn server(error: impl fmt::Display) -> Self {
        Self::new(FrontendApplicationErrorKind::Server, error)
    }

    pub(crate) fn with_cleanup_context(mut self, cleanup_error: impl fmt::Display) -> Self {
        self.message
            .push_str(&format!("; cleanup failed: {cleanup_error}"));
        self
    }

    pub const fn kind(&self) -> FrontendApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for FrontendApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for FrontendApplicationError {}

pub struct FrontendApplicationHost {
    connector_control: Arc<ConnectorControlHost>,
    catalog_runtime_projection: Arc<crate::catalog_application::CatalogRuntimeProjection>,
    statistics_service: Option<Arc<FrontendStatisticsService>>,
    dml_service: Option<Arc<DmlService>>,
    ctas_recovery_binding: Option<CtasRecoveryBinding>,
    statistics_application_service: Option<Arc<StatisticsApplicationService>>,
    statistics_application_port: Option<Arc<FrontendStatisticsApplicationPort>>,
    catalog_application_port: Option<Arc<FrontendCatalogApplicationPort>>,
    catalog_controller: Option<Arc<FrontendCatalogController>>,
    view_service: Option<Arc<dyn crate::view::ViewService>>,
    table_maintenance_service: Option<Arc<dyn TableMaintenanceService>>,
    mv_repository: Option<Arc<dyn crate::mv::domain::repository::MvRepository>>,
    mv_application_service: Option<Arc<dyn crate::mv::domain::application::MvApplicationService>>,
    mv_service: Option<Arc<FrontendMvService>>,
    mv_refresh_provider_activation: Option<Arc<FrontendMvRefreshProviderActivationPort>>,
    mv_background_engine_sink: Option<Arc<dyn crate::mv::background::MvBackgroundEngineSink>>,
    state_store_host: Option<StateStoreHost>,
    coordination: Option<Arc<FrontendCoordinationRuntime>>,
    query_execution: Option<QueryExecutionService>,
    query_control: crate::query_execution::control::QueryControlService,
    coordinator: Option<Arc<FrontendDistributedQueryCoordinator>>,
    execution_role: novarocks_types::ClusterRole,
    topology: Option<Arc<ClusterBackendService>>,
    optimizer_query_mem_limit_bytes: u64,
}

/// The one ordered handoff from Frontend composition to the CTAS recovery
/// facet.  It admits no SQL and cannot create a second scheduler: binding the
/// current-generation engine happens first, then the single controller starts.
#[derive(Clone)]
pub(crate) struct CtasRecoveryBinding {
    dml: Arc<DmlService>,
    controller: Arc<Mutex<Option<DmlRecoveryController>>>,
    installed: Arc<Mutex<bool>>,
    start_controller: bool,
}

impl CtasRecoveryBinding {
    fn new(dml: Arc<DmlService>, start_controller: bool) -> Self {
        Self {
            dml,
            controller: Arc::new(Mutex::new(None)),
            installed: Arc::new(Mutex::new(false)),
            start_controller,
        }
    }

    pub(crate) fn install_ctas_engine(&self, engine: Arc<dyn CtasEngine>) -> Result<(), String> {
        let mut installed = self
            .installed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *installed {
            return Err("CTAS recovery engine is already bound for this frontend host".to_string());
        }
        self.dml.install_ctas_recovery(engine);
        *installed = true;
        if self.start_controller {
            let mut controller = self
                .controller
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if controller.is_some() {
                return Err("DML recovery controller is already running".to_string());
            }
            *controller = Some(DmlRecoveryController::start(Arc::clone(&self.dml)));
        }
        Ok(())
    }

    async fn shutdown(&self) {
        let controller = self
            .controller
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(mut controller) = controller {
            controller.shutdown().await;
        }
    }
}

/// Matches the historical `[runtime] optimizer_query_mem_limit_bytes` default.
const DEFAULT_OPTIMIZER_QUERY_MEM_LIMIT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Query-control timeouts frozen from `[runtime]` at startup.
///
/// Coordinator code receives these; it never reads a process-global config
/// while admitting a query. Defaults mirror `RuntimeConfig`'s serde defaults so
/// a `FrontendExecutionConfig` built without a config file still validates.
#[derive(Clone, Copy, Debug)]
pub struct FrontendQueryControlTimeouts {
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub init_rpc_timeout_ms: u64,
    pub attach_timeout_ms: u64,
    pub stage_rpc_timeout_ms: u64,
    pub start_rpc_timeout_ms: u64,
    pub terminal_drain_timeout_ms: u64,
    pub terminal_ack_timeout_ms: u64,
    pub pre_start_timeout_ms: u64,
}

impl Default for FrontendQueryControlTimeouts {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 1_000,
            heartbeat_timeout_ms: 5_000,
            init_rpc_timeout_ms: 5_000,
            attach_timeout_ms: 5_000,
            stage_rpc_timeout_ms: 5_000,
            start_rpc_timeout_ms: 2_000,
            terminal_drain_timeout_ms: 30_000,
            terminal_ack_timeout_ms: 5_000,
            pre_start_timeout_ms: 30_000,
        }
    }
}

#[derive(Clone)]
pub struct FrontendExecutionConfig {
    advertised_report_host: String,
    configured_report_port: u16,
    runtime_filter_worker_count: NonZeroUsize,
    mv_scheduler: FrontendMvSchedulerConfig,
    mv_maintenance: MaintenanceCoordinatorConfig,
    /// Cost budget frozen from `[runtime]` and handed to statement admission.
    ///
    /// SQL costing only ever sees the value admission froze; it never consults
    /// a process-global configuration.
    optimizer_query_mem_limit_bytes: u64,
    /// Query-control timeouts frozen from `[runtime]` and handed to the
    /// coordinator, which validates them once at startup instead of per query.
    query_control_timeouts: FrontendQueryControlTimeouts,
    catalog_projection: CatalogProjectionConfig,
}

impl FrontendExecutionConfig {
    pub fn new(
        advertised_report_host: impl Into<String>,
        configured_report_port: u16,
        runtime_filter_worker_count: NonZeroUsize,
    ) -> Self {
        Self {
            advertised_report_host: advertised_report_host.into(),
            configured_report_port,
            runtime_filter_worker_count,
            mv_scheduler: FrontendMvSchedulerConfig::default(),
            mv_maintenance: MaintenanceCoordinatorConfig::default(),
            optimizer_query_mem_limit_bytes: DEFAULT_OPTIMIZER_QUERY_MEM_LIMIT_BYTES,
            query_control_timeouts: FrontendQueryControlTimeouts::default(),
            catalog_projection: CatalogProjectionConfig::default(),
        }
    }

    pub fn with_query_control_timeouts(mut self, timeouts: FrontendQueryControlTimeouts) -> Self {
        self.query_control_timeouts = timeouts;
        self
    }

    pub fn with_optimizer_query_mem_limit_bytes(mut self, bytes: u64) -> Self {
        self.optimizer_query_mem_limit_bytes = bytes;
        self
    }

    pub(crate) fn optimizer_query_mem_limit_bytes(&self) -> u64 {
        self.optimizer_query_mem_limit_bytes
    }

    pub fn with_mv_scheduler_config(mut self, config: FrontendMvSchedulerConfig) -> Self {
        self.mv_scheduler = config;
        self
    }

    pub fn with_mv_maintenance_config(mut self, config: MaintenanceCoordinatorConfig) -> Self {
        self.mv_maintenance = config;
        self
    }

    pub(crate) fn with_catalog_projection_config(
        mut self,
        config: CatalogProjectionConfig,
    ) -> Self {
        self.catalog_projection = config;
        self
    }
}

impl FrontendApplicationHost {
    pub async fn open(
        config: Option<StateStoreHostConfig>,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
    ) -> Result<Self, FrontendApplicationError> {
        Self::open_with_factories(config, execution, backend, Vec::new()).await
    }

    pub async fn open_with_factories(
        config: Option<StateStoreHostConfig>,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        connector_factories: Vec<Arc<dyn ConnectorControlFactory>>,
    ) -> Result<Self, FrontendApplicationError> {
        let catalog_runtime_projection =
            crate::catalog_application::CatalogRuntimeProjection::new();
        let mut host = Self {
            connector_control: Arc::new(
                ConnectorControlHost::with_factories(connector_factories).map_err(|error| {
                    FrontendApplicationError::new(
                        FrontendApplicationErrorKind::ConnectorControlHost,
                        error,
                    )
                })?,
            ),
            catalog_runtime_projection,
            statistics_service: None,
            dml_service: None,
            ctas_recovery_binding: None,
            statistics_application_service: None,
            statistics_application_port: None,
            catalog_application_port: None,
            catalog_controller: None,
            view_service: None,
            table_maintenance_service: None,
            mv_repository: None,
            mv_application_service: None,
            mv_service: None,
            mv_refresh_provider_activation: None,
            mv_background_engine_sink: None,
            state_store_host: None,
            coordination: None,
            query_execution: None,
            query_control: FrontendQueryControl::service(),
            coordinator: None,
            execution_role: backend.role(),
            topology: None,
            optimizer_query_mem_limit_bytes: DEFAULT_OPTIMIZER_QUERY_MEM_LIMIT_BYTES,
        };

        if let Some(config) = config
            && let Err(error) = host.open_configured(config).await
        {
            return Err(host.cleanup_open_error(error).await);
        }
        if let Some(store) = host.state_store() {
            match FrontendCoordinationRuntime::open(store).await {
                Ok(coordination) => host.coordination = Some(Arc::new(coordination)),
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CoordinationOpen,
                            error,
                        ))
                        .await);
                }
            }
        }
        host.catalog_application_port = match host.state_store() {
            Some(store) => match CatalogAttachmentRepository::open(store).await {
                Ok(repository) => Some(Arc::new(FrontendCatalogApplicationPort::new(
                    repository,
                    Arc::clone(&host.connector_control),
                    host.catalog_runtime_projection.publisher(),
                    tokio::runtime::Handle::current(),
                ))),
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                            error,
                        ))
                        .await);
                }
            },
            None => Some(Arc::new(FrontendCatalogApplicationPort::unavailable(
                Arc::clone(&host.connector_control),
                host.catalog_runtime_projection.publisher(),
                tokio::runtime::Handle::current(),
            ))),
        };
        if let Some(store) = host.state_store() {
            let controller = match FrontendCatalogController::new(
                store,
                Arc::clone(
                    host.catalog_application_port
                        .as_ref()
                        .expect("catalog application port is installed"),
                ),
                execution.catalog_projection.clone(),
            ) {
                Ok(controller) => controller,
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogControllerOpen,
                            error,
                        ))
                        .await);
                }
            };
            if let Err(error) = controller.bootstrap().await {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::CatalogControllerOpen,
                        error,
                    ))
                    .await);
            }
            if let Err(error) = controller.start() {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::CatalogControllerOpen,
                        error,
                    ))
                    .await);
            }
            host.catalog_controller = Some(controller);
        }
        match ClusterBackendService::open(
            backend,
            host.state_store(),
            tokio::runtime::Handle::current(),
        )
        .await
        {
            Ok(topology) => host.topology = Some(topology),
            Err(error) => {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::ClusterBackendOpen,
                        error,
                    ))
                    .await);
            }
        }
        host.statistics_service = Some(Arc::new(FrontendStatisticsService::new()));
        let journal = match host.state_store() {
            Some(store) => {
                match StateStoreOperationJournal::open(store, tokio::runtime::Handle::current())
                    .await
                {
                    Ok(journal) => Some(Arc::new(journal) as Arc<dyn crate::dml::OperationJournal>),
                    Err(error) => {
                        return Err(host
                            .cleanup_open_error(FrontendApplicationError::new(
                                FrontendApplicationErrorKind::DmlServiceOpen,
                                error,
                            ))
                            .await);
                    }
                }
            }
            None => None,
        };
        let statistics = host.statistics_service();
        let connector_control = host.connector_control_registry();
        host.dml_service = Some(Arc::new({
            let mut dml = match host.coordination() {
                Some(coordination) => DmlService::compose_with_coordination(
                    journal,
                    statistics,
                    coordination,
                    tokio::runtime::Handle::current(),
                ),
                None => DmlService::compose(journal, statistics),
            };
            // The bounded recovery controller can only classify a historical
            // TRUNCATE or ADD FILES through the current provider generation's
            // separately installed historical facet, so it is given the
            // control registry rather than any process-global state.
            dml.install_statement_recovery(
                crate::dml::statement_recovery::control_registry_resolver(Arc::clone(
                    &connector_control,
                )),
            );
            dml.install_ctas_write_recovery(crate::dml::write_recovery::control_registry_resolver(
                connector_control,
            ));
            dml
        }));
        host.ctas_recovery_binding = Some(CtasRecoveryBinding::new(
            host.dml_service(),
            host.coordination.is_some(),
        ));
        match FrontendViewService::open(host.state_store(), tokio::runtime::Handle::current()).await
        {
            Ok(view_service) => host.view_service = Some(Arc::new(view_service)),
            Err(error) => {
                let error = FrontendApplicationError::new(
                    FrontendApplicationErrorKind::ViewServiceOpen,
                    error,
                );
                return Err(host.cleanup_open_error(error).await);
            }
        }
        // A frontend that owns durable maintenance records also owns the lease
        // authority for them. `coordination` is present whenever a StateStore
        // is, so this never installs an unfenced durable owner.
        let table_maintenance_open = match host.coordination() {
            Some(coordination) => {
                FrontendTableMaintenanceService::open_with_coordination(
                    host.state_store(),
                    tokio::runtime::Handle::current(),
                    MaintenanceCoordination::from_frontend(
                        coordination.as_ref(),
                        tokio::runtime::Handle::current(),
                    ),
                )
                .await
            }
            None => {
                FrontendTableMaintenanceService::open(
                    host.state_store(),
                    tokio::runtime::Handle::current(),
                )
                .await
            }
        };
        match table_maintenance_open {
            Ok(service) => host.table_maintenance_service = Some(Arc::new(service)),
            Err(error) => {
                let error = FrontendApplicationError::new(
                    FrontendApplicationErrorKind::TableMaintenanceServiceOpen,
                    error,
                );
                return Err(host.cleanup_open_error(error).await);
            }
        }
        // The coordinator owns the immutable execution and connector-control
        // context consumed by frontend application services. Install it before
        // constructing those services so MV refresh never observes an
        // all-in-one-only direct execution fallback.
        host.optimizer_query_mem_limit_bytes = execution.optimizer_query_mem_limit_bytes();
        if let Err(error) = host.open_coordinator(execution.clone()) {
            return Err(host.cleanup_open_error(error).await);
        }
        match host.state_store() {
            Some(store) => {
                // The MV repository must observe the exact attachment version an
                // admitted catalog was resolved from, so a durable MV definition
                // can never outlive the attachment it references.
                let attachment_observations = host.catalog_application_port.as_ref().map(|port| {
                    Arc::clone(port)
                        as Arc<dyn crate::mv::repository::CatalogAttachmentObservationSource>
                });
                // Cluster-wide refresh ownership. The refresh path registers with
                // this registry before creating durable state, so installing it as
                // the repository's fence source below makes every durable refresh
                // transition prove ownership inside its own transaction.
                let ownership = match crate::mv::coordination::MvRefreshOwnershipContext::open(
                    Arc::clone(&store),
                )
                .await
                {
                    Ok(ownership) => Some(ownership),
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "frontend MV refresh ownership coordination unavailable; \
                             refreshes remain single-owner"
                        );
                        None
                    }
                };
                // Installing the registry as the repository's fence source is what
                // makes ownership binding: every durable refresh transition then
                // proves ownership inside its own transaction, so a superseded
                // owner's write fails at commit rather than racing.
                //
                // This must land together with the refresh path's acquisition, and
                // it does -- the registry is fail-closed for unregistered targets,
                // so installing it without acquisition refuses every refresh in the
                // cluster. `installing_a_fence_source_without_registration_stops_\
                // every_refresh` guards that combination.
                let refresh_fence = ownership.as_ref().map(|context| context.registry());
                match StateStoreMvRepository::open_with_observations_and_refresh_fence(
                    store,
                    tokio::runtime::Handle::current(),
                    attachment_observations,
                    refresh_fence,
                )
                .await
                {
                    Ok(repository) => {
                        let repository: Arc<dyn crate::mv::domain::repository::MvRepository> =
                            repository;
                        let provider_activation =
                            Arc::new(FrontendMvRefreshProviderActivationPort::new());
                        let service = Arc::new(FrontendMvService::with_refresh_dependencies(
                            Arc::clone(&repository),
                            host.query_execution_service(),
                            host.connector_control_registry(),
                            Arc::clone(&provider_activation),
                            host.execution_role,
                            host.backend_topology_port(),
                            execution.mv_scheduler.clone(),
                            execution.mv_maintenance.clone(),
                            host.table_maintenance_service(),
                            execution.optimizer_query_mem_limit_bytes(),
                            ownership,
                        ));
                        host.mv_background_engine_sink = Some(
                            FrontendMvService::background_engine_sink(Arc::clone(&service)),
                        );
                        let application_service: Arc<
                            dyn crate::mv::domain::application::MvApplicationService,
                        > = Arc::clone(&service)
                            as Arc<dyn crate::mv::domain::application::MvApplicationService>;
                        host.mv_application_service = Some(application_service);
                        host.mv_service = Some(service);
                        host.mv_repository = Some(repository);
                        host.mv_refresh_provider_activation = Some(provider_activation);
                    }
                    Err(error) => {
                        return Err(host
                            .cleanup_open_error(FrontendApplicationError::new(
                                FrontendApplicationErrorKind::MvServiceOpen,
                                error,
                            ))
                            .await);
                    }
                }
            }
            None => {
                let repository: Arc<dyn crate::mv::domain::repository::MvRepository> =
                    Arc::new(crate::mv::domain::repository::UnavailableMvRepository);
                let service = Arc::new(FrontendMvService::new(Arc::clone(&repository)));
                let application_service: Arc<
                    dyn crate::mv::domain::application::MvApplicationService,
                > = Arc::clone(&service)
                    as Arc<dyn crate::mv::domain::application::MvApplicationService>;
                host.mv_repository = Some(repository);
                host.mv_application_service = Some(application_service);
                host.mv_service = Some(service);
            }
        }
        if let Err(error) = host.topology().start_heartbeat_manager().map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::ClusterBackendOpen, error)
        }) {
            return Err(host.cleanup_open_error(error).await);
        }
        host.statistics_application_service = match host.state_store() {
            Some(store) => match StatisticsJobRepository::open(store).await {
                Ok(repository) => Some(Arc::new(StatisticsApplicationService::with_repository(
                    repository,
                ))),
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::StatisticsApplicationServiceOpen,
                            error,
                        ))
                        .await);
                }
            },
            None => Some(Arc::new(StatisticsApplicationService::unavailable())),
        };
        let statistics_application_port = match host.coordination() {
            Some(coordination) => FrontendStatisticsApplicationPort::with_frontend_coordination(
                host.statistics_application_service().as_ref().clone(),
                tokio::runtime::Handle::current(),
                coordination.as_ref(),
            )
            .map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::StatisticsApplicationServiceOpen,
                    error,
                )
            }),
            None => Ok(FrontendStatisticsApplicationPort::new(
                host.statistics_application_service().as_ref().clone(),
                tokio::runtime::Handle::current(),
            )),
        };
        match statistics_application_port {
            Ok(port) => host.statistics_application_port = Some(Arc::new(port)),
            Err(error) => return Err(host.cleanup_open_error(error).await),
        }

        Ok(host)
    }

    pub fn view_service(&self) -> Arc<dyn crate::view::ViewService> {
        Arc::clone(
            self.view_service
                .as_ref()
                .expect("frontend view service is installed before host open returns"),
        )
    }

    pub fn statistics_service(&self) -> Arc<FrontendStatisticsService> {
        self.statistics_service
            .as_ref()
            .expect("frontend statistics service is installed before host open returns")
            .clone()
    }

    pub fn dml_service(&self) -> Arc<DmlService> {
        Arc::clone(
            self.dml_service
                .as_ref()
                .expect("frontend DML service is installed before host open returns"),
        )
    }

    /// Returns the one-shot ordered CTAS recovery binding owned by this host.
    /// Server composition must complete it before it exposes the SQL listener.
    pub(crate) fn ctas_recovery_binding(&self) -> CtasRecoveryBinding {
        self.ctas_recovery_binding
            .as_ref()
            .expect("frontend CTAS recovery binding is installed before host open returns")
            .clone()
    }

    pub fn statistics_application_service(&self) -> Arc<StatisticsApplicationService> {
        Arc::clone(
            self.statistics_application_service
                .as_ref()
                .expect("statistics application service is installed before host open returns"),
        )
    }

    pub fn statistics_application_port(&self) -> Arc<FrontendStatisticsApplicationPort> {
        Arc::clone(
            self.statistics_application_port
                .as_ref()
                .expect("statistics application port is installed before host open returns"),
        )
    }

    pub fn catalog_application_port(
        &self,
    ) -> Arc<dyn crate::catalog_application::CatalogApplicationPort> {
        let application = Arc::clone(
            self.catalog_application_port
                .as_ref()
                .expect("catalog application port is installed before host open returns"),
        ) as Arc<dyn crate::catalog_application::CatalogApplicationPort>;
        self.catalog_runtime_projection
            .bind_application(application)
    }

    /// The publication set Core binds its query catalog registry to.
    ///
    /// It is handed out alongside `catalog_application_port` so a durable
    /// attachment only becomes a resolvable SQL name after this process
    /// published its exact local runtime generation.
    pub fn catalog_runtime_projection(
        &self,
    ) -> Arc<crate::catalog_application::CatalogRuntimeProjection> {
        Arc::clone(&self.catalog_runtime_projection)
    }

    pub fn table_maintenance_service(&self) -> Arc<dyn TableMaintenanceService> {
        Arc::clone(
            self.table_maintenance_service
                .as_ref()
                .expect("frontend table-maintenance service is installed before host open returns"),
        )
    }

    pub fn mv_repository(&self) -> Arc<dyn crate::mv::domain::repository::MvRepository> {
        Arc::clone(
            self.mv_repository
                .as_ref()
                .expect("frontend MV repository is installed before host open returns"),
        )
    }

    pub fn mv_application_service(
        &self,
    ) -> Arc<dyn crate::mv::domain::application::MvApplicationService> {
        Arc::clone(
            self.mv_application_service
                .as_ref()
                .expect("frontend MV application service is installed before host open returns"),
        )
    }

    pub fn mv_service(&self) -> Arc<FrontendMvService> {
        Arc::clone(
            self.mv_service
                .as_ref()
                .expect("frontend MV service is installed before host open returns"),
        )
    }

    pub fn mv_refresh_provider_activation_sink(
        &self,
    ) -> Option<Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivationSink>>
    {
        self.mv_refresh_provider_activation.as_ref().map(|port| {
            Arc::clone(port) as Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivationSink>
        })
    }

    pub(crate) fn mv_background_engine_sink(
        &self,
    ) -> Option<Arc<dyn crate::mv::background::MvBackgroundEngineSink>> {
        self.mv_background_engine_sink.as_ref().map(Arc::clone)
    }

    pub fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        self.state_store_host
            .as_ref()
            .and_then(StateStoreHost::state_store)
    }

    pub(crate) fn coordination(&self) -> Option<Arc<FrontendCoordinationRuntime>> {
        self.coordination.as_ref().map(Arc::clone)
    }

    pub fn execution_role(&self) -> novarocks_types::ClusterRole {
        self.execution_role
    }

    /// Cost budget frozen from `[runtime]`, handed to statement admission.
    pub fn optimizer_query_mem_limit_bytes(&self) -> u64 {
        self.optimizer_query_mem_limit_bytes
    }

    pub fn connector_control_registry(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> {
        Arc::clone(&self.connector_control)
            as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>
    }

    pub fn connector_control_factory_resolver(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorControlFactoryResolver> {
        Arc::clone(&self.connector_control)
            as Arc<dyn novarocks_spi::connector::ConnectorControlFactoryResolver>
    }

    pub fn state_store_provider_id(&self) -> Option<StateStoreProviderId> {
        self.state_store_host
            .as_ref()
            .map(StateStoreHost::provider_id)
    }

    pub fn query_execution_service(&self) -> QueryExecutionService {
        self.query_execution
            .as_ref()
            .expect("frontend query execution service is installed before host open returns")
            .clone()
    }

    pub fn query_control_service(&self) -> crate::query_execution::control::QueryControlService {
        self.query_control.clone()
    }

    pub fn terminal_ingress(&self) -> Arc<dyn crate::query_lifecycle::QueryTerminalIngress> {
        Arc::new(
            self.coordinator
                .as_ref()
                .expect("frontend coordinator is installed before host open returns")
                .terminal_ingress(),
        )
    }

    pub(crate) fn lifecycle_convergence_reader(&self) -> Arc<dyn QueryLifecycleConvergenceReader> {
        self.coordinator
            .as_ref()
            .expect("frontend coordinator is installed before host open returns")
            .convergence_reader()
    }

    #[cfg(test)]
    pub(crate) fn execute_distributed_query_for_test(
        &self,
        request: crate::query_execution::contract::DistributedQueryRequest,
    ) -> Result<
        crate::query_execution::contract::DistributedQueryOutcome,
        crate::query_execution::contract::DistributedQueryError,
    > {
        crate::query_execution::contract::DistributedQueryCoordinator::execute(
            self.coordinator
                .as_ref()
                .expect("frontend coordinator is installed before host open returns")
                .as_ref(),
            request,
        )
    }

    pub fn backend_query_activity(&self) -> BackendQueryActivity {
        self.coordinator
            .as_ref()
            .expect("frontend coordinator is installed before host open returns")
            .backend_query_activity()
    }

    pub fn backend_query_event_sink(
        &self,
    ) -> Arc<dyn crate::common::backend_topology::BackendQueryEventSink> {
        Arc::new(self.backend_query_activity())
    }

    pub fn coordinator_report_endpoint_sink(
        &self,
    ) -> Arc<dyn crate::common::backend_topology::CoordinatorReportEndpointSink> {
        self.coordinator
            .as_ref()
            .expect("frontend coordinator is installed before host open returns")
            .report_endpoint_sink()
    }

    /// Frontend composition-time topology leaf.  The all-in-one server uses it
    /// only to register its separately owned backend endpoint before it builds
    /// the same ready SQL session factory as role=fe.
    pub fn backend_topology_port(&self) -> crate::common::backend_topology::BackendTopologyService {
        Arc::clone(self.topology()) as crate::common::backend_topology::BackendTopologyService
    }

    pub async fn shutdown(mut self) -> Result<(), FrontendApplicationError> {
        self.release_resources().await.map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::Shutdown, error)
        })
    }

    async fn open_configured(
        &mut self,
        config: StateStoreHostConfig,
    ) -> Result<(), FrontendApplicationError> {
        let source = SqliteSingleFeDeploymentViewSource::try_from_state_store_config(
            &config.state_store.store,
        )
        .map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::DeploymentSource, error)
        })?;
        let deployment = source.snapshot().await.map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::DeploymentSource, error)
        })?;
        let registry = builtin_state_store_provider_registry().map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::StateStoreHost, error)
        })?;
        self.state_store_host = Some(
            StateStoreHost::open(
                &registry,
                config,
                deployment,
                Instant::now() + STATE_STORE_OPEN_TIMEOUT,
            )
            .await
            .map_err(|error| {
                FrontendApplicationError::new(FrontendApplicationErrorKind::StateStoreHost, error)
            })?,
        );

        Ok(())
    }

    fn open_coordinator(
        &mut self,
        execution: FrontendExecutionConfig,
    ) -> Result<(), FrontendApplicationError> {
        let coordinator = Arc::new(
            FrontendDistributedQueryCoordinator::new(
                execution.advertised_report_host,
                execution.configured_report_port,
                execution.runtime_filter_worker_count,
                execution.query_control_timeouts,
                self.backend_topology_port(),
                Arc::clone(&self.connector_control),
            )
            .map_err(FrontendApplicationError::server)?,
        );
        self.topology()
            .attach_query_events(Arc::new(coordinator.backend_query_activity()));
        self.query_execution = Some(QueryExecutionService::new(coordinator.clone()));
        self.coordinator = Some(coordinator);
        Ok(())
    }

    async fn cleanup_open_error(
        &mut self,
        primary: FrontendApplicationError,
    ) -> FrontendApplicationError {
        match self.release_resources().await {
            Ok(()) => primary,
            Err(cleanup_error) => primary.with_cleanup_context(cleanup_error),
        }
    }

    async fn release_resources(&mut self) -> Result<(), String> {
        // The worker owns durable attempt activity and must stop before the
        // coordinator/topology/StateStore it depends on are released.
        let mv_worker_error = self
            .mv_service
            .as_ref()
            .and_then(|service| service.shutdown_background_workers().err())
            .map(|error| format!("shutdown frontend MV background workers failed: {error}"));
        let statistics_worker_error = self
            .statistics_application_port
            .as_ref()
            .and_then(|port| port.shutdown_worker().err())
            .map(|error| format!("shutdown statistics analyze worker failed: {error}"));
        let mut primary_error = mv_worker_error;
        if let Some(error) = primary_error.take() {
            // The MV workers still own request/topology/StateStore references.
            // Preserve them so a caller sees the explicit shutdown failure
            // rather than pretending that teardown completed.
            return Err(error);
        }
        let table_maintenance_error = self
            .table_maintenance_service
            .as_ref()
            .and_then(|service| service.shutdown().err())
            .map(|error| format!("shutdown frontend table-maintenance service failed: {error}"));
        self.query_execution.take();
        self.coordinator.take();
        let heartbeat_result = self
            .topology
            .as_ref()
            .map(|topology| topology.stop_heartbeat_manager())
            .transpose();
        if let Some(topology) = self.topology.as_ref() {
            topology.detach_query_events();
        }
        self.topology.take();
        primary_error = heartbeat_result.err();
        if let Some(statistics_worker_error) = statistics_worker_error {
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {statistics_worker_error}"));
            } else {
                primary_error = Some(statistics_worker_error);
            }
        }
        if let Some(table_maintenance_error) = table_maintenance_error {
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {table_maintenance_error}"));
            } else {
                primary_error = Some(table_maintenance_error);
            }
        }
        if let Some(binding) = self.ctas_recovery_binding.as_ref() {
            binding.shutdown().await;
        }
        self.ctas_recovery_binding.take();
        let dml_coordination_error = if let Some(service) = self.dml_service.as_ref() {
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                service.shutdown_coordination(),
            )
            .await
            {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(format!(
                    "shutdown frontend DML coordination failed: {error}"
                )),
                Err(_) => {
                    Some("shutdown frontend DML coordination exceeded the 5s deadline".to_string())
                }
            }
        } else {
            None
        };
        if let Some(dml_coordination_error) = dml_coordination_error {
            if let Some(mut primary) = primary_error.take() {
                primary.push_str(&format!("; cleanup failed: {dml_coordination_error}"));
                // A timed-out or failed authority drain may still own the DML
                // service, coordination runtime, and StateStore. Preserve
                // those dependencies so the caller cannot mistake a partial
                // teardown for a completed shutdown.
                return Err(primary);
            } else {
                // See the branch above: no dependent resource is released
                // after an incomplete DML authority drain.
                return Err(dml_coordination_error);
            }
        }
        self.dml_service.take();
        self.table_maintenance_service.take();
        self.statistics_service.take();
        // The durable application service owns a StateStore reference through
        // its repository, so it must be released before StateStoreHost closes
        // its deployment lock.
        self.statistics_application_port.take();
        self.statistics_application_service.take();
        let catalog_controller_error = match self.catalog_controller.take() {
            Some(controller) => controller.shutdown().await.err(),
            None => None,
        };
        self.catalog_application_port.take();
        self.view_service.take();
        self.mv_application_service.take();
        self.mv_service.take();
        self.mv_refresh_provider_activation.take();
        self.mv_background_engine_sink.take();
        self.mv_repository.take();
        self.coordination.take();
        if let Some(catalog_controller_error) = catalog_controller_error {
            let error = format!("shutdown catalog controller failed: {catalog_controller_error}");
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {error}"));
            } else {
                primary_error = Some(error);
            }
        }
        if let Some(host) = self.state_store_host.as_mut() {
            if let Err(error) = host
                .shutdown(Instant::now() + STATE_STORE_SHUTDOWN_TIMEOUT)
                .await
            {
                let host_error = format!("shutdown frontend StateStore host failed: {error}");
                if let Some(primary) = primary_error.as_mut() {
                    primary.push_str(&format!("; cleanup failed: {host_error}"));
                } else {
                    primary_error = Some(host_error);
                }
            }
            self.state_store_host.take();
        }
        primary_error.map_or(Ok(()), Err)
    }

    fn topology(&self) -> &Arc<ClusterBackendService> {
        self.topology
            .as_ref()
            .expect("frontend cluster backend service is installed before host open returns")
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use bytes::Bytes;
    use novarocks_spi::state_store::{
        FeDeploymentView, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest,
        StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderInstance,
    };
    use novarocks_state_store::{
        SQLITE_STATE_STORE_PROVIDER_ID, StateStoreAppConfig, StateStoreConfig, StateStoreHost,
        StateStoreHostConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
        StateStoreProviderRegistration, StateStoreProviderRegistry,
    };

    use super::{
        FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
        FrontendExecutionConfig,
    };

    const SECRET_CONFIG_VALUE: &str = "client-secret-must-not-leak";
    const DESCRIPTOR: StateStoreProviderDescriptor =
        StateStoreProviderDescriptor::new(SQLITE_STATE_STORE_PROVIDER_ID);

    struct FailingFactory;

    #[async_trait]
    impl StateStoreProviderFactory for FailingFactory {
        fn descriptor(&self) -> &StateStoreProviderDescriptor {
            &DESCRIPTOR
        }

        async fn open(
            self: Box<Self>,
            _request: StateStoreOpenRequest,
        ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
            Err(StateStoreError::new(
                StateStoreErrorKind::Corruption,
                "injected provider primary failure",
            )
            .with_cleanup_context(StateStoreError::new(
                StateStoreErrorKind::DeadlineExceeded,
                "injected provider cleanup failure",
            )))
        }
    }

    #[tokio::test]
    async fn frontend_stringification_preserves_host_primary_and_cleanup_context() {
        let temp = tempfile::tempdir().expect("temporary host diagnostics directory");
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::available(
                SQLITE_STATE_STORE_PROVIDER_ID,
                novarocks_spi::state_store::MAX_KEY_BYTES,
                |_| Ok(Box::new(FailingFactory)),
            ))
            .expect("register diagnostic provider");
        let host_error = match StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "diagnostic-cluster".to_owned(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: temp.path().join("state-store.sqlite"),
                            deployment_owner: SECRET_CONFIG_VALUE.to_owned(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).unwrap(),
                topology_revision: Bytes::from_static(b"topology-r1"),
            },
            Instant::now() + Duration::from_secs(1),
        )
        .await
        {
            Ok(_) => panic!("injected provider failure must reject host open"),
            Err(error) => error,
        };

        assert_eq!(
            host_error.primary().map(StateStoreError::kind),
            Some(StateStoreErrorKind::Corruption)
        );
        let frontend_error =
            FrontendApplicationError::new(FrontendApplicationErrorKind::StateStoreHost, host_error);
        let diagnostic = frontend_error.to_string();

        assert!(diagnostic.contains("StateStoreHost"));
        assert!(diagnostic.contains("Open (sqlite)"));
        assert!(diagnostic.contains("injected provider primary failure"));
        assert!(diagnostic.contains("injected provider cleanup failure"));
        assert!(!diagnostic.contains(SECRET_CONFIG_VALUE));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_bootstraps_and_stops_catalog_projection_with_state_store() {
        let temp = tempfile::tempdir().expect("temporary catalog controller directory");
        let state_store = StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id: "catalog-controller-host-test".to_string(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Sqlite {
                        path: temp.path().join("state-store.sqlite"),
                        deployment_owner: "catalog-controller-host-test".to_string(),
                    },
                },
                mysql_client: None,
            },
            foundationdb_client: None,
        };
        let backend = crate::topology::ClusterBackendOpenConfig::new(
            novarocks_types::ClusterRole::AllInOne,
            Vec::new(),
            Duration::from_secs(1),
            1,
            Duration::from_secs(1),
        )
        .expect("valid all-in-one backend config");
        let host = FrontendApplicationHost::open(
            Some(state_store),
            FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
            ),
            backend,
        )
        .await
        .expect("host opens with the catalog controller");
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse("warehouse").expect("instance id");
        assert!(matches!(
            crate::catalog_application::CatalogApplicationPort::admit_catalog(
                host.catalog_application_port().as_ref(),
                &instance_id,
            ),
            crate::catalog_application::CatalogAdmission::Absent
        ));
        host.shutdown().await.expect("host shutdown");
    }
}
