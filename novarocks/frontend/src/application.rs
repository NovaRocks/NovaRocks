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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;

use crate::query_execution::service::QueryExecutionService;
use crate::state_store::{StateStoreHost, StateStoreHostInput, StateStoreProviderRegistry};
use novarocks_native_trust::NativeTrust;
use novarocks_spi::connector::ConnectorControlFactory;
use novarocks_spi::state_store::{StateStore, StateStoreProviderId};

use crate::catalog_application::desired_state::{
    CatalogDesiredStateSource, CatalogDesiredStateSourceMode,
};
use crate::catalog_application::{
    CatalogDesiredStateSnapshot, CatalogDesiredStateSourceInput, FrontendCatalogApplicationPort,
};
use crate::catalog_attachment::CatalogAttachmentRepository;
use crate::catalog_controller::{CatalogProjectionConfig, FrontendCatalogController};
use crate::common::admitted_query_context::LakePublicationRuntimePolicy;
use crate::connector::ConnectorControlHost;
use crate::coordinator::{FrontendDistributedQueryCoordinator, QueryLifecycleConvergenceReader};
use crate::dml::DmlService;
use crate::mv::maintenance::MaintenanceCoordinatorConfig;
use crate::mv::scheduler::FrontendMvSchedulerConfig;
use crate::mv::{
    FrontendMvRefreshProviderActivationPort, FrontendMvService, repository::StateStoreMvRepository,
};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::transport::FrontendNativeTransport;
use crate::query_control::FrontendQueryControl;
use crate::query_execution::maintenance::TableMaintenanceService;
use crate::statistics::FrontendStatisticsService;
use crate::statistics_jobs::service::{
    FrontendStatisticsApplicationPort, StatisticsApplicationService,
};
use crate::table_maintenance::FrontendTableMaintenanceService;
use crate::topology::{ClusterBackendOpenConfig, ClusterBackendService};
use crate::view::FrontendViewService;
use crate::workload_lifecycle::{
    FrontendCatalogCounts, FrontendCatalogSnapshotIdentity, FrontendCatalogSourceMode,
    FrontendServingLifecycle,
};

const STATE_STORE_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const STATE_STORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
fn test_native_trust() -> Arc<NativeTrust> {
    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, ValidatedSharedSecret,
    };

    Arc::new(NativeTrust::new(
        DeploymentId::parse("frontend-application-test").expect("fixed test deployment id"),
        ValidatedSharedSecret::new(novarocks_secret::SecretValue::new(
            "0123456789abcdef0123456789abcdef",
        ))
        .expect("fixed test shared secret"),
        NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("fixed test caller subject"),
        NativeTransportMode::Disabled,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendApplicationErrorKind {
    DeploymentSource,
    StateStoreHost,
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
    /// Coordinator-side typed connector controls, keyed by the exact binding
    /// generation. The composition root hands in the same instance its control
    /// factories install into, so planning and installation never disagree
    /// about which generation exists.
    typed_connector_control:
        Arc<crate::connector::typed_control_registry::ConnectorReadControlRegistry>,
    catalog_runtime_projection: Arc<crate::catalog_application::CatalogRuntimeProjection>,
    serving_lifecycle: Arc<FrontendServingLifecycle>,
    statistics_service: Option<Arc<FrontendStatisticsService>>,
    dml_service: Option<Arc<DmlService>>,
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
    query_execution: Option<QueryExecutionService>,
    query_control: crate::query_execution::control::QueryControlService,
    coordinator: Option<Arc<FrontendDistributedQueryCoordinator>>,
    execution_role: novarocks_types::ClusterRole,
    data_runtime: FrontendDataRuntime,
    topology: Option<Arc<ClusterBackendService>>,
    optimizer_query_mem_limit_bytes: u64,
    lake_publication_runtime_policy: LakePublicationRuntimePolicy,
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
    lake_publication_runtime_policy: LakePublicationRuntimePolicy,
    catalog_projection: CatalogProjectionConfig,
    /// The deployment's catalog desired-state authority, selected and validated
    /// once before this application opens any runtime resource.
    ///
    /// Frozen here rather than consulted per statement so that "which authority
    /// owns catalog desired state" cannot change while the process runs, and so
    /// an unimplemented mode is rejected before startup opens anything.
    catalog_desired_state_source: CatalogDesiredStateSourceInput,
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
            lake_publication_runtime_policy: LakePublicationRuntimePolicy::try_new(
                Duration::from_secs(30 * 60),
                Duration::from_secs(45 * 60),
                Duration::from_secs(60),
                Duration::from_secs(5 * 60),
                Duration::from_secs(60),
            )
            .expect("default lake publication policy is safe"),
            catalog_projection: CatalogProjectionConfig::default(),
            // This constructor remains an in-process test convenience. Server
            // composition always replaces it with its preflighted closed input.
            catalog_desired_state_source: CatalogDesiredStateSourceInput::StaticFile(
                CatalogDesiredStateSnapshot::try_new(CatalogDesiredStateSourceMode::StaticFile, [])
                    .expect("an empty static catalog snapshot is valid"),
            ),
        }
    }

    pub fn with_query_control_timeouts(mut self, timeouts: FrontendQueryControlTimeouts) -> Self {
        self.query_control_timeouts = timeouts;
        self
    }

    pub fn with_lake_publication_runtime_policy(
        mut self,
        policy: LakePublicationRuntimePolicy,
    ) -> Self {
        self.lake_publication_runtime_policy = policy;
        self
    }

    pub(crate) const fn lake_publication_runtime_policy(&self) -> LakePublicationRuntimePolicy {
        self.lake_publication_runtime_policy
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

    #[allow(
        dead_code,
        reason = "Retained for frontend application-builder coverage that injects projection timing."
    )]
    pub(crate) fn with_catalog_projection_config(
        mut self,
        config: CatalogProjectionConfig,
    ) -> Self {
        self.catalog_projection = config;
        self
    }

    /// Supplies the already preflighted, closed desired-state source input.
    pub fn with_catalog_desired_state_source(
        mut self,
        source: CatalogDesiredStateSourceInput,
    ) -> Self {
        self.catalog_desired_state_source = source;
        self
    }

    pub const fn catalog_desired_state_source(&self) -> &CatalogDesiredStateSourceInput {
        &self.catalog_desired_state_source
    }
}

impl FrontendApplicationHost {
    pub async fn open(
        state_store: Option<StateStoreHostInput>,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        data_runtime: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, FrontendApplicationError> {
        Self::open_with_factories(
            state_store,
            execution,
            backend,
            Vec::new(),
            data_runtime,
            native_trust,
            native_transport,
        )
        .await
    }

    pub async fn open_with_factories(
        state_store: Option<StateStoreHostInput>,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        connector_factories: Vec<Arc<dyn ConnectorControlFactory>>,
        data_runtime: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, FrontendApplicationError> {
        let registry = StateStoreProviderRegistry::new();
        Self::open_with_factories_and_state_store_registry(
            state_store,
            &registry,
            execution,
            backend,
            connector_factories,
            // No composition root supplied one, so this host owns a private,
            // empty registry: a typed scan then fails to resolve rather than
            // reaching some other host's generation.
            Arc::new(crate::connector::typed_control_registry::ConnectorReadControlRegistry::new()),
            data_runtime,
            native_trust,
            native_transport,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Server composition deliberately supplies StateStore, role execution, and immutable Native trust capabilities independently."
    )]
    pub async fn open_with_factories_and_state_store_registry(
        state_store: Option<StateStoreHostInput>,
        state_store_registry: &StateStoreProviderRegistry,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        connector_factories: Vec<Arc<dyn ConnectorControlFactory>>,
        typed_connector_control: Arc<
            crate::connector::typed_control_registry::ConnectorReadControlRegistry,
        >,
        data_runtime: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, FrontendApplicationError> {
        // The selected catalog desired-state source mode is decided here, ahead
        // of every startup side effect: nothing is open yet, no StateStore host
        // exists, no controller is running, so a mode this build implements no
        // authority for fails with nothing to unwind. Doing this later would
        // mean either a partially opened frontend to clean up or — worse — a
        // path that quietly serves an unimplemented mode from the dynamic
        // StateStore authority.
        let catalog_source_input = execution.catalog_desired_state_source().clone();
        let catalog_source_mode = catalog_source_input.mode();
        if let Err(error) = catalog_source_mode.require_implemented() {
            return Err(FrontendApplicationError::new(
                FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                error,
            ));
        }
        let data_runtime = FrontendDataRuntime::new_with_native_trust(
            data_runtime,
            native_trust,
            native_transport,
        );
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
            typed_connector_control,
            catalog_runtime_projection,
            serving_lifecycle: Arc::new(FrontendServingLifecycle::new()),
            statistics_service: None,
            dml_service: None,
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
            query_execution: None,
            query_control: FrontendQueryControl::service(),
            coordinator: None,
            execution_role: backend.role(),
            data_runtime: data_runtime.clone(),
            topology: None,
            optimizer_query_mem_limit_bytes: DEFAULT_OPTIMIZER_QUERY_MEM_LIMIT_BYTES,
            lake_publication_runtime_policy: execution.lake_publication_runtime_policy(),
        };

        if let Some(state_store) = state_store
            && let Err(error) = host
                .open_configured(state_store, state_store_registry)
                .await
        {
            return Err(host.cleanup_open_error(error).await);
        }
        let attachments = if catalog_source_mode == CatalogDesiredStateSourceMode::DynamicStateStore
        {
            let store = match host.state_store() {
                Some(store) => store,
                None => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                            "the dynamic StateStore catalog source requires a configured Frontend StateStore",
                        ))
                        .await);
                }
            };
            match CatalogAttachmentRepository::open(store).await {
                Ok(repository) => Some(repository),
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                            error,
                        ))
                        .await);
                }
            }
        } else {
            None
        };
        let catalog_source =
            match CatalogDesiredStateSource::from_input(catalog_source_input, attachments) {
                Ok(source) => source,
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                            error,
                        ))
                        .await);
                }
            };
        host.catalog_application_port = Some(Arc::new(FrontendCatalogApplicationPort::new(
            catalog_source,
            Arc::clone(&host.connector_control),
            host.catalog_runtime_projection.publisher(),
            tokio::runtime::Handle::current(),
        )));
        if catalog_source_mode == CatalogDesiredStateSourceMode::DynamicStateStore {
            let store = host
                .state_store()
                .expect("dynamic catalog source required a StateStore above");
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
            let counts = host
                .catalog_application_port
                .as_ref()
                .expect("catalog application port is installed")
                .projection_counts();
            host.serving_lifecycle.publish_catalog_bootstrap(
                FrontendCatalogSourceMode::DynamicStateStore,
                true,
                None,
                FrontendCatalogCounts {
                    desired: counts.ready + counts.unavailable,
                    ready: counts.ready,
                    unavailable: counts.unavailable,
                },
            );
            if let Err(error) = controller.start() {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::CatalogControllerOpen,
                        error,
                    ))
                    .await);
            }
            host.catalog_controller = Some(controller);
        } else if catalog_source_mode == CatalogDesiredStateSourceMode::StaticFile {
            let projection = Arc::clone(
                host.catalog_application_port
                    .as_ref()
                    .expect("catalog application port is installed"),
            );
            match projection
                .reconcile_snapshot_with_page_size(
                    execution.catalog_projection.page_size,
                    execution.catalog_projection.worker_count,
                )
                .await
            {
                Ok((snapshot, counts)) => {
                    host.serving_lifecycle.publish_catalog_bootstrap(
                        FrontendCatalogSourceMode::StaticFile,
                        true,
                        Some(
                            FrontendCatalogSnapshotIdentity::try_new(
                                snapshot.identity().catalog_count(),
                                snapshot.identity().short_digest(),
                            )
                            .expect("catalog snapshot identity has a fixed short digest"),
                        ),
                        FrontendCatalogCounts {
                            desired: snapshot.identity().catalog_count(),
                            ready: counts.ready,
                            unavailable: counts.unavailable,
                        },
                    );
                }
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                            error,
                        ))
                        .await);
                }
            }
        }
        match ClusterBackendService::open(backend, tokio::runtime::Handle::current(), data_runtime)
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
        let statistics = host.statistics_service();
        host.dml_service = Some(Arc::new(DmlService::new(statistics)));
        // Local views are process runtime state, so this service has nothing to
        // load and no store to fail against.
        host.view_service = Some(Arc::new(FrontendViewService::new()));
        let table_maintenance_open = FrontendTableMaintenanceService::open(
            host.state_store(),
            tokio::runtime::Handle::current(),
        )
        .await
        .map(|service| {
            service
                .with_lake_publication_runtime_policy(host.lake_publication_runtime_policy())
                .with_workload_lifecycle((*host.serving_lifecycle()).clone())
        });
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
        host.lake_publication_runtime_policy = execution.lake_publication_runtime_policy();
        if let Err(error) = host.open_coordinator(execution.clone()) {
            return Err(host.cleanup_open_error(error).await);
        }
        match host.state_store() {
            Some(store) => {
                match StateStoreMvRepository::open(store, tokio::runtime::Handle::current()).await {
                    Ok(repository) => {
                        let repository: Arc<dyn crate::mv::domain::repository::MvRepository> =
                            repository;
                        let provider_activation =
                            Arc::new(FrontendMvRefreshProviderActivationPort::new());
                        let service = Arc::new(
                            FrontendMvService::with_refresh_dependencies(
                                Arc::clone(&repository),
                                host.query_execution_service(),
                                Arc::clone(&host.connector_control)
                                    as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
                                Arc::clone(&provider_activation),
                                host.execution_role,
                                host.backend_topology_port(),
                                execution.mv_scheduler.clone(),
                                execution.mv_maintenance.clone(),
                                host.table_maintenance_service(),
                                host.optimizer_query_mem_limit_bytes,
                                Duration::from_secs(30 * 60),
                            )
                            .with_workload_lifecycle((*host.serving_lifecycle()).clone()),
                        );
                        let application_service: Arc<
                            dyn crate::mv::domain::application::MvApplicationService,
                        > = Arc::clone(&service)
                            as Arc<dyn crate::mv::domain::application::MvApplicationService>;
                        host.mv_background_engine_sink = Some(
                            FrontendMvService::background_engine_sink(Arc::clone(&service)),
                        );
                        host.mv_refresh_provider_activation = Some(provider_activation);
                        host.mv_application_service = Some(application_service);
                        host.mv_service = Some(service);
                        host.mv_repository = Some(repository);
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
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::MvServiceOpen,
                        "frontend MV Accelerator requires StateStore",
                    ))
                    .await);
            }
        }
        if let Err(error) = host.topology().start_heartbeat_manager().map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::ClusterBackendOpen, error)
        }) {
            return Err(host.cleanup_open_error(error).await);
        }
        host.statistics_application_service = Some(Arc::new(StatisticsApplicationService::new()));
        let statistics_application_port = Ok(FrontendStatisticsApplicationPort::new(
            host.statistics_application_service().as_ref().clone(),
            tokio::runtime::Handle::current(),
        )
        .with_workload_lifecycle((*host.serving_lifecycle()).clone()));
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

    /// FE-local serving lifecycle shared by SQL and background admission
    /// owners. Server orchestration alone owns its Ready/Draining transitions.
    pub fn serving_lifecycle(&self) -> Arc<FrontendServingLifecycle> {
        Arc::clone(&self.serving_lifecycle)
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

    pub fn execution_role(&self) -> novarocks_types::ClusterRole {
        self.execution_role
    }

    /// Cost budget frozen from `[runtime]`, handed to statement admission.
    pub fn optimizer_query_mem_limit_bytes(&self) -> u64 {
        self.optimizer_query_mem_limit_bytes
    }

    pub fn lake_publication_runtime_policy(&self) -> LakePublicationRuntimePolicy {
        self.lake_publication_runtime_policy
    }

    pub fn connector_control_registry(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> {
        Arc::clone(&self.connector_control)
            as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>
    }

    /// The typed connector control registry this host was composed with.
    ///
    /// It is a constructor-supplied capability, not something a request may
    /// resolve from the host: query preparation receives it once, when the
    /// session factory is built.
    pub fn typed_connector_control(
        &self,
    ) -> Arc<crate::connector::typed_control_registry::ConnectorReadControlRegistry> {
        Arc::clone(&self.typed_connector_control)
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

    pub fn terminal_ingress(&self) -> Arc<dyn crate::coordinator::QueryTerminalIngress> {
        Arc::new(
            self.coordinator
                .as_ref()
                .expect("frontend coordinator is installed before host open returns")
                .terminal_ingress(),
        )
    }

    pub(crate) fn backend_membership_ingress(&self) -> Arc<ClusterBackendService> {
        Arc::clone(self.topology())
    }

    pub fn start_report_server(
        &self,
        bind_addr: std::net::SocketAddr,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<crate::native::report_server::FrontendReportServerHandle, FrontendApplicationError>
    {
        crate::native::report_server::FrontendReportServerHandle::start(
            bind_addr,
            self.terminal_ingress(),
            self.backend_membership_ingress(),
            self.lifecycle_convergence_reader(),
            native_trust,
            native_transport,
        )
        .map_err(FrontendApplicationError::server)
    }

    pub(crate) fn start_report_server_from_host(
        &self,
        host: &str,
        port: u16,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<crate::native::report_server::FrontendReportServerHandle, FrontendApplicationError>
    {
        crate::native::report_server::FrontendReportServerHandle::start_from_host(
            host,
            port,
            self.terminal_ingress(),
            self.backend_membership_ingress(),
            self.lifecycle_convergence_reader(),
            native_trust,
            native_transport,
        )
        .map_err(FrontendApplicationError::server)
    }

    pub(crate) fn lifecycle_convergence_reader(&self) -> Arc<dyn QueryLifecycleConvergenceReader> {
        self.coordinator
            .as_ref()
            .expect("frontend coordinator is installed before host open returns")
            .convergence_reader()
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Retained as the integration-test entry point for the frontend coordinator."
    )]
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

    pub fn coordinator_report_endpoint_sink(
        &self,
    ) -> Arc<dyn crate::common::backend_topology::CoordinatorReportEndpointSink> {
        self.coordinator
            .as_ref()
            .expect("frontend coordinator is installed before host open returns")
            .report_endpoint_sink()
    }

    /// Frontend composition-time topology leaf used by FE-owned services.
    pub fn backend_topology_port(&self) -> crate::common::backend_topology::BackendTopologyService {
        Arc::clone(self.topology()) as crate::common::backend_topology::BackendTopologyService
    }

    pub async fn shutdown(mut self) -> Result<(), FrontendApplicationError> {
        self.shutdown_until(Instant::now() + STATE_STORE_SHUTDOWN_TIMEOUT)
            .await
    }

    /// Releases FE-owned resources under one absolute deadline. Individual
    /// cleanup owners must not start a fresh full timeout after another owner
    /// has already consumed the shutdown budget.
    pub async fn shutdown_until(
        mut self,
        deadline: Instant,
    ) -> Result<(), FrontendApplicationError> {
        self.release_resources_until(deadline)
            .await
            .map_err(|error| {
                FrontendApplicationError::new(FrontendApplicationErrorKind::Shutdown, error)
            })
    }

    async fn open_configured(
        &mut self,
        input: StateStoreHostInput,
        registry: &StateStoreProviderRegistry,
    ) -> Result<(), FrontendApplicationError> {
        self.state_store_host = Some(
            StateStoreHost::open(registry, input, Instant::now() + STATE_STORE_OPEN_TIMEOUT)
                .await
                .map_err(|error| {
                    FrontendApplicationError::new(
                        FrontendApplicationErrorKind::StateStoreHost,
                        error,
                    )
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
                self.data_runtime.clone(),
            )
            .map_err(FrontendApplicationError::server)?,
        );
        self.query_execution = Some(QueryExecutionService::new(coordinator.clone()));
        self.coordinator = Some(coordinator);
        Ok(())
    }

    async fn cleanup_open_error(
        &mut self,
        primary: FrontendApplicationError,
    ) -> FrontendApplicationError {
        match self
            .release_resources_until(Instant::now() + STATE_STORE_SHUTDOWN_TIMEOUT)
            .await
        {
            Ok(()) => primary,
            Err(cleanup_error) => primary.with_cleanup_context(cleanup_error),
        }
    }

    async fn release_resources_until(&mut self, deadline: Instant) -> Result<(), String> {
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
        self.dml_service.take();
        self.table_maintenance_service.take();
        self.statistics_service.take();
        // Process-local job services do not own StateStore job records. Release
        // their workers before closing the host's remaining durable owners.
        self.statistics_application_port.take();
        self.statistics_application_service.take();
        let catalog_controller_error = match self.catalog_controller.take() {
            Some(controller) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    Some(
                        "frontend cleanup deadline elapsed before catalog controller shutdown"
                            .to_string(),
                    )
                } else {
                    tokio::time::timeout(remaining, controller.shutdown())
                        .await
                        .map_err(|_| {
                            "frontend cleanup deadline elapsed shutting down catalog controller"
                                .to_string()
                        })
                        .and_then(|result| result)
                        .err()
                }
            }
            None => None,
        };
        self.catalog_application_port.take();
        self.view_service.take();
        self.mv_application_service.take();
        self.mv_service.take();
        self.mv_refresh_provider_activation.take();
        self.mv_background_engine_sink.take();
        self.mv_repository.take();
        if let Some(catalog_controller_error) = catalog_controller_error {
            let error = format!("shutdown catalog controller failed: {catalog_controller_error}");
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {error}"));
            } else {
                primary_error = Some(error);
            }
        }
        if let Some(host) = self.state_store_host.as_mut() {
            if let Err(error) = host.shutdown(deadline).await {
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

    use crate::state_store::{
        StateStoreHost, StateStoreProviderRegistration, StateStoreProviderRegistry,
        testing::{
            TEST_STATE_STORE_PROVIDER_ID, input as test_state_store_input,
            registry as test_state_store_registry,
        },
    };
    use async_trait::async_trait;
    use novarocks_spi::state_store::{
        StateStoreError, StateStoreErrorKind, StateStoreOpenRequest, StateStoreProviderDescriptor,
        StateStoreProviderFactory, StateStoreProviderInstance,
    };

    use super::{
        FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
        FrontendExecutionConfig, FrontendNativeTransport, test_native_trust,
    };

    const DESCRIPTOR: StateStoreProviderDescriptor = StateStoreProviderDescriptor::new(
        TEST_STATE_STORE_PROVIDER_ID,
        novarocks_spi::state_store::MAX_KEY_BYTES,
    );

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
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::new(DESCRIPTOR, |_| {
                Ok(Box::new(FailingFactory))
            }))
            .expect("register diagnostic provider");
        let host_error = match StateStoreHost::open(
            &registry,
            test_state_store_input("diagnostic-cluster"),
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
        assert!(diagnostic.contains("Open (frontend-unit-test)"));
        assert!(diagnostic.contains("injected provider primary failure"));
        assert!(diagnostic.contains("injected provider cleanup failure"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_bootstraps_and_stops_catalog_projection_with_state_store() {
        let state_store = test_state_store_input("catalog-controller-host-test");
        let registry = test_state_store_registry();
        let backend = crate::topology::ClusterBackendOpenConfig::new(
            novarocks_types::ClusterRole::Fe,
            Duration::from_secs(1),
            1,
            Duration::from_secs(1),
        )
        .expect("valid frontend backend config");
        let host = FrontendApplicationHost::open_with_factories_and_state_store_registry(
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new(
                "127.0.0.1",
                0,
                NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
            ),
            backend,
            Vec::new(),
            std::sync::Arc::new(
                crate::connector::typed_control_registry::ConnectorReadControlRegistry::new(),
            ),
            tokio::runtime::Handle::current(),
            test_native_trust(),
            FrontendNativeTransport::plaintext(),
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
