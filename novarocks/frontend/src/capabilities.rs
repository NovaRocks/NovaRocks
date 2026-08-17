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

//! Explicit Frontend-facing Core capability factories.
//!
//! Each factory accepts only the leaf ports for one command or query domain.
//! This module intentionally has no aggregate application context, no
//! application-facade input, and no default construction path.  Frontend
//! composition must therefore make every authority edge visible at startup.

use std::sync::Arc;

use novarocks_spi::connector::ConnectorControlRegistry;

use novarocks::catalog_application::CatalogApplicationPort;
use novarocks::catalog_application::query_catalog::QueryCatalogService;
use novarocks::catalog_application::system_catalog::SystemCatalog;
use novarocks::catalog_application::{command as catalog_command, iceberg_ref_command};
use novarocks::connector::UnifiedStatisticsResolver;
use novarocks::maintenance::TableMaintenanceService;
use novarocks::maintenance::command as maintenance_command;
use novarocks::mv::application::MvApplicationService;
use novarocks::mv::repository::MvRepository;
use novarocks::mv::storage_observation::MvStorageObservationPort;
use novarocks::query_execution::backend::BackendTopologyService;
use novarocks::query_execution::backend_command;
use novarocks::query_execution::dml::{add_files, ctas, delete, insert, mutation, truncate};
use novarocks::query_execution::kernels as domain;
use novarocks::query_execution::service::QueryExecutionService;
use novarocks::view::ViewService;

use crate::mv::{FrontendMvService, command as mv_command};
use crate::statistics::command::StatisticsCommandExecutor;
use crate::statistics_jobs::application::{
    ConnectorStatisticsTableReader, ConnectorStatisticsTargetResolver, StatisticsApplicationPort,
    StatisticsAttemptExecutor, StatisticsAttemptExecutorSink, StatisticsTableReaderSink,
    StatisticsTargetResolverSink,
};
use crate::view::command::ViewCommandExecutor;

use crate::query::compiler::FrontendQueryCompiler;

/// Leaf ports used by SQL query preparation.
///
/// This is one query-domain value, not an application-service bundle: it has
/// no command execution, durable job, or maintenance capability.
#[derive(Clone)]
pub struct QueryCompilerPorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    query_execution: QueryExecutionService,
    backend_topology: BackendTopologyService,
    exchange_port: u16,
    view_service: Arc<dyn ViewService>,
    system_catalog: Arc<dyn SystemCatalog>,
    mv_repository: Arc<dyn MvRepository>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl QueryCompilerPorts {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        query_execution: QueryExecutionService,
        backend_topology: BackendTopologyService,
        exchange_port: u16,
        view_service: Arc<dyn ViewService>,
        system_catalog: Arc<dyn SystemCatalog>,
        mv_repository: Arc<dyn MvRepository>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            unified_statistics,
            query_execution,
            backend_topology,
            exchange_port,
            view_service,
            system_catalog,
            mv_repository,
            mv_storage_observation,
        }
    }
}

/// Build the closed query-preparation capability from query-domain leaf ports.
pub(crate) fn query_compiler(ports: QueryCompilerPorts) -> FrontendQueryCompiler {
    let query = domain::QueryPreparationKernel::new(
        Arc::clone(&ports.catalog_service),
        ports.catalog_application.clone(),
        Arc::clone(&ports.connector_control),
        Arc::clone(&ports.unified_statistics),
        ports.query_execution.clone(),
        ports.backend_topology.clone(),
        ports.exchange_port,
    );
    let view = domain::ViewExecutionKernel::new(
        Arc::clone(&ports.catalog_service),
        ports.catalog_application.clone(),
        Arc::clone(&ports.connector_control),
        ports.view_service,
    );
    let system_tables = domain::SystemTableQueryKernel::new(
        ports.catalog_service,
        ports.connector_control,
        ports.system_catalog,
        Arc::clone(&ports.mv_repository),
    );
    FrontendQueryCompiler::new(
        query,
        view,
        system_tables,
        ports.mv_repository,
        ports.mv_storage_observation,
    )
}

/// Leaf ports shared by the closed foreground DML engines.
#[derive(Clone)]
pub struct DmlEnginePorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
}

impl DmlEnginePorts {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        query_execution: QueryExecutionService,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            unified_statistics,
            mv_storage_observation,
            query_execution,
        }
    }

    fn kernel(&self) -> domain::DmlExecutionKernel {
        domain::DmlExecutionKernel::new(
            Arc::clone(&self.catalog_service),
            self.catalog_application.clone(),
            Arc::clone(&self.connector_control),
            Arc::clone(&self.unified_statistics),
            Arc::clone(&self.mv_storage_observation),
            self.query_execution.clone(),
        )
    }
}

/// The complete closed DML capability set installed by Frontend.
///
/// All engines are independently owned trait objects so CP-3D may install the
/// CTAS engine before the sole recovery controller starts.
#[derive(Clone)]
pub struct DmlEngines {
    pub insert: Arc<dyn insert::InsertEngine>,
    pub delete: Arc<dyn delete::DeleteEngine>,
    pub mutation: Arc<dyn mutation::MutationEngine>,
    pub ctas: Arc<dyn ctas::CtasEngine>,
    pub truncate: Arc<dyn truncate::TruncateEngine>,
    pub add_files: Arc<dyn add_files::AddFilesEngine>,
}

/// Build all foreground DML engines from one DML-domain port set.
pub fn dml_engines(ports: DmlEnginePorts) -> DmlEngines {
    DmlEngines {
        insert: Arc::new(ports.kernel()),
        delete: Arc::new(ports.kernel()),
        mutation: Arc::new(ports.kernel()),
        ctas: Arc::new(ports.kernel()),
        truncate: Arc::new(ports.kernel()),
        add_files: Arc::new(ports.kernel()),
    }
}

/// Leaf ports for catalog DDL.
#[derive(Clone)]
pub struct CatalogCommandPorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    mv_repository: Arc<dyn MvRepository>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    view_service: Arc<dyn ViewService>,
}

impl CatalogCommandPorts {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        mv_repository: Arc<dyn MvRepository>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        view_service: Arc<dyn ViewService>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            mv_repository,
            mv_storage_observation,
            view_service,
        }
    }
}

pub fn catalog_command_executor(
    ports: CatalogCommandPorts,
) -> catalog_command::CatalogCommandExecutor {
    catalog_command::CatalogCommandExecutor::new(
        ports.catalog_service,
        ports.catalog_application,
        ports.connector_control,
        ports.mv_repository,
        ports.mv_storage_observation,
    )
}

/// Statistics SQL commands terminate at the frontend durable application
/// owner. They do not receive a query-execution kernel or Core composition
/// bundle.
pub(crate) fn statistics_command_executor(
    application: Arc<dyn StatisticsApplicationPort>,
) -> StatisticsCommandExecutor {
    StatisticsCommandExecutor::new(application)
}

/// Leaf port for FE-owned backend membership commands.
#[derive(Clone)]
pub struct BackendCommandPorts {
    topology: BackendTopologyService,
}

impl BackendCommandPorts {
    pub fn new(topology: BackendTopologyService) -> Self {
        Self { topology }
    }
}

pub fn backend_command_executor(
    ports: BackendCommandPorts,
) -> backend_command::BackendCommandExecutor {
    backend_command::BackendCommandExecutor::new(domain::BackendManagementKernel::new(
        ports.topology,
    ))
}

/// Leaf ports for `ALTER ICEBERG REF`.
#[derive(Clone)]
pub struct IcebergRefCommandPorts {
    connector_control: Arc<dyn ConnectorControlRegistry>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl IcebergRefCommandPorts {
    pub fn new(
        connector_control: Arc<dyn ConnectorControlRegistry>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            connector_control,
            mv_storage_observation,
        }
    }
}

pub fn iceberg_ref_command_executor(
    ports: IcebergRefCommandPorts,
) -> iceberg_ref_command::IcebergRefCommandExecutor {
    iceberg_ref_command::IcebergRefCommandExecutor::new(
        ports.connector_control,
        ports.mv_storage_observation,
    )
}

/// Leaf ports for foreground table-maintenance commands.
#[derive(Clone)]
pub struct MaintenanceCommandPorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
    service: Arc<dyn TableMaintenanceService>,
}

impl MaintenanceCommandPorts {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        query_execution: QueryExecutionService,
        service: Arc<dyn TableMaintenanceService>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            mv_storage_observation,
            query_execution,
            service,
        }
    }

    fn kernel(&self) -> domain::MaintenanceExecutionKernel {
        domain::MaintenanceExecutionKernel::new(
            Arc::clone(&self.catalog_service),
            self.catalog_application.clone(),
            Arc::clone(&self.connector_control),
            Arc::clone(&self.mv_storage_observation),
            self.query_execution.clone(),
            Arc::clone(&self.service),
        )
    }
}

pub fn maintenance_command_executor(
    ports: MaintenanceCommandPorts,
) -> maintenance_command::MaintenanceCommandExecutor {
    maintenance_command::MaintenanceCommandExecutor::new(domain::MaintenanceExecutionKernel::new(
        ports.catalog_service,
        ports.catalog_application,
        ports.connector_control,
        ports.mv_storage_observation,
        ports.query_execution,
        ports.service,
    ))
}

/// Build the read-only maintenance command capability.  It deliberately has
/// no catalog, provider, or request-execution port.
pub fn maintenance_read_command_executor(
    service: Arc<dyn TableMaintenanceService>,
) -> maintenance_command::MaintenanceReadCommandExecutor {
    maintenance_command::MaintenanceReadCommandExecutor::new(service)
}

/// Leaf ports for MV metadata and refresh execution.
#[derive(Clone)]
pub struct MvCommandPorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    repository: Arc<dyn MvRepository>,
    create_application: Arc<dyn MvApplicationService>,
    refresh_service: Arc<FrontendMvService>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
}

impl MvCommandPorts {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        repository: Arc<dyn MvRepository>,
        create_application: Arc<dyn MvApplicationService>,
        refresh_service: Arc<FrontendMvService>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        query_execution: QueryExecutionService,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            repository,
            create_application,
            refresh_service,
            storage_observation,
            query_execution,
        }
    }
}

pub fn mv_command_executor(ports: MvCommandPorts) -> mv_command::MvCommandExecutor {
    let iceberg_ports = novarocks::mv::iceberg_refresh::IcebergMvCorePorts::new(
        Arc::clone(&ports.catalog_service),
        ports.catalog_application.clone(),
        Arc::clone(&ports.connector_control),
        Arc::clone(&ports.repository),
        Arc::clone(&ports.storage_observation),
    );
    let backend = Arc::new(
        novarocks::mv::iceberg_backend::IcebergMvBackend::new_with_ports(iceberg_ports.clone()),
    );
    mv_command::MvCommandExecutor::new(
        iceberg_ports,
        ports.create_application,
        ports.refresh_service,
        Arc::clone(&ports.repository),
        Arc::clone(&ports.storage_observation),
        backend,
    )
}

/// Leaf ports for external-view commands.
#[derive(Clone)]
pub struct ViewCommandPorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    view_service: Arc<dyn ViewService>,
}

impl ViewCommandPorts {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        view_service: Arc<dyn ViewService>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            view_service,
        }
    }
}

pub(crate) fn view_command_executor(ports: ViewCommandPorts) -> ViewCommandExecutor {
    ViewCommandExecutor::new(domain::ViewExecutionKernel::new(
        ports.catalog_service,
        ports.catalog_application,
        ports.connector_control,
        ports.view_service,
    ))
}

/// Leaf ports for session catalog admission.
#[derive(Clone)]
pub struct SessionCatalogPorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
}

impl SessionCatalogPorts {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
        }
    }
}

pub fn session_catalog_resolver(
    ports: SessionCatalogPorts,
) -> novarocks::query_execution::kernels::SessionCatalogResolver {
    novarocks::query_execution::kernels::SessionCatalogResolver::new(
        ports.catalog_service,
        ports.catalog_application,
        ports.connector_control,
    )
}

/// Bind the Frontend-owned publication projection to the query catalog before
/// any startup restore can resolve externally attached catalog names.
///
/// The projection and control registry are distinct startup leaves: this
/// helper deliberately neither publishes a runtime nor creates a catalog
/// controller.
pub fn bind_catalog_runtime_projection(
    projection: &novarocks::catalog_application::CatalogRuntimeProjection,
    catalog_service: Arc<QueryCatalogService>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
) -> Result<(), String> {
    projection
        .bind_query_catalog(
            catalog_service,
            connector_control as Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
        )
        .map_err(|error| format!("bind catalog runtime projection failed: {error}"))
}

/// Exact query and MV leaves needed to activate a previously admitted MV
/// write.  This is intentionally separate from the interactive MV command
/// capability: durable refresh activation must not acquire a command router.
#[derive(Clone)]
pub struct MvRefreshProviderActivationPorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    query_execution: QueryExecutionService,
    backend_topology: BackendTopologyService,
    exchange_port: u16,
    mv_repository: Arc<dyn MvRepository>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl MvRefreshProviderActivationPorts {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        query_execution: QueryExecutionService,
        backend_topology: BackendTopologyService,
        exchange_port: u16,
        mv_repository: Arc<dyn MvRepository>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            unified_statistics,
            query_execution,
            backend_topology,
            exchange_port,
            mv_repository,
            mv_storage_observation,
        }
    }
}

/// Build the provider activation adapter used by the Frontend-owned MV
/// refresh controller.  The returned trait object contains no state facade
/// and retains the admitted query execution service for native writes.
pub fn mv_refresh_provider_activation(
    ports: MvRefreshProviderActivationPorts,
) -> Arc<dyn novarocks::query_execution::mv_native_write::MvRefreshProviderActivation> {
    let query_kernel = domain::QueryPreparationKernel::new(
        Arc::clone(&ports.catalog_service),
        ports.catalog_application.clone(),
        Arc::clone(&ports.connector_control),
        ports.unified_statistics,
        ports.query_execution,
        ports.backend_topology,
        ports.exchange_port,
    );
    let mv_ports = novarocks::mv::iceberg_refresh::IcebergMvCorePorts::new(
        ports.catalog_service,
        ports.catalog_application,
        ports.connector_control,
        ports.mv_repository,
        ports.mv_storage_observation,
    );
    Arc::new(
        novarocks::query_execution::mv_assembly::iceberg_activation::IcebergMvRefreshProviderActivation::new(
            query_kernel,
            mv_ports,
        ),
    )
}

/// Bind MV refresh activation before the Frontend performs startup restore.
pub fn bind_mv_refresh_provider_activation(
    sink: &dyn novarocks::query_execution::mv_native_write::MvRefreshProviderActivationSink,
    ports: MvRefreshProviderActivationPorts,
) -> Result<(), String> {
    sink.bind_mv_refresh_provider_activation(mv_refresh_provider_activation(ports))
}

/// Bind the short-lived, generation-fenced statistics target resolver.
pub fn bind_statistics_target_resolver(
    sink: &dyn StatisticsTargetResolverSink,
    connector_control: Arc<dyn ConnectorControlRegistry>,
) -> Result<(), String> {
    sink.bind_statistics_target_resolver(Arc::new(ConnectorStatisticsTargetResolver::new(
        connector_control,
    )))
}

/// Bind the short-lived, generation-fenced statistics reader.
pub fn bind_statistics_table_reader(
    sink: &dyn StatisticsTableReaderSink,
    connector_control: Arc<dyn ConnectorControlRegistry>,
) -> Result<(), String> {
    sink.bind_statistics_table_reader(Arc::new(ConnectorStatisticsTableReader::new(
        connector_control,
    )))
}

/// Exact leaves retained by the Frontend-owned durable ANALYZE worker.
///
/// The connector registry is intentionally absent: Core creates and retains
/// it inside the returned executor, preloaded with the same Iceberg MV
/// capability as the rest of this startup composition.
#[derive(Clone)]
pub struct StatisticsAttemptExecutorPorts {
    execution_role: novarocks_types::ClusterRole,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    backend_topology: BackendTopologyService,
    query_execution: QueryExecutionService,
}

impl StatisticsAttemptExecutorPorts {
    pub fn new(
        execution_role: novarocks_types::ClusterRole,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        backend_topology: BackendTopologyService,
        query_execution: QueryExecutionService,
    ) -> Self {
        Self {
            execution_role,
            connector_control,
            backend_topology,
            query_execution,
        }
    }
}

/// Build the native statistics attempt executor from Frontend-owned leaves.
pub fn statistics_attempt_executor(
    ports: StatisticsAttemptExecutorPorts,
) -> Arc<dyn StatisticsAttemptExecutor> {
    Arc::new(
        crate::statistics_jobs::attempt_executor::FrontendStatisticsAttemptExecutor::new(
            crate::statistics_jobs::attempt_executor::StatisticsAttemptExecutionPorts::new(
                ports.execution_role,
                ports.connector_control,
                ports.backend_topology,
                ports.query_execution,
            ),
        ),
    )
}

/// Bind the durable ANALYZE executor after connector control and native
/// coordinator leaves are ready.  A missing sink remains a Frontend decision;
/// this helper never supplies an in-memory job fallback.
pub fn bind_statistics_attempt_executor(
    sink: &dyn StatisticsAttemptExecutorSink,
    ports: StatisticsAttemptExecutorPorts,
) -> Result<(), String> {
    sink.bind_statistics_attempt_executor(statistics_attempt_executor(ports))
}

/// Build the automatic-maintenance engine from the same maintenance command
/// leaves plus a Frontend-supplied attempt factory.  Each automatic attempt
/// obtains a fresh topology and cancellation scope through that factory.
pub fn background_maintenance_engine(
    ports: MaintenanceCommandPorts,
    attempt_factory: Arc<dyn novarocks::maintenance::BackgroundMaintenanceAttemptFactory>,
) -> Arc<dyn novarocks::maintenance::TableMaintenanceEngine> {
    Arc::new(novarocks::maintenance::BackgroundMaintenanceEngine::new(
        ports.kernel(),
        attempt_factory,
    ))
}

/// Capture one automatic-maintenance attempt from the Frontend's live role
/// and topology. `QueryExecutionContext` remains opaque so callers cannot
/// manufacture a default topology, deadline, or cancellation identity.
pub fn background_maintenance_attempt(
    role: novarocks_types::ClusterRole,
    topology: BackendTopologyService,
) -> Result<novarocks::maintenance::BackgroundMaintenanceAttempt, String> {
    let topology = topology.snapshot().map_err(|error| error.to_string())?;
    let cancellation = novarocks::query_execution::cancellation::QueryCancellationSource::new();
    let execution = novarocks::query_execution::request_context::QueryExecutionContext::new(
        role,
        topology,
        None,
        cancellation.view(),
        novarocks::query_execution::request_context::SessionOptimizerSettings::default(),
    );
    let connector_context =
        novarocks::connector::connector_request_context_for_execution(None, &execution)?;
    Ok(novarocks::maintenance::BackgroundMaintenanceAttempt::new(
        execution,
        connector_context,
    ))
}

/// Leaf ports for the Frontend-owned MV background worker.
#[derive(Clone)]
pub(crate) struct MvBackgroundPorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    repository: Arc<dyn MvRepository>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl MvBackgroundPorts {
    pub(crate) fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        repository: Arc<dyn MvRepository>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            repository,
            storage_observation,
        }
    }
}

/// Build the two capabilities the Frontend binds into its MV background
/// runtime after restore and maintenance recovery have completed.
pub(crate) fn mv_background_bindings(
    ports: MvBackgroundPorts,
    table_maintenance_engine: Arc<dyn novarocks::maintenance::TableMaintenanceEngine>,
) -> crate::mv::background::MvBackgroundBindings {
    let iceberg_ports = novarocks::mv::iceberg_refresh::IcebergMvCorePorts::new(
        ports.catalog_service,
        ports.catalog_application,
        Arc::clone(&ports.connector_control),
        Arc::clone(&ports.repository),
        Arc::clone(&ports.storage_observation),
    );
    crate::mv::background::MvBackgroundBindings {
        engine: Arc::new(
            crate::mv::background_engine::StandaloneMvBackgroundEngine::new_with_ports(
                iceberg_ports,
                ports.connector_control,
                ports.repository,
                ports.storage_observation,
            ),
        ),
        table_maintenance_engine,
    }
}

/// Bind the MV background capability only after the Frontend has completed
/// its ordered restore and recovery sequence.
pub(crate) fn bind_mv_background_engine(
    sink: &dyn crate::mv::background::MvBackgroundEngineSink,
    ports: MvBackgroundPorts,
    table_maintenance_engine: Arc<dyn novarocks::maintenance::TableMaintenanceEngine>,
) -> Result<(), crate::mv::background::MvBackgroundEngineError> {
    sink.bind_mv_background_engine(mv_background_bindings(ports, table_maintenance_engine))
}
