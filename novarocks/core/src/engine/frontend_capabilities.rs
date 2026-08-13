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
//! `StandaloneState` input, and no default construction path.  Frontend
//! composition must therefore make every authority edge visible at startup.

use std::sync::Arc;

use novarocks_spi::connector::ConnectorControlRegistry;

use crate::catalog_application::CatalogApplicationPort;
use crate::engine::domain;
use crate::engine::query_planning::catalog_runtime::QueryCatalogService;
use crate::engine::statistics::StatisticsService;
use crate::engine::statistics_application::StatisticsApplicationPort;
use crate::engine::system_catalog::SystemCatalog;
use crate::engine::table_maintenance::TableMaintenanceService;
use crate::engine::view::ViewService;
use crate::engine::{
    StandaloneQueryCompiler, UnifiedStatisticsResolver, add_files_engine, backend_command,
    catalog_command, ctas_engine, delete_engine, iceberg_ref_command, insert_engine,
    maintenance_command, mutation_engine, mv_command, statistics_command, truncate_engine,
    view_command,
};
use crate::mv::application::MvApplicationService;
use crate::mv::repository::MvRepository;
use crate::mv::storage_observation::MvStorageObservationPort;
use crate::query_execution::backend::BackendTopologyService;
use crate::query_execution::service::QueryExecutionService;

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
pub fn query_compiler(ports: QueryCompilerPorts) -> StandaloneQueryCompiler {
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
    StandaloneQueryCompiler::from_domain_kernels(
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
    pub insert: Arc<dyn insert_engine::InsertEngine>,
    pub delete: Arc<dyn delete_engine::DeleteEngine>,
    pub mutation: Arc<dyn mutation_engine::MutationEngine>,
    pub ctas: Arc<dyn ctas_engine::CtasEngine>,
    pub truncate: Arc<dyn truncate_engine::TruncateEngine>,
    pub add_files: Arc<dyn add_files_engine::AddFilesEngine>,
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
    catalog_command::CatalogCommandExecutor::new(domain::CatalogCommandKernel::new(
        ports.catalog_service,
        ports.catalog_application,
        ports.connector_control,
        ports.mv_repository,
        ports.mv_storage_observation,
        ports.view_service,
    ))
}

/// Leaf ports for durable statistics command submission and observation.
#[derive(Clone)]
pub struct StatisticsCommandPorts {
    catalog_service: Arc<QueryCatalogService>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    statistics_service: Arc<dyn StatisticsService>,
    statistics_application: Arc<dyn StatisticsApplicationPort>,
    query_execution: QueryExecutionService,
}

impl StatisticsCommandPorts {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        statistics_service: Arc<dyn StatisticsService>,
        statistics_application: Arc<dyn StatisticsApplicationPort>,
        query_execution: QueryExecutionService,
    ) -> Self {
        Self {
            catalog_service,
            connector_control,
            unified_statistics,
            statistics_service,
            statistics_application,
            query_execution,
        }
    }
}

pub fn statistics_command_executor(
    ports: StatisticsCommandPorts,
) -> statistics_command::StatisticsCommandExecutor {
    statistics_command::StatisticsCommandExecutor::new(domain::StatisticsExecutionKernel::new(
        ports.catalog_service,
        ports.connector_control,
        ports.unified_statistics,
        ports.statistics_service,
        ports.statistics_application,
        ports.query_execution,
    ))
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
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    repository: Arc<dyn MvRepository>,
    application: Arc<dyn MvApplicationService>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
}

impl MvCommandPorts {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        repository: Arc<dyn MvRepository>,
        application: Arc<dyn MvApplicationService>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        query_execution: QueryExecutionService,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            unified_statistics,
            repository,
            application,
            storage_observation,
            query_execution,
        }
    }
}

pub fn mv_command_executor(ports: MvCommandPorts) -> mv_command::MvCommandExecutor {
    let iceberg_ports = crate::engine::mv::iceberg_refresh::IcebergMvCorePorts::new(
        Arc::clone(&ports.catalog_service),
        ports.catalog_application.clone(),
        Arc::clone(&ports.connector_control),
        Arc::clone(&ports.repository),
        Arc::clone(&ports.storage_observation),
    );
    let backend = Arc::new(
        crate::engine::mv::iceberg_backend::IcebergMvBackend::new_with_ports(iceberg_ports),
    );
    mv_command::MvCommandExecutor::new(domain::MvExecutionKernel::new(
        ports.catalog_service,
        ports.catalog_application,
        ports.connector_control,
        ports.unified_statistics,
        backend,
        ports.repository,
        ports.application,
        ports.storage_observation,
        ports.query_execution,
    ))
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

pub fn view_command_executor(ports: ViewCommandPorts) -> view_command::ViewCommandExecutor {
    view_command::ViewCommandExecutor::new(domain::ViewExecutionKernel::new(
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
) -> crate::engine::SessionCatalogResolver {
    crate::engine::SessionCatalogResolver::new(
        ports.catalog_service,
        ports.catalog_application,
        ports.connector_control,
    )
}
