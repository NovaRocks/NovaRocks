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
use tokio::runtime::Handle;

use crate::catalog_application::query_catalog::QueryCatalogService;
use crate::catalog_application::{command as catalog_command, iceberg_ref_command};
use crate::connector::UnifiedStatisticsResolver;
use crate::mv::domain::readiness::MvCandidateReader;
use crate::query_execution::dml::{add_files, ctas, delete, insert, mutation, truncate};
use crate::query_execution::kernels as domain;
use crate::query_execution::maintenance::command as maintenance_command;
use crate::query_execution::maintenance::{
    BackgroundMaintenanceAttempt, BackgroundMaintenanceAttemptFactory, BackgroundMaintenanceEngine,
    TableMaintenanceEngine, TableMaintenanceService,
};
use crate::query_execution::package_freeze::CompiledPackageCarrier;
use novarocks_catalog_application::CatalogApplicationPort;
use novarocks_query_application::api::BackendTopologyService;
use novarocks_query_application::api::{BackendCommandExecutor, BackendTopologyCommandPort};
use novarocks_query_application::system_catalog::SystemCatalog;
use novarocks_query_application::view::ViewService;
use novarocks_spi::connector::MvStorageObservationPort;

use crate::mv::{FrontendMvProductAdapter, command as mv_command};
use crate::statistics::command::StatisticsCommandExecutor;
use crate::statistics_jobs::application::StatisticsApplicationPort;
use crate::view::command::ViewCommandExecutor;

use crate::query::compiler::FrontendQueryCompiler;

/// Leaf ports used by SQL query preparation.
///
/// This is one query-domain value, not an application-service bundle: it has
/// no command execution, durable job, or maintenance capability.
#[derive(Clone)]
pub(crate) struct QueryCompilerPorts {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: CompiledPackageCarrier,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    backend_topology: BackendTopologyService,
    exchange_port: u16,
    view_service: Arc<dyn ViewService>,
    system_catalog: Arc<dyn SystemCatalog>,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    mv_candidate_reader: MvCandidateReader,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    connector_blocking_io: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
}

impl QueryCompilerPorts {
    #[expect(
        clippy::too_many_arguments,
        reason = "This constructor binds distinct FE capability owners; a second configuration aggregate would duplicate the authoritative capability shape."
    )]
    pub(crate) fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        backend_topology: BackendTopologyService,
        exchange_port: u16,
        view_service: Arc<dyn ViewService>,
        system_catalog: Arc<dyn SystemCatalog>,
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
        mv_candidate_reader: MvCandidateReader,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        connector_blocking_io: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        constant_policy: novarocks_functions::ConstantPolicy,
        static_plan_carrier: CompiledPackageCarrier,
    ) -> Self {
        Self {
            functions,
            constant_policy,
            static_plan_carrier,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control,
            unified_statistics,
            backend_topology,
            exchange_port,
            view_service,
            system_catalog,
            mv_readiness,
            mv_candidate_reader,
            mv_storage_observation,
            connector_blocking_io,
        }
    }
}

/// Build the closed query-preparation capability from query-domain leaf ports.
pub(crate) fn query_compiler(ports: QueryCompilerPorts) -> FrontendQueryCompiler {
    let query = domain::QueryPreparationKernel::new(
        Arc::clone(&ports.functions),
        Arc::clone(&ports.catalog_service),
        ports.catalog_application.clone(),
        Arc::clone(&ports.connector_control),
        Arc::clone(&ports.typed_connector_control),
        Arc::clone(&ports.unified_statistics),
        ports.backend_topology.clone(),
        ports.exchange_port,
        ports.constant_policy,
        ports.static_plan_carrier,
    );
    let view = domain::ViewExecutionKernel::new(
        Arc::clone(&ports.functions),
        Arc::clone(&ports.catalog_service),
        ports.catalog_application.clone(),
        Arc::clone(&ports.connector_control),
        ports.view_service,
        ports.constant_policy,
    );
    let system_tables = domain::SystemTableQueryKernel::new(
        Arc::new(
            crate::catalog_application::system_catalog_facts::FrontendSystemCatalogFacts::new(
                ports.catalog_service,
                ports.connector_control,
            ),
        ),
        ports.system_catalog,
        Arc::clone(&ports.mv_readiness),
    );
    FrontendQueryCompiler::new(
        ports.functions,
        query,
        view,
        system_tables,
        ports.mv_readiness,
        ports.mv_candidate_reader,
        ports.mv_storage_observation,
        ports.connector_blocking_io,
        ports.constant_policy,
    )
}

/// Leaf ports shared by the closed foreground DML engines.
#[derive(Clone)]
pub struct DmlEnginePorts {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: CompiledPackageCarrier,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    connector_runtime: tokio::runtime::Handle,
    lake_publication_runtime_policy:
        novarocks_query_application::publication::LakePublicationRuntimePolicy,
    write_runtime: Option<(
        crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        crate::native::data_runtime::FrontendDataRuntime,
    )>,
}

impl DmlEnginePorts {
    #[expect(
        clippy::too_many_arguments,
        reason = "This constructor binds distinct FE capability owners; a second configuration aggregate would duplicate the authoritative capability shape."
    )]
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
        connector_runtime: tokio::runtime::Handle,
        lake_publication_runtime_policy: novarocks_query_application::publication::LakePublicationRuntimePolicy,
        constant_policy: novarocks_functions::ConstantPolicy,
        static_plan_carrier: CompiledPackageCarrier,
    ) -> Self {
        Self {
            functions,
            constant_policy,
            static_plan_carrier,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control,
            unified_statistics,
            mv_storage_observation,
            mv_readiness,
            connector_runtime,
            write_runtime: None,
            lake_publication_runtime_policy,
        }
    }

    pub(crate) fn with_write_runtime(
        mut self,
        blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        data: crate::native::data_runtime::FrontendDataRuntime,
    ) -> Self {
        self.write_runtime = Some((blocking, data));
        self
    }

    fn kernel(&self) -> domain::DmlExecutionKernel {
        let kernel = domain::DmlExecutionKernel::new(
            domain::DmlPlanningServices::new(
                Arc::clone(&self.functions),
                Arc::clone(&self.catalog_service),
                self.constant_policy,
                self.static_plan_carrier.clone(),
            ),
            self.catalog_application.clone(),
            Arc::clone(&self.connector_control),
            Arc::clone(&self.typed_connector_control),
            Arc::clone(&self.unified_statistics),
            Arc::clone(&self.mv_storage_observation),
            Arc::clone(&self.mv_readiness),
        )
        .with_lake_publication_runtime_policy(self.lake_publication_runtime_policy)
        .with_connector_runtime(self.connector_runtime.clone());
        match &self.write_runtime {
            Some((blocking, data)) => kernel.with_write_runtime(blocking.clone(), data.clone()),
            None => kernel,
        }
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
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl CatalogCommandPorts {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            mv_readiness,
            mv_storage_observation,
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
        ports.mv_readiness,
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
    topology: Arc<dyn BackendTopologyCommandPort>,
}

impl BackendCommandPorts {
    pub fn new(topology: Arc<dyn BackendTopologyCommandPort>) -> Self {
        Self { topology }
    }
}

pub fn backend_command_executor(ports: BackendCommandPorts) -> BackendCommandExecutor {
    BackendCommandExecutor::new(ports.topology)
}

/// Leaf ports for `ALTER ICEBERG REF`.
#[derive(Clone)]
pub struct IcebergRefCommandPorts {
    connector_control: Arc<dyn ConnectorControlRegistry>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
}

impl IcebergRefCommandPorts {
    pub fn new(
        connector_control: Arc<dyn ConnectorControlRegistry>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    ) -> Self {
        Self {
            connector_control,
            mv_storage_observation,
            mv_readiness,
        }
    }
}

pub fn iceberg_ref_command_executor(
    ports: IcebergRefCommandPorts,
) -> iceberg_ref_command::IcebergRefCommandExecutor {
    iceberg_ref_command::IcebergRefCommandExecutor::new(
        ports.connector_control,
        ports.mv_storage_observation,
        ports.mv_readiness,
    )
}

/// Leaf ports for foreground table-maintenance commands.
#[derive(Clone)]
pub struct MaintenanceCommandPorts {
    state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    rewrite_runtime: Option<domain::MaintenanceRewriteRuntime>,
    constant_policy: novarocks_functions::ConstantPolicy,
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    static_plan_carrier: CompiledPackageCarrier,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    service: Arc<dyn TableMaintenanceService>,
    runtime: Handle,
}

impl MaintenanceCommandPorts {
    #[expect(
        clippy::too_many_arguments,
        reason = "This constructor binds distinct FE capability owners; a second configuration aggregate would duplicate the authoritative capability shape."
    )]
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
        service: Arc<dyn TableMaintenanceService>,
        runtime: Handle,
        constant_policy: novarocks_functions::ConstantPolicy,
        static_plan_carrier: CompiledPackageCarrier,
    ) -> Self {
        Self {
            state_store: None,
            rewrite_runtime: None,
            constant_policy,
            functions,
            static_plan_carrier,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control,
            mv_storage_observation,
            mv_readiness,
            service,
            runtime,
        }
    }

    pub(crate) fn with_state_store_original(
        mut self,
        binding: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    ) -> Self {
        self.state_store = binding;
        self
    }

    pub(crate) fn with_rewrite_runtime(
        mut self,
        runtime: domain::MaintenanceRewriteRuntime,
    ) -> Self {
        self.rewrite_runtime = Some(runtime);
        self
    }

    fn kernel(&self) -> domain::MaintenanceExecutionKernel {
        let kernel = domain::MaintenanceExecutionKernel::new(
            Arc::clone(&self.functions),
            Arc::clone(&self.catalog_service),
            self.catalog_application.clone(),
            Arc::clone(&self.connector_control),
            Arc::clone(&self.typed_connector_control),
            Arc::clone(&self.mv_storage_observation),
            Arc::clone(&self.mv_readiness),
            Arc::clone(&self.service),
            self.constant_policy,
            self.static_plan_carrier.clone(),
        )
        .with_state_store_original(self.state_store.clone());
        match self.rewrite_runtime.as_ref() {
            Some(runtime) => kernel.with_rewrite_runtime(runtime.clone()),
            None => kernel,
        }
    }
}

pub fn maintenance_command_executor(
    ports: MaintenanceCommandPorts,
) -> maintenance_command::MaintenanceCommandExecutor {
    maintenance_command::MaintenanceCommandExecutor::new(ports.kernel(), ports.runtime)
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
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    refresh_service: Arc<FrontendMvProductAdapter>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    management_entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
    management_continuation:
        Option<Arc<novarocks_mv_application::management::ManagementContinuationService>>,
    management_audit: Option<Arc<dyn novarocks_mv_application::management::ManagementAuditSink>>,
}

impl MvCommandPorts {
    #[expect(
        clippy::too_many_arguments,
        reason = "This constructor binds distinct FE capability owners; a second configuration aggregate would duplicate the authoritative capability shape."
    )]
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
        refresh_service: Arc<FrontendMvProductAdapter>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        management_entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
        management_continuation: Option<
            Arc<novarocks_mv_application::management::ManagementContinuationService>,
        >,
        management_audit: Option<
            Arc<dyn novarocks_mv_application::management::ManagementAuditSink>,
        >,
        constant_policy: novarocks_functions::ConstantPolicy,
    ) -> Self {
        Self {
            functions,
            constant_policy,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control,
            readiness: refresh_service.readiness_port(),
            refresh_service,
            storage_observation,
            management_entrance,
            management_continuation,
            management_audit,
        }
    }
}

pub fn mv_command_executor(ports: MvCommandPorts) -> mv_command::MvCommandExecutor {
    let iceberg_ports =
        crate::mv::domain::iceberg_refresh::IcebergMvCorePorts::new_with_management_entrance(
            Arc::clone(&ports.functions),
            Arc::clone(&ports.catalog_service),
            ports.catalog_application.clone(),
            Arc::clone(&ports.connector_control),
            Arc::clone(&ports.typed_connector_control),
            Arc::clone(&ports.readiness),
            Arc::clone(&ports.storage_observation),
            Arc::clone(&ports.management_entrance),
            ports.constant_policy,
        );
    mv_command::MvCommandExecutor::new(
        iceberg_ports,
        ports.refresh_service,
        Arc::clone(&ports.storage_observation),
        ports.management_continuation,
        ports.management_audit,
    )
}

/// Leaf ports for external-view commands.
#[derive(Clone)]
pub struct ViewCommandPorts {
    constant_policy: novarocks_functions::ConstantPolicy,
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    view_service: Arc<dyn ViewService>,
}

impl ViewCommandPorts {
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        view_service: Arc<dyn ViewService>,
        constant_policy: novarocks_functions::ConstantPolicy,
    ) -> Self {
        Self {
            constant_policy,
            functions,
            catalog_service,
            catalog_application,
            connector_control,
            view_service,
        }
    }
}

pub(crate) fn view_command_executor(ports: ViewCommandPorts) -> ViewCommandExecutor {
    ViewCommandExecutor::new(domain::ViewExecutionKernel::new(
        ports.functions,
        ports.catalog_service,
        ports.catalog_application,
        ports.connector_control,
        ports.view_service,
        ports.constant_policy,
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
) -> novarocks_query_application::sql::catalog::SessionCatalogService {
    Arc::new(
        crate::query_execution::kernels::SessionCatalogResolver::new(
            ports.catalog_service,
            ports.catalog_application,
            ports.connector_control,
        ),
    )
}

/// Bind the Frontend-owned publication projection to the query catalog before
/// any startup restore can resolve externally attached catalog names.
///
/// The projection and control registry are distinct startup leaves: this
/// helper deliberately neither publishes a runtime nor creates a catalog
/// controller.
pub fn bind_catalog_runtime_projection(
    projection: &crate::catalog_application::CatalogRuntimeProjection,
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
pub(crate) struct MvRefreshProviderActivationPorts {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: CompiledPackageCarrier,
    catalog_service: Arc<QueryCatalogService>,
    /// A durable MV refresh resolves an externally attached target while it
    /// recreates its write binding.  Unlike generic SQL kernels, this product
    /// activation has no meaningful no-catalog mode, so composition must
    /// supply the authority rather than defer absence to a request path.
    catalog_application: Arc<dyn CatalogApplicationPort>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    backend_topology: BackendTopologyService,
    exchange_port: u16,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_management_entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
}

impl MvRefreshProviderActivationPorts {
    #[expect(
        clippy::too_many_arguments,
        reason = "This constructor binds distinct FE capability owners; a second configuration aggregate would duplicate the authoritative capability shape."
    )]
    pub(crate) fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Arc<dyn CatalogApplicationPort>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        backend_topology: BackendTopologyService,
        exchange_port: u16,
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        mv_management_entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
        constant_policy: novarocks_functions::ConstantPolicy,
        static_plan_carrier: CompiledPackageCarrier,
    ) -> Self {
        Self {
            functions,
            constant_policy,
            static_plan_carrier,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control,
            unified_statistics,
            backend_topology,
            exchange_port,
            mv_readiness,
            mv_storage_observation,
            mv_management_entrance,
        }
    }
}

/// Build the provider activation adapter used by the Frontend-owned MV
/// refresh controller. The returned trait object retains the exact compiler
/// authorities used by its provider activation.
pub(crate) fn mv_refresh_provider_activation(
    ports: MvRefreshProviderActivationPorts,
) -> Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivation> {
    let query_kernel = domain::QueryPreparationKernel::new(
        Arc::clone(&ports.functions),
        Arc::clone(&ports.catalog_service),
        Some(Arc::clone(&ports.catalog_application)),
        Arc::clone(&ports.connector_control),
        Arc::clone(&ports.typed_connector_control),
        ports.unified_statistics,
        ports.backend_topology,
        ports.exchange_port,
        ports.constant_policy,
        ports.static_plan_carrier,
    );
    let mv_ports =
        crate::mv::domain::iceberg_refresh::IcebergMvCorePorts::new_with_management_entrance(
            ports.functions,
            ports.catalog_service,
            Some(ports.catalog_application),
            ports.connector_control,
            ports.typed_connector_control,
            ports.mv_readiness,
            ports.mv_storage_observation,
            ports.mv_management_entrance,
            ports.constant_policy,
        );
    Arc::new(
        crate::query_execution::mv_assembly::iceberg_activation::IcebergMvRefreshProviderActivation::new(
            query_kernel,
            mv_ports,
        ),
    )
}

pub use crate::statistics_jobs::attempt_executor::StatisticsAttemptExecutorPorts;

/// Build the product worker's explicit prepare/collect/publish adapter from
/// the same role-owned leaves. This is constructed before SQL serving opens;
/// it is deliberately not a late-bound sink on the product port.
pub(crate) fn statistics_attempt_executor(
    ports: StatisticsAttemptExecutorPorts,
) -> Arc<dyn novarocks_statistics_application::StatisticsAttemptExecutor> {
    Arc::new(
        crate::statistics_jobs::attempt_executor::FrontendStatisticsAttemptExecutor::new(ports),
    )
}

/// Build the automatic-maintenance engine from the same maintenance command
/// leaves plus a Frontend-supplied attempt factory.  Each automatic attempt
/// obtains a fresh topology and cancellation scope through that factory.
pub fn background_maintenance_engine(
    ports: MaintenanceCommandPorts,
    attempt_factory: Arc<dyn BackgroundMaintenanceAttemptFactory>,
) -> Arc<dyn TableMaintenanceEngine> {
    Arc::new(BackgroundMaintenanceEngine::new(
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
    max_attempt_duration: std::time::Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<BackgroundMaintenanceAttempt, String> {
    original_maintenance_context(role, topology, max_attempt_duration, runtime, None, None)
}

/// Bind only original Root control/deadline. MIO and the computation source
/// must install their own positive capacity before any physical work.
pub fn original_background_maintenance_attempt(
    role: novarocks_types::ClusterRole,
    topology: BackendTopologyService,
    max_attempt_duration: std::time::Duration,
    runtime: &tokio::runtime::Handle,
    owner: &novarocks_workload_control::WorkOwner,
) -> Result<BackgroundMaintenanceAttempt, String> {
    let root = owner.scope();
    root.check().map_err(|e| e.to_string())?;
    let capacity = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::from_root_owner(owner)
        .map_err(|error| error.to_string())?;
    original_maintenance_context(
        role,
        topology,
        max_attempt_duration,
        runtime,
        Some(&capacity),
        Some(&root),
    )
}

fn original_maintenance_context(
    role: novarocks_types::ClusterRole,
    topology: BackendTopologyService,
    max_attempt_duration: std::time::Duration,
    runtime: &tokio::runtime::Handle,
    capacity: Option<
        &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    >,
    original_root: Option<&novarocks_workload_control::WorkScope>,
) -> Result<BackgroundMaintenanceAttempt, String> {
    let topology = topology.snapshot().map_err(|error| error.to_string())?;
    let deadline = std::time::Instant::now()
        .checked_add(max_attempt_duration)
        .ok_or_else(|| "automatic maintenance deadline overflow".to_string())?;
    let (cancellation, deadline) = match original_root {
        Some(root) => {
            let view = root.cancellation().map_err(|error| error.to_string())?;
            let deadline = view
                .deadline()
                .map(std::time::Instant::from)
                .map_or(deadline, |root_deadline| root_deadline.min(deadline));
            (
                novarocks_query_application::cancellation::QueryCancellationView::governed(
                    view, None,
                ),
                deadline,
            )
        }
        None => (
            novarocks_query_application::cancellation::QueryCancellationSource::new().view(),
            deadline,
        ),
    };
    let execution = novarocks_query_application::admitted_query_context::QueryExecutionContext::new(
        role,
        topology,
        Some(deadline),
        cancellation,
        novarocks_sql::compiler::SessionOptimizerSettings::default(),
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
    );
    let execution = match capacity {
        Some(capacity) => execution
            .with_result_capacity(capacity.clone())
            .map_err(|error| error.to_string())?,
        None => execution,
    };
    let connector_context = crate::connector::connector_request_context_for_execution_on_runtime(
        None, &execution, runtime,
    )?
    .with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::JobAttempt);
    Ok(BackgroundMaintenanceAttempt::new(
        execution,
        connector_context,
    ))
}

/// Leaf ports for the Frontend-owned MV background worker.
#[derive(Clone)]
pub(crate) struct MvBackgroundPorts {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    management_entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
}

impl MvBackgroundPorts {
    pub(crate) fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        management_entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
        constant_policy: novarocks_functions::ConstantPolicy,
    ) -> Self {
        Self {
            functions,
            constant_policy,
            catalog_service,
            catalog_application,
            connector_control,
            readiness,
            storage_observation,
            management_entrance,
        }
    }
}

impl MvBackgroundPorts {
    /// The execution services retain the same role-composed typed host. Keep
    /// that leaf in the background preparation graph rather than rebuilding a
    /// registry or acquiring another catalog generation.
    pub(crate) fn into_core_ports(
        self,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    ) -> crate::mv::domain::iceberg_refresh::IcebergMvCorePorts {
        crate::mv::domain::iceberg_refresh::IcebergMvCorePorts::new_with_management_entrance(
            self.functions,
            self.catalog_service,
            self.catalog_application,
            self.connector_control,
            typed_connector_control,
            self.readiness,
            self.storage_observation,
            self.management_entrance,
            self.constant_policy,
        )
    }
}

/// Build the two capabilities the Frontend binds into its MV background
/// runtime after restore and maintenance recovery have completed.
pub(crate) fn mv_background_bindings(
    ports: MvBackgroundPorts,
    execution: crate::query::mv_action::MvExecutionServices,
) -> crate::mv::background::MvBackgroundBindings {
    let iceberg_ports = ports.into_core_ports(Arc::clone(&execution.connector_control));
    crate::mv::background::MvBackgroundBindings {
        ports: iceberg_ports,
        execution,
    }
}
