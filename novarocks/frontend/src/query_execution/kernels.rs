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

//! Cohesive Core execution kernels.
//!
//! These values are deliberately separate.  They are the replacement seams
//! for implementations that previously borrowed the Core application facade; they must not
//! be gathered into another application context or service locator.

use std::sync::Arc;

use crate::catalog_application::query_catalog::QueryCatalogService;
use crate::connector::unified_statistics::UnifiedStatisticsResolver;
use crate::mv::domain::application::MvApplicationService;
use crate::mv::domain::iceberg_backend::IcebergMvBackend;
use crate::mv::domain::readiness::MvReadinessPort;
use crate::mv::domain::repository::MvRepository;
use crate::query_execution::maintenance::TableMaintenanceService;
use crate::query_execution::service::QueryExecutionService;
use novarocks_catalog_application::CatalogApplicationPort;
use novarocks_catalog_application::ConnectorControlHost;
use novarocks_query_application::api::BackendTopologyService;
use novarocks_query_application::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_query_application::sql::catalog::SessionCatalogPort;
use novarocks_query_application::system_catalog::SystemCatalog;
use novarocks_query_application::system_catalog_rewrite::SystemCatalogFactsPort;
use novarocks_query_application::view::ViewService;
use novarocks_spi::connector::ConnectorControlRegistry;
use novarocks_spi::connector::MvStorageObservationPort;

/// Query compilation and distributed-query preparation dependencies.
///
/// Catalog state stays query-specific here.  DML/MV command execution receives
/// only the leaf ports it uses rather than a reference back to this kernel.
#[derive(Clone)]
pub struct QueryPreparationKernel {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<ConnectorControlHost>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    query_execution: QueryExecutionService,
    backend_topology: BackendTopologyService,
    exchange_port: u16,
}

/// Read-only system-table query dependencies.
///
/// `information_schema` materialization is query preparation, not a command
/// service.  It receives exactly the local catalog snapshot source, the
/// connector control resolver needed for namespace facts, the injected system
/// catalog, and the durable MV metadata reader.  In particular, it has no DML
/// or MV mutation capability.
#[derive(Clone)]
pub struct SystemTableQueryKernel {
    facts_port: Arc<dyn SystemCatalogFactsPort>,
    system_catalog: Arc<dyn SystemCatalog>,
    mv_readiness: Arc<MvReadinessPort>,
}

impl SystemTableQueryKernel {
    pub fn new(
        facts_port: Arc<dyn SystemCatalogFactsPort>,
        system_catalog: Arc<dyn SystemCatalog>,
        mv_readiness: Arc<MvReadinessPort>,
    ) -> Self {
        Self {
            facts_port,
            system_catalog,
            mv_readiness,
        }
    }

    pub fn facts_port(&self) -> &Arc<dyn SystemCatalogFactsPort> {
        &self.facts_port
    }

    pub fn system_catalog(&self) -> &Arc<dyn SystemCatalog> {
        &self.system_catalog
    }

    pub(crate) fn mv_readiness(&self) -> &Arc<MvReadinessPort> {
        &self.mv_readiness
    }
}

impl QueryPreparationKernel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<ConnectorControlHost>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        query_execution: QueryExecutionService,
        backend_topology: BackendTopologyService,
        exchange_port: u16,
    ) -> Self {
        Self {
            functions,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control,
            unified_statistics,
            query_execution,
            backend_topology,
            exchange_port,
        }
    }

    pub(crate) fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        &self.functions
    }

    /// The typed connector controls this statement may resolve, frozen with
    /// the composition root's one registry.
    pub(crate) fn typed_connector_control(&self) -> &Arc<ConnectorControlHost> {
        &self.typed_connector_control
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn unified_statistics(&self) -> &Arc<UnifiedStatisticsResolver> {
        &self.unified_statistics
    }

    pub(crate) fn query_execution(&self) -> &QueryExecutionService {
        &self.query_execution
    }

    #[allow(
        dead_code,
        reason = "Retained for query-preparation callers that require the admitted topology service."
    )]
    pub(crate) fn backend_topology(&self) -> &BackendTopologyService {
        &self.backend_topology
    }

    pub(crate) const fn exchange_port(&self) -> u16 {
        self.exchange_port
    }
}

/// Foreground and historical DML execution dependencies, including CTAS.
///
/// The CTAS recovery adapter must use this same connector-control generation;
/// it is not a separate recovery context or scheduler.
#[derive(Clone)]
pub struct DmlExecutionKernel {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<ConnectorControlHost>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
    lake_publication_runtime_policy:
        Option<novarocks_query_application::publication::LakePublicationRuntimePolicy>,
}

/// Immutable SQL planning authorities shared by every DML statement.
#[derive(Clone)]
pub struct DmlPlanningServices {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    catalog_service: Arc<QueryCatalogService>,
}

impl DmlPlanningServices {
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
    ) -> Self {
        Self {
            functions,
            catalog_service,
        }
    }
}

impl DmlExecutionKernel {
    pub fn new(
        planning: DmlPlanningServices,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<ConnectorControlHost>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        query_execution: QueryExecutionService,
    ) -> Self {
        let DmlPlanningServices {
            functions,
            catalog_service,
        } = planning;
        Self {
            functions,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control,
            unified_statistics,
            mv_storage_observation,
            query_execution,
            lake_publication_runtime_policy: None,
        }
    }

    pub(crate) fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        &self.functions
    }

    /// The typed connector controls this statement may resolve.
    pub(crate) fn typed_connector_control(&self) -> &Arc<ConnectorControlHost> {
        &self.typed_connector_control
    }

    pub fn with_lake_publication_runtime_policy(
        mut self,
        policy: novarocks_query_application::publication::LakePublicationRuntimePolicy,
    ) -> Self {
        self.lake_publication_runtime_policy = Some(policy);
        self
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn unified_statistics(&self) -> &Arc<UnifiedStatisticsResolver> {
        &self.unified_statistics
    }

    pub(crate) fn mv_storage_observation(&self) -> &Arc<dyn MvStorageObservationPort> {
        &self.mv_storage_observation
    }

    pub(crate) fn query_execution(&self) -> &QueryExecutionService {
        &self.query_execution
    }

    pub(crate) fn lake_publication_runtime_policy(
        &self,
    ) -> Option<novarocks_query_application::publication::LakePublicationRuntimePolicy> {
        self.lake_publication_runtime_policy
    }
}

/// Catalog DDL dependencies.
///
/// This is intentionally a catalog-only kernel: it can mutate catalog facts
/// and enforce catalog-adjacent MV/view guards, but has no query execution,
/// statistics, DML writer or MV refresh capability.
#[derive(Clone)]
pub struct CatalogCommandKernel {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    mv_readiness: Arc<MvReadinessPort>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    view_service: Arc<dyn ViewService>,
}

impl CatalogCommandKernel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        mv_readiness: Arc<MvReadinessPort>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        view_service: Arc<dyn ViewService>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            mv_readiness,
            mv_storage_observation,
            view_service,
        }
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn mv_readiness(&self) -> &Arc<MvReadinessPort> {
        &self.mv_readiness
    }

    pub(crate) fn mv_storage_observation(&self) -> &Arc<dyn MvStorageObservationPort> {
        &self.mv_storage_observation
    }

    pub fn view_service(&self) -> &Arc<dyn ViewService> {
        &self.view_service
    }
}

/// MV metadata and refresh execution dependencies.
///
/// The backend is injected directly; the obsolete string-keyed
/// `ConnectorRegistry` is intentionally not represented here.
#[derive(Clone)]
#[allow(
    dead_code,
    reason = "The MV kernel keeps all owned ports explicit for refresh and activation paths compiled in other targets."
)]
pub struct MvExecutionKernel {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    mv_backend: Arc<IcebergMvBackend>,
    repository: Arc<dyn MvRepository>,
    application: Arc<dyn MvApplicationService>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
}

#[allow(
    dead_code,
    reason = "MV kernel accessors are retained as narrow ports for refresh and activation paths compiled in other targets."
)]
impl MvExecutionKernel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        mv_backend: Arc<IcebergMvBackend>,
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
            mv_backend,
            repository,
            application,
            storage_observation,
            query_execution,
        }
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn unified_statistics(&self) -> &Arc<UnifiedStatisticsResolver> {
        &self.unified_statistics
    }

    pub(crate) fn mv_backend(&self) -> &Arc<IcebergMvBackend> {
        &self.mv_backend
    }

    pub(crate) fn repository(&self) -> &Arc<dyn MvRepository> {
        &self.repository
    }

    pub(crate) fn application(&self) -> &Arc<dyn MvApplicationService> {
        &self.application
    }

    pub(crate) fn storage_observation(&self) -> &Arc<dyn MvStorageObservationPort> {
        &self.storage_observation
    }

    pub(crate) fn query_execution(&self) -> &QueryExecutionService {
        &self.query_execution
    }
}

/// View command dependencies.
#[derive(Clone)]
pub struct ViewExecutionKernel {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    view_service: Arc<dyn ViewService>,
}

impl ViewExecutionKernel {
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        view_service: Arc<dyn ViewService>,
    ) -> Self {
        Self {
            functions,
            catalog_service,
            catalog_application,
            connector_control,
            view_service,
        }
    }

    pub(crate) fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        &self.functions
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub fn view_service(&self) -> &Arc<dyn ViewService> {
        &self.view_service
    }
}

/// Table-maintenance command dependencies.
#[derive(Clone)]
pub struct MaintenanceExecutionKernel {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<ConnectorControlHost>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    query_execution: QueryExecutionService,
    service: Arc<dyn TableMaintenanceService>,
}

impl MaintenanceExecutionKernel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<ConnectorControlHost>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        query_execution: QueryExecutionService,
        service: Arc<dyn TableMaintenanceService>,
    ) -> Self {
        Self {
            functions,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control,
            mv_storage_observation,
            query_execution,
            service,
        }
    }

    pub(crate) fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        &self.functions
    }

    /// The typed connector controls a maintenance-owned read may resolve.
    pub(crate) fn typed_connector_control(&self) -> &Arc<ConnectorControlHost> {
        &self.typed_connector_control
    }

    pub(crate) fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }

    pub(crate) fn catalog_application(&self) -> Option<&Arc<dyn CatalogApplicationPort>> {
        self.catalog_application.as_ref()
    }

    pub(crate) fn connector_control(&self) -> &Arc<dyn ConnectorControlRegistry> {
        &self.connector_control
    }

    pub(crate) fn mv_storage_observation(&self) -> &Arc<dyn MvStorageObservationPort> {
        &self.mv_storage_observation
    }

    pub(crate) fn query_execution(&self) -> &QueryExecutionService {
        &self.query_execution
    }

    pub(crate) fn service(&self) -> &Arc<dyn TableMaintenanceService> {
        &self.service
    }
}

// Ownership: `CatalogAdmission` is Core's contract for target resolution, but
// each `impl` belongs to the value that holds the port. These blocks therefore
// live beside the kernels and move to Frontend together with them.
macro_rules! impl_kernel_catalog_admission {
    ($kernel:ty) => {
        impl crate::catalog_application::resolver::CatalogAdmission for $kernel {
            fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
                self.catalog_application().map(Arc::as_ref)
            }
        }
    };
}

impl_kernel_catalog_admission!(QueryPreparationKernel);
impl_kernel_catalog_admission!(CatalogCommandKernel);
impl_kernel_catalog_admission!(DmlExecutionKernel);
impl_kernel_catalog_admission!(MvExecutionKernel);
impl_kernel_catalog_admission!(ViewExecutionKernel);
impl_kernel_catalog_admission!(MaintenanceExecutionKernel);

/// Session catalog admission and namespace lookup.
///
/// This is deliberately not part of generic command dispatch: `USE` and
/// `SET CATALOG` are session admission operations.
#[derive(Clone)]
pub struct SessionCatalogResolver {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
}

impl SessionCatalogResolver {
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

impl SessionCatalogPort for SessionCatalogResolver {
    fn database_exists(&self, database_name: &str) -> Result<bool, QueryServiceError> {
        self.catalog_service
            .local()
            .read()
            .map_err(|_| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    "query catalog read lock poisoned",
                )
            })?
            .database_exists(database_name)
            .map_err(|error| QueryServiceError::new(QueryServiceErrorKind::Internal, error))
    }

    fn require_external_catalog_ready(&self, catalog_name: &str) -> Result<(), QueryServiceError> {
        let application = self.catalog_application.as_ref().ok_or_else(|| {
            QueryServiceError::new(
                QueryServiceErrorKind::Unavailable,
                "external catalogs require a configured frontend catalog application",
            )
        })?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog_name)
            .map_err(|error| {
                QueryServiceError::new(
                    QueryServiceErrorKind::BadDatabase,
                    format!("invalid catalog connector instance ID: {error}"),
                )
            })?;
        application
            .admit_catalog(&instance_id)
            .require_ready(&instance_id)
            .map(|_| ())
            .map_err(|error| {
                let kind = match error.kind() {
                    novarocks_catalog_application::CatalogApplicationErrorKind::Unavailable => {
                        QueryServiceErrorKind::Unavailable
                    }
                    _ => QueryServiceErrorKind::BadDatabase,
                };
                QueryServiceError::new(kind, error.to_string())
            })
    }

    fn external_namespace_exists(
        &self,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<bool, QueryServiceError> {
        let context = crate::connector::connector_request_context(
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .map_err(|error| QueryServiceError::new(QueryServiceErrorKind::Internal, error))?;
        crate::connector::metadata_namespace_exists(
            self.connector_control.as_ref(),
            context,
            catalog_name,
            namespace_name,
        )
        .map_err(|error| QueryServiceError::new(QueryServiceErrorKind::Internal, error))
    }
}

#[cfg(test)]
mod session_catalog_tests {
    use std::sync::Arc;

    use novarocks_query_application::session_error::QueryServiceErrorKind;
    use novarocks_query_application::sql::catalog::SessionCatalogPort;
    use novarocks_types::naming::DEFAULT_DATABASE;

    use super::SessionCatalogResolver;

    fn resolver_without_catalog_application() -> SessionCatalogResolver {
        SessionCatalogResolver::new(
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default()),
        )
    }

    #[test]
    fn local_database_lookup_uses_the_query_catalog_snapshot() {
        let resolver = resolver_without_catalog_application();

        assert!(
            resolver
                .database_exists(DEFAULT_DATABASE)
                .expect("built-in local database exists")
        );
    }

    #[test]
    fn external_catalog_admission_fails_closed_without_its_frontend_owner() {
        let resolver = resolver_without_catalog_application();

        let error = resolver
            .require_external_catalog_ready("warehouse")
            .expect_err("external catalog cannot bypass frontend catalog admission");
        assert_eq!(error.kind(), QueryServiceErrorKind::Unavailable);
    }
}
