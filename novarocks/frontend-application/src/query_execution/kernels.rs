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
use crate::mv::domain::readiness::MvReadinessPort;
use crate::query_execution::maintenance::TableMaintenanceService;
use crate::query_execution::package_freeze::CompiledPackageCarrier;
use novarocks_catalog_application::CatalogApplicationPort;
use novarocks_catalog_application::ConnectorControlHost;
use novarocks_query_application::api::BackendTopologyService;
use novarocks_query_application::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_query_application::sql::catalog::SessionCatalogPort;
use novarocks_query_application::system_tables::SystemTableRegistry;
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
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: CompiledPackageCarrier,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<ConnectorControlHost>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    backend_topology: BackendTopologyService,
}

/// Read-only system-table query dependencies.
///
/// Immutable system declarations only. Statement capabilities are owned by
/// the original completion Source, never by this static kernel.
#[derive(Clone)]
pub struct SystemTableQueryKernel {
    system_relations: Arc<SystemTableRegistry>,
}
impl SystemTableQueryKernel {
    pub fn new(system_relations: Arc<SystemTableRegistry>) -> Self {
        Self { system_relations }
    }
    pub fn system_relations(&self) -> &Arc<SystemTableRegistry> {
        &self.system_relations
    }
}

impl QueryPreparationKernel {
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<ConnectorControlHost>,
        unified_statistics: Arc<UnifiedStatisticsResolver>,
        backend_topology: BackendTopologyService,
        _exchange_port: u16,
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
        }
    }

    pub(crate) const fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
    }

    pub(crate) fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        &self.functions
    }

    /// The static carrier this kernel freezes completed plans into.
    pub(crate) const fn static_plan_carrier(&self) -> &CompiledPackageCarrier {
        &self.static_plan_carrier
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

    #[allow(
        dead_code,
        reason = "Retained for query-preparation callers that require the admitted topology service."
    )]
    pub(crate) fn backend_topology(&self) -> &BackendTopologyService {
        &self.backend_topology
    }
}

/// Foreground and historical DML execution dependencies, including CTAS.
///
/// The CTAS recovery adapter must use this same connector-control generation;
/// it is not a separate recovery context or scheduler.
#[derive(Clone)]
pub struct DmlExecutionKernel {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: CompiledPackageCarrier,
    catalog_service: Arc<QueryCatalogService>,
    view_service: Arc<dyn ViewService>,
    system_relations: Arc<SystemTableRegistry>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<ConnectorControlHost>,
    unified_statistics: Arc<UnifiedStatisticsResolver>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    connector_runtime: Option<tokio::runtime::Handle>,
    lake_publication_runtime_policy:
        Option<novarocks_query_application::publication::LakePublicationRuntimePolicy>,
    write_runtime: Option<(
        crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        crate::native::data_runtime::FrontendDataRuntime,
    )>,
}

/// Immutable SQL planning authorities shared by every DML statement.
#[derive(Clone)]
pub struct DmlPlanningServices {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: CompiledPackageCarrier,
    catalog_service: Arc<QueryCatalogService>,
    view_service: Arc<dyn ViewService>,
    system_relations: Arc<SystemTableRegistry>,
}

impl DmlPlanningServices {
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        constant_policy: novarocks_functions::ConstantPolicy,
        static_plan_carrier: CompiledPackageCarrier,
        view_service: Arc<dyn ViewService>,
        system_relations: Arc<SystemTableRegistry>,
    ) -> Self {
        Self {
            functions,
            constant_policy,
            static_plan_carrier,
            catalog_service,
            view_service,
            system_relations,
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
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    ) -> Self {
        let DmlPlanningServices {
            view_service,
            system_relations,
            functions,
            constant_policy,
            static_plan_carrier,
            catalog_service,
        } = planning;
        Self {
            view_service,
            system_relations,
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
            connector_runtime: None,
            state_store: None,
            write_runtime: None,
            lake_publication_runtime_policy: None,
        }
    }

    pub(crate) fn with_state_store_original(
        mut self,
        binding: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    ) -> Self {
        self.state_store = binding;
        self
    }
    pub(crate) fn snapshot_io_profile(
        &self,
        profile: novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile,
    ) -> Result<
        novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile,
        novarocks_spi::connector::ConnectorError,
    > {
        let Some(binding) = &self.state_store else {
            return Ok(profile);
        };
        let mut dependencies = profile.initial_dependencies().to_vec();
        dependencies.extend_from_slice(binding.blocking_dependency_facts());
        dependencies.sort_unstable();
        dependencies.dedup();
        novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile::from_geometry(
            *profile.geometry(),
            dependencies,
        )
    }

    /// Original role-owned handles, installed before any INSERT action.
    pub(crate) fn with_write_runtime(
        mut self,
        blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        data: crate::native::data_runtime::FrontendDataRuntime,
    ) -> Self {
        self.write_runtime = Some((blocking, data));
        self
    }

    pub(crate) fn write_runtime(
        &self,
    ) -> Result<
        &(
            crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
            crate::native::data_runtime::FrontendDataRuntime,
        ),
        String,
    > {
        self.write_runtime
            .as_ref()
            .ok_or_else(|| "INSERT original execution handles are not installed".to_owned())
    }

    pub(crate) fn completion_fact_owners(
        &self,
    ) -> Result<crate::query_execution::completion_facts::CompletionFactOwners, String> {
        let (blocking, _) = self.write_runtime()?;
        Ok(
            crate::query_execution::completion_facts::CompletionFactOwners::new(
                Arc::clone(&self.catalog_service),
                self.catalog_application.clone(),
                Arc::clone(&self.typed_connector_control),
                Arc::clone(&self.unified_statistics),
                self.mv_readiness.candidate_reader().clone(),
                Arc::clone(&self.mv_storage_observation),
                blocking.clone(),
                Arc::clone(&self.view_service),
                self.system_relations.clone(),
                Arc::clone(&self.mv_readiness),
            )
            .with_state_store_original(self.state_store.clone()),
        )
    }

    pub(crate) fn with_connector_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        self.connector_runtime = Some(runtime);
        self
    }

    pub(crate) fn connector_request_context_for_execution(
        &self,
        query_options: Option<&novarocks_proto_codec::lifecycle::QueryOptions>,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    ) -> Result<novarocks_spi::connector::ConnectorRequestContext, String> {
        match &self.connector_runtime {
            Some(runtime) => crate::connector::connector_request_context_for_execution_on_runtime(
                query_options,
                execution,
                runtime,
            ),
            None => {
                crate::connector::connector_request_context_for_execution(query_options, execution)
            }
        }
    }

    pub(crate) const fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
    }

    pub(crate) fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        &self.functions
    }

    /// The static carrier this kernel freezes completed plans into.
    pub(crate) const fn static_plan_carrier(&self) -> &CompiledPackageCarrier {
        &self.static_plan_carrier
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

    pub(crate) fn mv_readiness(&self) -> &Arc<MvReadinessPort> {
        &self.mv_readiness
    }

    pub(crate) fn lake_publication_runtime_policy(
        &self,
    ) -> Option<novarocks_query_application::publication::LakePublicationRuntimePolicy> {
        self.lake_publication_runtime_policy
    }
}

/// View command dependencies.
#[derive(Clone)]
pub struct ViewExecutionKernel {
    constant_policy: novarocks_functions::ConstantPolicy,
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    view_service: Arc<dyn ViewService>,
    system_relations: Arc<SystemTableRegistry>,
}

impl ViewExecutionKernel {
    pub(crate) fn system_relations(&self) -> &Arc<SystemTableRegistry> {
        &self.system_relations
    }
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        view_service: Arc<dyn ViewService>,
        constant_policy: novarocks_functions::ConstantPolicy,
        system_relations: Arc<SystemTableRegistry>,
    ) -> Self {
        Self {
            constant_policy,
            functions,
            catalog_service,
            catalog_application,
            connector_control,
            view_service,
            system_relations,
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
    pub(crate) const fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
    }
}

/// Table-maintenance command dependencies.
#[derive(Clone)]
pub struct MaintenanceExecutionKernel {
    state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    rewrite_runtime: Option<MaintenanceRewriteRuntime>,
    constant_policy: novarocks_functions::ConstantPolicy,
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    static_plan_carrier: CompiledPackageCarrier,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<ConnectorControlHost>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    service: Arc<dyn TableMaintenanceService>,
}

/// The exact existing host leaves used by one maintenance Action. This owns
/// neither a new Root nor a runtime, and exposes no caller-selected CPU work.
#[derive(Clone)]
pub(crate) struct MaintenanceRewriteRuntime {
    pub(crate) launcher:
        Arc<dyn crate::query_execution::logical_execution::LogicalExecutionLauncher>,
    pub(crate) completion: crate::query_execution::completion_facts::CompletionFactOwners,
    pub(crate) execution: (
        crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        crate::native::data_runtime::FrontendDataRuntime,
    ),
}

impl MaintenanceExecutionKernel {
    pub fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<ConnectorControlHost>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
        service: Arc<dyn TableMaintenanceService>,
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
        }
    }

    pub(crate) fn with_state_store_original(
        mut self,
        binding: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    ) -> Self {
        self.state_store = binding;
        self
    }
    pub(crate) fn state_store_original(
        &self,
    ) -> Result<
        &crate::state_store::execution::StateStoreOriginalBinding,
        novarocks_state_store_api::StateStoreOriginalUnsupported,
    > {
        self.state_store
            .as_ref()
            .ok_or(novarocks_state_store_api::StateStoreOriginalUnsupported)
    }

    pub(crate) fn with_rewrite_runtime(mut self, original: MaintenanceRewriteRuntime) -> Self {
        self.rewrite_runtime = Some(original);
        self
    }
    pub(crate) fn rewrite_runtime(
        &self,
    ) -> Result<&MaintenanceRewriteRuntime, novarocks_workload_control::WorkError> {
        self.rewrite_runtime
            .as_ref()
            .ok_or(novarocks_workload_control::WorkError::NotReady)
    }

    pub(crate) fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        &self.functions
    }

    /// The static carrier this kernel freezes completed plans into.
    pub(crate) const fn static_plan_carrier(&self) -> &CompiledPackageCarrier {
        &self.static_plan_carrier
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

    pub(crate) fn mv_readiness(&self) -> &Arc<MvReadinessPort> {
        &self.mv_readiness
    }

    pub(crate) fn service(&self) -> &Arc<dyn TableMaintenanceService> {
        &self.service
    }
    pub(crate) const fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
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
impl_kernel_catalog_admission!(DmlExecutionKernel);
impl_kernel_catalog_admission!(ViewExecutionKernel);
impl_kernel_catalog_admission!(MaintenanceExecutionKernel);

/// Session catalog admission and namespace lookup.
///
/// This is deliberately not part of generic command dispatch: `USE` and
/// property admission remain separate from generic command dispatch.
#[derive(Clone)]
pub struct SessionCatalogResolver {
    system_relations: Arc<SystemTableRegistry>,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    session_properties:
        Arc<novarocks_query_application::sql::session::registry::SessionPropertyRegistry>,
}

impl SessionCatalogResolver {
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        session_properties: Arc<
            novarocks_query_application::sql::session::registry::SessionPropertyRegistry,
        >,
        system_relations: Arc<SystemTableRegistry>,
    ) -> Self {
        Self {
            system_relations,
            catalog_service,
            catalog_application,
            connector_control,
            session_properties,
        }
    }
}

impl novarocks_query_application::sql::session::registry::SessionCatalogPropertyPort
    for SessionCatalogResolver
{
    fn session_property_catalog(
        &self,
        catalog: &str,
    ) -> Result<
        novarocks_query_application::sql::session::registry::SessionCatalogPropertyAdmission,
        QueryServiceError,
    > {
        use novarocks_catalog_application::CatalogConfigurationAdmission;
        use novarocks_query_application::sql::session::registry::{
            CatalogPropertyIdentity, CatalogPropertyMetadata, SessionCatalogPropertyAdmission,
        };
        if catalog == novarocks_types::naming::DEFAULT_CATALOG
            || catalog == novarocks_types::naming::SYSTEM_CATALOG
        {
            let _local = self.catalog_service.local().read().map_err(|_| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    "query catalog read lock poisoned",
                )
            })?;
            return Ok(SessionCatalogPropertyAdmission::Ready(Arc::new(
                CatalogPropertyMetadata::local(catalog),
            )));
        }
        let application = self.catalog_application.as_ref().ok_or_else(|| {
            QueryServiceError::new(
                QueryServiceErrorKind::Unavailable,
                "external catalog configuration admission is not installed",
            )
        })?;
        let instance = match novarocks_spi::connector::ConnectorInstanceId::parse(catalog) {
            Ok(instance) if instance.as_str() == catalog => instance,
            _ => return Ok(SessionCatalogPropertyAdmission::Absent),
        };
        match application.admit_catalog_configuration(&instance) {
            CatalogConfigurationAdmission::Absent => Ok(SessionCatalogPropertyAdmission::Absent),
            CatalogConfigurationAdmission::Unavailable { reason } => {
                Ok(SessionCatalogPropertyAdmission::Unavailable(
                    QueryServiceError::new(QueryServiceErrorKind::Unavailable, reason),
                ))
            }
            CatalogConfigurationAdmission::Ready(facts) => {
                let runtime = facts.runtime;
                let identity = CatalogPropertyIdentity {
                    attachment_id: runtime.attachment_id,
                    instance_id: runtime.instance_id,
                    provider_id: runtime.provider_id,
                    generation: runtime.generation,
                };
                let metadata = self
                    .session_properties
                    .bind_catalog(catalog, identity, facts.configuration.durable_properties())
                    .map_err(novarocks_query_application::sql::session::session_property_error)?;
                Ok(SessionCatalogPropertyAdmission::Ready(Arc::new(metadata)))
            }
        }
    }
}

#[async_trait::async_trait]
impl SessionCatalogPort for SessionCatalogResolver {
    fn database_exists(&self, database_name: &str) -> Result<bool, QueryServiceError> {
        if database_name == novarocks_types::naming::INFORMATION_SCHEMA {
            return Ok(true);
        }
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
        if catalog_name == novarocks_types::naming::SYSTEM_CATALOG {
            return Ok(());
        }
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

    async fn external_namespace_exists(
        &self,
        request: novarocks_spi::connector::ConnectorRequestContext,
        _capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<bool, QueryServiceError> {
        if catalog_name == novarocks_types::naming::SYSTEM_CATALOG {
            return Ok(self
                .system_relations
                .schemas(&novarocks_sql::system_table::SystemTableBinding::System)
                .into_iter()
                .chain(self.system_relations.schemas(
                    &novarocks_sql::system_table::SystemTableBinding::Catalog(catalog_name.into()),
                ))
                .any(|schema| schema == namespace_name));
        }
        if namespace_name == novarocks_types::naming::INFORMATION_SCHEMA {
            self.require_external_catalog_ready(catalog_name)?;
            return Ok(true);
        }
        crate::connector::metadata_namespace_exists_async(
            self.connector_control.as_ref(),
            request,
            catalog_name,
            namespace_name,
        )
        .await
        .map_err(|error| QueryServiceError::new(QueryServiceErrorKind::Internal, error))
    }
}

#[cfg(test)]
mod session_catalog_tests {
    use std::sync::Arc;

    use novarocks_query_application::session_error::QueryServiceErrorKind;
    use novarocks_query_application::sql::catalog::SessionCatalogPort;
    use novarocks_types::naming::DEFAULT_DATABASE;

    use super::{SessionCatalogResolver, SystemTableRegistry};

    #[tokio::test]
    async fn built_in_system_session_catalog_uses_the_installed_registry_without_external_admission()
     {
        use novarocks_query_application::sql::session::registry::{
            SessionCatalogPropertyAdmission, SessionCatalogPropertyPort,
        };
        let resolver = resolver_without_catalog_application();
        resolver.require_external_catalog_ready("system").unwrap();
        let SessionCatalogPropertyAdmission::Ready(metadata) =
            resolver.session_property_catalog("system").unwrap()
        else {
            panic!("built-in catalog property metadata");
        };
        assert_eq!(metadata.catalog(), "system");
        assert!(metadata.identity().is_none());
        assert_eq!(metadata.declarations().count(), 0);
        assert!(resolver.database_exists("information_schema").unwrap());
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let context = crate::connector::connector_request_context(None, stop.view()).unwrap();
        let (_control, _root, capacity, _positions) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        for (namespace, expected) in [
            ("metadata", true),
            ("information_schema", true),
            ("absent", false),
        ] {
            assert_eq!(
                resolver
                    .external_namespace_exists(context.clone(), &capacity, "system", namespace)
                    .await
                    .unwrap(),
                expected
            );
        }
    }

    fn resolver_without_catalog_application() -> SessionCatalogResolver {
        SessionCatalogResolver::new(
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default()),
            Arc::new(
                novarocks_query_application::sql::session::registry::SessionPropertyRegistry::new()
                    .unwrap(),
            ),
            Arc::new(SystemTableRegistry::try_new([]).unwrap()),
        )
    }

    #[tokio::test]
    async fn local_database_lookup_uses_the_query_catalog_snapshot() {
        let resolver = resolver_without_catalog_application();

        assert!(
            resolver
                .database_exists(DEFAULT_DATABASE)
                .expect("built-in local database exists")
        );
    }

    #[tokio::test]
    async fn external_catalog_admission_fails_closed_without_its_frontend_owner() {
        let resolver = resolver_without_catalog_application();

        let error = resolver
            .require_external_catalog_ready("warehouse")
            .expect_err("external catalog cannot bypass frontend catalog admission");
        assert_eq!(error.kind(), QueryServiceErrorKind::Unavailable);
    }

    #[tokio::test]
    async fn cancelled_external_namespace_lookup_stops_before_provider_call() {
        let resolver = SessionCatalogResolver::new(
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default()),
            Arc::new(
                novarocks_query_application::sql::session::registry::SessionPropertyRegistry::new()
                    .unwrap(),
            ),
            Arc::new(SystemTableRegistry::try_new([]).unwrap()),
        );
        let cancellation = novarocks_spi::connector::ConnectorStopOwner::new();
        let request = crate::connector::connector_request_context(None, cancellation.view())
            .expect("connector request context");
        cancellation.request_stop();
        let (control, root, window) = crate::task_execution::blocking_io::tests::admitted_class(
            novarocks_workload_control::ResultWindowClass::Local,
        );
        let capacity = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(
            &root.owner.scope(), window.retain_alias(),
        ).unwrap();
        let error = resolver
            .external_namespace_exists(request, &capacity, "warehouse", "analytics")
            .await
            .expect_err("cancelled request must not reach the provider");
        drop(capacity);
        root.owner.complete();
        root.business.release();
        drop(window);
        crate::task_execution::blocking_io::tests::until(|| control.snapshot().scopes.is_empty());
        control.close_admission();
        assert!(control.shutdown().is_ok());
        assert_eq!(error.kind(), QueryServiceErrorKind::Internal);
        assert!(error.message().contains("connector request was cancelled"));
    }
}
