#![allow(dead_code)]
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

use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::runtime::Handle;

use crate::mv::refresh::execution_context::MvRefreshPruningLimits;
pub use crate::query_execution::post_compile::{
    NativeFragmentEncodingInput, PreparedDistributedQueryAssembly,
};
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::query_execution::{PreparedImmediateQuery, PreparedQueryCompletion, StatementResult};
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::query_result::{
    QueryResult, QueryResultColumn, build_string_query_result, record_batch_to_chunk,
};
use novarocks_execution::runtime::query_options::QueryOptions;

use crate::catalog_application::query_catalog::{QueryCatalogService, new_query_catalog_service};
use crate::connector::UnifiedStatisticsResolver;
#[cfg(test)]
use crate::mv::application::UnavailableMvApplicationService;
use crate::mv::application::{MvApplicationService, MvRefreshProviderActivation};
use crate::mv::repository::MvRepository;
#[cfg(test)]
use crate::mv::repository::UnavailableMvRepository;
use novarocks_catalog::identifier::normalize_identifier;
#[cfg(test)]
use novarocks_catalog::memory::DEFAULT_DATABASE;
pub use novarocks_sql::planning::catalog::TableLookupMode;

use crate::catalog_application::resolver as backend_resolver;
use crate::catalog_application::statement::{
    execute_create_database_statement, execute_create_table_statement,
    execute_drop_catalog_statement, execute_drop_database_statement, execute_drop_table_statement,
    looks_like_add_equality_delete, looks_like_alter_iceberg_properties,
    looks_like_alter_iceberg_schema, looks_like_alter_partition_column,
    looks_like_show_create_table, parse_alter_iceberg_properties_sql,
    parse_alter_partition_column_sql, parse_show_create_table,
};
use crate::query_execution::kernels as domain;
use crate::query_execution::planning::time_travel::{
    has_time_travel_refs, rewrite_time_travel_refs,
};
#[cfg(test)]
use novarocks_sql::syntax::{sql_type_to_arrow_type, sqlparser_expr_to_literal};

use novarocks_catalog::partition::LegacyRangePartition;

use crate::catalog_application::query_catalog::{CatalogServiceSource, catalog_service_snapshot};

macro_rules! impl_kernel_catalog_service_source {
    ($kernel:ty) => {
        impl CatalogServiceSource for $kernel {
            fn catalog_service(&self) -> &Arc<QueryCatalogService> {
                self.catalog_service()
            }
        }
    };
}

impl_kernel_catalog_service_source!(domain::QueryPreparationKernel);
impl_kernel_catalog_service_source!(domain::DmlExecutionKernel);
impl_kernel_catalog_service_source!(domain::CatalogCommandKernel);
impl_kernel_catalog_service_source!(domain::MvExecutionKernel);
impl_kernel_catalog_service_source!(domain::StatisticsExecutionKernel);
impl_kernel_catalog_service_source!(domain::ViewExecutionKernel);
impl_kernel_catalog_service_source!(domain::MaintenanceExecutionKernel);

/// Freeze the catalog source for one Frontend-admitted query.  The returned
/// value is owned by the caller so the paired materializer can borrow the
/// same snapshot throughout parse, compile, and post-compile preparation.
pub fn query_catalog_service_snapshot(
    query_kernel: &domain::QueryPreparationKernel,
) -> QueryCatalogService {
    catalog_service_snapshot(query_kernel)
}

/// Build the only request-local catalog materializer available to Frontend
/// query admission.  It allocates the paired binding store inside Core; the
/// caller can pass the materializer to SQL and post-compile preparation but
/// cannot inject a different store.
pub fn build_query_catalog_materializer<'a>(
    query_kernel: &'a domain::QueryPreparationKernel,
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    lookup_mode: TableLookupMode,
) -> crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer<'a> {
    build_catalog_service_provider(
        current_catalog,
        catalog_service,
        query_kernel.connector_control().as_ref(),
        connector_context,
        lookup_mode,
        query_kernel.catalog_application().map(Arc::as_ref),
    )
}

/// Freeze optional MV rewrite candidates through the request's exact Core
/// ports.  Frontend chooses whether an unavailable repository means no
/// candidates; it never gains connector-control access directly.
pub fn freeze_query_mv_rewrite_definition_index(
    query_kernel: &domain::QueryPreparationKernel,
    repository: &dyn MvRepository,
    storage_observation: &dyn crate::mv::storage_observation::MvStorageObservationPort,
) -> Result<novarocks_sql::compiler::MvRewriteDefinitionIndex, String> {
    crate::mv::rewrite_prep::freeze_mv_rewrite_definition_index_with_ports(
        repository,
        query_kernel.connector_control().as_ref(),
        storage_observation,
    )
}

/// Freeze request-local statistics evidence from the same catalog binding
/// store used by SQL analysis.  It never resolves a newer connector state.
pub fn query_statistics_snapshot(
    query_kernel: &domain::QueryPreparationKernel,
    analyzer_catalog: &crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer<'_>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::query_execution::planning::statistics::QueryStatisticsContext, String> {
    crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
        query_kernel,
        analyzer_catalog.query_table_bindings(),
        connector_context,
    )
}

/// Leaf ports used to compile and submit a foreground DML query.
///
/// The optional maintenance-context fallback remains only for legacy callers.
/// A Frontend-composed [`domain::DmlExecutionKernel`] rejects that fallback so
/// foreground DML must arrive with its admitted request context.
pub(crate) trait DmlQueryExecutionKernel:
    CatalogServiceSource
    + crate::query_execution::planning::time_travel::TimeTravelResolver
    + crate::query_execution::planning::statistics::QueryStatisticsResolver
{
    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlResolver;
    fn catalog_application(
        &self,
    ) -> Option<&dyn crate::catalog_application::CatalogApplicationPort>;
    fn query_execution(&self) -> &crate::query_execution::service::QueryExecutionService;
    fn capture_dml_fallback_execution(
        &self,
    ) -> Result<crate::query_execution::request_context::QueryExecutionContext, String>;
}

impl DmlQueryExecutionKernel for domain::DmlExecutionKernel {
    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlResolver {
        self.connector_control().as_ref()
    }

    fn catalog_application(
        &self,
    ) -> Option<&dyn crate::catalog_application::CatalogApplicationPort> {
        self.catalog_application().map(Arc::as_ref)
    }

    fn query_execution(&self) -> &crate::query_execution::service::QueryExecutionService {
        self.query_execution()
    }

    fn capture_dml_fallback_execution(
        &self,
    ) -> Result<crate::query_execution::request_context::QueryExecutionContext, String> {
        Err("foreground DML requires an admitted query execution context".to_string())
    }
}

/// MV activation compiles an already-admitted write against the same query
/// kernel that froze its catalog/statistics facts. It must not recover the
/// legacy aggregate just to enter the generic Iceberg-write preparation path.
impl DmlQueryExecutionKernel for domain::QueryPreparationKernel {
    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlResolver {
        self.connector_control().as_ref()
    }

    fn catalog_application(
        &self,
    ) -> Option<&dyn crate::catalog_application::CatalogApplicationPort> {
        self.catalog_application().map(Arc::as_ref)
    }

    fn query_execution(&self) -> &crate::query_execution::service::QueryExecutionService {
        self.query_execution()
    }

    fn capture_dml_fallback_execution(
        &self,
    ) -> Result<crate::query_execution::request_context::QueryExecutionContext, String> {
        Err("MV activation requires an admitted query execution context".to_string())
    }
}

/// Builds the request-local SQL materializer behind the Frontend-owned catalog
/// admission gate.
///
/// Every analyzer entry point passes the state's application port: an external
/// table can only be materialized while its attachment is `Ready` in this
/// process, and there is no ungated variant to fall back to.
pub(crate) fn build_catalog_service_provider<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    _lookup_mode: TableLookupMode,
    catalog_application: Option<&'a dyn crate::catalog_application::CatalogApplicationPort>,
) -> crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer<'a> {
    build_catalog_service_provider_with_query_local_overlays(
        current_catalog,
        catalog_service,
        controls,
        connector_context,
        _lookup_mode,
        Vec::new(),
        catalog_application,
    )
}

/// Build the application catalog facade for one admitted query, optionally
/// supplying generated relations that are scoped to that request.  These
/// overlays are projected into SQL binding tokens before analysis and never
/// enter the shared local catalog.
pub(crate) fn build_catalog_service_provider_with_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    _lookup_mode: TableLookupMode,
    overlays: Vec<crate::query_execution::planning::catalog_materializer::QueryLocalTableOverlay>,
    catalog_application: Option<&'a dyn crate::catalog_application::CatalogApplicationPort>,
) -> crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer<'a> {
    let bindings = Arc::new(
        crate::query_execution::planning::bindings::QueryTableBindingStore::try_new()
            .expect("query table binding scope allocation must not fail"),
    );
    build_catalog_service_provider_with_bindings_and_query_local_overlays(
        current_catalog,
        catalog_service,
        controls,
        connector_context,
        bindings,
        overlays,
        catalog_application,
    )
}

pub(crate) fn build_catalog_service_provider_with_bindings_and_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    bindings: Arc<crate::query_execution::planning::bindings::QueryTableBindingStore>,
    overlays: Vec<crate::query_execution::planning::catalog_materializer::QueryLocalTableOverlay>,
    catalog_application: Option<&'a dyn crate::catalog_application::CatalogApplicationPort>,
) -> crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer<'a> {
    let loader = crate::query_execution::planning::statistics::iceberg_table_binding_loader(
        controls,
        connector_context,
    );
    crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
        current_catalog,
        catalog_service,
        bindings,
        loader,
        overlays,
    )
    .with_catalog_application(catalog_application)
}

#[cfg(test)]
pub(crate) struct TestConnectorControlRegistry {
    active: std::sync::Mutex<
        std::collections::HashMap<
            novarocks_spi::connector::ConnectorInstanceId,
            Arc<novarocks_spi::connector::ConnectorControlBinding>,
        >,
    >,
    factories: std::collections::HashMap<
        novarocks_spi::connector::ConnectorProviderId,
        Arc<dyn novarocks_spi::connector::ConnectorControlFactory>,
    >,
}

#[cfg(test)]
impl Default for TestConnectorControlRegistry {
    fn default() -> Self {
        let factory: Arc<dyn novarocks_spi::connector::ConnectorControlFactory> =
            Arc::new(TestConnectorControlFactory);
        Self {
            active: std::sync::Mutex::new(std::collections::HashMap::new()),
            factories: std::collections::HashMap::from([(factory.provider_id().clone(), factory)]),
        }
    }
}

#[cfg(test)]
struct TestConnectorControlFactory;

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlFactory for TestConnectorControlFactory {
    fn provider_id(&self) -> &novarocks_spi::connector::ConnectorProviderId {
        static PROVIDER_ID: std::sync::OnceLock<novarocks_spi::connector::ConnectorProviderId> =
            std::sync::OnceLock::new();
        PROVIDER_ID.get_or_init(|| {
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("test provider ID")
        })
    }

    fn create_control(
        &self,
        request: novarocks_spi::connector::ConnectorControlFactoryRequest,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlCreation,
        novarocks_spi::connector::ConnectorError,
    > {
        let durable_properties = request.properties().to_vec();
        let binding = crate::connector::scan_model::planned_files_fixture_binding_for_provider(
            request.provider_id().clone(),
            request.instance_id().as_str(),
            std::collections::HashMap::new(),
            None,
        );
        novarocks_spi::connector::ConnectorControlCreation::try_new(
            &request,
            binding,
            durable_properties,
        )
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlResolver for TestConnectorControlRegistry {
    fn observe_current_binding(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorExecutionBindingKey,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
        Ok(novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        })
    }

    fn acquire_current(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlPlanningLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
        Ok(novarocks_spi::connector::ConnectorControlPlanningLease::new(binding, || {}))
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorCatalogMutationResolver for TestConnectorControlRegistry {
    fn acquire_current_mutation(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorCatalogMutationLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active mutation binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        let mutation = binding.mutation().cloned().ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "test connector control binding has no mutation capability",
            )
        })?;
        novarocks_spi::connector::ConnectorCatalogMutationLease::new(
            binding.descriptor().clone(),
            binding.incarnation(),
            mutation,
            || {},
        )
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorDataMutationResolver for TestConnectorControlRegistry {
    fn acquire_current_data_mutation(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorDataMutationLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active data mutation binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        test_data_mutation_lease(binding)
    }

    fn acquire_exact_data_mutation(
        &self,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        novarocks_spi::connector::ConnectorDataMutationLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(&key.instance_id)
            .filter(|binding| binding.incarnation() == key.incarnation)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "exact connector data mutation generation is unavailable",
                )
            })?;
        test_data_mutation_lease(binding)
    }
}

#[cfg(test)]
fn test_data_mutation_lease(
    binding: Arc<novarocks_spi::connector::ConnectorControlBinding>,
) -> Result<
    novarocks_spi::connector::ConnectorDataMutationLease,
    novarocks_spi::connector::ConnectorError,
> {
    let mutation = binding.data_mutation().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no data mutation capability",
        )
    })?;
    let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorDataMutationLease::new(
        binding.descriptor().clone(),
        key,
        Arc::clone(binding.metadata()),
        mutation,
        || {},
    )
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorMetadataMaintenanceResolver
    for TestConnectorControlRegistry
{
    fn acquire_current_metadata_maintenance(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorMetadataMaintenanceLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active metadata maintenance binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        test_metadata_maintenance_lease(binding)
    }

    fn acquire_exact_metadata_maintenance(
        &self,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        novarocks_spi::connector::ConnectorMetadataMaintenanceLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(&key.instance_id)
            .filter(|binding| binding.incarnation() == key.incarnation)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "exact connector metadata maintenance generation is unavailable",
                )
            })?;
        test_metadata_maintenance_lease(binding)
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorCleanupMaintenanceResolver
    for TestConnectorControlRegistry
{
    fn acquire_current_cleanup_maintenance(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorCleanupMaintenanceLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "test connector cleanup binding is not active",
                )
            })?;
        test_cleanup_maintenance_lease(binding)
    }

    fn acquire_exact_cleanup_maintenance(
        &self,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        novarocks_spi::connector::ConnectorCleanupMaintenanceLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(&key.instance_id)
            .filter(|binding| binding.incarnation() == key.incarnation)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "exact test connector cleanup generation is unavailable",
                )
            })?;
        test_cleanup_maintenance_lease(binding)
    }
}

#[cfg(test)]
fn test_cleanup_maintenance_lease(
    binding: Arc<novarocks_spi::connector::ConnectorControlBinding>,
) -> Result<
    novarocks_spi::connector::ConnectorCleanupMaintenanceLease,
    novarocks_spi::connector::ConnectorError,
> {
    let cleanup = binding.cleanup_maintenance().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no cleanup maintenance capability",
        )
    })?;
    let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorCleanupMaintenanceLease::new(
        binding.descriptor().clone(),
        key,
        Arc::clone(binding.metadata()),
        cleanup,
        || {},
    )
}

#[cfg(test)]
fn test_metadata_maintenance_lease(
    binding: Arc<novarocks_spi::connector::ConnectorControlBinding>,
) -> Result<
    novarocks_spi::connector::ConnectorMetadataMaintenanceLease,
    novarocks_spi::connector::ConnectorError,
> {
    let maintenance = binding.metadata_maintenance().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no metadata maintenance capability",
        )
    })?;
    let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorMetadataMaintenanceLease::new(
        binding.descriptor().clone(),
        key,
        Arc::clone(binding.metadata()),
        maintenance,
        || {},
    )
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorDistributedRewriteResolver
    for TestConnectorControlRegistry
{
    fn acquire_current_distributed_rewrite(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorDistributedRewriteLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active distributed rewrite binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        test_distributed_rewrite_lease(binding)
    }

    fn acquire_exact_distributed_rewrite(
        &self,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        novarocks_spi::connector::ConnectorDistributedRewriteLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(&key.instance_id)
            .filter(|binding| binding.incarnation() == key.incarnation)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "exact connector distributed rewrite generation is unavailable",
                )
            })?;
        test_distributed_rewrite_lease(binding)
    }
}

#[cfg(test)]
fn test_distributed_rewrite_lease(
    binding: Arc<novarocks_spi::connector::ConnectorControlBinding>,
) -> Result<
    novarocks_spi::connector::ConnectorDistributedRewriteLease,
    novarocks_spi::connector::ConnectorError,
> {
    let rewrite = binding.distributed_rewrite().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no distributed rewrite capability",
        )
    })?;
    let write = binding.write().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no distributed write capability",
        )
    })?;
    let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorDistributedRewriteLease::new(
        binding.descriptor().clone(),
        key,
        novarocks_spi::connector::ConnectorControlPlanningLease::new(binding.clone(), || {}),
        Arc::clone(binding.metadata()),
        Arc::clone(binding.planning()),
        rewrite,
        write,
        binding.execution_distribution().clone(),
        || {},
    )
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorHistoricalMaintenanceResolver
    for TestConnectorControlRegistry
{
    fn acquire_current_historical_maintenance(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorHistoricalMaintenanceLease,
        novarocks_spi::connector::ConnectorError,
    > {
        // The test registry installs no historical recovery capability. This
        // is the production-shaped answer for a provider that does not offer
        // one, and it is what keeps such an operation unresolved.
        Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            format!(
                "connector control instance `{}` has no historical maintenance recovery \
                 capability",
                instance_id.as_str()
            ),
        ))
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorStatisticsResolver for TestConnectorControlRegistry {
    fn acquire_current_statistics(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorStatisticsLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active statistics binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        let statistics = binding.statistics().cloned().ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "test connector control binding has no statistics capability",
            )
        })?;
        novarocks_spi::connector::ConnectorStatisticsLease::new(
            binding.descriptor().clone(),
            binding.incarnation(),
            statistics,
            || {},
        )
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlRegistry for TestConnectorControlRegistry {
    fn register(
        &self,
        binding: novarocks_spi::connector::ConnectorControlBinding,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        let instance_id = binding.descriptor().instance_id.clone();
        let incarnation = binding.incarnation();
        let mut active = self.active.lock().map_err(|_| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "test connector control registry lock poisoned",
            )
        })?;
        if let Some(existing) = active.get(&instance_id) {
            if existing.incarnation() == incarnation {
                return Ok(());
            }
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                format!(
                    "connector control instance `{}` already has an active generation",
                    instance_id.as_str()
                ),
            ));
        }
        active.insert(instance_id, Arc::new(binding));
        Ok(())
    }

    fn retire_current(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        let removed = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .remove(instance_id);
        removed.map(|_| ()).ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::NotFound,
                format!(
                    "connector control instance `{}` is not active",
                    instance_id.as_str()
                ),
            )
        })
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlFactoryResolver for TestConnectorControlRegistry {
    fn create_control(
        &self,
        request: novarocks_spi::connector::ConnectorControlFactoryRequest,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlCreation,
        novarocks_spi::connector::ConnectorError,
    > {
        let factory = self.factories.get(request.provider_id()).ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::NotFound,
                "test connector control factory is not installed",
            )
        })?;
        factory.create_control(request)
    }
}

#[cfg(test)]
struct RejectingTestDistributedQueryCoordinator;

#[cfg(test)]
impl crate::query_execution::contract::DistributedQueryCoordinator
    for RejectingTestDistributedQueryCoordinator
{
    fn begin_write_operation(
        &self,
        registration: crate::query_execution::contract::ConnectorWriteOperationRegistration,
        lease: novarocks_spi::connector::ConnectorWriteLease,
    ) -> Result<
        crate::query_execution::write_operation::ConnectorWriteOperationSession,
        crate::query_execution::contract::DistributedQueryError,
    > {
        crate::query_execution::write_operation::ConnectorWriteOperationSession::try_begin(
            registration,
            lease,
        )
        .map_err(|error| {
            crate::query_execution::contract::DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::Failed,
                format!("seal test connector write operation cohorts: {error}"),
            )
        })
    }

    fn execute(
        &self,
        request: crate::query_execution::contract::DistributedQueryRequest,
    ) -> Result<
        crate::query_execution::contract::DistributedQueryOutcome,
        crate::query_execution::contract::DistributedQueryError,
    > {
        let intent = request.intent();
        Err(
            crate::query_execution::contract::DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::Rejected,
                format!(
                    "core unit-test query coordinator does not execute native {intent:?} fragments; \
                 assert request shaping locally or use Backend/all-in-one integration coverage"
                ),
            ),
        )
    }
}

#[cfg(test)]
pub(crate) fn test_query_execution_service()
-> crate::query_execution::service::QueryExecutionService {
    crate::query_execution::service::QueryExecutionService::new(std::sync::Arc::new(
        RejectingTestDistributedQueryCoordinator,
    ))
}

#[cfg(test)]
pub(crate) struct TestSerializationGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
unsafe impl Send for TestSerializationGuard {}

#[cfg(test)]
unsafe impl Sync for TestSerializationGuard {}

#[cfg(test)]
pub(crate) fn acquire_standalone_test_guard() -> TestSerializationGuard {
    use std::sync::{Mutex, OnceLock};
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    TestSerializationGuard { _guard: guard }
}

#[cfg(test)]
pub(crate) fn test_mv_repository() -> Arc<dyn MvRepository> {
    Arc::new(crate::mv::test_repository::InMemoryMvRepository::default())
}

#[cfg(test)]
pub enum TestPreparedQueryOperation {
    Immediate(PreparedImmediateQuery),
    Distributed {
        assembly: PreparedDistributedQueryAssembly,
        completion: PreparedQueryCompletion,
    },
}

/// Narrow core compiler kernel consumed by frontend QueryService.
///
/// It deliberately exposes neither a composition aggregate nor connector internals.
/// Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
#[cfg(test)]
#[derive(Clone)]
pub struct TestQueryCompiler {
    query: domain::QueryPreparationKernel,
    view: domain::ViewExecutionKernel,
    system_tables: domain::SystemTableQueryKernel,
    mv_repository: Arc<dyn MvRepository>,
    mv_storage_observation: Arc<dyn crate::mv::storage_observation::MvStorageObservationPort>,
}

#[cfg(test)]
impl TestQueryCompiler {
    pub fn from_domain_kernels(
        query: domain::QueryPreparationKernel,
        view: domain::ViewExecutionKernel,
        system_tables: domain::SystemTableQueryKernel,
        mv_repository: Arc<dyn MvRepository>,
        mv_storage_observation: Arc<dyn crate::mv::storage_observation::MvStorageObservationPort>,
    ) -> Self {
        Self {
            query,
            view,
            system_tables,
            mv_repository,
            mv_storage_observation,
        }
    }

    pub fn prepare(
        &self,
        sql: &str,
        context: &crate::query_execution::request_context::RequestContext,
        query_opts: Option<QueryOptions>,
    ) -> Result<TestPreparedQueryOperation, String> {
        let connector_context = crate::connector::connector_request_context_for_query(
            query_opts.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        self.prepare_with_connector_context(sql, context, query_opts, connector_context)
    }

    fn prepare_with_connector_context(
        &self,
        sql: &str,
        request_context: &crate::query_execution::request_context::RequestContext,
        query_opts: Option<QueryOptions>,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<TestPreparedQueryOperation, String> {
        if !is_query_sql(sql) {
            return Err(
                "non-query statements must be executed through a typed command capability".into(),
            );
        }
        use sqlparser::ast as sqlast;
        let current_catalog = request_context.session().current_catalog();
        let current_database = request_context.session().current_database();
        let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql)?;
        let (parse_sql, forced_explain_level, force_logical_explain) =
            if let Some((rewritten, level)) = split_explain_logical_sql(&normalized) {
                (rewritten, Some(level), true)
            } else if let Some((rewritten, level)) = split_explain_costs_sql(&normalized) {
                (rewritten, Some(level), false)
            } else {
                (normalized.clone(), None, false)
            };
        let stmt = novarocks_sql::syntax::parse_normalized_sql_raw(&parse_sql)
            .map_err(|error| format_parser_error(&error.to_string()))?;
        match stmt {
            sqlast::Statement::Explain {
                statement,
                verbose,
                analyze: false,
                ..
            } => {
                let sqlast::Statement::Query(ref query) = *statement else {
                    return Err("EXPLAIN only supports SELECT queries".to_string());
                };
                let prepared = prepare_explain_query_with_ports(
                    &self.query,
                    &self.view,
                    current_catalog,
                    current_database,
                    query,
                    &connector_context,
                )?;
                let level = forced_explain_level.unwrap_or(if verbose {
                    novarocks_sql::compiler::ExplainLevel::Verbose
                } else {
                    novarocks_sql::compiler::ExplainLevel::Normal
                });
                let catalog_service_snapshot = catalog_service_snapshot(&self.query);
                let analyzer_provider = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service_snapshot,
                    self.query.connector_control().as_ref(),
                    connector_context.clone(),
                    TableLookupMode::ExplainStats,
                    self.query.catalog_application().map(Arc::as_ref),
                );
                let result = explain_query_with_sql_compiler_kernel_with_ports(
                    &prepared,
                    &analyzer_provider,
                    current_catalog,
                    current_database,
                    &self.query,
                    self.mv_repository.as_ref(),
                    self.mv_storage_observation.as_ref(),
                    &connector_context,
                    request_context.execution(),
                    level,
                    force_logical_explain,
                )?;
                Ok(TestPreparedQueryOperation::Immediate(
                    PreparedImmediateQuery::new(StatementResult::Query(result)),
                ))
            }
            sqlast::Statement::Explain {
                statement,
                analyze: true,
                ..
            } => {
                let sqlast::Statement::Query(ref query) = *statement else {
                    return Err("EXPLAIN ANALYZE only supports SELECT queries".to_string());
                };
                self.prepare_explain_analyze(
                    query,
                    current_catalog,
                    current_database,
                    query_opts,
                    &connector_context,
                    request_context.execution(),
                )
            }
            sqlast::Statement::Query(ref query) => {
                if let Some(result) =
                    crate::catalog_application::information_schema::try_query_materialized_views(
                        self.system_tables.mv_repository().as_ref(),
                        query,
                    )?
                {
                    return Ok(TestPreparedQueryOperation::Immediate(
                        PreparedImmediateQuery::new(result),
                    ));
                }
                let mut prepared = query.as_ref().clone();
                self.view.view_service().rewrite_query(
                    &self.view,
                    &mut prepared,
                    crate::view::ViewRequestContext {
                        current_catalog,
                        current_database,
                        connector_context: Some(&connector_context),
                    },
                )?;
                crate::catalog_application::virtual_table::rewrite_query(
                    self.system_tables.catalog_service(),
                    self.system_tables.connector_control().as_ref(),
                    self.system_tables.system_catalog().as_ref(),
                    &mut prepared,
                )?;
                if has_time_travel_refs(&prepared) {
                    rewrite_time_travel_refs(
                        &self.query,
                        current_catalog,
                        current_database,
                        &mut prepared,
                        &connector_context,
                    )?;
                }
                let catalog_service_snapshot = catalog_service_snapshot(&self.query);
                let analyzer_provider = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service_snapshot,
                    self.query.connector_control().as_ref(),
                    connector_context.clone(),
                    TableLookupMode::SchemaOnly,
                    self.query.catalog_application().map(Arc::as_ref),
                );
                let (assembly, _, _) = prepare_query_with_sql_compiler_kernel_with_ports(
                    &prepared,
                    &analyzer_provider,
                    current_catalog,
                    current_database,
                    &self.query,
                    self.mv_repository.as_ref(),
                    self.mv_storage_observation.as_ref(),
                    &connector_context,
                    query_opts,
                    request_context.execution(),
                    novarocks_sql::compiler::SqlCompileIntent::Query,
                    true,
                )?;
                Ok(TestPreparedQueryOperation::Distributed {
                    assembly,
                    completion: PreparedQueryCompletion::result(),
                })
            }
            _ => Err("query compiler only supports SELECT and EXPLAIN statements".to_string()),
        }
    }

    fn prepare_explain_analyze(
        &self,
        query: &sqlparser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_opts: Option<QueryOptions>,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &crate::query_execution::request_context::QueryExecutionContext,
    ) -> Result<TestPreparedQueryOperation, String> {
        let query = prepare_explain_query_with_ports(
            &self.query,
            &self.view,
            current_catalog,
            current_database,
            query,
            connector_context,
        )?;
        let catalog_service_snapshot = catalog_service_snapshot(&self.query);
        let analyzer_provider = build_catalog_service_provider(
            current_catalog,
            &catalog_service_snapshot,
            self.query.connector_control().as_ref(),
            connector_context.clone(),
            TableLookupMode::ExplainStats,
            self.query.catalog_application().map(Arc::as_ref),
        );
        let planning_start = std::time::Instant::now();
        let (assembly, distributed_plan, connector_static_planning) =
            prepare_query_with_sql_compiler_kernel_with_ports(
                &query,
                &analyzer_provider,
                current_catalog,
                current_database,
                &self.query,
                self.mv_repository.as_ref(),
                self.mv_storage_observation.as_ref(),
                connector_context,
                Some(query_options_for_explain_analyze(query_opts)),
                execution,
                novarocks_sql::compiler::SqlCompileIntent::Explain {
                    level: novarocks_sql::compiler::ExplainLevel::Analyze,
                    analyze: true,
                },
                true,
            )?;
        Ok(TestPreparedQueryOperation::Distributed {
            assembly,
            completion: PreparedQueryCompletion::profile(
                distributed_plan,
                planning_start.elapsed(),
                std::time::Instant::now(),
                connector_static_planning,
            ),
        })
    }
}

#[cfg(test)]
fn is_query_sql(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    match words.next().map(|word| word.to_ascii_lowercase()) {
        Some(keyword) if matches!(keyword.as_str(), "select" | "with") => true,
        Some(keyword) if keyword == "explain" => {
            let mut target = words.next().map(|word| word.to_ascii_lowercase());
            while matches!(
                target.as_deref(),
                Some("analyze" | "verbose" | "costs" | "logical")
            ) {
                target = words.next().map(|word| word.to_ascii_lowercase());
            }
            matches!(target.as_deref(), Some("select" | "with"))
        }
        _ => false,
    }
}

#[cfg(test)]
fn test_request_context(
    current_catalog: Option<&str>,
    current_database: &str,
) -> crate::query_execution::request_context::RequestContext {
    test_request_context_with_role(
        current_catalog,
        current_database,
        novarocks_types::ClusterRole::AllInOne,
    )
}

#[cfg(test)]
fn test_request_context_with_role(
    current_catalog: Option<&str>,
    current_database: &str,
    role: novarocks_types::ClusterRole,
) -> crate::query_execution::request_context::RequestContext {
    use crate::query_execution::backend::{BackendTopologySnapshot, LiveBackendTarget};
    use crate::query_execution::cancellation::QueryCancellationSource;
    use crate::query_execution::request_context::{
        QueryExecutionContext, RequestContext, RequestSessionContext,
    };

    let cancellation = QueryCancellationSource::new();
    RequestContext::new(
        RequestSessionContext::new(
            current_catalog.map(str::to_string),
            current_database.to_string(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
        ),
        QueryExecutionContext::new(
            role,
            // Test composition supplies an admitted loopback backend explicitly.
            // Production compilation still rejects an empty topology instead of
            // inferring one from the all-in-one role.
            BackendTopologySnapshot::try_new(
                0,
                vec![LiveBackendTarget::new(
                    0,
                    "127.0.0.1:9030".parse().expect("loopback test backend"),
                    1,
                )],
            )
            .expect("non-empty test topology"),
            None,
            cancellation.view(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
        ),
    )
}

fn resolve_default_view_database(
    name: &novarocks_sql::syntax::ObjectName,
    current_catalog: Option<&str>,
) -> Result<Option<String>, String> {
    let database = match name.parts.as_slice() {
        [database]
            if current_catalog
                .is_some_and(|catalog| catalog.eq_ignore_ascii_case("default_catalog")) =>
        {
            database
        }
        [catalog, database] if catalog.eq_ignore_ascii_case("default_catalog") => database,
        _ => return Ok(None),
    };
    normalize_identifier(database).map(Some)
}

fn standalone_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn rewrite_named_partition_insert_overwrite(sql: &str) -> Result<String, String> {
    let re = regex::Regex::new(
        r#"(?is)^\s*insert\s+overwrite\s+(?:table\s+)?(?P<table>(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)){0,2})\s+partition\s*\([^)]*\)\s+(?P<rest>.*)$"#,
    )
    .map_err(|e| format!("compile INSERT OVERWRITE partition rewrite regex failed: {e}"))?;
    let Some(captures) = re.captures(sql) else {
        return Ok(sql.to_string());
    };
    let table = captures.name("table").expect("table capture").as_str();
    let rest = captures.name("rest").expect("rest capture").as_str();
    Ok(format!("INSERT OVERWRITE PARTITIONS {table} {rest}"))
}

// ---------------------------------------------------------------------------
// Custom statement dispatch
// ---------------------------------------------------------------------------

pub(crate) fn statistics_application_target(
    name: &novarocks_sql::syntax::ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<crate::statistics::application::StatisticsTableTarget, String> {
    let default_catalog = current_catalog.unwrap_or("default_catalog");
    let (catalog, namespace, table) = match name.parts.as_slice() {
        [table] => (default_catalog, current_database, table.as_str()),
        [namespace, table] => (default_catalog, namespace.as_str(), table.as_str()),
        [catalog, namespace, table] => (catalog.as_str(), namespace.as_str(), table.as_str()),
        _ => {
            return Err(format!(
                "statistics table name must be table, db.table, or catalog.db.table: {}",
                name.parts.join(".")
            ));
        }
    };
    Ok(crate::statistics::application::StatisticsTableTarget {
        catalog: normalize_identifier(catalog)?,
        namespace: normalize_identifier(namespace)?,
        table: normalize_identifier(table)?,
    })
}

pub(crate) fn statistics_application_result(
    result: crate::statistics::application::StatisticsApplicationResult,
) -> Result<StatementResult, String> {
    use crate::statistics::application::StatisticsApplicationResult;

    match result {
        StatisticsApplicationResult::JobSubmitted(_)
        | StatisticsApplicationResult::JobCancellationRequested(_) => Ok(StatementResult::Ok),
        StatisticsApplicationResult::AnalyzeJobs(jobs) => statistics_string_result(
            &[
                "job_id",
                "operation_id",
                "state",
                "attempt",
                "catalog",
                "namespace",
                "table",
            ],
            jobs.into_iter()
                .map(|job| {
                    vec![
                        Some(job.job_id.to_string()),
                        Some(job.operation_id.to_string()),
                        Some(job.state),
                        Some(job.attempt.to_string()),
                        Some(job.target.catalog),
                        Some(job.target.namespace),
                        Some(job.target.table),
                    ]
                })
                .collect(),
        ),
        StatisticsApplicationResult::TableStats(rows) => statistics_string_result(
            &[
                "metric",
                "value",
                "status",
                "basis_version",
                "source",
                "numeric_nature",
                "basis_relation",
            ],
            rows.into_iter()
                .map(|row| {
                    vec![
                        Some(row.metric),
                        row.value,
                        Some(row.status),
                        Some(row.basis_version),
                        Some(row.source),
                        Some(row.numeric_nature),
                        Some(row.basis_relation),
                    ]
                })
                .collect(),
        ),
    }
}

fn statistics_string_result(
    names: &[&str],
    rows: Vec<Vec<Option<String>>>,
) -> Result<StatementResult, String> {
    if rows.iter().any(|row| row.len() != names.len()) {
        return Err("statistics application returned malformed tabular result".to_string());
    }
    let columns = names
        .iter()
        .map(|name| QueryResultColumn {
            name: (*name).to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays = (0..names.len())
        .map(|column| {
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row[column].clone())
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|error| format!("build statistics application result failed: {error}"))?;
    Ok(StatementResult::Query(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    }))
}

fn require_backend_management_role(
    statement: &str,
    role: novarocks_types::ClusterRole,
) -> Result<(), String> {
    match role {
        novarocks_types::ClusterRole::Fe => Ok(()),
        novarocks_types::ClusterRole::Be => Err(format!(
            "{statement} is not available in role=be; backend management is owned by StarRocks FE"
        )),
        novarocks_types::ClusterRole::AllInOne => Err(format!("{statement} requires role=fe")),
    }
}

// ---------------------------------------------------------------------------
// Local parquet table helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

pub(crate) fn block_on_standalone_async<F>(future: F) -> Result<F::Output, String>
where
    F: std::future::Future,
{
    if let Ok(handle) = Handle::try_current() {
        return Ok(handle.block_on(future));
    }
    data_block_on(future)
}

// ---------------------------------------------------------------------------
// Query plan build + execute (delegates to novarocks_sql::*)
// ---------------------------------------------------------------------------

pub(crate) fn ensure_mainline_distributed_execution(
    has_terminal_sink: bool,
    exchange_port: u16,
) -> Result<(), String> {
    if has_terminal_sink {
        return Err(
            "terminal sink execution requires mainline DistributedPlan sink support; direct execution fallback was removed"
                .to_string(),
        );
    }
    if exchange_port == 0 {
        return Err(
            "distributed execution requires an exchange backend; tests must install a loopback exchange backend instead of direct fallback"
                .to_string(),
        );
    }
    Ok(())
}

fn optimizer_settings_for_execution(
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
) -> novarocks_sql::compiler::SessionOptimizerSettings {
    let mut settings = execution
        .map(|execution| execution.optimizer_settings().clone())
        .unwrap_or_default();
    if settings.cbo_broadcast_backend_count.is_none() {
        if let Some(execution) = execution {
            settings.effective_backend_count = Some(execution.topology().targets().len() as f64);
        }
    }
    settings
}

pub(crate) fn scan_preparation_options(
    settings: &novarocks_sql::compiler::SessionOptimizerSettings,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<crate::query_execution::preparation::ScanPreparationOptions, String> {
    let target_parallelism = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .or_else(|| {
            // Unit fixtures deliberately use an empty synthetic topology. A
            // production request must instead fail before provider planning.
            #[cfg(test)]
            {
                Some(std::num::NonZeroUsize::new(1).expect("one is non-zero"))
            }
            #[cfg(not(test))]
            {
                None
            }
        })
        .ok_or_else(|| {
            "connector split preparation requires a non-empty admitted backend topology".to_string()
        })?;
    Ok(
        crate::query_execution::preparation::ScanPreparationOptions::new(
            settings.connector_static_predicate_pushdown_enabled(),
            target_parallelism,
            None,
        ),
    )
}

pub(crate) fn connector_static_planning_metrics(
    prepared: &crate::query_execution::preparation::PreparedFragmentSet,
) -> Result<crate::query_execution::profile::ConnectorStaticPlanningMetrics, String> {
    let mut metrics = crate::query_execution::profile::ConnectorStaticPlanningMetrics::default();
    for read in prepared.scan_bindings().connector_reads() {
        metrics.record(read.planning_metrics)?;
    }
    Ok(metrics)
}

#[cfg(test)]
fn prepare_explain_query_with_ports(
    query_kernel: &domain::QueryPreparationKernel,
    view_kernel: &domain::ViewExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<sqlparser::ast::Query, String> {
    let mut prepared = query.clone();
    view_kernel.view_service().rewrite_query(
        view_kernel,
        &mut prepared,
        crate::view::ViewRequestContext {
            current_catalog,
            current_database,
            connector_context: Some(connector_context),
        },
    )?;

    // Time-travel refs become synthetic local tables. Ordinary Iceberg refs
    // remain untouched and resolve through the query catalog materializer during analysis.
    if has_time_travel_refs(&prepared) {
        rewrite_time_travel_refs(
            query_kernel,
            current_catalog,
            current_database,
            &mut prepared,
            connector_context,
        )?;
    }

    Ok(prepared)
}

#[cfg(test)]
fn query_options_for_explain_analyze(query_options: Option<QueryOptions>) -> QueryOptions {
    let mut query_options = query_options.unwrap_or_default();
    query_options.enable_profile = true;
    query_options
}

pub(crate) fn iceberg_write_shuffle_by_output_name(
    output_name: impl Into<String>,
) -> novarocks_sql::compiler::RootDistributionRequirement {
    novarocks_sql::compiler::RootDistributionRequirement::ShuffleOutputName(output_name.into())
}

pub(crate) fn iceberg_write_shuffle_by_output_index(
    output_index: usize,
) -> novarocks_sql::compiler::RootDistributionRequirement {
    novarocks_sql::compiler::RootDistributionRequirement::ShuffleOutputOrdinal(output_index)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_query_as_iceberg_write(
    state: &impl DmlQueryExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::query_execution::planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
) -> Result<PreparedDmlWriteAssembly, String> {
    // This public write helper is also used by non-session transaction executors,
    // so it owns an operation-scoped context when no request signal is available.
    let connector_context = crate::connector::connector_request_context(
        query_opts.as_ref(),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    prepare_query_as_iceberg_write_with_connector_context(
        state,
        current_catalog,
        current_database,
        query,
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        &connector_context,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_query_as_iceberg_write_with_connector_context(
    state: &impl DmlQueryExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::query_execution::planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: Option<crate::query_execution::contract::ConnectorWritePlanningTemplate>,
) -> Result<PreparedDmlWriteAssembly, String> {
    prepare_query_as_iceberg_write_with_connector_binding(
        state,
        current_catalog,
        current_database,
        query,
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        connector_context,
        connector_write.map(DistributedConnectorWrite::Begin),
        None,
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_query_as_iceberg_write_in_operation_with_connector_context(
    state: &impl DmlQueryExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::query_execution::planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWriteExecutionRegistration,
) -> Result<PreparedDmlWriteAssembly, String> {
    prepare_query_as_iceberg_write_with_connector_binding(
        state,
        current_catalog,
        current_database,
        query,
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        connector_context,
        Some(DistributedConnectorWrite::Sealed(connector_write)),
        None,
        &[],
    )
}

/// Execute one generated write query with request-local relation overlays.
/// This is used by COW rewrite slices whose frozen file input is not a durable
/// catalog table.  The overlay is consumed by the application materializer
/// and is never registered in the shared local catalog.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_query_as_iceberg_write_in_operation_with_query_local_overlays(
    state: &impl DmlQueryExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::query_execution::planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWriteExecutionRegistration,
    scan_resolver: &dyn crate::query_execution::preparation::scan::ScanBindingResolver,
    overlays: &[crate::query_execution::planning::catalog_materializer::QueryLocalTableOverlay],
) -> Result<PreparedDmlWriteAssembly, String> {
    prepare_query_as_iceberg_write_with_connector_binding(
        state,
        current_catalog,
        current_database,
        query,
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        connector_context,
        Some(DistributedConnectorWrite::Sealed(connector_write)),
        Some(scan_resolver),
        overlays,
    )
}

pub(crate) enum DistributedConnectorWrite {
    Begin(crate::query_execution::contract::ConnectorWritePlanningTemplate),
    Sealed(crate::query_execution::contract::ConnectorWriteExecutionRegistration),
}

/// Core-sealed one-shot DML request awaiting Frontend native wire assembly.
///
/// The encoder can only borrow the frozen input. Once Frontend returns its
/// bundle, `finish` consumes that same input to construct and execute the
/// request, so no caller can substitute another plan/preparation pair.
pub(crate) struct PreparedDmlWriteAssembly {
    encoding: NativeFragmentEncodingInput,
    query_options: Option<QueryOptions>,
    execution: crate::query_execution::request_context::QueryExecutionContext,
    query_execution: crate::query_execution::service::QueryExecutionService,
    connector_write: Option<DistributedConnectorWrite>,
}

impl PreparedDmlWriteAssembly {
    fn new(
        encoding: NativeFragmentEncodingInput,
        query_options: Option<QueryOptions>,
        execution: crate::query_execution::request_context::QueryExecutionContext,
        query_execution: crate::query_execution::service::QueryExecutionService,
        connector_write: Option<DistributedConnectorWrite>,
    ) -> Self {
        Self {
            encoding,
            query_options,
            execution,
            query_execution,
            connector_write,
        }
    }

    pub(crate) fn encoding(&self) -> &NativeFragmentEncodingInput {
        &self.encoding
    }

    pub(crate) fn finish(
        self,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
        if !self.encoding.matches_native_attachment(&native_bundle) {
            return Err(
                "native fragment bundle does not match the sealed DML encoding input".into(),
            );
        }
        let (_, prepared) = self.encoding.into_parts();
        execute_distributed_write_with_execution(
            &self.query_execution,
            prepared,
            native_bundle,
            self.query_options,
            &self.execution,
            self.connector_write,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_query_as_iceberg_write_with_connector_binding(
    state: &impl DmlQueryExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::query_execution::planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: Option<DistributedConnectorWrite>,
    scan_resolver: Option<&dyn crate::query_execution::preparation::scan::ScanBindingResolver>,
    query_local_overlays: &[crate::query_execution::planning::catalog_materializer::QueryLocalTableOverlay],
) -> Result<PreparedDmlWriteAssembly, String> {
    let maintenance_execution;
    let execution = match execution {
        Some(execution) => execution,
        None => {
            maintenance_execution = state.capture_dml_fallback_execution()?;
            &maintenance_execution
        }
    };
    let optimizer_settings = optimizer_settings_for_execution(Some(execution));
    // Time-travel: a branch DML write's scan carries `FOR VERSION AS OF '<branch>'`
    // (delete_flow's DV position scan; the MOR-UPDATE branch row scan). Resolve those
    // version-bearing refs to synthetic per-snapshot tables bound to the BRANCH head
    // BEFORE snapshotting the catalog, exactly as the read path does. Without this the
    // analyzer silently drops the version clause and the scan reads the table's current
    // (main) snapshot, so a branch DELETE/UPDATE finds rows in the wrong data files and
    // no-ops on the branch. No-op when the query has no version ref (INSERT / main
    // writes), so those paths are unchanged.
    let mut prepared = query.clone();
    if has_time_travel_refs(&prepared) {
        rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut prepared,
            connector_context,
        )?;
    }

    let catalog_service_snapshot = catalog_service_snapshot(state);
    let analyzer_provider = build_catalog_service_provider_with_bindings_and_query_local_overlays(
        current_catalog,
        &catalog_service_snapshot,
        DmlQueryExecutionKernel::connector_control(state),
        connector_context.clone(),
        Arc::clone(&table_bindings),
        query_local_overlays.to_vec(),
        DmlQueryExecutionKernel::catalog_application(state),
    );

    let resolved_bindings = analyzer_provider.query_table_bindings();
    if !Arc::ptr_eq(&table_bindings, &resolved_bindings) {
        return Err(
            "SQL write catalog materializer replaced the admitted binding store".to_string(),
        );
    }
    let catalog_snapshot =
        novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_provider);
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            "Iceberg write requires a non-empty admitted backend topology".to_string()
        })?;
    let analyze_request = novarocks_sql::compiler::SqlAnalyzeRequest::new(
        novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(prepared)),
        novarocks_sql::compiler::SqlCompileIntent::IcebergWrite { root_distribution },
        novarocks_sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        novarocks_sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog_snapshot,
        novarocks_sql::compiler::builtin_sql_function_catalog(),
        None,
        novarocks_sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::query_execution::planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let analyzed = novarocks_sql::compiler::SqlCompiler::analyze(analyze_request)
        .map_err(|error| error.to_string())?
        .into_pending()
        .map_err(|error| error.to_string())?;
    let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
        state,
        Arc::clone(&table_bindings),
        connector_context,
    )?;
    let distributed_plan = novarocks_sql::planning::dml::compile_connector_write_distributed_plan(
        novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics),
        sink,
        &optimizer_settings,
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        DmlQueryExecutionKernel::connector_control(state),
        &connector_context,
        Some(table_bindings.as_ref()),
        scan_resolver,
        scan_preparation_options(&optimizer_settings, execution)?,
    )?;
    Ok(PreparedDmlWriteAssembly::new(
        NativeFragmentEncodingInput::new(distributed_plan, prepared),
        query_opts,
        execution.clone(),
        state.query_execution().clone(),
        connector_write,
    ))
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChangeStreamWriteEntrypoint {
    PhysicalPlan,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct ChangeStreamWriteBuildObservation {
    entrypoint: ChangeStreamWriteEntrypoint,
    effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
    writer_fragment_ids: Vec<Option<novarocks_sql::plan_read::FragmentId>>,
}

#[cfg(test)]
#[derive(Debug)]
struct ChangeStreamWriteTestObserverState {
    short_circuit_after_build: bool,
    observations: Vec<ChangeStreamWriteBuildObservation>,
}

#[cfg(test)]
fn change_stream_write_test_observer()
-> &'static std::sync::Mutex<Option<ChangeStreamWriteTestObserverState>> {
    static OBSERVER: std::sync::OnceLock<
        std::sync::Mutex<Option<ChangeStreamWriteTestObserverState>>,
    > = std::sync::OnceLock::new();
    OBSERVER.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
pub(crate) struct ChangeStreamWriteTestObserverGuard;

#[cfg(test)]
impl ChangeStreamWriteTestObserverGuard {
    fn take_observations(&self) -> Vec<ChangeStreamWriteBuildObservation> {
        change_stream_write_test_observer()
            .lock()
            .expect("change-stream write test observer lock")
            .as_mut()
            .expect("change-stream write test observer installed")
            .observations
            .drain(..)
            .collect()
    }
}

#[cfg(test)]
impl Drop for ChangeStreamWriteTestObserverGuard {
    fn drop(&mut self) {
        *change_stream_write_test_observer()
            .lock()
            .expect("change-stream write test observer lock") = None;
    }
}

#[cfg(test)]
pub(crate) fn install_change_stream_write_test_observer(
    short_circuit_after_build: bool,
) -> ChangeStreamWriteTestObserverGuard {
    let mut observer = change_stream_write_test_observer()
        .lock()
        .expect("change-stream write test observer lock");
    assert!(
        observer.is_none(),
        "change-stream write test observer already installed"
    );
    *observer = Some(ChangeStreamWriteTestObserverState {
        short_circuit_after_build,
        observations: Vec::new(),
    });
    ChangeStreamWriteTestObserverGuard
}

#[cfg(test)]
pub(crate) fn observe_change_stream_write_build_for_test(
    writer_routes: &[novarocks_sql::planning::dml::DmlChangeStreamWriterRoute],
) -> Option<crate::query_execution::outcome::QueryExecutionResult> {
    let mut observer = change_stream_write_test_observer()
        .lock()
        .expect("change-stream write test observer lock");
    let observer = observer.as_mut()?;
    observer
        .observations
        .push(ChangeStreamWriteBuildObservation {
            entrypoint: ChangeStreamWriteEntrypoint::PhysicalPlan,
            effects: writer_routes
                .iter()
                .flat_map(|route| route.accepted_effects.iter().copied())
                .collect(),
            writer_fragment_ids: writer_routes
                .iter()
                .map(|route| Some(route.writer_fragment_id))
                .collect(),
        });
    if observer.short_circuit_after_build {
        Some(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::QueryResult::empty(),
            write_commit: None,
            write_abort: None,
            connector_completion: None,
            fragment_profiles: Vec::new(),
        })
    } else {
        None
    }
}

pub(crate) struct PlannedIcebergChangeStreamWrite {
    pub(crate) encoding: NativeFragmentEncodingInput,
    /// SQL owns the mutable change-stream topology.  Core retains only the
    /// sealed writer-route projection required for operation registration.
    pub(crate) writer_routes: Vec<novarocks_sql::planning::dml::DmlChangeStreamWriterRoute>,
}

/// Prepare an already sealed SQL change-stream plan for native dispatch.
///
/// SQL owns all optimizer, physical-plan, and writer-topology construction.
/// Core only resolves the frozen bindings while preparing fragments and keeps
/// the resulting writer/cohort map for application-owned operation fencing.
pub(crate) fn prepare_dml_change_stream_write_with_execution(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    plan: novarocks_sql::planning::dml::DmlChangeStreamPlan,
    query_table_bindings: &crate::query_execution::planning::bindings::QueryTableBindingStore,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PlannedIcebergChangeStreamWrite, String> {
    crate::connector::validate_request_context(connector_context)?;
    let (distributed_plan, writer_routes) = plan.into_parts();
    let optimizer_settings = change_stream_write_optimizer_settings();
    let scan_resolver =
        crate::query_execution::planning::delta_scan::QueryTableBindingScanResolver::new(
            query_table_bindings,
        );
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        connector_control,
        connector_context,
        Some(query_table_bindings),
        Some(&scan_resolver),
        scan_preparation_options(&optimizer_settings, execution)?,
    )?;
    Ok(PlannedIcebergChangeStreamWrite {
        encoding: NativeFragmentEncodingInput::new(distributed_plan, prepared),
        writer_routes,
    })
}

/// Prepare an already sealed SQL connector-write plan for the frontend-owned
/// MV lifecycle.  SQL owns all compile/physical decisions; Core only pairs the
/// sealed plan with the exact admitted bindings and connector write template.
pub(crate) fn prepare_sealed_iceberg_write_native_assembly(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    distributed_plan: novarocks_sql::plan_read::DistributedPlan,
    query_table_bindings: &crate::query_execution::planning::bindings::QueryTableBindingStore,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
) -> Result<crate::mv::application::PreparedMvNativeWriteAssembly, String> {
    crate::connector::validate_request_context(connector_context)?;
    let scan_resolver =
        crate::query_execution::planning::delta_scan::QueryTableBindingScanResolver::new(
            query_table_bindings,
        );
    let settings = optimizer_settings_for_execution(Some(execution));
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        connector_control,
        connector_context,
        Some(query_table_bindings),
        Some(&scan_resolver),
        scan_preparation_options(&settings, execution)?,
    )?;
    let cohort_id = connector_write.cohort_id();
    let exact_lease = connector_write.lease();
    Ok(crate::mv::application::PreparedMvNativeWriteAssembly::new(
        NativeFragmentEncodingInput::new(distributed_plan, prepared),
        None,
        crate::query_execution::contract::ConnectorWriteOperationRegistration::single(
            connector_write,
        ),
        cohort_id,
        exact_lease,
    ))
}

/// Convert an already planned MV change-stream writer into an inert native
/// assembly handoff. The caller retains responsibility for the exact connector
/// write lease and Frontend later encodes the sealed pair before submission.
pub(crate) fn prepare_planned_iceberg_change_stream_write(
    encoding: NativeFragmentEncodingInput,
    query_opts: Option<QueryOptions>,
    connector_write: Option<DistributedConnectorWrite>,
) -> Result<crate::mv::application::PreparedMvNativeWriteAssembly, String> {
    let Some(DistributedConnectorWrite::Begin(template)) = connector_write else {
        return Err("prepared connector write requires an unsealed write template".to_string());
    };
    let cohort_id = template.cohort_id();
    let exact_lease = template.lease();
    Ok(crate::mv::application::PreparedMvNativeWriteAssembly::new(
        encoding,
        query_opts,
        crate::query_execution::contract::ConnectorWriteOperationRegistration::single(template),
        cohort_id,
        exact_lease,
    ))
}

fn prepare_distributed_write_request_with_execution(
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    query_options: Option<QueryOptions>,
    _execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_write: Option<DistributedConnectorWrite>,
) -> Result<PreparedDistributedWriteRequest, String> {
    let Some(DistributedConnectorWrite::Begin(template)) = connector_write else {
        return Err("prepared connector write requires an unsealed write template".to_string());
    };
    let cohort_id = template.cohort_id();
    let exact_lease = template.lease();
    PreparedDistributedWriteRequest::new(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::ConnectorWriteOperationRegistration::single(template),
        cohort_id,
        exact_lease,
    )
    .map_err(|error| error.to_string())
}

/// The request is bound to a newly-created exact connector operation session.
/// Callers that need typed abort certainty retain `session` until a terminal
/// commit or abort decision instead of letting an intermediate error discard
/// that provider-owned capability.
pub(crate) struct BoundDistributedWriteRequest {
    pub(crate) request: crate::query_execution::contract::DistributedQueryRequest,
    pub(crate) session: crate::query_execution::write_operation::ConnectorWriteOperationSession,
}

/// A request-construction failure after `begin_write_operation` still owns an
/// exact provider session.  The caller must issue its terminal abort through
/// that session rather than dropping it as an ordinary planning error.
pub(crate) enum BoundDistributedWriteBinding {
    Bound(BoundDistributedWriteRequest),
    AbortRequired {
        session: crate::query_execution::write_operation::ConnectorWriteOperationSession,
        reason: String,
    },
}

pub(crate) fn bind_prepared_distributed_write_request(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    prepared: PreparedDistributedWriteRequest,
) -> Result<BoundDistributedWriteBinding, String> {
    let cohort_id = prepared.write_cohort_id();
    let session = query_execution
        .begin_write_operation(prepared.registration(), prepared.lease())
        .map_err(|error| error.to_string())?;
    let registration =
        match crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
            session.clone(),
            cohort_id,
        ) {
            Ok(registration) => registration,
            Err(error) => {
                return Ok(BoundDistributedWriteBinding::AbortRequired {
                    session,
                    reason: error.to_string(),
                });
            }
        };
    let request = match prepared.into_request(execution, registration) {
        Ok(request) => request,
        Err(error) => {
            return Ok(BoundDistributedWriteBinding::AbortRequired {
                session,
                reason: error.to_string(),
            });
        }
    };
    Ok(BoundDistributedWriteBinding::Bound(
        BoundDistributedWriteRequest { request, session },
    ))
}

fn execute_bound_distributed_write_request(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    request: crate::query_execution::contract::DistributedQueryRequest,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let (query_result, write_commit, write_abort, connector_completion) = query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_write)
        .map(crate::query_execution::outcome::WriteExecutionOutcome::into_parts_with_connector)
        .map_err(|error| error.to_string())?;
    Ok(crate::query_execution::outcome::QueryExecutionResult {
        query_result,
        write_commit,
        write_abort,
        connector_completion,
        fragment_profiles: Vec::new(),
    })
}

fn change_stream_write_optimizer_settings() -> novarocks_sql::compiler::SessionOptimizerSettings {
    let mut settings = novarocks_sql::compiler::SessionOptimizerSettings::default();
    // A change-stream write carries old/new row pairs and target locators across
    // independent fragments. A query runtime filter may describe only one data
    // branch, so pushing it into a locator scan can suppress rows required by a
    // DELETE. Keep this system-generated mutation plan free of runtime filters;
    // its explicit predicates and connector pruning remain enabled.
    settings.enable_global_runtime_filter = Some(false);
    settings
}

/// Application-owned post-compile assembly for the canonical SQL kernel.
///
/// View/virtual rewrites and topology admission happened before this point.
/// The compiler receives only their immutable SQL projection; preparation and
/// native encoding receive the exact binding store returned by that same
/// compilation request.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn prepare_query_with_sql_compiler_kernel_with_ports(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer<'_>,
    current_catalog: Option<&str>,
    current_database: &str,
    query_kernel: &domain::QueryPreparationKernel,
    mv_repository: &dyn crate::mv::repository::MvRepository,
    mv_storage_observation: &dyn crate::mv::storage_observation::MvStorageObservationPort,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    query_opts: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    intent: novarocks_sql::compiler::SqlCompileIntent,
    allow_mv_rewrite_candidates: bool,
) -> Result<
    (
        PreparedDistributedQueryAssembly,
        novarocks_sql::plan_read::DistributedPlan,
        crate::query_execution::profile::ConnectorStaticPlanningMetrics,
    ),
    String,
> {
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            "SQL compilation requires a non-empty admitted backend topology".to_string()
        })?;
    let table_bindings = analyzer_catalog.query_table_bindings();
    let catalog_snapshot = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(analyzer_catalog);
    // MV rewrite is an optional SQL optimization. An application composition
    // without an MV repository supplies no snapshot; a repository that is
    // available but fails to freeze remains a planning error.
    let mv_definitions =
        if allow_mv_rewrite_candidates && mv_repository.availability().is_available() {
            Some(
                crate::mv::rewrite_prep::freeze_mv_rewrite_definition_index_with_ports(
                    mv_repository,
                    query_kernel.connector_control().as_ref(),
                    mv_storage_observation,
                )?,
            )
        } else {
            None
        };
    let distributed_intent = match &intent {
        novarocks_sql::compiler::SqlCompileIntent::Explain { analyze: true, .. } => {
            crate::query_execution::contract::DistributedQueryIntent::Profile
        }
        _ => crate::query_execution::contract::DistributedQueryIntent::Result,
    };
    let analyze_request = novarocks_sql::compiler::SqlAnalyzeRequest::new(
        novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(query.clone())),
        intent,
        novarocks_sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        novarocks_sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog_snapshot,
        novarocks_sql::compiler::builtin_sql_function_catalog(),
        mv_definitions.as_ref(),
        novarocks_sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::query_execution::planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let planning_inputs = crate::query_execution::planning::QueryPlanningInputs {
        analyze_request,
        post_compile: crate::query_execution::planning::PostCompilePlanningContext {
            table_bindings,
            connector_controls: query_kernel.connector_control().as_ref(),
            connector_context,
        },
    };
    let analyzed = novarocks_sql::compiler::SqlCompiler::analyze(planning_inputs.analyze_request)
        .map_err(|error| error.to_string())?
        .into_pending()
        .map_err(|error| error.to_string())?;
    let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
        query_kernel,
        planning_inputs.post_compile.table_bindings.clone(),
        connector_context,
    )?;
    let distributed_plan = novarocks_sql::compiler::SqlCompiler::optimize(
        novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics),
    )
    .map_err(|error| error.to_string())?
    .into_distributed_plan()
    .map_err(|error| error.to_string())?;
    ensure_mainline_distributed_execution(false, query_kernel.exchange_port())?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        planning_inputs.post_compile.connector_controls,
        planning_inputs.post_compile.connector_context,
        Some(planning_inputs.post_compile.table_bindings.as_ref()),
        None,
        scan_preparation_options(execution.optimizer_settings(), execution)?,
    )?;
    let connector_static_planning = connector_static_planning_metrics(&prepared)?;
    let assembly = PreparedDistributedQueryAssembly::new(
        NativeFragmentEncodingInput::new(distributed_plan.clone(), prepared),
        query_opts,
        distributed_intent,
        execution.clone(),
    );
    Ok((assembly, distributed_plan, connector_static_planning))
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn explain_query_with_sql_compiler_kernel_with_ports(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer<'_>,
    current_catalog: Option<&str>,
    current_database: &str,
    query_kernel: &domain::QueryPreparationKernel,
    mv_repository: &dyn crate::mv::repository::MvRepository,
    mv_storage_observation: &dyn crate::mv::storage_observation::MvStorageObservationPort,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    level: novarocks_sql::compiler::ExplainLevel,
    logical: bool,
) -> Result<QueryResult, String> {
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            "SQL compilation requires a non-empty admitted backend topology".to_string()
        })?;
    let table_bindings = analyzer_catalog.query_table_bindings();
    let catalog_snapshot = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(analyzer_catalog);
    let mv_definitions = crate::mv::rewrite_prep::freeze_mv_rewrite_definition_index_with_ports(
        mv_repository,
        query_kernel.connector_control().as_ref(),
        mv_storage_observation,
    )?;
    let intent = if logical {
        novarocks_sql::compiler::SqlCompileIntent::LogicalOnly
    } else {
        novarocks_sql::compiler::SqlCompileIntent::Explain {
            level,
            analyze: false,
        }
    };
    let planning_inputs = crate::query_execution::planning::QueryPlanningInputs {
        analyze_request: novarocks_sql::compiler::SqlAnalyzeRequest::new(
            novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(query.clone())),
            intent,
            novarocks_sql::compiler::SqlSessionContext {
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
                optimizer_settings: execution.optimizer_settings().clone(),
            },
            novarocks_sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
            &catalog_snapshot,
            novarocks_sql::compiler::builtin_sql_function_catalog(),
            Some(&mv_definitions),
            novarocks_sql::compiler::SqlCompileControl::new(
                execution.deadline(),
                crate::query_execution::planning::sql_cancellation_observation(
                    execution.cancellation().clone(),
                ),
            ),
        ),
        post_compile: crate::query_execution::planning::PostCompilePlanningContext {
            table_bindings,
            connector_controls: query_kernel.connector_control().as_ref(),
            connector_context,
        },
    };
    let analyzed = novarocks_sql::compiler::SqlCompiler::analyze(planning_inputs.analyze_request)
        .map_err(|error| error.to_string())?;
    let compiled = if logical {
        analyzed
            .into_complete()
            .map_err(|error| error.to_string())?
    } else {
        let analyzed = analyzed.into_pending().map_err(|error| error.to_string())?;
        let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
            query_kernel,
            planning_inputs.post_compile.table_bindings.clone(),
            connector_context,
        )?;
        novarocks_sql::compiler::SqlCompiler::optimize(
            novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics),
        )
        .map_err(|error| error.to_string())?
    };
    let lines = compiled
        .into_explain_lines(level, logical)
        .map_err(|error| error.to_string())?;
    build_string_query_result("Explain String", lines)
}

fn execute_distributed_result_with_execution(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    query_options: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<QueryResult, String> {
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::DistributedQueryIntent::Result,
        execution,
    )
    .map_err(|error| error.to_string())?;
    query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_result)
        .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
        .map_err(|error| error.to_string())
}

fn execute_distributed_write_with_execution(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    query_options: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_write: Option<DistributedConnectorWrite>,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::DistributedQueryIntent::Write,
        execution,
    )
    .map_err(|error| error.to_string())?;
    let request = match connector_write {
        Some(DistributedConnectorWrite::Begin(template)) => {
            let cohort_id = template.cohort_id();
            let exact_lease = template.lease();
            let session = query_execution
                .begin_write_operation(
                    crate::query_execution::contract::ConnectorWriteOperationRegistration::single(
                        template,
                    ),
                    exact_lease,
                )
                .map_err(|error| error.to_string())?;
            let registration =
                crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
                    session, cohort_id,
                )
                .map_err(|error| error.to_string())?;
            crate::query_execution::contract::with_connector_write_operation(request, registration)
                .map_err(|error| error.to_string())?
        }
        Some(DistributedConnectorWrite::Sealed(registration)) => {
            crate::query_execution::contract::with_connector_write_operation(request, registration)
                .map_err(|error| error.to_string())?
        }
        None => request,
    };
    let (query_result, write_commit, write_abort, connector_completion) = query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_write)
        .map(crate::query_execution::outcome::WriteExecutionOutcome::into_parts_with_connector)
        .map_err(|error| error.to_string())?;
    Ok(crate::query_execution::outcome::QueryExecutionResult {
        query_result,
        write_commit,
        write_abort,
        connector_completion,
        fragment_profiles: Vec::new(),
    })
}

fn execute_distributed_profile_with_execution(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    query_options: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::DistributedQueryIntent::Profile,
        execution,
    )
    .map_err(|error| error.to_string())?;
    let (query_result, fragment_profiles) = query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_profile)
        .map(crate::query_execution::outcome::ProfileExecutionOutcome::into_parts)
        .map_err(|error| error.to_string())?;
    Ok(crate::query_execution::outcome::QueryExecutionResult {
        query_result,
        write_commit: None,
        write_abort: None,
        connector_completion: None,
        fragment_profiles: fragment_profiles.into_profiles(),
    })
}

#[cfg(test)]
pub(crate) struct StandaloneLoopbackTestBackend {
    pub(crate) exchange_port: u16,
    _test_guard: TestSerializationGuard,
}

#[cfg(test)]
pub(crate) fn install_all_in_one_loopback_backend_for_test() -> StandaloneLoopbackTestBackend {
    let test_guard = acquire_standalone_test_guard();
    StandaloneLoopbackTestBackend {
        exchange_port: in_process_exchange_endpoint_sentinel(),
        _test_guard: test_guard,
    }
}

#[cfg(test)]
const fn in_process_exchange_endpoint_sentinel() -> u16 {
    // The test coordinator is in-process and never opens a native listener.
    // The lifetime-held TestSerializationGuard keeps this nonzero topology
    // marker isolated from concurrent standalone semantic tests.
    1
}

// ---------------------------------------------------------------------------
// EXPLAIN COSTS helper
// ---------------------------------------------------------------------------

fn trim_ascii_whitespace_end(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 && bytes[idx - 1].is_ascii_whitespace() {
        idx -= 1;
    }
    idx
}

fn find_table_ref_start(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 {
        let b = bytes[idx - 1];
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$' | b'.' | b'`') {
            idx -= 1;
        } else {
            break;
        }
    }
    idx
}

fn find_word_start(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 && is_identifier_byte(Some(bytes[idx - 1])) {
        idx -= 1;
    }
    idx
}

fn skip_ascii_whitespace(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn is_identifier_byte(byte: Option<u8>) -> bool {
    byte.is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
}

fn find_matching_paren(sql: &str, open: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut idx = open;
    while idx < bytes.len() {
        let b = bytes[idx];
        if in_single {
            if b == b'\'' {
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            if b == b'"' {
                in_double = false;
            }
            idx += 1;
            continue;
        }
        match b {
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

/// Wrap a sqlparser error message in the `sql parser error: ...` envelope
/// and append a StarRocks-style `Unexpected input '<token>'` clause when
/// the underlying error mentions the offending token (`found: <token>`),
/// so tests can assert against the StarRocks-FE-style wording.
#[cfg(test)]
fn format_parser_error(raw: &str) -> String {
    let mut out = format!("sql parser error: {raw}");
    if let Some(start) = raw.find("found: ") {
        let after = &raw[start + "found: ".len()..];
        let token = after
            .split(|c: char| c.is_whitespace() || c == ',')
            .next()
            .unwrap_or("")
            .trim()
            .trim_matches(|c: char| c == '`' || c == '"');
        if !token.is_empty() {
            out.push_str(&format!(" Unexpected input '{token}'."));
        }
    }
    out
}

#[cfg(test)]
fn split_explain_costs_sql(sql: &str) -> Option<(String, novarocks_sql::compiler::ExplainLevel)> {
    let body = consume_leading_keyword(consume_leading_keyword(sql, "EXPLAIN")?, "COSTS")?;
    Some((
        format!("EXPLAIN {}", body.trim_start()),
        novarocks_sql::compiler::ExplainLevel::Costs,
    ))
}

#[cfg(test)]
fn split_explain_logical_sql(sql: &str) -> Option<(String, novarocks_sql::compiler::ExplainLevel)> {
    let mut body = consume_leading_keyword(consume_leading_keyword(sql, "EXPLAIN")?, "LOGICAL")?;
    let mut level = novarocks_sql::compiler::ExplainLevel::Normal;
    for (keyword, candidate) in [
        ("VERBOSE", novarocks_sql::compiler::ExplainLevel::Verbose),
        ("COSTS", novarocks_sql::compiler::ExplainLevel::Costs),
    ] {
        if let Some(rest) = consume_leading_keyword(body, keyword) {
            level = candidate;
            body = rest;
            break;
        }
    }

    Some((format!("EXPLAIN {}", body.trim_start()), level))
}

#[cfg(test)]
fn consume_leading_keyword<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = sql.trim_start();
    let head = trimmed.as_bytes().get(..keyword.len())?;
    if !head.eq_ignore_ascii_case(keyword.as_bytes()) {
        return None;
    }

    let rest = &trimmed[keyword.len()..];
    if rest
        .chars()
        .next()
        .is_some_and(|ch| !ch.is_ascii_whitespace())
    {
        return None;
    }
    Some(rest)
}

fn parse_explain_refresh_materialized_view(
    sql: &str,
) -> Option<
    Result<
        (
            novarocks_sql::syntax::RefreshMaterializedViewStmt,
            novarocks_sql::compiler::ExplainLevel,
            bool,
        ),
        String,
    >,
> {
    let trimmed = sql.trim_start();
    let prefixes = [
        (
            "EXPLAIN ANALYZE REFRESH ",
            novarocks_sql::compiler::ExplainLevel::Analyze,
            true,
        ),
        (
            "EXPLAIN VERBOSE REFRESH ",
            novarocks_sql::compiler::ExplainLevel::Verbose,
            false,
        ),
        (
            "EXPLAIN COSTS REFRESH ",
            novarocks_sql::compiler::ExplainLevel::Costs,
            false,
        ),
        (
            "EXPLAIN REFRESH ",
            novarocks_sql::compiler::ExplainLevel::Normal,
            false,
        ),
    ];
    for (prefix, level, analyze) in prefixes {
        if trimmed
            .as_bytes()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
        {
            let body = format!("REFRESH {}", trimmed[prefix.len()..].trim_start());
            let statement = match novarocks_sql::syntax::parse_mv_admitted_statement(&body) {
                Ok(novarocks_sql::syntax::MvAdmittedStatement::Refresh(statement)) => statement,
                Ok(_) => {
                    return Some(Err(
                        "EXPLAIN REFRESH only supports REFRESH MATERIALIZED VIEW".to_string(),
                    ));
                }
                Err(error) => return Some(Err(error)),
            };
            return Some(Ok((statement, level, analyze)));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::QueryResult;
    use crate::statistics::application::{
        StatisticsApplicationCommand, StatisticsApplicationError, StatisticsApplicationPort,
        StatisticsApplicationResult, StatisticsJobView, StatisticsTableStatView,
        StatisticsTableTarget,
    };
    use arrow::array::{Array, StringArray};
    use novarocks_execution::exec::spill::{SpillConfig, SpillMode};
    use novarocks_execution::runtime::query_options::QueryOptions;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingStatisticsApplicationPort {
        commands: Mutex<Vec<StatisticsApplicationCommand>>,
    }

    impl RecordingStatisticsApplicationPort {
        fn commands(&self) -> Vec<StatisticsApplicationCommand> {
            self.commands.lock().expect("statistics commands").clone()
        }
    }

    impl StatisticsApplicationPort for RecordingStatisticsApplicationPort {
        fn execute(
            &self,
            command: StatisticsApplicationCommand,
        ) -> Result<StatisticsApplicationResult, StatisticsApplicationError> {
            self.commands
                .lock()
                .expect("statistics commands")
                .push(command.clone());
            match command {
                StatisticsApplicationCommand::AnalyzeTable { target, .. } => Ok(
                    StatisticsApplicationResult::JobSubmitted(StatisticsJobView {
                        job_id: Uuid::nil(),
                        operation_id: Uuid::nil(),
                        state: "SUBMITTED".into(),
                        attempt: 0,
                        target,
                    }),
                ),
                StatisticsApplicationCommand::ShowAnalyzeJobs
                | StatisticsApplicationCommand::CancelAnalyze { .. } => {
                    Ok(StatisticsApplicationResult::AnalyzeJobs(Vec::new()))
                }
                StatisticsApplicationCommand::ShowTableStats { .. } => {
                    Ok(StatisticsApplicationResult::TableStats(vec![
                        StatisticsTableStatView {
                            metric: "row_count".into(),
                            value: Some("42".into()),
                            status: "AVAILABLE".into(),
                            basis_version: "SAME".into(),
                            source: "PROVIDER_ARTIFACT".into(),
                            numeric_nature: "EXACT".into(),
                            basis_relation: "IDENTICAL".into(),
                        },
                    ]))
                }
            }
        }
    }

    #[test]
    fn explain_analyze_query_options_only_enable_profile() {
        assert_eq!(
            super::query_options_for_explain_analyze(None),
            QueryOptions {
                enable_profile: true,
                ..Default::default()
            }
        );

        let spill = SpillConfig {
            enable_spill: true,
            spill_mode: SpillMode::Auto,
            spill_mem_limit_threshold: Some(0.7),
            spill_operator_min_bytes: Some(64),
            spill_operator_max_bytes: Some(1024),
            spill_encode_level: Some(3),
            enable_spill_buffer_read: Some(true),
            max_spill_read_buffer_bytes_per_driver: Some(4096),
            spill_mem_table_size: Some(256),
            spill_mem_table_num: Some(4),
        };
        let options = QueryOptions {
            pipeline_dop: Some(3),
            query_timeout: Some(90),
            spill: Some(spill),
            ..Default::default()
        };
        let mut expected = options.clone();
        expected.enable_profile = true;

        assert_eq!(
            super::query_options_for_explain_analyze(Some(options)),
            expected
        );
    }

    #[test]
    fn parse_explain_refresh_materialized_view_supports_verbose_and_costs() {
        let verbose = super::parse_explain_refresh_materialized_view(
            "EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW mv1",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(verbose.0.name.parts, vec!["mv1"]);
        assert_eq!(verbose.1, novarocks_sql::compiler::ExplainLevel::Verbose);
        assert!(!verbose.2);

        let costs = super::parse_explain_refresh_materialized_view(
            "EXPLAIN COSTS REFRESH MATERIALIZED VIEW db.mv1",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(costs.0.name.parts, vec!["db", "mv1"]);
        assert_eq!(costs.1, novarocks_sql::compiler::ExplainLevel::Costs);
        assert!(!costs.2);
    }

    #[test]
    fn parse_explain_refresh_materialized_view_marks_analyze() {
        let parsed = super::parse_explain_refresh_materialized_view(
            "EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW mv1",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(parsed.1, novarocks_sql::compiler::ExplainLevel::Analyze);
        assert!(parsed.2);
    }

    #[test]
    fn split_explain_logical_sql_rewrites_to_plain_explain() {
        let (rewritten, level) =
            super::split_explain_logical_sql(" EXPLAIN LOGICAL SELECT * FROM t")
                .expect("recognized");
        assert_eq!(rewritten, "EXPLAIN SELECT * FROM t");
        assert_eq!(level, novarocks_sql::compiler::ExplainLevel::Normal);

        let (rewritten, level) =
            super::split_explain_logical_sql("explain logical verbose select k from t")
                .expect("recognized");
        assert_eq!(rewritten, "EXPLAIN select k from t");
        assert_eq!(level, novarocks_sql::compiler::ExplainLevel::Verbose);

        let (rewritten, level) =
            super::split_explain_logical_sql("EXPLAIN\nLOGICAL\nSELECT k FROM t")
                .expect("recognized");
        assert_eq!(rewritten, "EXPLAIN SELECT k FROM t");
        assert_eq!(level, novarocks_sql::compiler::ExplainLevel::Normal);
    }

    fn string_cell(result: &QueryResult, row: usize, col: usize) -> String {
        let batch = &result.chunks[0].batch;
        let array = batch
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray column");
        array.value(row).to_string()
    }

    #[test]
    fn typed_statistics_statements_use_the_injected_application_port() {
        let port = Arc::new(RecordingStatisticsApplicationPort::default());
        let executor = crate::statistics::command::StatisticsCommandExecutor::new(
            crate::query_execution::kernels::StatisticsExecutionKernel::new(
                Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
                Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default()),
                Arc::new(crate::connector::UnifiedStatisticsResolver::default()),
                Arc::new(crate::statistics::EmptyStatisticsService),
                Arc::clone(&port) as Arc<dyn StatisticsApplicationPort>,
                crate::query_execution::compiler::test_query_execution_service(),
            ),
        );

        assert!(
            executor
                .try_execute(
                    "ANALYZE TABLE ice.analytics.orders (order_id)",
                    None,
                    "default",
                )
                .expect("submit typed analyze")
                .is_some()
        );
        let show_stats = executor
            .try_execute("SHOW TABLE STATS ice.analytics.orders", None, "default")
            .expect("show typed table stats")
            .expect("statistics command result");
        let crate::query_execution::StatementResult::Query(show_stats) = show_stats else {
            panic!("SHOW TABLE STATS must return a query result");
        };
        assert_eq!(show_stats.columns[0].name, "metric");
        assert_eq!(show_stats.columns[1].name, "value");
        assert_eq!(string_cell(&show_stats, 0, 0), "row_count");
        assert_eq!(string_cell(&show_stats, 0, 1), "42");

        assert_eq!(
            port.commands(),
            vec![
                StatisticsApplicationCommand::AnalyzeTable {
                    target: StatisticsTableTarget {
                        catalog: "ice".into(),
                        namespace: "analytics".into(),
                        table: "orders".into(),
                    },
                    columns: vec!["order_id".into()],
                },
                StatisticsApplicationCommand::ShowTableStats {
                    target: StatisticsTableTarget {
                        catalog: "ice".into(),
                        namespace: "analytics".into(),
                        table: "orders".into(),
                    },
                },
            ]
        );
    }
}
