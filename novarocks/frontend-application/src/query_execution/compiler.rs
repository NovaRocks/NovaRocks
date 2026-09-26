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

use std::sync::Arc;

use crate::query_execution::completion::{PreparedImmediateQuery, PreparedQueryCompletion};
use novarocks_parser::ast::Query;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::api::QueryResult;
#[cfg(test)]
use novarocks_query_application::api::build_string_query_result;
#[cfg(test)]
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
#[cfg(test)]
use novarocks_sql::planning::catalog::TableLookupMode;

use crate::catalog_application::query_catalog::QueryCatalogService;
#[cfg(test)]
use crate::catalog_application::query_materializer::build_catalog_service_provider;
use novarocks_types::naming::normalize_identifier;

use crate::catalog_application::query_catalog::{CatalogServiceSource, catalog_service_snapshot};
use crate::query_execution::kernels as domain;
#[cfg(test)]
use crate::query_execution::planning::time_travel::TimeTravelRewriteError;
use crate::query_execution::planning::time_travel::{
    has_time_travel_refs, rewrite_time_travel_refs,
};

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

/// Freeze optional MV rewrite candidates through the request's exact Core
/// ports.  Frontend chooses whether an unavailable repository means no
/// candidates; it never gains connector-control access directly.
pub(crate) fn freeze_query_mv_rewrite_definition_index(
    query_kernel: &domain::QueryPreparationKernel,
    candidate_reader: &crate::mv::domain::readiness::MvCandidateReader,
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
) -> Result<novarocks_sql::compiler::MvRewriteDefinitionIndex, String> {
    crate::mv::domain::rewrite_prep::freeze_mv_rewrite_definition_index_with_ports(
        candidate_reader,
        query_kernel.connector_control().as_ref(),
        storage_observation,
    )
}

/// Freeze request-local statistics evidence from the same catalog binding
/// store used by SQL analysis.  It never resolves a newer connector state.
pub fn query_statistics_snapshot(
    query_kernel: &domain::QueryPreparationKernel,
    analyzer_catalog: &crate::catalog_application::query_materializer::CatalogServiceMaterializer<
        '_,
    >,
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
    fn function_catalog(&self) -> &novarocks_functions::EngineFunctionCatalog;
    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlResolver;
    /// The statement's typed connector control registry, supplied once when
    /// the kernel was composed.
    fn typed_connector_control(
        &self,
    ) -> &std::sync::Arc<novarocks_catalog_application::ConnectorControlHost>;
    fn catalog_application(
        &self,
    ) -> Option<&dyn novarocks_catalog_application::CatalogApplicationPort>;
    fn query_execution(&self) -> &crate::query_execution::service::QueryExecutionService;
    fn capture_dml_fallback_execution(
        &self,
    ) -> Result<novarocks_query_application::admitted_query_context::QueryExecutionContext, String>;
}

impl DmlQueryExecutionKernel for domain::DmlExecutionKernel {
    fn function_catalog(&self) -> &novarocks_functions::EngineFunctionCatalog {
        self.function_catalog().as_ref()
    }

    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlResolver {
        self.connector_control().as_ref()
    }

    fn typed_connector_control(
        &self,
    ) -> &std::sync::Arc<novarocks_catalog_application::ConnectorControlHost> {
        self.typed_connector_control()
    }

    fn catalog_application(
        &self,
    ) -> Option<&dyn novarocks_catalog_application::CatalogApplicationPort> {
        self.catalog_application().map(Arc::as_ref)
    }

    fn query_execution(&self) -> &crate::query_execution::service::QueryExecutionService {
        self.query_execution()
    }

    fn capture_dml_fallback_execution(
        &self,
    ) -> Result<novarocks_query_application::admitted_query_context::QueryExecutionContext, String>
    {
        Err("foreground DML requires an admitted query execution context".to_string())
    }
}

/// MV activation compiles an already-admitted write against the same query
/// kernel that froze its catalog/statistics facts. It must not recover the
/// legacy aggregate just to enter the generic Iceberg-write preparation path.
impl DmlQueryExecutionKernel for domain::QueryPreparationKernel {
    fn function_catalog(&self) -> &novarocks_functions::EngineFunctionCatalog {
        self.function_catalog().as_ref()
    }

    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlResolver {
        self.connector_control().as_ref()
    }

    fn typed_connector_control(
        &self,
    ) -> &std::sync::Arc<novarocks_catalog_application::ConnectorControlHost> {
        self.typed_connector_control()
    }

    fn catalog_application(
        &self,
    ) -> Option<&dyn novarocks_catalog_application::CatalogApplicationPort> {
        self.catalog_application().map(Arc::as_ref)
    }

    fn query_execution(&self) -> &crate::query_execution::service::QueryExecutionService {
        self.query_execution()
    }

    fn capture_dml_fallback_execution(
        &self,
    ) -> Result<novarocks_query_application::admitted_query_context::QueryExecutionContext, String>
    {
        Err("MV activation requires an admitted query execution context".to_string())
    }
}

#[cfg(test)]
pub(crate) struct TestConnectorControlRegistry {
    active: std::sync::Mutex<
        std::collections::HashMap<
            novarocks_spi::connector::ConnectorInstanceId,
            Arc<novarocks_spi::connector::ConnectorControlBinding>,
        >,
    >,
}

#[cfg(test)]
impl Default for TestConnectorControlRegistry {
    fn default() -> Self {
        Self {
            active: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlResolver for TestConnectorControlRegistry {
    fn observe_current_binding(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorProviderBindingKey,
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
        Ok(novarocks_spi::connector::ConnectorProviderBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        })
    }

    fn observe_current_control_runtime(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlRuntimeId,
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
        Ok(binding.control_runtime_id())
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
            binding.control_runtime_id(),
            binding.incarnation(),
            mutation,
            || {},
        )
    }

    fn acquire_exact_mutation(
        &self,
        control_runtime_id: novarocks_spi::connector::ConnectorControlRuntimeId,
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
            .values()
            .find(|binding| binding.control_runtime_id() == control_runtime_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "exact connector control runtime is unavailable",
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
            binding.control_runtime_id(),
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
        control_runtime_id: novarocks_spi::connector::ConnectorControlRuntimeId,
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
            .values()
            .find(|binding| binding.control_runtime_id() == control_runtime_id)
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
    let key = novarocks_spi::connector::ConnectorProviderBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorDataMutationLease::new(
        binding.descriptor().clone(),
        binding.control_runtime_id(),
        key.incarnation,
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
        control_runtime_id: novarocks_spi::connector::ConnectorControlRuntimeId,
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
            .values()
            .find(|binding| binding.control_runtime_id() == control_runtime_id)
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
        control_runtime_id: novarocks_spi::connector::ConnectorControlRuntimeId,
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
            .values()
            .find(|binding| binding.control_runtime_id() == control_runtime_id)
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
    let key = novarocks_spi::connector::ConnectorProviderBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorCleanupMaintenanceLease::new(
        binding.descriptor().clone(),
        binding.control_runtime_id(),
        key.incarnation,
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
    let key = novarocks_spi::connector::ConnectorProviderBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorMetadataMaintenanceLease::new(
        binding.descriptor().clone(),
        binding.control_runtime_id(),
        key.incarnation,
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
        control_runtime_id: novarocks_spi::connector::ConnectorControlRuntimeId,
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
            .values()
            .find(|binding| binding.control_runtime_id() == control_runtime_id)
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
    novarocks_spi::connector::ConnectorDistributedRewriteLease::new(
        binding.descriptor().clone(),
        binding.control_runtime_id(),
        binding.incarnation(),
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
struct RejectingTestDistributedQueryCoordinator;

#[cfg(test)]
impl crate::query_execution::contract::DistributedQueryCoordinator
    for RejectingTestDistributedQueryCoordinator
{
    fn execute(
        &self,
        request: crate::query_execution::contract::DistributedQueryRequest,
    ) -> Result<
        crate::query_execution::outcome::DistributedQueryOutcome,
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
#[allow(
    dead_code,
    reason = "All-in-one integration fixture serializes tests that share a loopback backend."
)]
pub(crate) struct TestSerializationGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
unsafe impl Send for TestSerializationGuard {}

#[cfg(test)]
unsafe impl Sync for TestSerializationGuard {}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "All-in-one integration fixture acquires the guard that serializes shared loopback tests."
)]
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
#[allow(
    dead_code,
    reason = "Shared frontend test fixture preserves explicit all-in-one request contexts."
)]
fn test_request_context(
    current_catalog: Option<&str>,
    current_database: &str,
) -> novarocks_query_application::admitted_query_context::RequestContext {
    test_request_context_with_role(
        current_catalog,
        current_database,
        novarocks_types::ClusterRole::Fe,
    )
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Shared frontend test fixture preserves explicit role-scoped request contexts."
)]
fn test_request_context_with_role(
    current_catalog: Option<&str>,
    current_database: &str,
    role: novarocks_types::ClusterRole,
) -> novarocks_query_application::admitted_query_context::RequestContext {
    use novarocks_execution_contract::{BackendProcessDescriptor, RuntimeEndpoint};
    use novarocks_query_application::admitted_query_context::{
        QueryExecutionContext, RequestContext,
    };
    use novarocks_query_application::api::{BackendTopologySnapshot, LiveBackendTarget};
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use novarocks_query_application::request_session::RequestSessionContext;
    use novarocks_types::BackendProcessId;

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
                    BackendProcessDescriptor::try_new(
                        BackendProcessId::new_v7(),
                        RuntimeEndpoint::new("127.0.0.1", 9030).expect("valid loopback endpoint"),
                        "test-deployment",
                        "test-build",
                        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                        4096,
                    )
                    .expect("valid test descriptor"),
                    novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes(
                        [0x61; 16],
                    )
                    .expect("nonzero epoch"),
                )],
            )
            .expect("non-empty test topology"),
            None,
            cancellation.view(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
        ),
    )
}

#[allow(
    dead_code,
    reason = "Retained for the view compilation compatibility path exercised by integration tests."
)]
fn resolve_default_view_database(
    name: &novarocks_sql::semantic::ObjectName,
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

#[allow(
    dead_code,
    reason = "Retained as the standalone planning timestamp helper for integration tests."
)]
fn standalone_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[allow(
    dead_code,
    reason = "Retained for explicit backend-management role validation coverage."
)]
fn require_backend_management_role(
    statement: &str,
    role: novarocks_types::ClusterRole,
) -> Result<(), String> {
    match role {
        novarocks_types::ClusterRole::Fe => Ok(()),
        novarocks_types::ClusterRole::Be => Err(format!(
            "{statement} is not available in role=be; backend management is owned by StarRocks FE"
        )),
    }
}

// ---------------------------------------------------------------------------
// Local parquet table helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Query plan build + execute (delegates to novarocks_sql::*)
// ---------------------------------------------------------------------------

/// The connector session one statement's typed scans are prepared under.
///
/// Preparation runs before the coordinator mints a `QueryExecutionId`, so the
/// statement has no execution identity yet: this session gets a fresh
/// preparation identity instead of borrowing one that does not exist. It
/// deliberately carries no credential — the connector authenticates through
/// its installed control.
pub(crate) fn typed_connector_session()
-> Result<novarocks_spi::connector::read_stack::ConnectorSession, String> {
    novarocks_spi::connector::read_stack::ConnectorSession::try_new(
        uuid::Uuid::now_v7().to_string(),
        "",
        "UTC",
        "en_US",
        std::time::SystemTime::now(),
    )
    .map_err(|error| format!("typed connector scan session: {error}"))
}

#[cfg(test)]
fn prepare_explain_query_with_ports(
    query_kernel: &domain::QueryPreparationKernel,
    view_kernel: &domain::ViewExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Query, String> {
    let mut prepared = query.clone();
    view_kernel.view_service().rewrite_query(
        &crate::view::engine::FrontendViewEngine::new(view_kernel.clone()),
        &mut prepared,
        novarocks_query_application::view::ViewRequestContext {
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
        )
        .map_err(test_time_travel_rewrite_error)?;
    }

    Ok(prepared)
}

#[cfg(test)]
fn test_time_travel_rewrite_error(error: TimeTravelRewriteError) -> String {
    match error {
        TimeTravelRewriteError::Engine(error) => error,
        TimeTravelRewriteError::Analyze(error) => error.to_string(),
    }
}

#[cfg(test)]
fn query_options_for_explain_analyze(query_options: Option<QueryOptions>) -> QueryOptions {
    let mut raw = query_options
        .as_ref()
        .map(|options| *options.as_proto())
        .unwrap_or_default();
    raw.enable_profile = true;
    QueryOptions::parse(raw).expect("enabling query profiling does not invalidate query options")
}

#[allow(
    dead_code,
    reason = "Test-only convenience helper preserves output-name shuffle assertions."
)]
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

/// Compile one DML write query into the NCP-6 dataflow shape, bound to the
/// session that already admitted it.
///
/// The session is both the reason the plan is a dataflow and the source of the
/// recipes it carries, so it is taken here rather than attached later: a plan
/// and the session that sealed it must not be separable.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_query_as_iceberg_write_with_write_session(
    state: &impl DmlQueryExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &Query,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: Option<&novarocks_query_application::admitted_query_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
) -> Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError> {
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
        write_session,
        None,
        &[],
    )
}

/// Compile one query of a *multi-target* write session, at the sealed ordinal
/// that query writes to.
///
/// A session spanning several queries -- a copy-on-write mutation compiles one
/// per rewritten file -- cannot read its ordinal off the sealed set: the set
/// holds every target, and `sole_target_ordinal` refuses exactly that by
/// design. Each query names its own, so one file's replacement rows can never
/// be attributed to another file's writer.
///
/// The query-local overlays are how a copy-on-write rewrite reads the file it
/// replaces: the frozen single-file source is not a durable catalog table, and
/// the overlay is consumed by the application materializer without ever being
/// registered in the shared local catalog.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_query_as_iceberg_write_at_write_target(
    state: &impl DmlQueryExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &Query,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: Option<&novarocks_query_application::admitted_query_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    overlays: &[crate::catalog_application::query_materializer::QueryLocalTableOverlay],
) -> Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError> {
    prepare_query_as_iceberg_write_with_connector_binding(
        state,
        current_catalog,
        current_database,
        query,
        sink,
        table_bindings,
        None,
        root_distribution,
        execution,
        connector_context,
        write_session,
        Some(write_target_ordinal),
        overlays,
    )
}

/// Core-sealed one-shot DML request awaiting Frontend native wire assembly.
///
/// The encoder can only borrow the frozen input. Once Frontend returns its
/// bundle, `finish` consumes that same input to construct and execute the
/// request, so no caller can substitute another plan/preparation pair.
/// One admitted write, planned and encoded, waiting to be submitted.
///
/// The plan and the session that commits it are held together because they
/// are one decision: the writer handle the plan states is the handle this
/// session sealed.
pub(crate) struct PreparedDmlWriteAssembly {
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    query_options: Option<QueryOptions>,
    execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
    query_execution: crate::query_execution::service::QueryExecutionService,
    write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedDmlWriteAssembly {
    pub(crate) fn new(
        encoded: crate::query_execution::physical_encoding::EncodedCompletedPlan,
        version: novarocks_physical_plan::PlanVersionId,
        query_options: Option<QueryOptions>,
        execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
        query_execution: crate::query_execution::service::QueryExecutionService,
        write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ) -> Result<Self, String> {
        let (template, candidate) = encoded.into_attempt_template_with_candidate(version);
        let description =
            novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
                novarocks_query_application::api::QueryExecutionKind::Write,
                candidate,
                template
                    .attempt_scheduling_facts()?
                    .fragments
                    .iter()
                    .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                    .collect(),
                novarocks_query_application::preparation::OutputContract::CompletionOnly,
                novarocks_query_application::coordination::ExecutionEffect::External,
                novarocks_query_application::coordination::RecoveryMode::NoRecovery,
                Vec::new(),
                novarocks_query_application::preparation::FrozenCostEstimate::unknown(
                    novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
                ),
                novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
                    novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
                ),
            )?;
        Ok(Self {
            description,
            template,
            query_options,
            execution,
            query_execution,
            write_session,
        })
    }

    /// Consume this one-shot assembly into its exact distributed write
    /// request.
    pub(crate) fn into_request(
        self,
    ) -> Result<
        (
            crate::query_execution::service::QueryExecutionService,
            crate::query_execution::contract::DistributedQueryRequest,
        ),
        String,
    > {
        let request = crate::query_execution::contract::build_request_from_finalized_execution(
            crate::query_execution::post_compile::FinalizedDistributedExecution::for_completed_plan(
                self.description,
                self.template,
            ),
            self.query_options,
            crate::query_execution::contract::DistributedQueryIntent::Write,
            &self.execution,
            None,
        )
        .map_err(|error| error.to_string())?;
        let request = crate::query_execution::contract::with_connector_write_session(
            request,
            self.write_session,
        )
        .map_err(|error| error.to_string())?;
        Ok((self.query_execution, request))
    }

    pub(crate) fn finish(
        self,
    ) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
        let (query_execution, request) = self.into_request()?;
        execute_distributed_write_request(&query_execution, request)
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_query_as_iceberg_write_with_connector_binding(
    state: &impl DmlQueryExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &Query,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: Option<&novarocks_query_application::admitted_query_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    // `write_target_ordinal` is the sealed target this one query writes to. A
    // caller that drives several queries against one session names it; a
    // single-query write leaves it out and the session's sole sealed target is
    // used.
    write_target_ordinal: Option<novarocks_spi::connector::write_stack::WriteTargetOrdinal>,
    query_local_overlays: &[crate::catalog_application::query_materializer::QueryLocalTableOverlay],
) -> Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError> {
    let maintenance_execution;
    let execution = match execution {
        Some(execution) => execution,
        None => {
            maintenance_execution = state.capture_dml_fallback_execution()?;
            &maintenance_execution
        }
    };
    let optimizer_settings = execution.optimizer_settings().clone();
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
    let analyzer_provider = crate::catalog_application::query_materializer::build_catalog_service_provider_with_bindings_and_query_local_overlays(
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
            "SQL write catalog materializer replaced the admitted binding store"
                .to_string()
                .into(),
        );
    }
    let catalog_snapshot =
        novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_provider);
    let compile_control = novarocks_sql::compiler::SqlCompileControl::new(
        execution.deadline(),
        crate::query_execution::planning::sql_cancellation_observation(
            execution.cancellation().clone(),
        ),
    );
    let analyze_request = novarocks_sql::compiler::SqlAnalyzeRequest::new(
        novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(prepared)),
        novarocks_sql::compiler::SqlCompileIntent::IcebergWrite { root_distribution },
        novarocks_sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        novarocks_sql::compiler::SqlPlanningEnvironment::Distributed,
        &catalog_snapshot,
        DmlQueryExecutionKernel::function_catalog(state),
        crate::query_execution::constant_eval::constant_evaluator(),
        None,
        compile_control.clone(),
    );
    let analyzed = novarocks_sql::compiler::SqlCompiler::analyze(analyze_request)
        .map_err(crate::dml::error::DmlExecutionError::from_compile)?
        .into_pending()
        .map_err(|error| error.to_string())?;
    let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
        state,
        Arc::clone(&table_bindings),
        connector_context,
    )?;
    let optimize_request =
        novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics, compile_control);
    // A write session both selects the dataflow shape and owns the recipes
    // that shape needs, so the two are decided together rather than by two
    // independent caller choices that could disagree.
    let sealed_write_targets = write_session
        .seal_write_targets()
        .map_err(|error| crate::dml::error::DmlExecutionError::from(error.to_string()))?;
    let ordinal = match write_target_ordinal {
        Some(ordinal) => ordinal,
        None => sealed_write_targets
            .sole_target_ordinal()
            .map_err(crate::dml::error::DmlExecutionError::from)?,
    };
    let statistics_requirements = write_session
        .statistics_requirements(ordinal)
        .map_err(|error| crate::dml::error::DmlExecutionError::from(error.to_string()))?;
    let write_target = write_session
        .targets()
        .iter()
        .find(|target| target.ordinal() == ordinal)
        .ok_or_else(|| {
            crate::dml::error::DmlExecutionError::from(format!(
                "write session did not seal target {}",
                ordinal.get()
            ))
        })?;
    // The handle the plan states is the same one the session sealed, encoded
    // as the payload a plan carries rather than as the wire form the encoder
    // stamps. The provider matches its own schema by name, so the names of the
    // fields this target accepts travel with it.
    let write_handle = write_session
        .encode_writer_handle_payload(write_target.handle())
        .map_err(|error| crate::dml::error::DmlExecutionError::from(error.to_string()))?;
    let write_target_facts = crate::query_execution::physical_encoding::WriteTargetFacts {
        sealed: &sealed_write_targets,
        field_names: std::collections::BTreeMap::from([(
            ordinal,
            sink.accepted_field_names().into_iter().collect(),
        )]),
    };

    // Optimize the write, then freeze the reads it states. The plan addresses
    // each scan by the occurrence its read was accounted for under, so the
    // reads are frozen before the plan is lowered rather than after.
    let (completion, needs) = novarocks_sql::planning::dml::begin_final_connector_write_plan(
        optimize_request,
        sink,
        ordinal,
        statistics_requirements,
        &optimizer_settings,
    )?;
    let connector_session = typed_connector_session()?;
    let access_sink = novarocks_query_application::preparation::ReadAccessSink::new();
    let mut facts = Vec::with_capacity(needs.len());
    for need in &needs {
        facts.push(
            crate::query_execution::provider_read_facts::freeze_one_read(
                need,
                state.typed_connector_control().as_ref(),
                table_bindings.as_ref(),
                &connector_session,
                connector_context,
                &access_sink.deposits(),
            )?,
        );
    }
    let access = access_sink
        .try_into_access()
        .map_err(|(error, _returned)| error.to_string())?;
    let read_budget = dml_scan_read_budget();
    let plan = completion.finish(
        crate::query_execution::physical_encoding::mint_plan_version(),
        crate::query_execution::contract::completed_plan_dop_domain(query_opts.as_ref())?,
        novarocks_sql::planning::dml::DmlFinalizedProviderReadSet::try_new(facts.into_iter().map(
            |fact| novarocks_sql::planning::dml::DmlFinalizedProviderRead { fact, read_budget },
        ))?,
        novarocks_sql::planning::dml::DmlFinalizedWriteTargetSet::try_new([
            novarocks_sql::planning::dml::DmlFinalizedWriteTarget {
                ordinal,
                handle: write_handle,
            },
        ])?,
    )?;
    let version = plan.version();
    let candidate =
        novarocks_query_application::preparation::CompletedPhysicalPlanCandidate::for_program(plan)
            .map_err(|error| error.to_string())?;
    let paired = novarocks_query_application::preparation::CompletedPlanWithAccess::try_pair(
        candidate, access,
    )
    .map_err(|(error, _returned)| error.to_string())?;
    let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
        paired,
        DmlQueryExecutionKernel::function_catalog(state),
        Some(&write_target_facts),
    )?;
    Ok(PreparedDmlWriteAssembly::new(
        encoded,
        version,
        query_opts,
        execution.clone(),
        state.query_execution().clone(),
        write_session,
    )?)
}

/// How much one write's scan may return in a batch.
const fn dml_scan_read_budget() -> novarocks_physical_plan::ScanReadBudget {
    novarocks_physical_plan::ScanReadBudget {
        max_batch_rows: novarocks_physical_plan::MAX_SCAN_BATCH_ROWS,
        max_batch_bytes: novarocks_physical_plan::MAX_SCAN_BATCH_BYTES,
    }
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
    writer_fragment_ids: Vec<Option<novarocks_physical_plan::FragmentId>>,
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
#[allow(
    dead_code,
    reason = "Test observer guard is retained for cross-module change-stream write assertions."
)]
pub(crate) struct ChangeStreamWriteTestObserverGuard;

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Test observer guard exposes captured change-stream build observations."
)]
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
#[allow(
    dead_code,
    reason = "Test-only observer installation is retained for cross-module change-stream write assertions."
)]
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
    writer_routes: &[novarocks_sql::planning::dml::DmlFinalChangeStreamWriterRoute],
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
            query_result: QueryResult::empty(),
            write_session: None,
            fragment_profiles: Vec::new(),
        })
    } else {
        None
    }
}

pub(crate) struct PlannedIcebergChangeStreamWrite {
    pub(crate) assembly: PreparedDmlWriteAssembly,
    /// SQL owns the mutable change-stream topology.  Core retains only the
    /// sealed writer-route projection required for operation registration.
    pub(crate) writer_routes: Vec<novarocks_sql::planning::dml::DmlFinalChangeStreamWriterRoute>,
}

#[allow(
    dead_code,
    reason = "Test-only execution helper preserves bound distributed write terminal-path assertions."
)]
fn execute_bound_distributed_write_request(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    request: crate::query_execution::contract::DistributedQueryRequest,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    query_execution
        .execute(request)
        .and_then(crate::query_execution::outcome::DistributedQueryOutcome::into_write)
        .map(crate::query_execution::outcome::WriteExecutionOutcome::into_execution_result)
        .map_err(|error| error.to_string())
}

fn change_stream_write_optimizer_settings() -> novarocks_sql::compiler::SessionOptimizerSettings {
    // A change-stream write carries old/new row pairs and target locators across
    // independent fragments. A query runtime filter may describe only one data
    // branch, so pushing it into a locator scan can suppress rows required by a
    // DELETE. Keep this system-generated mutation plan free of runtime filters;
    // its explicit predicates and connector pruning remain enabled.
    novarocks_sql::compiler::SessionOptimizerSettings {
        enable_global_runtime_filter: Some(false),
        ..Default::default()
    }
}

fn execute_distributed_write_request(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    request: crate::query_execution::contract::DistributedQueryRequest,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    query_execution
        .execute(request)
        .and_then(crate::query_execution::outcome::DistributedQueryOutcome::into_write)
        .map(crate::query_execution::outcome::WriteExecutionOutcome::into_execution_result)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "All-in-one integration fixture retains a loopback backend for frontend-only tests."
)]
pub(crate) struct StandaloneLoopbackTestBackend {
    pub(crate) exchange_port: u16,
    _test_guard: TestSerializationGuard,
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "All-in-one integration fixture installs the loopback backend used by frontend-only tests."
)]
pub(crate) fn install_all_in_one_loopback_backend_for_test() -> StandaloneLoopbackTestBackend {
    let test_guard = acquire_standalone_test_guard();
    StandaloneLoopbackTestBackend {
        exchange_port: in_process_exchange_endpoint_sentinel(),
        _test_guard: test_guard,
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "All-in-one integration fixture retains the in-process exchange endpoint sentinel."
)]
const fn in_process_exchange_endpoint_sentinel() -> u16 {
    // The test coordinator is in-process and never opens a native listener.
    // The lifetime-held TestSerializationGuard keeps this nonzero topology
    // marker isolated from concurrent standalone semantic tests.
    1
}

// ---------------------------------------------------------------------------
// EXPLAIN COSTS helper
// ---------------------------------------------------------------------------

#[allow(
    dead_code,
    reason = "Parser helper is retained for EXPLAIN compatibility parsing coverage."
)]
fn trim_ascii_whitespace_end(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 && bytes[idx - 1].is_ascii_whitespace() {
        idx -= 1;
    }
    idx
}

#[allow(
    dead_code,
    reason = "Parser helper is retained for EXPLAIN compatibility parsing coverage."
)]
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

#[allow(
    dead_code,
    reason = "Parser helper is retained for EXPLAIN compatibility parsing coverage."
)]
fn find_word_start(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 && is_identifier_byte(Some(bytes[idx - 1])) {
        idx -= 1;
    }
    idx
}

#[allow(
    dead_code,
    reason = "Parser helper is retained for EXPLAIN compatibility parsing coverage."
)]
fn skip_ascii_whitespace(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

#[allow(
    dead_code,
    reason = "Parser helper is retained for EXPLAIN compatibility parsing coverage."
)]
fn is_identifier_byte(byte: Option<u8>) -> bool {
    byte.is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
}

#[allow(
    dead_code,
    reason = "Parser helper is retained for EXPLAIN compatibility parsing coverage."
)]
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use novarocks_proto_codec::lifecycle::QueryOptions;
    use novarocks_proto_models::novarocks;

    #[test]
    fn explain_analyze_query_options_only_enable_profile() {
        assert_eq!(
            super::query_options_for_explain_analyze(None),
            QueryOptions::parse(novarocks::QueryOptions {
                enable_profile: true,
                ..Default::default()
            })
            .expect("profile-only query options are valid")
        );

        let options = QueryOptions::parse(novarocks::QueryOptions {
            pipeline_dop: 3,
            query_timeout: 90,
            enable_spill: true,
            spill_options: Some(novarocks::SpillOptions {
                spill_mode: 0,
                spill_mem_limit_threshold: 0.7,
                spill_operator_min_bytes: 64,
                spill_operator_max_bytes: 1024,
                spill_encode_level: 3,
                enable_spill_buffer_read: true,
                max_spill_read_buffer_bytes_per_driver: 4096,
                spill_mem_table_size: 256,
                spill_mem_table_num: 4,
            }),
            ..Default::default()
        })
        .expect("valid protocol query options");
        let mut expected_raw = *options.as_proto();
        expected_raw.enable_profile = true;
        let expected = QueryOptions::parse(expected_raw).expect("valid profile options");

        assert_eq!(
            super::query_options_for_explain_analyze(Some(options)),
            expected
        );
    }
}
