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

use novarocks_parser::ast::Query;
use novarocks_proto_codec::lifecycle::QueryOptions;

use crate::catalog_application::query_catalog::QueryCatalogService;
use novarocks_types::naming::normalize_identifier;

use crate::catalog_application::query_catalog::{CatalogServiceSource, catalog_service_snapshot};
use crate::query_execution::kernels as domain;
use crate::query_execution::planning::time_travel::has_time_travel_refs;

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

/// Leaf ports used by admitted asynchronous DML compilation and final encoding.
pub(crate) trait DmlQueryExecutionKernel:
    CatalogServiceSource
    + crate::query_execution::planning::time_travel::TimeTravelResolver
    + crate::query_execution::planning::statistics::QueryStatisticsResolver
{
    fn function_catalog(&self) -> &novarocks_functions::EngineFunctionCatalog;
    fn constant_policy(&self) -> novarocks_functions::ConstantPolicy;
    /// The static carrier the kernel's composition chose for every plan.
    fn static_plan_carrier(
        &self,
    ) -> &crate::query_execution::package_freeze::CompiledPackageCarrier;
}

impl DmlQueryExecutionKernel for domain::DmlExecutionKernel {
    fn function_catalog(&self) -> &novarocks_functions::EngineFunctionCatalog {
        self.function_catalog().as_ref()
    }

    fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy()
    }

    fn static_plan_carrier(
        &self,
    ) -> &crate::query_execution::package_freeze::CompiledPackageCarrier {
        self.static_plan_carrier()
    }
}

/// The query kernel exposes the same pure compiler facts for final encoding.
impl DmlQueryExecutionKernel for domain::QueryPreparationKernel {
    fn function_catalog(&self) -> &novarocks_functions::EngineFunctionCatalog {
        self.function_catalog().as_ref()
    }

    fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy()
    }

    fn static_plan_carrier(
        &self,
    ) -> &crate::query_execution::package_freeze::CompiledPackageCarrier {
        self.static_plan_carrier()
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
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
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
                        RuntimeEndpoint::new("control-0.test.invalid", 19061)
                            .expect("valid control endpoint"),
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
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
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
fn query_options_for_explain_analyze(query_options: Option<QueryOptions>) -> QueryOptions {
    let mut raw = query_options
        .as_ref()
        .map(|options| *options.as_proto())
        .unwrap_or_default();
    raw.enable_profile = true;
    QueryOptions::from_proto(raw)
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

/// Compile the original INSERT source after asynchronously observing its
/// catalog, statistics and provider facts. The statement action must retain
/// `provider_source` until its close proves the original creator actually exited.
#[expect(
    clippy::too_many_arguments,
    reason = "The async write boundary retains the original statement, binding store and sealed session explicitly."
)]
pub(crate) async fn prepare_query_as_iceberg_write_with_write_session_and_constants_async(
    state: &domain::DmlExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &Query,
    constants: Option<novarocks_sql::compiler::SqlExpressionConstants>,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
) -> Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError> {
    prepare_query_as_iceberg_write_with_connector_binding_async(
        state,
        current_catalog,
        current_database,
        query,
        constants.map(DmlMaterializedWriteSource::Expressions),
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        connector_context,
        write_session,
        None,
        &[],
        provider_source,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "The target ordinal and original materialized source are independently frozen write facts."
)]
pub(crate) async fn prepare_query_as_iceberg_write_at_write_target_async(
    state: &domain::DmlExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &Query,
    materialized_site: novarocks_sql::compiler::SqlMaterializedValuesSite,
    materialized_rows: std::sync::Arc<[Box<[novarocks_functions::ConstantValue]>]>,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    overlays: &[crate::catalog_application::query_materializer::QueryLocalTableOverlay],
    provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
) -> Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError> {
    prepare_query_as_iceberg_write_with_connector_binding_async(
        state,
        current_catalog,
        current_database,
        query,
        Some(DmlMaterializedWriteSource::Complete(
            materialized_site,
            materialized_rows,
        )),
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        connector_context,
        write_session,
        Some(ordinal),
        overlays,
        provider_source,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "The shared async author retains exact session, ordinal, materialized values and query-local overlays."
)]
async fn prepare_query_as_iceberg_write_with_connector_binding_async(
    state: &domain::DmlExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &Query,
    materialized: Option<DmlMaterializedWriteSource>,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: novarocks_sql::compiler::RootDistributionRequirement,
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    write_target_ordinal: Option<novarocks_spi::connector::write_stack::WriteTargetOrdinal>,
    overlays: &[crate::catalog_application::query_materializer::QueryLocalTableOverlay],
    provider_source: &crate::query_execution::completion_facts::FrontendCompletionCustody,
) -> Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError> {
    let query_opts = Some(query_opts.unwrap_or_else(|| {
        crate::query_execution::contract::synthetic_statement_query_options(execution)
    }));
    let capacity = execution
        .result_capacity()
        .cloned()
        .ok_or_else(|| "async DML compilation requires the admitted result capacity".to_owned())?;
    let scope = crate::query_execution::completion_facts::StatementFactScope::binding_only(
        Arc::clone(&table_bindings),
        connector_context.clone(),
        current_catalog,
        capacity.scope(),
        capacity.clone(),
    )
    .map_err(|error| error.to_string())?
    .with_query_local_overlays(overlays.to_vec());
    crate::query_execution::completion_facts::install_frontend_fact_source(
        provider_source,
        state.completion_fact_owners()?,
        scope,
        typed_connector_session()?,
    )?;
    let mut prepared = query.clone();
    if has_time_travel_refs(&prepared) {
        crate::query_execution::planning::time_travel::rewrite_time_travel_refs_async(
            state,
            current_catalog,
            current_database,
            &mut prepared,
            connector_context,
        )
        .await?;
    }
    let compile_control =
        crate::query_execution::planning::sql_compile_control_from_execution(execution);
    let statement = match materialized {
        Some(DmlMaterializedWriteSource::Complete(site, rows)) => {
            novarocks_sql::compiler::SqlStatementInput::materialized_values_query(
                Box::new(prepared),
                site,
                rows,
                &compile_control,
            )
            .map_err(crate::dml::error::DmlExecutionError::from_compile)?
        }
        Some(DmlMaterializedWriteSource::Expressions(constants)) => {
            novarocks_sql::compiler::SqlStatementInput::query_with_expression_constants(
                Box::new(prepared),
                constants,
                &compile_control,
            )
            .map_err(crate::dml::error::DmlExecutionError::from_compile)?
        }
        None => novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(prepared)),
    };
    let preparation = novarocks_sql::compiler::SqlDmlCatalogPreparation::try_new(
        statement,
        novarocks_sql::compiler::SqlSessionContext {
            sql_semantics: execution.sql_semantics().clone(),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        compile_control.clone(),
        root_distribution,
    )
    .map_err(crate::dml::error::DmlExecutionError::from_compile)?;
    let catalog_facts = provider_source
        .resolve_catalog_relations(preparation.needs())
        .await?;
    let analyzed = preparation
        .analyze(
            &catalog_facts,
            DmlQueryExecutionKernel::function_catalog(state),
            crate::query_execution::constant_eval::constant_evaluator(),
            DmlQueryExecutionKernel::constant_policy(state),
        )
        .map_err(crate::dml::error::DmlExecutionError::from_compile)?;
    let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings_async(
        state, Arc::clone(&table_bindings), connector_context,
    ).await?;
    let optimize_request = novarocks_sql::compiler::SqlOptimizeRequest::new(
        analyzed,
        &statistics,
        compile_control.clone(),
    );
    let sealed = write_session
        .seal_write_targets()
        .map_err(|error| error.to_string())?;
    let ordinal = match write_target_ordinal {
        Some(ordinal)
            if write_session
                .targets()
                .iter()
                .any(|target| target.ordinal() == ordinal) =>
        {
            ordinal
        }
        Some(ordinal) => {
            return Err(format!("write session did not seal target {}", ordinal.get()).into());
        }
        None => sealed.sole_target_ordinal()?,
    };
    let requirements = write_session
        .statistics_requirements(ordinal)
        .map_err(|error| error.to_string())?;
    let target = write_session
        .targets()
        .iter()
        .find(|target| target.ordinal() == ordinal)
        .ok_or_else(|| format!("write session did not seal target {}", ordinal.get()))?;
    let write_handle = write_session
        .encode_writer_handle_payload(target.handle())
        .map_err(|error| error.to_string())?;
    let decimal_overflow_policy = optimize_request.decimal_overflow_policy();
    let (completion, needs) = novarocks_sql::planning::dml::begin_final_connector_write_plan(
        optimize_request,
        sink,
        ordinal,
        requirements,
        execution.optimizer_settings(),
        decimal_overflow_policy,
    )
    .map_err(crate::dml::error::DmlExecutionError::from_compile)?;
    let access_sink = novarocks_query_application::preparation::ReadAccessSink::new();
    let facts = provider_source
        .resolve_provider_reads(&needs, &access_sink)
        .await?;
    // The original Provider retains every pending creator. Do not extract the
    // final access set until its local creator exit is proven by the same source.
    provider_source
        .close()
        .await
        .map_err(|error| format!("{}: {}", error.detail(), error.cause()))?;
    let access = access_sink
        .try_into_access()
        .map_err(|(error, _returned)| error.to_string())?;
    finish_prepared_iceberg_write_assembly(
        state,
        completion,
        facts,
        access,
        novarocks_sql::planning::dml::DmlFinalizedWriteTarget {
            ordinal,
            handle: write_handle,
        },
        query_opts,
        write_session,
        &compile_control,
    )
}

/// One admitted write, planned and encoded, waiting to be submitted.
///
/// The plan and the session that commits it are held together because they
/// are one decision: the writer handle the plan states is the handle this
/// session sealed.
pub(crate) struct PreparedDmlWriteAssembly {
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    query_options: Option<QueryOptions>,
    write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedDmlWriteAssembly {
    pub(crate) fn new(
        encoded: crate::query_execution::physical_encoding::EncodedCompletedPlan,
        version: novarocks_physical_plan::PlanVersionId,
        query_options: Option<QueryOptions>,
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
                novarocks_coordination_contract::ExecutionEffect::External,
                novarocks_coordination_contract::RecoveryMode::NoRecovery,
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
            write_session,
        })
    }

    /// Consume the same completed write through the closed internal consumer.
    /// The original reservation and stable session cross once into the common
    /// logical execution path; this does not start a Native attempt.
    pub(crate) fn prepare_computation(
        self,
        initial: crate::query_execution::logical_execution::InitialComputationReservation,
        runtime: crate::query_execution::internal_consumer::decoded::DecodedConsumerRuntime,
    ) -> Result<
        (
            crate::query_execution::logical_execution::PreparedLogicalComputation,
            novarocks_query_application::api::ReservedExecutionIdentity,
            crate::query_execution::internal_consumer::write::WriteComputationOutput,
        ),
        novarocks_query_application::api::QueryExecutionError,
    > {
        crate::query_execution::internal_consumer::write::prepare_write_computation(
            self.description,
            self.template,
            Arc::new(
                crate::query_execution::contract::ResolvedQueryOptions::from_upstream(
                    self.query_options,
                ),
            ),
            self.write_session,
            initial,
            runtime,
        )
    }
}

enum DmlMaterializedWriteSource {
    Complete(
        novarocks_sql::compiler::SqlMaterializedValuesSite,
        std::sync::Arc<[Box<[novarocks_functions::ConstantValue]>]>,
    ),
    Expressions(novarocks_sql::compiler::SqlExpressionConstants),
}

fn finish_prepared_iceberg_write_assembly(
    state: &impl DmlQueryExecutionKernel,
    completion: novarocks_sql::planning::dml::DmlWriteCompletion,
    facts: Vec<novarocks_sql::compiler::ProviderReadFact>,
    access: novarocks_query_application::preparation::FinalPlanRuntimeAccess<
        crate::query_execution::provider_read_facts::FrozenProviderRead,
    >,
    target: novarocks_sql::planning::dml::DmlFinalizedWriteTarget,
    query_opts: Option<QueryOptions>,
    write_session: Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    completion_control: &novarocks_sql::compiler::SqlCompileControl,
) -> Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError> {
    let write_target_facts = crate::query_execution::physical_encoding::WriteTargetFacts {
        session: write_session.as_ref(),
    };
    let read_budget = dml_scan_read_budget();
    let plan = completion.finish(
        crate::query_execution::physical_encoding::mint_plan_version(),
        crate::query_execution::contract::completed_plan_dop_domain(query_opts.as_ref())?,
        novarocks_sql::planning::dml::DmlFinalizedProviderReadSet::try_new(facts.into_iter().map(
            |fact| novarocks_sql::planning::dml::DmlFinalizedProviderRead { fact, read_budget },
        ))?,
        novarocks_sql::planning::dml::DmlFinalizedWriteTargetSet::try_new([target])?,
        completion_control,
    )?;
    let version = plan.plan().version();
    let candidate =
        novarocks_query_application::preparation::CompletedPhysicalPlanCandidate::for_sql_program(
            plan,
            completion_control,
        )
        .and_then(|candidate| {
            candidate.freeze_root_output(
                novarocks_result_contract::FrozenRootOutput::InternalFacts(
                    novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
                ),
            )
        })
        .map_err(crate::dml::error::DmlExecutionError::from_completion)?;
    let paired = novarocks_query_application::preparation::CompletedPlanWithAccess::try_pair(
        candidate, access,
    )
    .map_err(|(error, _returned)| error.to_string())?;
    let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
        paired,
        DmlQueryExecutionKernel::static_plan_carrier(state),
        DmlQueryExecutionKernel::constant_policy(state),
        Some(&write_target_facts),
        completion_control,
    )?;
    Ok(PreparedDmlWriteAssembly::new(
        encoded,
        version,
        query_opts,
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
            QueryOptions::from_proto(novarocks::QueryOptions {
                enable_profile: true,
                ..Default::default()
            })
        );

        let options = QueryOptions::from_proto(novarocks::QueryOptions {
            pipeline_dop: 3,
            query_timeout: 90,
            ..Default::default()
        });
        let mut expected_raw = *options.as_proto();
        expected_raw.enable_profile = true;
        let expected = QueryOptions::from_proto(expected_raw);

        assert_eq!(
            super::query_options_for_explain_analyze(Some(options)),
            expected
        );
    }
}
