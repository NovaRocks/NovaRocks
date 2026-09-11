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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use novarocks_catalog_application::{
    CatalogGenerationError, CatalogGenerationLease, CatalogGenerationOwner,
    PreparedCatalogGeneration,
};

use novarocks_spi::connector::{
    CatalogHandle, ConnectorCatalogMutationLease, ConnectorCatalogMutationResolver,
    ConnectorCleanupMaintenanceLease, ConnectorCleanupMaintenanceResolver, ConnectorControlBinding,
    ConnectorControlPlanningLease, ConnectorControlRegistry, ConnectorControlResolver,
    ConnectorControlRuntimeId, ConnectorDataMutationLease, ConnectorDataMutationResolver,
    ConnectorDistributedRewriteLease, ConnectorDistributedRewriteResolver, ConnectorError,
    ConnectorErrorKind, ConnectorInstanceId, ConnectorMetadataMaintenanceLease,
    ConnectorMetadataMaintenanceResolver, ConnectorProviderBindingKey, ConnectorProviderId,
    ConnectorStatisticsLease, ConnectorStatisticsResolver, ConnectorWriteLease,
};
use novarocks_spi::connector::{
    ConnectorControlReadBinding, ConnectorControlRoleBinding, ConnectorControlRoleBindingFactory,
    ConnectorControlWriteBinding,
};

/// FE process owner of logical Connector control generations. It contains no
/// BE reader/runtime state and exposes only a narrow planning resolver to core.
#[derive(Clone)]
// Design: ADR-0130 (docs/adr/ADR-0130-connector-role-binding-generation-ownership.md)
pub struct ConnectorControlHost {
    generations: CatalogGenerationOwner<ControlGeneration>,
    /// Serializes lifecycle mutations without storing any generation facts.
    /// `CatalogGenerationOwner` remains the sole routing and retention owner.
    lifecycle: Arc<Mutex<()>>,
    compatibility: Arc<Mutex<CompatibilityState>>,
    role_factories: Arc<BTreeMap<ConnectorProviderId, Arc<dyn ConnectorControlRoleBindingFactory>>>,
}

#[derive(Default)]
struct CompatibilityState {
    /// Temporary bridge for the legacy FE effect contract.
    /// It is not a control-generation owner and is removed with that contract.
    legacy_execution_index: BTreeMap<ConnectorProviderBindingKey, ConnectorControlRuntimeId>,
    /// Compatibility-only evidence retained until the FE effect contract stops
    /// carrying legacy execution keys. It never drives BE retirement.
    installed_backends: BTreeMap<ConnectorProviderBindingKey, BTreeSet<String>>,
    ready_retires: Vec<ConnectorControlRetirement>,
}

impl CompatibilityState {
    fn runtime_for_legacy_effect(
        &self,
        key: &ConnectorProviderBindingKey,
    ) -> Result<ConnectorControlRuntimeId, ConnectorError> {
        self.legacy_execution_index
            .get(key)
            .copied()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "connector legacy effect generation is not registered",
                )
            })
    }
}

// Design: ADR-0017 (docs/adr/ADR-0017-connector-catalog-mutation-outcomes.md)
struct ControlGeneration {
    binding: Arc<ConnectorControlBinding>,
    /// The complete role generation is the only typed-read authority.  The
    /// generic binding remains separately stored only while legacy SPI callers
    /// still consume it; a planning lease always resolves the typed group from
    /// this same exact generation rather than through a second registry.
    role_binding: Option<Arc<ConnectorControlRoleBinding>>,
    legacy_execution_key: ConnectorProviderBindingKey,
}

/// Compatibility-only retirement evidence for the remaining FE effect bridge.
/// It is local bookkeeping; BE catalog eviction is driven only by complete
/// reachability snapshots and `PruneCatalogs`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorControlRetirement {
    pub key: ConnectorProviderBindingKey,
    pub installed_backends: Vec<String>,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
impl ConnectorControlHost {
    pub fn new() -> Self {
        Self::from_factory_map(BTreeMap::new())
    }

    /// Creates the production host from exactly one complete role factory per
    /// provider.  Provider code is selected at composition time; request
    /// paths can only acquire already-published exact generations.
    pub fn with_role_factories(
        factories: Vec<Arc<dyn ConnectorControlRoleBindingFactory>>,
    ) -> Result<Self, ConnectorError> {
        let mut factory_map = BTreeMap::new();
        for factory in factories {
            let provider_id = factory.provider_id();
            if factory_map.insert(provider_id.clone(), factory).is_some() {
                return Err(invalid(format!(
                    "duplicate connector control role factory for provider `{}`",
                    provider_id.as_str()
                )));
            }
        }
        Ok(Self::from_factory_map(factory_map))
    }

    fn from_factory_map(
        role_factories: BTreeMap<ConnectorProviderId, Arc<dyn ConnectorControlRoleBindingFactory>>,
    ) -> Self {
        let compatibility = Arc::new(Mutex::new(CompatibilityState::default()));
        let weak_compatibility = Arc::downgrade(&compatibility);
        let generations = CatalogGenerationOwner::with_retirement_observer(
            move |_runtime_id, _handle, generation: &ControlGeneration| {
                let Some(compatibility) = weak_compatibility.upgrade() else {
                    return;
                };
                let Ok(mut compatibility) = compatibility.lock() else {
                    return;
                };
                compatibility
                    .legacy_execution_index
                    .remove(&generation.legacy_execution_key);
                let installed_backends = compatibility
                    .installed_backends
                    .remove(&generation.legacy_execution_key)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                compatibility
                    .ready_retires
                    .push(ConnectorControlRetirement {
                        key: generation.legacy_execution_key.clone(),
                        installed_backends,
                    });
            },
        );
        Self {
            generations,
            lifecycle: Arc::new(Mutex::new(())),
            compatibility,
            role_factories: Arc::new(role_factories),
        }
    }

    /// Resolve the one factory selected for a provider during composition.
    /// The returned object owns no host state and cannot publish a generation
    /// by itself.
    pub(crate) fn role_factory(
        &self,
        provider_id: &ConnectorProviderId,
    ) -> Result<Arc<dyn ConnectorControlRoleBindingFactory>, ConnectorError> {
        self.role_factories
            .get(provider_id)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control role factory for provider `{}` is not installed",
                        provider_id.as_str()
                    ),
                )
            })
    }

    /// Every exact catalog handle still protected by this FE process.
    ///
    /// Retiring generations remain present until their last planning or effect
    /// lease drains. Callers combine this with one complete desired-state
    /// snapshot before issuing a best-effort BE prune.
    pub(crate) fn reachable_catalog_handles(
        &self,
    ) -> Result<BTreeSet<CatalogHandle>, ConnectorError> {
        self.generations
            .retained_handles()
            .map(|handles| handles.into_iter().collect())
            .map_err(map_generation_error)
    }

    pub fn register(&self, binding: ConnectorControlBinding) -> Result<(), ConnectorError> {
        self.register_generation(Arc::new(binding), None)
    }

    /// Publishes one complete, immutable FE role generation.  The generic SPI
    /// control binding and every typed-read capability are installed together;
    /// a subsequent planning lease cannot pair one generation's generic
    /// control with another generation's encoder or request factory.
    pub fn register_role_binding(
        &self,
        role_binding: ConnectorControlRoleBinding,
    ) -> Result<(), ConnectorError> {
        let role_binding = Arc::new(role_binding);
        self.register_generation(role_binding.control_arc(), Some(role_binding))
    }

    fn register_generation(
        &self,
        binding: Arc<ConnectorControlBinding>,
        role_binding: Option<Arc<ConnectorControlRoleBinding>>,
    ) -> Result<(), ConnectorError> {
        let _lifecycle = self.lock_lifecycle()?;
        let legacy_execution_key = ConnectorProviderBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        };
        let control_runtime_id = binding.control_runtime_id();
        let instance_id = binding.descriptor().instance_id.clone();
        let catalog_handle = binding.execution_catalog_handle().cloned();
        match self.generations.acquire_current(&instance_id) {
            Ok(current) => {
                if current.runtime_id() == control_runtime_id {
                    return Ok(());
                }
                drop(current);
                return Err(invalid(format!(
                    "connector control instance `{}` already has an active generation",
                    instance_id.as_str()
                )));
            }
            Err(CatalogGenerationError::NotFound) => {}
            Err(error) => return Err(map_generation_error(error)),
        }
        let mut compatibility = self.lock_compatibility()?;
        if compatibility
            .legacy_execution_index
            .contains_key(&legacy_execution_key)
        {
            return Err(invalid(
                "connector legacy effect generation is already registered",
            ));
        }
        let generation = ControlGeneration {
            binding,
            role_binding,
            legacy_execution_key: legacy_execution_key.clone(),
        };
        let prepared = match catalog_handle {
            Some(catalog_handle) => {
                PreparedCatalogGeneration::new(control_runtime_id, catalog_handle, generation)
            }
            None => PreparedCatalogGeneration::without_execution_handle(
                control_runtime_id,
                instance_id,
                generation,
            ),
        };
        self.generations
            .publish(prepared)
            .map_err(map_generation_error)?;
        compatibility
            .legacy_execution_index
            .insert(legacy_execution_key.clone(), control_runtime_id);
        Ok(())
    }

    /// Returns the typed-read group carried by the exact generation already
    /// retained by `planning_lease`.  This is a snapshot lookup only; it does
    /// not acquire another host lease or consult a parallel typed registry.
    pub(crate) fn typed_read_for_planning_lease(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
    ) -> Result<ConnectorControlReadBinding, ConnectorError> {
        let runtime_id = planning_lease.binding().control_runtime_id();
        let generation = self.exact_generation(runtime_id)?;
        if !Arc::ptr_eq(&generation.runtime().binding, planning_lease.binding()) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector control planning lease does not match the host generation",
            ));
        }
        generation
            .runtime()
            .role_binding
            .as_ref()
            .and_then(|binding| binding.read().cloned())
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no typed read capability",
                )
            })
    }

    /// Prevents new planning immediately. Existing leases retain their exact
    /// effect owner until their final local release, after which the control
    /// generation is removed. BE catalog eviction is independently driven by
    /// the complete desired-state snapshot.
    pub fn retire_current(&self, instance_id: &ConnectorInstanceId) -> Result<(), ConnectorError> {
        let _lifecycle = self.lock_lifecycle()?;
        self.generations
            .retire(instance_id)
            .map_err(map_generation_error)
    }

    /// Records compatibility evidence from the retired FE effect bridge. This
    /// data is not used to manage BE catalog lifetime.
    pub fn record_installed_backend(
        &self,
        key: &ConnectorProviderBindingKey,
        endpoint: impl Into<String>,
    ) -> Result<(), ConnectorError> {
        let mut compatibility = self.lock_compatibility()?;
        compatibility.runtime_for_legacy_effect(key)?;
        compatibility
            .installed_backends
            .entry(key.clone())
            .or_default()
            .insert(endpoint.into());
        Ok(())
    }

    /// Returns compatibility retirement evidence. There is deliberately no
    /// production dispatch sink for it.
    pub fn take_ready_retires(&self) -> Result<Vec<ConnectorControlRetirement>, ConnectorError> {
        let mut compatibility = self.lock_compatibility()?;
        Ok(std::mem::take(&mut compatibility.ready_retires))
    }

    fn acquire(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        let binding = Arc::clone(&generation.runtime().binding);
        Ok(ConnectorControlPlanningLease::new(binding, move || {
            drop(generation);
        }))
    }

    fn acquire_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        Self::build_mutation_lease(generation)
    }

    fn acquire_exact_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        let generation = self.exact_generation(control_runtime_id)?;
        Self::build_mutation_lease(generation)
    }

    fn build_mutation_lease(
        generation: CatalogGenerationLease<ControlGeneration>,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        let control_runtime_id = generation.runtime_id();
        let (descriptor, provider_incarnation, mutation) = {
            let binding = &generation.runtime().binding;
            let mutation = binding.mutation().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no catalog mutation capability",
                )
            })?;
            (
                binding.descriptor().clone(),
                binding.incarnation(),
                mutation,
            )
        };
        ConnectorCatalogMutationLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            mutation,
            move || drop(generation),
        )
    }

    fn acquire_current_data_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        Self::build_data_mutation_lease(generation)
    }

    fn acquire_exact_data_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        let generation = self.exact_generation(control_runtime_id)?;
        Self::build_data_mutation_lease(generation)
    }

    fn build_data_mutation_lease(
        generation: CatalogGenerationLease<ControlGeneration>,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        let control_runtime_id = generation.runtime_id();
        let (descriptor, provider_incarnation, metadata, mutation) = {
            let binding = &generation.runtime().binding;
            let mutation = binding.data_mutation().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no data mutation capability",
                )
            })?;
            (
                binding.descriptor().clone(),
                binding.incarnation(),
                Arc::clone(binding.metadata()),
                mutation,
            )
        };
        ConnectorDataMutationLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            metadata,
            mutation,
            move || drop(generation),
        )
    }

    fn acquire_current_metadata_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        Self::build_metadata_maintenance_lease(generation)
    }

    fn acquire_exact_metadata_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        let generation = self.exact_generation(control_runtime_id)?;
        Self::build_metadata_maintenance_lease(generation)
    }

    fn build_metadata_maintenance_lease(
        generation: CatalogGenerationLease<ControlGeneration>,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        let control_runtime_id = generation.runtime_id();
        let (descriptor, provider_incarnation, metadata, maintenance) = {
            let binding = &generation.runtime().binding;
            let maintenance = binding.metadata_maintenance().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no metadata maintenance capability",
                )
            })?;
            (
                binding.descriptor().clone(),
                binding.incarnation(),
                Arc::clone(binding.metadata()),
                maintenance,
            )
        };
        ConnectorMetadataMaintenanceLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            metadata,
            maintenance,
            move || drop(generation),
        )
    }

    fn acquire_current_cleanup_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        Self::build_cleanup_maintenance_lease(generation)
    }

    fn acquire_exact_cleanup_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        let generation = self.exact_generation(control_runtime_id)?;
        Self::build_cleanup_maintenance_lease(generation)
    }

    /// Acquires metadata and cleanup from one exact control generation. Cleanup
    /// is FE-only and its lease keeps a retiring generation alive for replay of
    /// immutable prepared evidence; it never substitutes a current generation.
    fn build_cleanup_maintenance_lease(
        generation: CatalogGenerationLease<ControlGeneration>,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        let control_runtime_id = generation.runtime_id();
        let (descriptor, provider_incarnation, metadata, cleanup) = {
            let binding = &generation.runtime().binding;
            let cleanup = binding.cleanup_maintenance().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no cleanup maintenance capability",
                )
            })?;
            (
                binding.descriptor().clone(),
                binding.incarnation(),
                Arc::clone(binding.metadata()),
                cleanup,
            )
        };
        ConnectorCleanupMaintenanceLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            metadata,
            cleanup,
            move || drop(generation),
        )
    }

    fn acquire_current_distributed_rewrite(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        self.build_distributed_rewrite_lease(generation)
    }

    fn acquire_exact_distributed_rewrite(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        let generation = self.exact_generation(control_runtime_id)?;
        self.build_distributed_rewrite_lease(generation)
    }

    /// Acquire the metadata, rewrite planning, write-control, and execution
    /// distribution capabilities from exactly one registered generation. The
    /// resulting lease and its derived planning lease retain the same exact
    /// generation rather than acquiring a separate current generation.
    fn build_distributed_rewrite_lease(
        &self,
        generation: CatalogGenerationLease<ControlGeneration>,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        let control_runtime_id = generation.runtime_id();
        let (
            binding,
            descriptor,
            provider_incarnation,
            metadata,
            planning,
            rewrite,
            write,
            distribution,
        ) = {
            let control = generation.runtime();
            let rewrite = control
                .binding
                .distributed_rewrite()
                .cloned()
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "connector control generation has no distributed rewrite capability",
                    )
                })?;
            let write = control.binding.write().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no distributed write capability",
                )
            })?;
            (
                Arc::clone(&control.binding),
                control.binding.descriptor().clone(),
                control.binding.incarnation(),
                Arc::clone(control.binding.metadata()),
                Arc::clone(control.binding.planning()),
                rewrite,
                write,
                Arc::clone(control.binding.execution_distribution()),
            )
        };
        let planning_generation = self.exact_generation(control_runtime_id)?;
        let planning_lease = ConnectorControlPlanningLease::new(binding, move || {
            drop(planning_generation);
        });
        ConnectorDistributedRewriteLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            planning_lease,
            metadata,
            planning,
            rewrite,
            write,
            distribution,
            move || drop(generation),
        )
    }

    fn acquire_write(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorWriteLease, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        let runtime_id = generation.runtime_id();
        let (write, provider_id, distribution, catalog_properties, legacy_key, runtime_id) = {
            let control = generation.runtime();
            let write = control.binding.write().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no distributed write capability",
                )
            })?;
            let provider_id = control.binding.descriptor().provider_id.clone();
            let distribution = control.binding.execution_distribution().clone();
            let catalog_properties = control.binding.catalog_properties()?.clone();
            (
                write,
                provider_id,
                distribution,
                catalog_properties,
                control.legacy_execution_key.clone(),
                runtime_id,
            )
        };
        ConnectorWriteLease::new_with_execution_distribution(
            runtime_id,
            legacy_key,
            write,
            provider_id,
            distribution,
            move || drop(generation),
        )
        .and_then(|lease| lease.with_catalog_properties(catalog_properties))
    }

    /// Acquire the complete typed write group of the generation a caller has
    /// already retained.
    ///
    /// A write must commit through the same incarnation that planned it, so the
    /// generation is named explicitly rather than resolved as "whatever is
    /// active now": between planning and commit the active generation can be
    /// replaced, and committing through the replacement would attach staged
    /// work to a runtime that never admitted it. Callers pass the runtime id of
    /// the planning lease they are already holding.
    pub(crate) fn acquire_exact_write_stack(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorWriteStackLease, ConnectorError> {
        let generation = self.exact_generation(control_runtime_id)?;
        Self::build_write_stack_lease(generation)
    }

    /// Acquire the complete typed write group of the currently active
    /// generation.
    ///
    /// Only for a caller that has not already pinned one; anything that planned
    /// against a retained generation must use [`Self::acquire_exact_write_stack`].
    pub(crate) fn acquire_current_write_stack(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorWriteStackLease, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        Self::build_write_stack_lease(generation)
    }

    fn build_write_stack_lease(
        generation: CatalogGenerationLease<ControlGeneration>,
    ) -> Result<ConnectorWriteStackLease, ConnectorError> {
        let control_runtime_id = generation.runtime_id();
        let group = generation
            .runtime()
            .role_binding
            .as_ref()
            .and_then(|binding| binding.write().cloned())
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no distributed write capability",
                )
            })?;
        Ok(ConnectorWriteStackLease::new(
            control_runtime_id,
            group,
            move || drop(generation),
        ))
    }

    fn acquire_statistics(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorStatisticsLease, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        let (descriptor, incarnation, statistics) = {
            let binding = &generation.runtime().binding;
            let statistics = binding.statistics().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no statistics capability",
                )
            })?;
            (
                binding.descriptor().clone(),
                binding.incarnation(),
                statistics,
            )
        };
        ConnectorStatisticsLease::new(descriptor, incarnation, statistics, move || {
            drop(generation);
        })
    }

    fn current_generation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<CatalogGenerationLease<ControlGeneration>, ConnectorError> {
        self.generations
            .acquire_current(instance_id)
            .map_err(map_generation_error)
    }

    fn exact_generation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<CatalogGenerationLease<ControlGeneration>, ConnectorError> {
        self.generations
            .acquire_runtime(control_runtime_id)
            .map_err(map_generation_error)
    }

    fn lock_compatibility(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, CompatibilityState>, ConnectorError> {
        self.compatibility.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector control compatibility lock poisoned",
            )
        })
    }

    fn lock_lifecycle(&self) -> Result<std::sync::MutexGuard<'_, ()>, ConnectorError> {
        self.lifecycle.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector control lifecycle lock poisoned",
            )
        })
    }
}
impl ConnectorControlResolver for ConnectorControlHost {
    fn observe_current_binding(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorProviderBindingKey, ConnectorError> {
        let generation = self.current_generation(instance_id)?;
        Ok(generation.runtime().legacy_execution_key.clone())
    }

    fn observe_current_control_runtime(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlRuntimeId, ConnectorError> {
        Ok(self.current_generation(instance_id)?.runtime_id())
    }

    fn acquire_current(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError> {
        self.acquire(instance_id)
    }
}

impl ConnectorCatalogMutationResolver for ConnectorControlHost {
    fn acquire_current_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        self.acquire_mutation(instance_id)
    }

    fn acquire_exact_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        Self::acquire_exact_mutation(self, control_runtime_id)
    }
}

impl ConnectorDataMutationResolver for ConnectorControlHost {
    fn acquire_current_data_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        Self::acquire_current_data_mutation(self, instance_id)
    }

    fn acquire_exact_data_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        Self::acquire_exact_data_mutation(self, control_runtime_id)
    }
}

impl ConnectorMetadataMaintenanceResolver for ConnectorControlHost {
    fn acquire_current_metadata_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        Self::acquire_current_metadata_maintenance(self, instance_id)
    }

    fn acquire_exact_metadata_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
        Self::acquire_exact_metadata_maintenance(self, control_runtime_id)
    }
}

impl ConnectorCleanupMaintenanceResolver for ConnectorControlHost {
    fn acquire_current_cleanup_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        Self::acquire_current_cleanup_maintenance(self, instance_id)
    }

    fn acquire_exact_cleanup_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError> {
        Self::acquire_exact_cleanup_maintenance(self, control_runtime_id)
    }
}

impl ConnectorDistributedRewriteResolver for ConnectorControlHost {
    fn acquire_current_distributed_rewrite(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        Self::acquire_current_distributed_rewrite(self, instance_id)
    }

    fn acquire_exact_distributed_rewrite(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError> {
        Self::acquire_exact_distributed_rewrite(self, control_runtime_id)
    }
}

impl ConnectorStatisticsResolver for ConnectorControlHost {
    fn acquire_current_statistics(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorStatisticsLease, ConnectorError> {
        self.acquire_statistics(instance_id)
    }
}

impl ConnectorControlRegistry for ConnectorControlHost {
    fn register(&self, binding: ConnectorControlBinding) -> Result<(), ConnectorError> {
        Self::register(self, binding)
    }

    fn retire_current(&self, instance_id: &ConnectorInstanceId) -> Result<(), ConnectorError> {
        Self::retire_current(self, instance_id)
    }
}

/// A retained hold on one exact generation's complete typed write group.
///
/// The generation cannot retire out from under an in-flight write while this
/// lease lives, which is what makes "the recipe that planned the write and the
/// authority that commits it are the same generation" a fact rather than a
/// hope. The commit authority itself is reached only through `session()`, and
/// there is deliberately no way to clone the lease into another owner.
pub struct ConnectorWriteStackLease {
    control_runtime_id: ConnectorControlRuntimeId,
    group: ConnectorControlWriteBinding,
    release: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl ConnectorWriteStackLease {
    pub(crate) fn new(
        control_runtime_id: ConnectorControlRuntimeId,
        group: ConnectorControlWriteBinding,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Self {
        Self {
            control_runtime_id,
            group,
            release: Some(Box::new(release)),
        }
    }

    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }

    /// The begin/finish/abort/reconcile authority. Frontend only.
    pub fn session(&self) -> Arc<dyn novarocks_spi::connector::write_stack::ConnectorWriteControl> {
        self.group.session()
    }

    /// Encodes a logical recipe for submission. It cannot decode one back.
    pub fn handle_encoder(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorWriteHandleWireEncoder> {
        self.group.handle_encoder()
    }

    /// Decodes a staged artifact the backends reported. It cannot forge one.
    pub fn fragment_decoder(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorWriteFragmentWireDecoder> {
        self.group.fragment_decoder()
    }
}

impl Drop for ConnectorWriteStackLease {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

impl std::fmt::Debug for ConnectorWriteStackLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorWriteStackLease")
            .field("control_runtime_id", &self.control_runtime_id)
            .finish_non_exhaustive()
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn map_generation_error(error: CatalogGenerationError) -> ConnectorError {
    let kind = match error {
        CatalogGenerationError::NotFound => ConnectorErrorKind::NotFound,
        CatalogGenerationError::DuplicateRuntime => ConnectorErrorKind::InvalidRequest,
        CatalogGenerationError::LeaseOverflow => ConnectorErrorKind::ResourceExhausted,
        CatalogGenerationError::CorruptOwner | CatalogGenerationError::OwnerUnavailable => {
            ConnectorErrorKind::Internal
        }
    };
    ConnectorError::new(kind, error.to_string())
}

impl Default for ConnectorControlHost {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_connector_starrocks::{
        StarRocksCapabilitySnapshot, StarRocksConnectorConfig, StarRocksControlGeneration,
        StarRocksMetadataSource, StarRocksResolvedTable,
    };
    use novarocks_spi::connector::{
        ConnectorBeginScanRequest, ConnectorError, ConnectorExecutionDistribution,
        ConnectorInstanceDescriptor, ConnectorListTablesRequest, ConnectorMetadata,
        ConnectorNamespaceRequest, ConnectorProviderBinding, ConnectorProviderId, ConnectorScan,
        ConnectorScanHandle, ConnectorScanPlanning, ConnectorSplitPlanningRequest,
        ConnectorTableHandle, ConnectorTableMetadata, ConnectorTableRequest, ProviderBindingEpoch,
    };

    use super::*;

    struct NeverCancelled;

    impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct StarRocksFixtureSource;

    impl StarRocksMetadataSource for StarRocksFixtureSource {
        fn namespace_exists(
            &self,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn table_exists(
            &self,
            _: &str,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }
        fn list_tables(
            &self,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<Vec<String>, ConnectorError> {
            Ok(vec![])
        }
        fn load_table(
            &self,
            _: &str,
            _: &str,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<StarRocksResolvedTable, ConnectorError> {
            StarRocksResolvedTable::try_new(
                "db",
                "table",
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                Bytes::from_static(b"schema-v1"),
                Bytes::from_static(b"data-v1"),
                StarRocksCapabilitySnapshot {
                    api_contract_version: 1,
                },
            )
        }
    }

    fn starrocks_binding() -> ConnectorControlBinding {
        let config = StarRocksConnectorConfig::new(
            ConnectorInstanceId::parse("catalog.starrocks").expect("instance ID"),
            novarocks_connector_starrocks::StarRocksLocalBindingRef::parse("test")
                .expect("binding"),
        );
        StarRocksControlGeneration::try_new(config, Arc::new(StarRocksFixtureSource))
            .expect("StarRocks control binding")
    }

    fn starrocks_context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            16 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .expect("context")
    }

    struct TestControl {
        instance_id: ConnectorInstanceId,
        incarnation: ProviderBindingEpoch,
    }

    impl ConnectorMetadata for TestControl {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn namespace_exists(
            &self,
            _request: ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            Err(unsupported())
        }

        fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            Err(unsupported())
        }

        fn list_tables(
            &self,
            _request: ConnectorListTablesRequest,
        ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
            Err(unsupported())
        }

        fn load_table(
            &self,
            _request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            Err(unsupported())
        }
    }

    impl ConnectorScanPlanning for TestControl {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn begin_scan(
            &self,
            _table: &ConnectorTableHandle,
            _request: ConnectorBeginScanRequest,
        ) -> Result<ConnectorScan, ConnectorError> {
            Err(unsupported())
        }

        fn plan_splits(
            &self,
            _scan: &ConnectorScanHandle,
            _request: ConnectorSplitPlanningRequest,
        ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError>
        {
            Err(unsupported())
        }
    }

    impl ConnectorExecutionDistribution for TestControl {
        fn declaration(
            &self,
            _context: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<ConnectorProviderBinding, ConnectorError> {
            ConnectorProviderBinding::iceberg(
                self.instance_id.as_str(),
                self.incarnation.to_bytes(),
                "default",
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
        }
    }

    fn binding(incarnation: u8) -> ConnectorControlBinding {
        test_control_binding_for(
            ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
            incarnation,
        )
    }

    pub(crate) fn test_control_binding(incarnation: u8) -> ConnectorControlBinding {
        binding(incarnation)
    }

    /// A control binding for an arbitrary instance ID, so a factory fixture can
    /// answer whichever catalog name the request carries.
    pub(crate) fn test_control_binding_for(
        instance_id: ConnectorInstanceId,
        incarnation: u8,
    ) -> ConnectorControlBinding {
        let provider = Arc::new(TestControl {
            instance_id,
            incarnation: ProviderBindingEpoch::from_bytes([incarnation; 16]),
        });
        ConnectorControlBinding::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: provider.instance_id.clone(),
            },
            provider.incarnation,
            provider.clone(),
            provider.clone(),
            provider,
            None,
        )
        .expect("control binding")
    }

    /// StarRocks is read-only external data. Its control generation installs
    /// no write capability, so a statement that tries to write to a StarRocks
    /// catalog is refused as unsupported while it is still deriving its
    /// write-stack lease -- before a plan is compiled, a recipe is sealed, or
    /// a fragment is submitted.
    #[test]
    fn a_starrocks_catalog_has_no_distributed_write_capability() {
        let host = ConnectorControlHost::new();
        let instance_id = ConnectorInstanceId::parse("catalog.starrocks").expect("instance ID");
        host.register(starrocks_binding())
            .expect("register StarRocks generation");
        let planning_lease = host.acquire_current(&instance_id).expect("planning lease");

        let error =
            crate::connector::write_target::derive_write_stack_lease(&host, &planning_lease)
                .expect_err("StarRocks has no distributed write capability");
        assert!(
            error.contains("no distributed write capability"),
            "unexpected error: {error}"
        );

        let typed_error = host
            .acquire_exact_write_stack(planning_lease.control_runtime_id())
            .expect_err("StarRocks has no distributed write capability");
        assert_eq!(typed_error.kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn observing_current_binding_does_not_require_a_planning_lease() {
        let host = ConnectorControlHost::new();
        let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
        let binding = binding(7);
        let control_runtime_id = binding.control_runtime_id();
        host.register(binding).expect("register generation");

        assert_eq!(
            host.observe_current_binding(&instance_id)
                .expect("observe active generation")
                .incarnation
                .to_bytes(),
            [7; 16]
        );
        assert_eq!(
            host.observe_current_control_runtime(&instance_id)
                .expect("observe active control runtime"),
            control_runtime_id
        );
        let planning_lease = host.acquire_current(&instance_id).expect("planning lease");
        assert_eq!(planning_lease.control_runtime_id(), control_runtime_id);
        drop(planning_lease);
        host.retire_current(&instance_id)
            .expect("retire unleased generation");
        assert!(
            host.acquire_current(&instance_id).is_err(),
            "an observation must not keep a retiring generation live"
        );
    }

    #[test]
    fn retiring_generation_waits_for_planning_lease_before_remote_retire() {
        let host = ConnectorControlHost::new();
        let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
        host.register(binding(7)).expect("register old generation");
        let lease = host.acquire_current(&instance_id).expect("planning lease");
        let old_key = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        };
        host.record_installed_backend(&old_key, "be-1")
            .expect("record ensure ack");
        host.retire_current(&instance_id)
            .expect("retire old generation");
        assert!(host.take_ready_retires().expect("retire queue").is_empty());

        host.register(binding(8))
            .expect("register replacement generation");
        assert_eq!(lease.binding().incarnation().to_bytes(), [7; 16]);
        drop(lease);

        let ready = host.take_ready_retires().expect("retire queue");
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].key, old_key);
        assert_eq!(ready[0].installed_backends, vec![String::from("be-1")]);
        assert_eq!(
            host.acquire_current(&instance_id)
                .expect("replacement planning lease")
                .binding()
                .incarnation()
                .to_bytes(),
            [8; 16]
        );
    }

    #[test]
    fn starrocks_control_host_keeps_the_retiring_generation_leased_and_accepts_its_replacement() {
        let host = ConnectorControlHost::new();
        let first = starrocks_binding();
        let instance = first.descriptor().instance_id.clone();
        let first_incarnation = first.incarnation();
        host.register(first)
            .expect("register first StarRocks generation");
        let lease = host
            .acquire_current(&instance)
            .expect("acquire first lease");
        let declaration = lease
            .binding()
            .provider_binding(&starrocks_context())
            .expect("declaration");
        assert_eq!(
            declaration.binding_key().incarnation(),
            first_incarnation.to_bytes()
        );

        host.retire_current(&instance)
            .expect("retire first generation");
        host.register(starrocks_binding())
            .expect("register replacement generation");
        assert_eq!(lease.binding().incarnation(), first_incarnation);
        drop(lease);

        assert_ne!(
            host.acquire_current(&instance)
                .expect("acquire replacement")
                .binding()
                .incarnation(),
            first_incarnation
        );
    }

    fn unsupported() -> ConnectorError {
        ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "test-only control capability",
        )
    }
}
