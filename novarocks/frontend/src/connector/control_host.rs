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
use std::sync::{Arc, Mutex, Weak};

use novarocks_spi::connector::{
    ConnectorCatalogMutationLease, ConnectorCatalogMutationResolver, ConnectorControlBinding,
    ConnectorControlPlanningLease, ConnectorControlRegistry, ConnectorControlResolver,
    ConnectorDataMutationLease, ConnectorDataMutationResolver, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorStatisticsLease,
    ConnectorStatisticsResolver, ConnectorWriteLease, ConnectorWriteResolver,
};

/// FE process owner of logical Connector control generations. It contains no
/// BE reader/runtime state and exposes only a narrow planning resolver to core.
#[derive(Clone, Default)]
pub struct ConnectorControlHost {
    state: Arc<Mutex<ControlHostState>>,
    retirement_sink: Arc<Mutex<Option<Arc<dyn ConnectorControlRetirementSink>>>>,
}

#[derive(Default)]
struct ControlHostState {
    active: BTreeMap<ConnectorInstanceId, ConnectorExecutionBindingKey>,
    generations: BTreeMap<ConnectorExecutionBindingKey, ControlGeneration>,
    retired: BTreeSet<ConnectorExecutionBindingKey>,
    installed_backends: BTreeMap<ConnectorExecutionBindingKey, BTreeSet<String>>,
    ready_retires: Vec<ConnectorControlRetirement>,
}

// Design: ADR-0017 (docs/adr/ADR-0017-connector-catalog-mutation-outcomes.md)
struct ControlGeneration {
    binding: Arc<ConnectorControlBinding>,
    state: ControlGenerationState,
    planning_leases: usize,
    mutation_leases: usize,
    data_mutation_leases: usize,
    write_leases: usize,
    statistics_leases: usize,
}

impl ControlGeneration {
    fn all_leases_released(&self) -> bool {
        self.planning_leases == 0
            && self.mutation_leases == 0
            && self.data_mutation_leases == 0
            && self.write_leases == 0
            && self.statistics_leases == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlGenerationState {
    Active,
    Retiring,
}

/// A retiring generation plus the BE endpoints that successfully ensured it.
/// The frontend dispatcher owns the best-effort remote retirement dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorControlRetirement {
    pub key: ConnectorExecutionBindingKey,
    pub installed_backends: Vec<String>,
}

/// Frontend-composed best-effort transport for a retired execution binding.
/// It deliberately receives only an exact binding key and previously ACKed
/// endpoints; no connector control or execution object crosses this seam.
pub trait ConnectorControlRetirementSink: Send + Sync + 'static {
    fn retire(&self, retirement: ConnectorControlRetirement);
}

impl ConnectorControlHost {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_retirement_sink(&self, sink: Arc<dyn ConnectorControlRetirementSink>) {
        let Ok(mut slot) = self.retirement_sink.lock() else {
            return;
        };
        *slot = Some(Arc::clone(&sink));
        // Keep the sink lock while taking the fallback queue. A concurrent
        // retirement either observes this sink or queues itself before this
        // drain; it cannot be stranded between the two operations.
        let ready = match self.lock_state() {
            Ok(mut state) => std::mem::take(&mut state.ready_retires),
            Err(error) => {
                tracing::warn!(%error, "connector control retirement sink was not installed");
                return;
            }
        };
        drop(slot);
        for retirement in ready {
            sink.retire(retirement);
        }
    }

    pub fn register(&self, binding: ConnectorControlBinding) -> Result<(), ConnectorError> {
        let binding = Arc::new(binding);
        let key = ConnectorExecutionBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        };
        let mut state = self.lock_state()?;
        if state.retired.contains(&key) {
            return Err(invalid(
                "retired connector control generation cannot be registered again",
            ));
        }
        if let Some(existing) = state.generations.get(&key) {
            if existing.state == ControlGenerationState::Active {
                return Ok(());
            }
            return Err(invalid(
                "retiring connector control generation cannot be registered again",
            ));
        }
        if state.active.contains_key(&key.instance_id) {
            return Err(invalid(format!(
                "connector control instance `{}` already has an active generation",
                key.instance_id.as_str()
            )));
        }
        state.active.insert(key.instance_id.clone(), key.clone());
        state.generations.insert(
            key,
            ControlGeneration {
                binding,
                state: ControlGenerationState::Active,
                planning_leases: 0,
                mutation_leases: 0,
                data_mutation_leases: 0,
                write_leases: 0,
                statistics_leases: 0,
            },
        );
        Ok(())
    }

    /// Prevents new planning immediately. Existing leases retain their exact
    /// binding through the ensure barrier; once the last lease drops, callers
    /// may dispatch the returned best-effort BE retire work.
    pub fn retire_current(&self, instance_id: &ConnectorInstanceId) -> Result<(), ConnectorError> {
        let mut state = self.lock_state()?;
        let key = state.active.remove(instance_id).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!(
                    "connector control instance `{}` is not active",
                    instance_id.as_str()
                ),
            )
        })?;
        let generation = state.generations.get_mut(&key).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "active connector control generation is missing",
            )
        })?;
        generation.state = ControlGenerationState::Retiring;
        let retirement = generation
            .all_leases_released()
            .then(|| queue_retirement(&mut state, key))
            .flatten();
        drop(state);
        self.dispatch_retirement(retirement);
        Ok(())
    }

    /// Records a successful BE ensure acknowledgement. This is accepted for a
    /// retiring generation because the planning lease that caused it is allowed
    /// to finish its barrier before best-effort retirement is dispatched.
    pub fn record_installed_backend(
        &self,
        key: &ConnectorExecutionBindingKey,
        endpoint: impl Into<String>,
    ) -> Result<(), ConnectorError> {
        let mut state = self.lock_state()?;
        if !state.generations.contains_key(key) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "connector control generation is not registered",
            ));
        }
        state
            .installed_backends
            .entry(key.clone())
            .or_default()
            .insert(endpoint.into());
        Ok(())
    }

    pub fn take_ready_retires(&self) -> Result<Vec<ConnectorControlRetirement>, ConnectorError> {
        let mut state = self.lock_state()?;
        Ok(std::mem::take(&mut state.ready_retires))
    }

    fn acquire(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError> {
        let (binding, key) = {
            let mut state = self.lock_state()?;
            let key = state.active.get(instance_id).cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
            let generation = state.generations.get_mut(&key).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "active connector control generation is missing",
                )
            })?;
            if generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            generation.planning_leases = generation.planning_leases.saturating_add(1);
            (Arc::clone(&generation.binding), key)
        };
        let state = Arc::downgrade(&self.state);
        let retirement_sink = Arc::downgrade(&self.retirement_sink);
        Ok(ConnectorControlPlanningLease::new(binding, move || {
            release_lease(&state, &retirement_sink, key, LeaseKind::Planning);
        }))
    }

    fn acquire_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError> {
        let (descriptor, incarnation, mutation, key) = {
            let mut state = self.lock_state()?;
            let key = state.active.get(instance_id).cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
            let generation = state.generations.get_mut(&key).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "active connector control generation is missing",
                )
            })?;
            if generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let mutation = generation.binding.mutation().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no catalog mutation capability",
                )
            })?;
            generation.mutation_leases = generation.mutation_leases.saturating_add(1);
            (
                generation.binding.descriptor().clone(),
                generation.binding.incarnation(),
                mutation,
                key,
            )
        };
        let state = Arc::downgrade(&self.state);
        let retirement_sink = Arc::downgrade(&self.retirement_sink);
        ConnectorCatalogMutationLease::new(descriptor, incarnation, mutation, move || {
            release_lease(&state, &retirement_sink, key, LeaseKind::Mutation);
        })
    }

    fn acquire_current_data_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        let key = {
            let state = self.lock_state()?;
            state.active.get(instance_id).cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?
        };
        self.acquire_data_mutation(&key, true)
    }

    fn acquire_exact_data_mutation(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        self.acquire_data_mutation(key, false)
    }

    fn acquire_data_mutation(
        &self,
        key: &ConnectorExecutionBindingKey,
        require_active: bool,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        let (descriptor, metadata, mutation) = {
            let mut state = self.lock_state()?;
            let generation = state.generations.get_mut(key).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "connector control generation is not registered",
                )
            })?;
            if require_active && generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let mutation = generation.binding.data_mutation().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no data mutation capability",
                )
            })?;
            generation.data_mutation_leases = generation.data_mutation_leases.saturating_add(1);
            (
                generation.binding.descriptor().clone(),
                Arc::clone(generation.binding.metadata()),
                mutation,
            )
        };
        let state = Arc::downgrade(&self.state);
        let retirement_sink = Arc::downgrade(&self.retirement_sink);
        let lease_key = key.clone();
        ConnectorDataMutationLease::new(descriptor, key.clone(), metadata, mutation, move || {
            release_lease(&state, &retirement_sink, lease_key, LeaseKind::DataMutation);
        })
    }

    fn acquire_write(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorWriteLease, ConnectorError> {
        let (write, distribution, key) = {
            let mut state = self.lock_state()?;
            let key = state.active.get(instance_id).cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
            let generation = state.generations.get_mut(&key).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "active connector control generation is missing",
                )
            })?;
            if generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let write = generation.binding.write().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no distributed write capability",
                )
            })?;
            let distribution = generation.binding.execution_distribution().clone();
            generation.write_leases = generation.write_leases.saturating_add(1);
            (write, distribution, key)
        };
        let state = Arc::downgrade(&self.state);
        let retirement_sink = Arc::downgrade(&self.retirement_sink);
        ConnectorWriteLease::new_with_execution_distribution(
            key.clone(),
            write,
            distribution,
            move || release_lease(&state, &retirement_sink, key, LeaseKind::Write),
        )
    }

    fn acquire_statistics(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorStatisticsLease, ConnectorError> {
        let (descriptor, incarnation, statistics, key) = {
            let mut state = self.lock_state()?;
            let key = state.active.get(instance_id).cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
            let generation = state.generations.get_mut(&key).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "active connector control generation is missing",
                )
            })?;
            if generation.state != ControlGenerationState::Active {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "connector control generation is retiring",
                ));
            }
            let statistics = generation.binding.statistics().cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no statistics capability",
                )
            })?;
            generation.statistics_leases = generation.statistics_leases.saturating_add(1);
            (
                generation.binding.descriptor().clone(),
                generation.binding.incarnation(),
                statistics,
                key,
            )
        };
        let state = Arc::downgrade(&self.state);
        let retirement_sink = Arc::downgrade(&self.retirement_sink);
        ConnectorStatisticsLease::new(descriptor, incarnation, statistics, move || {
            release_lease(&state, &retirement_sink, key, LeaseKind::Statistics);
        })
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ControlHostState>, ConnectorError> {
        self.state.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector control host lock poisoned",
            )
        })
    }

    fn dispatch_retirement(&self, retirement: Option<ConnectorControlRetirement>) {
        let Some(retirement) = retirement else { return };
        let Ok(slot) = self.retirement_sink.lock() else {
            return;
        };
        let sink = slot.clone();
        if let Some(sink) = sink {
            drop(slot);
            sink.retire(retirement);
        } else if let Ok(mut state) = self.lock_state() {
            // Keep the sink lock while publishing the fallback so a sink
            // installation cannot drain the queue before this push.
            state.ready_retires.push(retirement);
        }
    }
}

impl ConnectorControlResolver for ConnectorControlHost {
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
        key: &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorDataMutationLease, ConnectorError> {
        Self::acquire_exact_data_mutation(self, key)
    }
}

impl ConnectorWriteResolver for ConnectorControlHost {
    fn acquire_current_write(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorWriteLease, ConnectorError> {
        self.acquire_write(instance_id)
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

#[derive(Clone, Copy)]
enum LeaseKind {
    Planning,
    Mutation,
    DataMutation,
    Write,
    Statistics,
}

fn release_lease(
    state: &Weak<Mutex<ControlHostState>>,
    retirement_sink: &Weak<Mutex<Option<Arc<dyn ConnectorControlRetirementSink>>>>,
    key: ConnectorExecutionBindingKey,
    kind: LeaseKind,
) {
    let Some(host_state) = state.upgrade() else {
        return;
    };
    let Ok(mut state) = host_state.lock() else {
        return;
    };
    let Some(generation) = state.generations.get_mut(&key) else {
        return;
    };
    match kind {
        LeaseKind::Planning => {
            generation.planning_leases = generation.planning_leases.saturating_sub(1);
        }
        LeaseKind::Mutation => {
            generation.mutation_leases = generation.mutation_leases.saturating_sub(1);
        }
        LeaseKind::DataMutation => {
            generation.data_mutation_leases = generation.data_mutation_leases.saturating_sub(1);
        }
        LeaseKind::Write => {
            generation.write_leases = generation.write_leases.saturating_sub(1);
        }
        LeaseKind::Statistics => {
            generation.statistics_leases = generation.statistics_leases.saturating_sub(1);
        }
    }
    let retirement = (generation.state == ControlGenerationState::Retiring
        && generation.all_leases_released())
    .then(|| queue_retirement(&mut state, key))
    .flatten();
    drop(state);
    let Some(retirement) = retirement else {
        return;
    };
    let Some(slot) = retirement_sink.upgrade() else {
        return;
    };
    let Ok(slot) = slot.lock() else {
        return;
    };
    let sink = slot.clone();
    if let Some(sink) = sink {
        drop(slot);
        sink.retire(retirement);
    } else if let Ok(mut state) = host_state.lock() {
        // See ConnectorControlHost::dispatch_retirement: keep the sink lock
        // while publishing the fallback queue entry.
        state.ready_retires.push(retirement);
    }
}

fn queue_retirement(
    state: &mut ControlHostState,
    key: ConnectorExecutionBindingKey,
) -> Option<ConnectorControlRetirement> {
    let Some(generation) = state.generations.remove(&key) else {
        return None;
    };
    debug_assert_eq!(generation.state, ControlGenerationState::Retiring);
    state.retired.insert(key.clone());
    let installed_backends = state
        .installed_backends
        .remove(&key)
        .unwrap_or_default()
        .into_iter()
        .collect();
    Some(ConnectorControlRetirement {
        key,
        installed_backends,
    })
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_connector_starrocks::{
        StarRocksCapabilitySnapshot, StarRocksConnectorConfig, StarRocksControlGeneration,
        StarRocksDirectColumnBinding, StarRocksDirectLocation, StarRocksDirectLocationSource,
        StarRocksDirectMetadataLayout, StarRocksDirectSplitPlanner,
        StarRocksDirectTabletDescriptor, StarRocksDirectTabletPlanningSource,
        StarRocksMetadataSource, StarRocksReadPolicy, StarRocksResolvedTable,
        StarRocksRpcSplitPlanner, StarRocksRpcTransport, StarRocksSharedDataDirectPlanner,
        StarRocksSplitPlanningInput, StarRocksStorageBindingRef, StarRocksStrategySplit,
    };
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorExecutionDeclaration,
        ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceIncarnation,
        ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest,
        ConnectorProviderId, ConnectorScan, ConnectorScanHandle, ConnectorScanPlanning,
        ConnectorSplitPlanningRequest, ConnectorTableHandle, ConnectorTableIdentity,
        ConnectorTableMetadata, ConnectorTableRequest, ConnectorTableResolution,
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
                novarocks_connector_starrocks::StarRocksTopology::SharedData,
                Bytes::from_static(b"schema-v1"),
                Bytes::from_static(b"data-v1"),
                StarRocksCapabilitySnapshot {
                    api_contract_version: 1,
                    rpc_transports: Default::default(),
                    rpc_ready: false,
                    direct_contract_version: Some(1),
                    direct_ready: true,
                },
            )
        }
    }

    struct StarRocksFixturePlanner;

    impl StarRocksRpcSplitPlanner for StarRocksFixturePlanner {
        fn plan_rpc_splits(
            &self,
            _: &StarRocksSplitPlanningInput,
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError> {
            Err(unsupported())
        }
    }

    impl StarRocksDirectSplitPlanner for StarRocksFixturePlanner {
        fn plan_direct_splits(
            &self,
            _: &StarRocksSplitPlanningInput,
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksStrategySplit>, ConnectorError> {
            Err(unsupported())
        }
    }

    struct DirectTablets;
    impl StarRocksDirectTabletPlanningSource for DirectTablets {
        fn plan_tablets(
            &self,
            _: &StarRocksSplitPlanningInput,
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksDirectTabletDescriptor>, ConnectorError> {
            Ok(vec![StarRocksDirectTabletDescriptor::try_new(
                1,
                1,
                1,
                StarRocksDirectMetadataLayout::Standalone,
                "meta/1.meta",
                vec![StarRocksDirectColumnBinding::try_new(
                    0, 1, "id", "BIGINT", false, None,
                )?],
                None,
            )?])
        }
    }
    struct DirectLocations;
    impl StarRocksDirectLocationSource for DirectLocations {
        fn resolve_locations(
            &self,
            _: &[i64],
            _: &ConnectorSplitPlanningRequest,
        ) -> Result<Vec<StarRocksDirectLocation>, ConnectorError> {
            Ok(vec![StarRocksDirectLocation::try_new(
                1,
                "s3://bucket/tablet/1",
                StarRocksStorageBindingRef::parse("fixture")?,
                "fixture",
            )?])
        }
    }

    fn starrocks_binding() -> ConnectorControlBinding {
        let config = StarRocksConnectorConfig::new(
            ConnectorInstanceId::parse("catalog.starrocks").expect("instance ID"),
            StarRocksReadPolicy::Auto,
            StarRocksRpcTransport::BrpcChunk,
            novarocks_connector_starrocks::StarRocksLocalBindingRef::parse("test")
                .expect("binding"),
        );
        StarRocksControlGeneration::try_new(
            config,
            Arc::new(StarRocksFixtureSource),
            Arc::new(StarRocksFixturePlanner),
            Arc::new(StarRocksFixturePlanner),
        )
        .expect("StarRocks control binding")
    }

    fn starrocks_direct_binding() -> ConnectorControlBinding {
        let config = StarRocksConnectorConfig::new(
            ConnectorInstanceId::parse("catalog.starrocks").expect("instance ID"),
            StarRocksReadPolicy::Direct,
            StarRocksRpcTransport::BrpcChunk,
            novarocks_connector_starrocks::StarRocksLocalBindingRef::parse("fixture")
                .expect("binding"),
        );
        StarRocksControlGeneration::try_new(
            config,
            Arc::new(StarRocksFixtureSource),
            Arc::new(StarRocksFixturePlanner),
            Arc::new(StarRocksSharedDataDirectPlanner::new(
                Arc::new(DirectTablets),
                Arc::new(DirectLocations),
            )),
        )
        .expect("StarRocks direct control binding")
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
        incarnation: ConnectorInstanceIncarnation,
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
        ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
            ConnectorExecutionDeclaration::try_new(
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                    instance_id: self.instance_id.clone(),
                },
                self.incarnation,
                Bytes::from_static(b"binding=default"),
            )
        }
    }

    fn binding(incarnation: u8) -> ConnectorControlBinding {
        let provider = Arc::new(TestControl {
            instance_id: ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([incarnation; 16]),
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

    #[test]
    fn retiring_generation_waits_for_planning_lease_before_remote_retire() {
        let host = ConnectorControlHost::new();
        let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
        host.register(binding(7)).expect("register old generation");
        let lease = host.acquire_current(&instance_id).expect("planning lease");
        let old_key = ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
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

    #[derive(Default)]
    struct RecordingRetirementSink(Mutex<Vec<ConnectorControlRetirement>>);

    impl ConnectorControlRetirementSink for RecordingRetirementSink {
        fn retire(&self, retirement: ConnectorControlRetirement) {
            self.0.lock().expect("retirement sink").push(retirement);
        }
    }

    #[test]
    fn retiring_generation_dispatches_when_the_last_planning_lease_drains() {
        let host = ConnectorControlHost::new();
        let sink = Arc::new(RecordingRetirementSink::default());
        host.set_retirement_sink(sink.clone());
        let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
        host.register(binding(7)).expect("register old generation");
        let lease = host.acquire_current(&instance_id).expect("planning lease");
        let old_key = ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        host.record_installed_backend(&old_key, "127.0.0.1:18080")
            .expect("record ensure ack");
        host.retire_current(&instance_id)
            .expect("retire old generation");
        assert!(sink.0.lock().expect("retirement sink").is_empty());

        drop(lease);

        let dispatched = sink.0.lock().expect("retirement sink");
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].key, old_key);
        assert_eq!(
            dispatched[0].installed_backends,
            vec![String::from("127.0.0.1:18080")]
        );
        assert!(host.take_ready_retires().expect("retire queue").is_empty());
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
            .execution_declaration(&starrocks_context())
            .expect("declaration");
        assert_eq!(declaration.binding_key().incarnation, first_incarnation);

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

    #[test]
    fn starrocks_control_host_direct_read_plans_a_frozen_fixture_split() {
        let host = ConnectorControlHost::new();
        let binding = starrocks_direct_binding();
        let instance = binding.descriptor().instance_id.clone();
        host.register(binding).expect("register direct generation");
        let lease = host
            .acquire_current(&instance)
            .expect("direct planning lease");
        let context = starrocks_context();
        let table = lease
            .binding()
            .metadata()
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: instance.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("table"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            })
            .expect("load fixture table");
        let scan = lease
            .binding()
            .planning()
            .begin_scan(
                &table.table,
                ConnectorBeginScanRequest {
                    projection: vec![],
                    static_predicates: vec![],
                    selector: novarocks_spi::connector::ConnectorReadSelector::Current,
                    limit: None,
                    batch: ConnectorBatchBudget {
                        max_rows: NonZeroUsize::new(16).unwrap(),
                        max_bytes: NonZeroUsize::new(4096).unwrap(),
                    },
                    context: context.clone(),
                },
            )
            .expect("freeze direct scan");
        let splits = lease
            .binding()
            .planning()
            .plan_splits(
                &scan.handle,
                ConnectorSplitPlanningRequest {
                    target_parallelism: NonZeroUsize::new(1).unwrap(),
                    max_split_bytes: None,
                    context,
                },
            )
            .expect("plan direct split");
        assert_eq!(splits.splits.len(), 1);
        host.retire_current(&instance)
            .expect("retire direct generation");
        drop(lease);
        assert_eq!(host.take_ready_retires().expect("ready retire").len(), 1);
    }

    fn unsupported() -> ConnectorError {
        ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "test-only control capability",
        )
    }
}
