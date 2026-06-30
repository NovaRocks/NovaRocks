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
//! Hash-join build sink for materializing build-side hash structures.
//!
//! Responsibilities:
//! - Consumes build-side chunks and inserts keys/rows into hash-table artifacts.
//! - Publishes finalized artifacts to shared state once build input is exhausted.
//!
//! Key exported interfaces:
//! - Types: `HashJoinBuildSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};

use super::build_artifact::JoinBuildArtifact;
use super::build_requirements::{NullKeyRequirement, required_build_components};
use super::build_state::JoinBuildSinkState;
use super::join_hash_map::build_store::BuildStoreBuilder;
use super::join_hash_map::method::{BuildKeyBatch, JoinHashMap, JoinHashMapBuildOptions};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::{JoinDistributionMode, JoinRuntimeFilterSpec, JoinType};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::runtime_filter::{
    LocalRuntimeFilterSet, LocalRuntimeInFilterSet, MAX_RUNTIME_IN_FILTER_CONDITIONS,
    PartialRuntimeInFilterMerger, RUNTIME_FILTER_JOIN_MODE_BROADCAST,
    RUNTIME_FILTER_JOIN_MODE_PARTITIONED, RuntimeBloomFilter, RuntimeEmptyFilter,
    RuntimeFilterMergeDropCounters, RuntimeFilterType, RuntimeInFilter,
    RuntimeMembershipBuildOptions, RuntimeMembershipFilter, RuntimeMembershipFilterBuildParam,
    RuntimeMinMaxFilter, arrow_type_to_proto_type_desc, encode_starrocks_bitset_filter,
    encode_starrocks_bloom_filter, encode_starrocks_empty_filter,
    maybe_build_runtime_bitset_filter,
};
use crate::novarocks_logging::{debug, warn};
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::runtime::profile::clamp_u128_to_i64;
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
use crate::runtime::runtime_state::RuntimeState;
use crate::service::exchange_sender;
use std::collections::{HashMap, HashSet};

/// Factory for hash-join build sinks that construct build-side hash structures.
pub struct HashJoinBuildSinkFactory {
    name: String,
    node_id: i32,
    arena: Arc<ExprArena>,
    join_type: JoinType,
    has_residual_predicate: bool,
    probe_is_left: bool,
    has_equi_keys: bool,
    build_keys: Vec<ExprId>,
    eq_null_safe: Vec<bool>,
    runtime_filters: Vec<JoinRuntimeFilterSpec>,
    distribution_mode: JoinDistributionMode,
    state: Arc<dyn JoinBuildSinkState>,
    runtime_filter_hub: Arc<RuntimeFilterHub>,
    runtime_in_filter_merger: Option<Arc<PartialRuntimeInFilterMerger>>,
}

impl HashJoinBuildSinkFactory {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        join_type: JoinType,
        has_residual_predicate: bool,
        probe_is_left: bool,
        has_equi_keys: bool,
        build_keys: Vec<ExprId>,
        eq_null_safe: Vec<bool>,
        runtime_filters: Vec<JoinRuntimeFilterSpec>,
        distribution_mode: JoinDistributionMode,
        state: Arc<dyn JoinBuildSinkState>,
        runtime_filter_hub: Arc<RuntimeFilterHub>,
        runtime_in_filter_merger: Option<Arc<PartialRuntimeInFilterMerger>>,
    ) -> Self {
        let node_id = parse_join_node_id_from_dep_key(state.dep_name(0));
        if node_id >= 0 {
            // Register the local RF dependency upfront so probe-side operators only wait for
            // build nodes that actually exist in this query's hash-join pipeline.
            let _ = runtime_filter_hub.local_dependency(node_id);
        }
        Self {
            name: format!("HASH_JOIN (id={})", node_id),
            node_id,
            arena,
            join_type,
            has_residual_predicate,
            probe_is_left,
            has_equi_keys,
            build_keys,
            eq_null_safe,
            runtime_filters,
            distribution_mode,
            state,
            runtime_filter_hub,
            runtime_in_filter_merger,
        }
    }
}

impl OperatorFactory for HashJoinBuildSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let partition = self.state.partition_for_driver(driver_id);
        let dist = match self.distribution_mode {
            JoinDistributionMode::Broadcast => "BROADCAST",
            JoinDistributionMode::Partitioned => "PARTITIONED",
        };
        let requirements = required_build_components(
            self.join_type,
            self.has_residual_predicate,
            self.probe_is_left,
            self.has_equi_keys,
        );
        debug!(
            "HashJoinBuildSink create: node_id={} driver_id={} partition={} join_type={} residual_predicate={} dist={} build_keys={} null_safe_keys={} runtime_filters={}",
            self.node_id,
            driver_id,
            partition,
            join_type_str(self.join_type),
            self.has_residual_predicate,
            dist,
            self.build_keys.len(),
            self.eq_null_safe.iter().filter(|v| **v).count(),
            self.runtime_filters.len()
        );
        Box::new(HashJoinBuildSinkOperator {
            name: self.name.clone(),
            node_id: self.node_id,
            driver_id,
            arena: Arc::clone(&self.arena),
            join_type: self.join_type,
            has_residual_predicate: self.has_residual_predicate,
            probe_is_left: self.probe_is_left,
            has_equi_keys: self.has_equi_keys,
            build_keys: self.build_keys.clone(),
            eq_null_safe: self.eq_null_safe.clone(),
            runtime_filter_specs: self.runtime_filters.clone(),
            distribution_mode: self.distribution_mode,
            state: Arc::clone(&self.state),
            partition,
            runtime_filter_hub: Arc::clone(&self.runtime_filter_hub),
            runtime_in_filter_merger: self.runtime_in_filter_merger.as_ref().map(Arc::clone),
            build_store_builder: BuildStoreBuilder::new(),
            build_input_chunks: Vec::new(),
            build_key_batches: Vec::new(),
            build_key_batches_retained_bytes: 0,
            build_key_batches_accounting: None,
            build_key_batches_mem_tracker: None,
            build_table: None,
            runtime_filters: None,
            runtime_in_filters: None,
            finished: false,
            build_row_count: 0,
            build_has_null_key: false,
            build_null_key_rows: match requirements.null_keys {
                NullKeyRequirement::NullKeyRows => Some(Vec::new()),
                NullKeyRequirement::NotNeeded | NullKeyRequirement::HasAnyNullKey => None,
            },
            logged_first_input: false,
            profile_initialized: false,
            profiles: None,
            input_rows: 0,
            input_chunks: 0,
            build_input_chunks_mem_tracker: None,
            build_table_mem_tracker: None,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct HashJoinBuildSinkOperator {
    name: String,
    node_id: i32,
    driver_id: i32,
    arena: Arc<ExprArena>,
    join_type: JoinType,
    has_residual_predicate: bool,
    probe_is_left: bool,
    has_equi_keys: bool,
    build_keys: Vec<ExprId>,
    eq_null_safe: Vec<bool>,
    runtime_filter_specs: Vec<JoinRuntimeFilterSpec>,
    distribution_mode: JoinDistributionMode,
    state: Arc<dyn JoinBuildSinkState>,
    partition: usize,
    runtime_filter_hub: Arc<RuntimeFilterHub>,
    runtime_in_filter_merger: Option<Arc<PartialRuntimeInFilterMerger>>,
    build_store_builder: BuildStoreBuilder,
    build_input_chunks: Vec<Chunk>,
    build_key_batches: Vec<BuildKeyBatch>,
    build_key_batches_retained_bytes: usize,
    build_key_batches_accounting: Option<TrackedBytes>,
    build_key_batches_mem_tracker: Option<Arc<MemTracker>>,
    build_table: Option<JoinHashMap>,
    runtime_filters: Option<LocalRuntimeFilterSet>,
    runtime_in_filters: Option<LocalRuntimeInFilterSet>,
    finished: bool,
    build_row_count: usize,
    build_has_null_key: bool,
    build_null_key_rows: Option<Vec<u32>>,
    logged_first_input: bool,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    input_rows: u64,
    input_chunks: u64,
    build_input_chunks_mem_tracker: Option<Arc<MemTracker>>,
    build_table_mem_tracker: Option<Arc<MemTracker>>,
}

impl Operator for HashJoinBuildSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let chunks = MemTracker::new_child("BuildInputChunks", &tracker);
        self.build_input_chunks_mem_tracker = Some(Arc::clone(&chunks));
        for chunk in self.build_input_chunks.iter_mut() {
            chunk.transfer_to(&chunks);
        }

        let table = MemTracker::new_child("BuildHashTable", &tracker);
        self.build_table_mem_tracker = Some(Arc::clone(&table));
        if let Some(build_table) = self.build_table.as_mut() {
            build_table.set_mem_tracker(table);
        }

        let key_batches = MemTracker::new_child("BuildKeyBatches", &tracker);
        self.build_key_batches_mem_tracker = Some(Arc::clone(&key_batches));
        self.refresh_build_key_batches_accounting();
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for HashJoinBuildSinkOperator {
    fn need_input(&self) -> bool {
        !self.is_finished()
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, mut chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.init_profile_if_needed();
        if !matches!(
            self.join_type,
            JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::NullAwareLeftAnti
        ) {
            return Err("unsupported join type for hash join build".to_string());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        if !self.logged_first_input {
            self.logged_first_input = true;
            debug!(
                "HashJoinBuildSink received first input: dep_key={} driver_id={} partition={} node_id={} rows={}",
                self.state.dep_name(self.partition),
                self.driver_id,
                self.partition,
                self.node_id,
                chunk.len()
            );
        }
        self.input_rows = self.input_rows.saturating_add(chunk.len() as u64);
        self.input_chunks = self.input_chunks.saturating_add(1);
        let base_row_id = self.build_row_count;
        self.build_row_count = self.build_row_count.saturating_add(chunk.len());
        let requirements = required_build_components(
            self.join_type,
            self.has_residual_predicate,
            self.probe_is_left,
            self.has_equi_keys,
        );
        let retain_build_rows = requirements.requires_row_payload();
        if retain_build_rows {
            self.build_store_builder.push_chunk(&chunk)?;

            if let Some(tracker) = self.build_input_chunks_mem_tracker.as_ref() {
                chunk.transfer_to(tracker);
            }
            self.build_input_chunks.push(chunk.clone());
        }

        if self.build_keys.is_empty() {
            return Ok(());
        }

        let mut key_arrays = Vec::with_capacity(self.build_keys.len());
        for expr in &self.build_keys {
            let array = self.arena.eval(*expr, &chunk).map_err(|e| e.to_string())?;
            key_arrays.push(array);
        }
        if self.eq_null_safe.len() != key_arrays.len() {
            return Err(format!(
                "hash join build null-safe key count mismatch: flags={} keys={}",
                self.eq_null_safe.len(),
                key_arrays.len()
            ));
        }

        if !self.build_has_null_key {
            self.build_has_null_key = key_arrays.iter().any(|a| a.null_count() > 0);
        }
        if let Some(null_key_rows) = self.build_null_key_rows.as_mut() {
            for row in 0..chunk.len() {
                let has_forbidden_null =
                    key_arrays.iter().enumerate().any(|(key_idx, key_array)| {
                        !self.eq_null_safe.get(key_idx).copied().unwrap_or(false)
                            && !key_array.is_valid(row)
                    });
                if has_forbidden_null {
                    let flat_row_id = base_row_id
                        .checked_add(row)
                        .ok_or_else(|| "join build null-key row id overflow".to_string())?;
                    null_key_rows.push(
                        u32::try_from(flat_row_id)
                            .map_err(|_| "join build null-key row id overflow".to_string())?,
                    );
                }
            }
        }

        let retained_bytes = retained_key_arrays_bytes(&key_arrays, &chunk, retain_build_rows);
        self.build_key_batches
            .push(BuildKeyBatch::new(key_arrays.clone(), chunk.len())?);
        self.build_key_batches_retained_bytes = self
            .build_key_batches_retained_bytes
            .saturating_add(retained_bytes);
        self.refresh_build_key_batches_accounting();
        if !self.runtime_filter_specs.is_empty() {
            if self.build_keys.is_empty() {
                return Err("runtime filters require join build keys".to_string());
            }
            if self.build_row_count > MAX_RUNTIME_IN_FILTER_CONDITIONS {
                self.runtime_in_filters = None;
            } else {
                if self.runtime_in_filters.is_none() {
                    self.runtime_in_filters = Some(LocalRuntimeInFilterSet::new(
                        &self.runtime_filter_specs,
                        &key_arrays,
                    )?);
                }
                if let Some(filters) = self.runtime_in_filters.as_mut() {
                    filters.add_build_arrays(&key_arrays)?;
                }
            }
            if self.runtime_filters.is_none() {
                let hash_seed = self.runtime_filter_hash_seed()?;
                self.runtime_filters = Some(LocalRuntimeFilterSet::new(
                    &self.runtime_filter_specs,
                    hash_seed,
                ));
            }
            if let Some(filters) = self.runtime_filters.as_mut() {
                filters.add_build_arrays(&key_arrays)?;
            }
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.init_profile_if_needed();
        debug!(
            "HashJoinBuildSink set_finishing: dep_key={} driver_id={} partition={} node_id={} input_rows={} input_chunks={}",
            self.state.dep_name(self.partition),
            self.driver_id,
            self.partition,
            self.node_id,
            self.input_rows,
            self.input_chunks
        );
        self.finished = true;

        let requirements = required_build_components(
            self.join_type,
            self.has_residual_predicate,
            self.probe_is_left,
            self.has_equi_keys,
        );
        let retain_build_rows = requirements.requires_row_payload();
        let build_ht_timer = self
            .profiles
            .as_ref()
            .map(|p| p.common.add_timer("BuildHashTableTime"));
        if !self.build_key_batches.is_empty() && self.build_table.is_none() {
            let key_types = self
                .build_key_batches
                .first()
                .expect("first build key batch")
                .arrays()
                .iter()
                .map(|array| array.data_type().clone())
                .collect::<Vec<_>>();
            let start = std::time::Instant::now();
            let table = JoinHashMap::build_from_key_batches_with_tracker(
                key_types,
                self.eq_null_safe.clone(),
                &self.build_key_batches,
                JoinHashMapBuildOptions {
                    purpose: requirements.join_hash_map_purpose().ok_or_else(|| {
                        "hash join lookup purpose missing for keyed build".to_string()
                    })?,
                    ..JoinHashMapBuildOptions::default()
                },
                self.build_table_mem_tracker.as_ref().map(Arc::clone),
            )?;
            if let Some(timer) = build_ht_timer.as_ref() {
                timer.add(clamp_u128_to_i64(start.elapsed().as_nanos()));
            }
            debug!(
                "HashJoinBuildSink selected join map: dep_key={} partition={} method={:?}",
                self.state.dep_name(self.partition),
                self.partition,
                table.method_kind()
            );
            if let Some(profile) = self.profiles.as_ref() {
                profile
                    .common
                    .add_info_string("JoinHashMapMethod", table.method_kind().as_profile_str());
            }
            self.build_table = Some(table);
        }

        self.publish_runtime_filters(state)?;
        debug!(
            "HashJoinBuildSink mark_local_filters_ready: dep_key={} driver_id={} partition={} node_id={} input_rows={} input_chunks={}",
            self.state.dep_name(self.partition),
            self.driver_id,
            self.partition,
            self.node_id,
            self.input_rows,
            self.input_chunks
        );
        self.runtime_filter_hub
            .mark_local_filters_ready(self.node_id);

        let build_store_rows = if retain_build_rows {
            self.build_store_builder.row_count()
        } else {
            0
        };
        let table_present = self.build_table.is_some();
        let input_chunks = std::mem::take(&mut self.build_input_chunks);
        let mut table = self.build_table.take();
        let mut build_store = if retain_build_rows {
            std::mem::replace(&mut self.build_store_builder, BuildStoreBuilder::new()).finish()?
        } else {
            self.build_store_builder = BuildStoreBuilder::new();
            None
        };
        drop(input_chunks);

        if let Some(root) = state.mem_tracker() {
            let label = format!("JoinBuildArtifact: {}", self.state.dep_name(self.partition));
            let artifact = MemTracker::new_child(label, &root);
            let artifact_table = MemTracker::new_child("BuildHashTable", &artifact);
            if let Some(table) = table.as_mut() {
                table.set_mem_tracker(artifact_table);
            }
            let artifact_build_store = MemTracker::new_child("BuildStore", &artifact);
            if let Some(store) = build_store.as_mut() {
                store.transfer_to(&artifact_build_store);
            }
        }
        let runtime_filters = self.runtime_filters.take().map(Arc::new);
        self.clear_build_key_batches();
        let build_null_key_rows = self.build_null_key_rows.take().map(Arc::new);
        let join_map_method = table
            .as_ref()
            .map(|t| t.method_kind().as_profile_str())
            .unwrap_or("None");
        let artifact = Arc::new(JoinBuildArtifact::new(
            requirements,
            build_store,
            table,
            self.build_row_count,
            self.build_has_null_key,
            build_null_key_rows,
            runtime_filters,
        ));
        artifact.validate_components(requirements)?;
        self.state
            .set_build(self.partition, artifact)
            .map_err(|e| e.to_string())?;
        debug!(
            "HashJoinBuildSink finished: dep_key={} driver_id={} partition={} node_id={} join_type={} input_rows={} input_chunks={} build_row_count={} build_has_null_key={} build_store_rows={} build_table={} build_keys={} join_map_method={}",
            self.state.dep_name(self.partition),
            self.driver_id,
            self.partition,
            self.node_id,
            join_type_str(self.join_type),
            self.input_rows,
            self.input_chunks,
            self.build_row_count,
            self.build_has_null_key,
            build_store_rows,
            table_present,
            self.build_keys.len(),
            join_map_method
        );
        Ok(())
    }
}

impl HashJoinBuildSinkOperator {
    fn refresh_build_key_batches_accounting(&mut self) {
        let Some(tracker) = self.build_key_batches_mem_tracker.as_ref() else {
            return;
        };
        if self.build_key_batches_retained_bytes == 0 {
            self.build_key_batches_accounting = None;
            return;
        }
        match self.build_key_batches_accounting.as_mut() {
            Some(accounting)
                if accounting.bytes() as usize == self.build_key_batches_retained_bytes =>
            {
                accounting.transfer_to(Arc::clone(tracker));
            }
            _ => {
                let old_accounting = self.build_key_batches_accounting.take();
                drop(old_accounting);
                self.build_key_batches_accounting = Some(TrackedBytes::new(
                    self.build_key_batches_retained_bytes,
                    Arc::clone(tracker),
                ));
            }
        }
    }

    fn clear_build_key_batches(&mut self) {
        self.build_key_batches.clear();
        self.build_key_batches_retained_bytes = 0;
        self.build_key_batches_accounting = None;
    }

    fn publish_runtime_filters(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.runtime_filter_specs.is_empty() {
            return Ok(());
        }
        if self.build_keys.is_empty() {
            return Err("runtime filters require join build keys".to_string());
        }
        let _timer = self
            .profiles
            .as_ref()
            .map(|p| p.common.scoped_timer("RuntimeFilterBuildTime"));

        let hash_seed = self.runtime_filter_hash_seed()?;
        if self.runtime_filters.is_none() {
            self.runtime_filters = Some(LocalRuntimeFilterSet::new(
                &self.runtime_filter_specs,
                hash_seed,
            ));
        }

        let mut in_filters = Vec::new();
        if self.build_row_count > 0
            && self.build_row_count <= MAX_RUNTIME_IN_FILTER_CONDITIONS
            && let Some(filters) = self.runtime_in_filters.take()
        {
            in_filters = filters.into_filters();
        }

        let membership_params = self.build_membership_filter_params()?;
        let membership_build_options = self.membership_build_options(state);

        if let Some(merger) = self.runtime_in_filter_merger.as_ref() {
            let merged_in = merger.add_partial(self.partition, self.build_row_count, in_filters)?;
            let merged_membership = merger.add_partial_membership(
                self.partition,
                membership_params,
                membership_build_options,
            )?;
            self.add_runtime_filter_drop_counters_to_profile(merger.drain_drop_counters());
            if let (Some(in_filters), Some(membership_filters)) = (merged_in, merged_membership) {
                self.log_in_filters("publish", &in_filters);
                self.log_membership_filters("publish", &membership_filters);
                if let Some(profile) = self.profiles.as_ref() {
                    profile
                        .common
                        .counter_set_unit("RuntimeFilterNum", membership_filters.len() as i64);
                    profile
                        .common
                        .counter_set_unit("RuntimeInFilterNum", in_filters.len() as i64);
                }
                let local_filters = self.build_local_filters(&in_filters);
                let membership_for_publish = membership_filters.clone();
                let membership_for_remote = self.filter_membership_for_remote(&membership_filters);
                self.runtime_filter_hub
                    .publish_filters(&local_filters, &membership_for_publish);
                self.send_runtime_filters_remote(state, &membership_for_remote)?;
            }
        } else {
            let in_filters = in_filters;
            let membership_filters = self.build_membership_filters_from_params(
                self.build_row_count as u64,
                &membership_params,
                membership_build_options,
            )?;
            self.log_in_filters("publish", &in_filters);
            self.log_membership_filters("publish", &membership_filters);
            if let Some(profile) = self.profiles.as_ref() {
                profile
                    .common
                    .counter_set_unit("RuntimeFilterNum", membership_filters.len() as i64);
                profile
                    .common
                    .counter_set_unit("RuntimeInFilterNum", in_filters.len() as i64);
            }
            let local_filters = self.build_local_filters(&in_filters);
            let membership_for_publish = membership_filters.clone();
            let membership_for_remote = self.filter_membership_for_remote(&membership_filters);
            self.runtime_filter_hub
                .publish_filters(&local_filters, &membership_for_publish);
            self.send_runtime_filters_remote(state, &membership_for_remote)?;
        }
        Ok(())
    }

    fn send_runtime_filters_remote(
        &self,
        state: &RuntimeState,
        filters: &[RuntimeMembershipFilter],
    ) -> Result<(), String> {
        let remote_specs: Vec<&JoinRuntimeFilterSpec> = self
            .runtime_filter_specs
            .iter()
            .filter(|spec| spec.has_remote_targets)
            .collect();
        if remote_specs.is_empty() {
            return Ok(());
        }

        let Some(query_id) = state.query_id() else {
            return Ok(());
        };
        let id_to_probers = state
            .runtime_filter_params()
            .and_then(|params| params.id_to_prober_params.as_ref());
        let filters: Vec<RuntimeMembershipFilter> = if filters.is_empty() {
            self.build_empty_remote_membership_filters(&remote_specs)?
        } else {
            filters.to_vec()
        };
        if filters.is_empty() {
            return Ok(());
        }
        let build_be_number = state.backend_num().unwrap_or(0);
        let finst_id = state.fragment_instance_id().map(|id| {
            crate::service::grpc_client::proto::starrocks::PUniqueId {
                hi: id.hi,
                lo: id.lo,
            }
        });

        let mut encoded_bytes: i64 = 0;
        for filter in &filters {
            if self.distribution_mode == JoinDistributionMode::Broadcast && filter.size() == 0 {
                continue;
            }
            let Some(spec) = self
                .runtime_filter_specs
                .iter()
                .find(|spec| spec.filter_id == filter.filter_id())
            else {
                continue;
            };
            let use_merge_nodes = !spec.merge_nodes.is_empty();
            if !use_merge_nodes && self.distribution_mode == JoinDistributionMode::Partitioned {
                warn!(
                    "partitioned runtime filter missing merge nodes: filter_id={}",
                    filter.filter_id()
                );
                continue;
            }

            let data = match filter {
                RuntimeMembershipFilter::Bloom(bloom) => {
                    match encode_starrocks_bloom_filter(bloom) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(
                                "skip remote runtime filter encode: filter_id={} err={}",
                                filter.filter_id(),
                                e
                            );
                            continue;
                        }
                    }
                }
                RuntimeMembershipFilter::Empty(empty) => {
                    match encode_starrocks_empty_filter(empty) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(
                                "skip remote runtime filter encode: filter_id={} err={}",
                                filter.filter_id(),
                                e
                            );
                            continue;
                        }
                    }
                }
                RuntimeMembershipFilter::Bitset(bitset) => {
                    match encode_starrocks_bitset_filter(bitset) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(
                                "skip remote runtime filter encode: filter_id={} err={}",
                                filter.filter_id(),
                                e
                            );
                            continue;
                        }
                    }
                }
            };
            encoded_bytes = encoded_bytes.saturating_add(data.len() as i64);

            let mut seen_hosts = HashSet::new();
            if use_merge_nodes {
                for addr in &spec.merge_nodes {
                    if addr.host.is_empty() {
                        continue;
                    }
                    if !seen_hosts.insert(addr.host.clone()) {
                        continue;
                    }
                    let req =
                        crate::service::grpc_client::proto::starrocks::PTransmitRuntimeFilterParams {
                            is_partial: Some(true),
                            query_id: Some(crate::service::grpc_client::proto::starrocks::PUniqueId {
                                hi: query_id.hi,
                                lo: query_id.lo,
                            }),
                            filter_id: Some(filter.filter_id()),
                            finst_id,
                            build_be_number: Some(build_be_number),
                            column_type: arrow_type_to_proto_type_desc(&spec.build_data_type),
                            data: Some(data.clone()),
                            ..Default::default()
                        };
                    let dest_port = addr.port as u16;
                    if let Err(e) = exchange_sender::send_runtime_filter(&addr.host, dest_port, req)
                    {
                        warn!(
                            "send runtime filter failed: dest={} filter_id={} err={}",
                            addr.host,
                            filter.filter_id(),
                            e
                        );
                    }
                }
            } else {
                let Some(id_to_probers) = id_to_probers else {
                    continue;
                };
                let Some(probers) = id_to_probers.get(&filter.filter_id()) else {
                    continue;
                };
                for prober in probers {
                    let Some(addr) = prober.fragment_instance_address.as_ref() else {
                        continue;
                    };
                    if !seen_hosts.insert(addr.hostname.clone()) {
                        continue;
                    }
                    let req =
                        crate::service::grpc_client::proto::starrocks::PTransmitRuntimeFilterParams {
                            is_partial: Some(false),
                            query_id: Some(crate::service::grpc_client::proto::starrocks::PUniqueId {
                                hi: query_id.hi,
                                lo: query_id.lo,
                            }),
                            filter_id: Some(filter.filter_id()),
                            finst_id,
                            data: Some(data.clone()),
                            ..Default::default()
                        };
                    let dest_port = addr.port as u16;
                    if let Err(e) =
                        exchange_sender::send_runtime_filter(&addr.hostname, dest_port, req)
                    {
                        warn!(
                            "send runtime filter failed: dest={} filter_id={} err={}",
                            addr.hostname,
                            filter.filter_id(),
                            e
                        );
                    }
                }
            }
        }
        if encoded_bytes > 0
            && let Some(profile) = self.profiles.as_ref()
        {
            profile
                .common
                .counter_add_bytes("PartialRuntimeMembershipFilterBytes", encoded_bytes);
        }
        Ok(())
    }

    fn filter_membership_for_remote(
        &self,
        filters: &[RuntimeMembershipFilter],
    ) -> Vec<RuntimeMembershipFilter> {
        if filters.is_empty() {
            return Vec::new();
        }
        let mut spec_by_id = HashMap::with_capacity(self.runtime_filter_specs.len());
        for spec in &self.runtime_filter_specs {
            spec_by_id.insert(spec.filter_id, spec);
        }
        filters
            .iter()
            .filter(|filter| {
                spec_by_id
                    .get(&filter.filter_id())
                    .map(|spec| spec.has_remote_targets)
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    fn log_in_filters(&self, label: &str, filters: &[RuntimeInFilter]) {
        debug!(
            "runtime in filters {}: node_id={} partition={} count={}",
            label,
            self.node_id,
            self.partition,
            filters.len()
        );
        for filter in filters {
            debug!(
                "runtime in filter {}: node_id={} partition={} filter_id={} slot_id={:?} empty={}",
                label,
                self.node_id,
                self.partition,
                filter.filter_id(),
                filter.slot_id(),
                filter.is_empty()
            );
        }
    }

    fn log_membership_filters(&self, label: &str, filters: &[RuntimeMembershipFilter]) {
        debug!(
            "runtime membership filters {}: node_id={} partition={} count={}",
            label,
            self.node_id,
            self.partition,
            filters.len()
        );
        for filter in filters {
            let kind = match filter {
                RuntimeMembershipFilter::Bloom(_) => "bloom",
                RuntimeMembershipFilter::Bitset(_) => "bitset",
                RuntimeMembershipFilter::Empty(_) => "empty",
            };
            debug!(
                "runtime membership filter {}: node_id={} partition={} filter_id={} kind={} slot_id={:?} ltype={:?} size={} has_null={} join_mode={} empty={}",
                label,
                self.node_id,
                self.partition,
                filter.filter_id(),
                kind,
                filter.slot_id(),
                filter.ltype(),
                filter.size(),
                filter.has_null(),
                filter.join_mode(),
                filter.is_empty()
            );
        }
    }

    fn add_runtime_filter_drop_counters_to_profile(
        &self,
        counters: RuntimeFilterMergeDropCounters,
    ) {
        let Some(profile) = self.profiles.as_ref() else {
            return;
        };
        if counters.in_filters > 0 {
            profile
                .common
                .counter_add_unit("RuntimeInFilterDropped", counters.in_filters as i64);
        }
        if counters.membership_filters > 0 {
            profile.common.counter_add_unit(
                "RuntimeMembershipFilterDropped",
                counters.membership_filters as i64,
            );
        }
    }

    fn build_membership_filter_params(
        &self,
    ) -> Result<Vec<RuntimeMembershipFilterBuildParam>, String> {
        if self.runtime_filter_specs.is_empty() {
            return Ok(Vec::new());
        }
        let join_mode: i8 = match self.distribution_mode {
            JoinDistributionMode::Broadcast => RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            JoinDistributionMode::Partitioned => RUNTIME_FILTER_JOIN_MODE_PARTITIONED,
        };
        let mut params = Vec::with_capacity(self.runtime_filter_specs.len());
        for spec in &self.runtime_filter_specs {
            let Some(data_type) = self
                .build_keys
                .get(spec.expr_order)
                .and_then(|id| self.arena.data_type(*id))
            else {
                return Err(format!(
                    "runtime membership filter missing build key type: filter_id={} expr_order={}",
                    spec.filter_id, spec.expr_order
                ));
            };
            let ltype = RuntimeFilterType::from_arrow_data_type(data_type).map_err(|e| {
                format!(
                    "runtime membership filter unsupported build key type: filter_id={} expr_order={} err={}",
                    spec.filter_id, spec.expr_order, e
                )
            })?;
            params.push(RuntimeMembershipFilterBuildParam::new(
                spec.filter_id,
                spec.probe_slot_id,
                ltype,
                join_mode,
            ));
        }

        if self.build_row_count == 0 || params.is_empty() {
            return Ok(params);
        }

        for chunk in &self.build_input_chunks {
            let mut key_arrays = Vec::with_capacity(self.build_keys.len());
            for expr in &self.build_keys {
                let array = self.arena.eval(*expr, chunk).map_err(|e| e.to_string())?;
                key_arrays.push(array);
            }
            for (idx, spec) in self.runtime_filter_specs.iter().enumerate() {
                let Some(array) = key_arrays.get(spec.expr_order) else {
                    continue;
                };
                if let Some(param) = params.get_mut(idx) {
                    param.add_array(Arc::clone(array));
                }
            }
        }

        Ok(params)
    }

    fn build_membership_filters_from_params(
        &self,
        total_rows: u64,
        params: &[RuntimeMembershipFilterBuildParam],
        options: RuntimeMembershipBuildOptions,
    ) -> Result<Vec<RuntimeMembershipFilter>, String> {
        if params.is_empty() {
            return Ok(Vec::new());
        }
        let mut filters = Vec::with_capacity(params.len());
        for param in params {
            let min_max = RuntimeMinMaxFilter::from_arrays(param.ltype(), param.arrays())?;
            if total_rows == 0 {
                filters.push(RuntimeMembershipFilter::Empty(RuntimeEmptyFilter::new(
                    param.filter_id(),
                    param.slot_id(),
                    param.ltype(),
                    false,
                    param.join_mode(),
                    0,
                    min_max,
                )));
                continue;
            }
            let can_try_bitset = options.enable_join_runtime_bitset_filter
                && total_rows <= options.global_runtime_filter_build_max_size
                && param.join_mode() == RUNTIME_FILTER_JOIN_MODE_BROADCAST;
            if can_try_bitset
                && let Some(bitset) = maybe_build_runtime_bitset_filter(
                    param.filter_id(),
                    param.slot_id(),
                    param.ltype(),
                    param.join_mode(),
                    total_rows,
                    param.arrays(),
                    min_max.clone(),
                )?
            {
                filters.push(RuntimeMembershipFilter::Bitset(bitset));
                continue;
            }
            let mut bloom = RuntimeBloomFilter::with_capacity(
                param.filter_id(),
                param.slot_id(),
                param.ltype(),
                param.join_mode(),
                total_rows,
                min_max,
            );
            for array in param.arrays() {
                bloom.insert_array(array)?;
            }
            filters.push(RuntimeMembershipFilter::Bloom(bloom));
        }
        Ok(filters)
    }

    fn membership_build_options(&self, state: &RuntimeState) -> RuntimeMembershipBuildOptions {
        let opts = state.query_options();
        let enabled = opts
            .and_then(|o| o.enable_join_runtime_bitset_filter)
            .unwrap_or(true)
            && self.distribution_mode == JoinDistributionMode::Broadcast;
        let global_limit = opts
            .and_then(|o| o.global_runtime_filter_build_max_size)
            .and_then(|v| (v > 0).then_some(v as u64))
            .unwrap_or(u64::MAX);
        RuntimeMembershipBuildOptions {
            enable_join_runtime_bitset_filter: enabled,
            global_runtime_filter_build_max_size: global_limit,
        }
    }

    fn build_local_filters(&self, filters: &[RuntimeInFilter]) -> Vec<RuntimeInFilter> {
        if filters.len() == self.runtime_filter_specs.len() {
            return filters.to_vec();
        }
        let mut by_id: HashMap<i32, RuntimeInFilter> = HashMap::new();
        for filter in filters {
            by_id.insert(filter.filter_id(), filter.clone());
        }
        for spec in &self.runtime_filter_specs {
            if by_id.contains_key(&spec.filter_id) {
                continue;
            }
            let Some(expr) = self.build_keys.get(spec.expr_order) else {
                continue;
            };
            let Some(data_type) = self.arena.data_type(*expr) else {
                continue;
            };
            match RuntimeInFilter::empty(spec.filter_id, spec.probe_slot_id, data_type) {
                Ok(filter) => {
                    by_id.insert(spec.filter_id, filter);
                }
                Err(e) => warn!(
                    "skip empty runtime filter for local: filter_id={} err={}",
                    spec.filter_id, e
                ),
            }
        }
        by_id.into_values().collect()
    }

    #[allow(dead_code)]
    fn build_empty_remote_filters(&self) -> Vec<RuntimeInFilter> {
        let mut filters = Vec::new();
        for spec in &self.runtime_filter_specs {
            let Some(expr) = self.build_keys.get(spec.expr_order) else {
                continue;
            };
            let Some(data_type) = self.arena.data_type(*expr) else {
                continue;
            };
            match RuntimeInFilter::empty(spec.filter_id, spec.probe_slot_id, data_type) {
                Ok(filter) => filters.push(filter),
                Err(e) => warn!(
                    "skip empty runtime filter for remote: filter_id={} err={}",
                    spec.filter_id, e
                ),
            }
        }
        filters
    }

    fn build_empty_remote_membership_filters(
        &self,
        specs: &[&JoinRuntimeFilterSpec],
    ) -> Result<Vec<RuntimeMembershipFilter>, String> {
        let join_mode: i8 = match self.distribution_mode {
            JoinDistributionMode::Broadcast => RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            JoinDistributionMode::Partitioned => RUNTIME_FILTER_JOIN_MODE_PARTITIONED,
        };
        let mut filters = Vec::new();
        for spec in specs {
            let Some(data_type) = self
                .build_keys
                .get(spec.expr_order)
                .and_then(|id| self.arena.data_type(*id))
            else {
                return Err(format!(
                    "runtime membership filter missing build key type: filter_id={} expr_order={}",
                    spec.filter_id, spec.expr_order
                ));
            };
            let ltype = RuntimeFilterType::from_arrow_data_type(data_type).map_err(|e| {
                format!(
                    "runtime membership filter unsupported build key type: filter_id={} expr_order={} err={}",
                    spec.filter_id, spec.expr_order, e
                )
            })?;
            let min_max = if self.build_row_count == 0 {
                RuntimeMinMaxFilter::empty_range(ltype)?
            } else {
                RuntimeMinMaxFilter::full_range(ltype)?
            };
            filters.push(RuntimeMembershipFilter::Empty(RuntimeEmptyFilter::new(
                spec.filter_id,
                spec.probe_slot_id,
                ltype,
                false,
                join_mode,
                0,
                min_max,
            )));
        }
        Ok(filters)
    }

    fn runtime_filter_hash_seed(&self) -> Result<u64, String> {
        if let Some(table) = self.build_table.as_ref() {
            return Ok(table.hash_seed());
        }
        let mut key_types = Vec::with_capacity(self.build_keys.len());
        for expr in &self.build_keys {
            let data_type = self
                .arena
                .data_type(*expr)
                .ok_or_else(|| "runtime filter build key type missing".to_string())?;
            key_types.push(data_type.clone());
        }
        let temp_table = JoinHashMap::new_chained(key_types, self.eq_null_safe.clone())
            .map_err(|e| e.to_string())?;
        Ok(temp_table.hash_seed())
    }

    fn init_profile_if_needed(&mut self) {
        if self.profile_initialized {
            return;
        }
        self.profile_initialized = true;
        if let Some(profile) = self.profiles.as_ref() {
            profile
                .common
                .add_info_string("JoinType", join_type_str(self.join_type));
            let mode = match self.distribution_mode {
                JoinDistributionMode::Broadcast => "BROADCAST",
                JoinDistributionMode::Partitioned => "PARTITIONED",
            };
            profile.common.add_info_string("DistributionMode", mode);
            profile.common.add_timer("RuntimeFilterBuildTime");
            profile.common.add_timer("BuildHashTableTime");
            profile.common.add_unit_counter("RuntimeFilterNum");
            profile.common.add_unit_counter("RuntimeInFilterNum");
            profile.common.add_unit_counter("RuntimeInFilterDropped");
            profile
                .common
                .add_unit_counter("RuntimeMembershipFilterDropped");
            profile
                .common
                .add_bytes_counter("PartialRuntimeMembershipFilterBytes");
        }
    }
}

fn parse_join_node_id_from_dep_key(dep_key: &str) -> i32 {
    let key = dep_key
        .strip_prefix("broadcast_join_build:")
        .or_else(|| dep_key.strip_prefix("join_build:"))
        .unwrap_or(dep_key);
    let node_id_str = key.split(':').next().unwrap_or("");
    node_id_str.parse::<i32>().unwrap_or(-1)
}

fn retained_key_arrays_bytes(
    key_arrays: &[ArrayRef],
    chunk: &Chunk,
    chunk_columns_are_retained: bool,
) -> usize {
    key_arrays
        .iter()
        .filter(|key_array| {
            if !chunk_columns_are_retained {
                return true;
            }
            !chunk
                .columns()
                .iter()
                .any(|chunk_array| Arc::ptr_eq(key_array, chunk_array))
        })
        .map(|array| array.get_array_memory_size())
        .sum()
}

fn join_type_str(join_type: JoinType) -> &'static str {
    match join_type {
        JoinType::Inner => "INNER",
        JoinType::LeftOuter => "LEFT_OUTER",
        JoinType::RightOuter => "RIGHT_OUTER",
        JoinType::FullOuter => "FULL_OUTER",
        JoinType::LeftSemi => "LEFT_SEMI",
        JoinType::RightSemi => "RIGHT_SEMI",
        JoinType::LeftAnti => "LEFT_ANTI",
        JoinType::RightAnti => "RIGHT_ANTI",
        JoinType::NullAwareLeftAnti => "NULL_AWARE_LEFT_ANTI",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::{ExprNode, LiteralValue};
    use crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMapMethodKind;
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::runtime::profile::{OperatorProfiles, RuntimeProfile};

    #[derive(Default)]
    struct TestBuildState {
        artifact: Mutex<Option<Arc<JoinBuildArtifact>>>,
    }

    impl JoinBuildSinkState for TestBuildState {
        fn partition_for_driver(&self, _driver_id: i32) -> usize {
            0
        }

        fn dep_name(&self, _partition: usize) -> &str {
            "join_build:1"
        }

        fn set_build(
            &self,
            _partition: usize,
            artifact: Arc<JoinBuildArtifact>,
        ) -> Result<(), String> {
            *self.artifact.lock().expect("artifact lock") = Some(artifact);
            Ok(())
        }
    }

    fn int32_chunk(values: Vec<i32>) -> Chunk {
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    fn nullable_int32_chunk(values: Vec<Option<i32>>) -> Chunk {
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    fn direct_int_build_operator(state: Arc<TestBuildState>) -> HashJoinBuildSinkOperator {
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        HashJoinBuildSinkOperator {
            name: "HASH_JOIN (id=1)".to_string(),
            node_id: 1,
            driver_id: 0,
            arena: Arc::new(arena),
            join_type: JoinType::Inner,
            has_residual_predicate: false,
            probe_is_left: true,
            has_equi_keys: true,
            build_keys: vec![build_key],
            eq_null_safe: vec![false],
            runtime_filter_specs: Vec::new(),
            distribution_mode: JoinDistributionMode::Broadcast,
            state,
            partition: 0,
            runtime_filter_hub: Arc::new(RuntimeFilterHub::new(DependencyManager::new())),
            runtime_in_filter_merger: None,
            build_store_builder: BuildStoreBuilder::new(),
            build_input_chunks: Vec::new(),
            build_key_batches: Vec::new(),
            build_key_batches_retained_bytes: 0,
            build_key_batches_accounting: None,
            build_key_batches_mem_tracker: None,
            build_table: None,
            runtime_filters: None,
            runtime_in_filters: None,
            finished: false,
            build_row_count: 0,
            build_has_null_key: false,
            build_null_key_rows: None,
            logged_first_input: false,
            profile_initialized: false,
            profiles: None,
            input_rows: 0,
            input_chunks: 0,
            build_input_chunks_mem_tracker: None,
            build_table_mem_tracker: None,
        }
    }

    fn direct_set_build_operator(state: Arc<TestBuildState>) -> HashJoinBuildSinkOperator {
        let mut operator = direct_int_build_operator(state);
        operator.join_type = JoinType::LeftSemi;
        operator.has_residual_predicate = false;
        operator
    }

    fn null_aware_build_operator(
        state: Arc<TestBuildState>,
        has_residual_predicate: bool,
    ) -> Box<dyn Operator> {
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let sink_state: Arc<dyn JoinBuildSinkState> = state;
        let factory = HashJoinBuildSinkFactory::new(
            Arc::new(arena),
            JoinType::NullAwareLeftAnti,
            has_residual_predicate,
            true,
            true,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Broadcast,
            sink_state,
            Arc::new(RuntimeFilterHub::new(DependencyManager::new())),
            None,
        );
        factory.create(1, 0)
    }

    fn no_key_inner_build_operator(state: Arc<TestBuildState>) -> HashJoinBuildSinkOperator {
        let mut operator = direct_int_build_operator(state);
        operator.join_type = JoinType::Inner;
        operator.has_residual_predicate = false;
        operator.probe_is_left = true;
        operator.has_equi_keys = false;
        operator.build_keys.clear();
        operator.eq_null_safe.clear();
        operator
    }

    #[test]
    fn defers_hash_map_construction_until_build_finish() {
        let state = Arc::new(TestBuildState::default());
        let mut operator = direct_int_build_operator(state.clone());

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![1, 2, 3]))
            .expect("push chunk");

        assert!(operator.build_table.is_none());
        assert_eq!(operator.build_key_batches.len(), 1);

        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        let artifact = state
            .artifact
            .lock()
            .expect("artifact lock")
            .clone()
            .expect("artifact");
        let build_table = artifact.build_table.as_ref().expect("build table");
        assert!(matches!(
            build_table.method_kind(),
            JoinHashMapMethodKind::DirectInt { .. }
        ));
        assert!(operator.build_key_batches.is_empty());
    }

    #[test]
    fn records_join_hash_map_method_in_profile() {
        let state = Arc::new(TestBuildState::default());
        let mut operator = direct_int_build_operator(state);
        let profiles = OperatorProfiles::new(RuntimeProfile::new("HASH_JOIN"));
        operator.set_profiles(profiles.clone());

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![1, 2, 3]))
            .expect("push chunk");
        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        assert_eq!(
            profiles.common.get_info_string("JoinHashMapMethod"),
            Some("DirectIntNotNull".to_string())
        );
    }

    #[test]
    fn records_direct_set_method_for_left_semi_without_residual() {
        let state = Arc::new(TestBuildState::default());
        let mut operator = direct_set_build_operator(Arc::clone(&state));
        let profiles = OperatorProfiles::new(RuntimeProfile::new("HASH_JOIN"));
        operator.set_profiles(profiles.clone());

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![1, 1_000_000]))
            .expect("push chunk");
        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        assert_eq!(
            profiles.common.get_info_string("JoinHashMapMethod"),
            Some("DirectIntSetNotNull".to_string())
        );
        let artifact = state
            .artifact
            .lock()
            .expect("artifact lock")
            .clone()
            .expect("artifact");
        assert_eq!(
            artifact.provided,
            required_build_components(JoinType::LeftSemi, false, true, true)
        );
        assert!(artifact.build_store.is_none());
        assert_eq!(artifact.build_row_count, 2);
        assert_eq!(operator.build_store_builder.row_count(), 0);
        assert!(operator.build_input_chunks.is_empty());
    }

    #[test]
    fn records_row_match_method_for_left_semi_with_residual() {
        let state = Arc::new(TestBuildState::default());
        let mut operator = direct_set_build_operator(Arc::clone(&state));
        operator.has_residual_predicate = true;
        let profiles = OperatorProfiles::new(RuntimeProfile::new("HASH_JOIN"));
        operator.set_profiles(profiles.clone());

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![1, 1_000_000]))
            .expect("push chunk");
        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        assert_eq!(
            profiles.common.get_info_string("JoinHashMapMethod"),
            Some("Chained".to_string())
        );
        let artifact = state
            .artifact
            .lock()
            .expect("artifact lock")
            .clone()
            .expect("artifact");
        assert_eq!(
            artifact.provided,
            required_build_components(JoinType::LeftSemi, true, true, true)
        );
        assert!(artifact.build_store.is_some());
        assert_eq!(artifact.build_store.as_ref().expect("build store").len(), 2);
        assert_eq!(artifact.build_row_count, 2);
    }

    #[test]
    fn records_row_match_method_for_right_probe_left_semi_without_residual() {
        let state = Arc::new(TestBuildState::default());
        let mut operator = direct_set_build_operator(Arc::clone(&state));
        operator.probe_is_left = false;
        let profiles = OperatorProfiles::new(RuntimeProfile::new("HASH_JOIN"));
        operator.set_profiles(profiles.clone());

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![1, 1_000_000]))
            .expect("push chunk");
        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        assert_eq!(
            profiles.common.get_info_string("JoinHashMapMethod"),
            Some("Chained".to_string())
        );
        let artifact = state
            .artifact
            .lock()
            .expect("artifact lock")
            .clone()
            .expect("artifact");
        assert_eq!(
            artifact.provided,
            required_build_components(JoinType::LeftSemi, false, false, true)
        );
        assert!(artifact.build_store.is_some());
        assert_eq!(artifact.build_store.as_ref().expect("build store").len(), 2);
        assert_eq!(artifact.build_row_count, 2);
    }

    #[test]
    fn publishes_no_key_inner_artifact_without_hash_map() {
        let state = Arc::new(TestBuildState::default());
        let mut operator = no_key_inner_build_operator(Arc::clone(&state));
        let profiles = OperatorProfiles::new(RuntimeProfile::new("HASH_JOIN"));
        operator.set_profiles(profiles.clone());

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![1, 2, 3]))
            .expect("push chunk");
        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        assert_eq!(profiles.common.get_info_string("JoinHashMapMethod"), None);
        let artifact = state
            .artifact
            .lock()
            .expect("artifact lock")
            .clone()
            .expect("artifact");
        assert_eq!(
            artifact.provided,
            required_build_components(JoinType::Inner, false, true, false)
        );
        assert!(artifact.build_table.is_none());
        assert_eq!(artifact.build_row_count, 3);
        assert_eq!(artifact.build_store.as_ref().expect("build store").len(), 3);
    }

    #[test]
    fn no_residual_null_aware_left_anti_tracks_has_null_key_without_null_key_rows() {
        let state = Arc::new(TestBuildState::default());
        let mut operator = null_aware_build_operator(Arc::clone(&state), false);

        let processor = operator.as_processor_mut().expect("processor");
        processor
            .push_chunk(
                &RuntimeState::default(),
                nullable_int32_chunk(vec![Some(1), None, Some(2)]),
            )
            .expect("push chunk");
        processor
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        let artifact = state
            .artifact
            .lock()
            .expect("artifact lock")
            .clone()
            .expect("artifact");
        assert_eq!(
            artifact.provided,
            required_build_components(JoinType::NullAwareLeftAnti, false, true, true)
        );
        assert!(artifact.build_has_null_key);
        assert!(artifact.build_null_key_rows.is_none());
    }

    #[test]
    fn residual_null_aware_left_anti_tracks_null_key_rows() {
        let state = Arc::new(TestBuildState::default());
        let mut operator = null_aware_build_operator(Arc::clone(&state), true);

        let processor = operator.as_processor_mut().expect("processor");
        processor
            .push_chunk(
                &RuntimeState::default(),
                nullable_int32_chunk(vec![Some(1), None, Some(2)]),
            )
            .expect("push chunk");
        processor
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        let artifact = state
            .artifact
            .lock()
            .expect("artifact lock")
            .clone()
            .expect("artifact");
        assert_eq!(
            artifact.provided,
            required_build_components(JoinType::NullAwareLeftAnti, true, true, true)
        );
        let null_key_rows = artifact
            .build_null_key_rows
            .as_ref()
            .expect("null-key rows");
        assert_eq!(null_key_rows.as_slice(), &[1]);
    }

    #[test]
    fn publishes_empty_keyed_membership_build_without_artifacts() {
        let state = Arc::new(TestBuildState::default());
        let mut operator = direct_set_build_operator(Arc::clone(&state));

        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        let artifact = state
            .artifact
            .lock()
            .expect("artifact lock")
            .clone()
            .expect("artifact");
        assert_eq!(
            artifact.provided,
            required_build_components(JoinType::LeftSemi, false, true, true)
        );
        assert_eq!(artifact.build_row_count, 0);
        assert!(artifact.build_store.is_none());
        assert!(artifact.build_table.is_none());
    }

    #[test]
    fn computed_build_key_batches_are_accounted_and_released() {
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let zero = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(0)), DataType::Int32);
        let build_key = arena.push_typed(ExprNode::Add(slot, zero), DataType::Int32);
        let arena = Arc::new(arena);
        let state = Arc::new(TestBuildState::default());
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        let root = MemTracker::new_root("hash-build-test");
        let mut operator = HashJoinBuildSinkOperator {
            name: "HASH_JOIN (id=1)".to_string(),
            node_id: 1,
            driver_id: 0,
            arena,
            join_type: JoinType::Inner,
            has_residual_predicate: false,
            probe_is_left: true,
            has_equi_keys: true,
            build_keys: vec![build_key],
            eq_null_safe: vec![false],
            runtime_filter_specs: Vec::new(),
            distribution_mode: JoinDistributionMode::Broadcast,
            state,
            partition: 0,
            runtime_filter_hub,
            runtime_in_filter_merger: None,
            build_store_builder: BuildStoreBuilder::new(),
            build_input_chunks: Vec::new(),
            build_key_batches: Vec::new(),
            build_key_batches_retained_bytes: 0,
            build_key_batches_accounting: None,
            build_key_batches_mem_tracker: None,
            build_table: None,
            runtime_filters: None,
            runtime_in_filters: None,
            finished: false,
            build_row_count: 0,
            build_has_null_key: false,
            build_null_key_rows: None,
            logged_first_input: false,
            profile_initialized: false,
            profiles: None,
            input_rows: 0,
            input_chunks: 0,
            build_input_chunks_mem_tracker: None,
            build_table_mem_tracker: None,
        };

        operator.set_mem_tracker(Arc::clone(&root));
        let key_batches_tracker = root
            .children()
            .into_iter()
            .find(|child| child.label() == "BuildKeyBatches")
            .expect("BuildKeyBatches tracker");

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![1, 2, 3]))
            .expect("push chunk");

        assert!(key_batches_tracker.current() > 0);

        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        assert_eq!(key_batches_tracker.current(), 0);
        let hash_table_tracker = root
            .children()
            .into_iter()
            .find(|child| child.label() == "BuildHashTable")
            .expect("BuildHashTable tracker");
        assert!(hash_table_tracker.current() > 0);
    }

    #[test]
    fn presence_only_slot_key_batches_are_accounted_without_input_chunk_storage() {
        let state = Arc::new(TestBuildState::default());
        let root = MemTracker::new_root("presence-only-build-test");
        let mut operator = direct_set_build_operator(state);

        operator.set_mem_tracker(Arc::clone(&root));
        let children = root.children();
        let key_batches_tracker = children
            .iter()
            .find(|child| child.label() == "BuildKeyBatches")
            .expect("BuildKeyBatches tracker")
            .clone();
        let input_chunks_tracker = children
            .iter()
            .find(|child| child.label() == "BuildInputChunks")
            .expect("BuildInputChunks tracker")
            .clone();

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![1, 2, 3]))
            .expect("push chunk");

        assert_eq!(operator.build_store_builder.row_count(), 0);
        assert!(operator.build_input_chunks.is_empty());
        assert_eq!(input_chunks_tracker.current(), 0);
        assert!(key_batches_tracker.current() > 0);

        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        assert_eq!(key_batches_tracker.current(), 0);
    }

    #[test]
    fn computed_build_key_batches_peak_is_not_inflated_across_pushes() {
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let zero = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(0)), DataType::Int32);
        let build_key = arena.push_typed(ExprNode::Add(slot, zero), DataType::Int32);
        let arena = Arc::new(arena);
        let state = Arc::new(TestBuildState::default());
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        let root = MemTracker::new_root("hash-build-test");
        let mut operator = HashJoinBuildSinkOperator {
            name: "HASH_JOIN (id=1)".to_string(),
            node_id: 1,
            driver_id: 0,
            arena,
            join_type: JoinType::Inner,
            has_residual_predicate: false,
            probe_is_left: true,
            has_equi_keys: true,
            build_keys: vec![build_key],
            eq_null_safe: vec![false],
            runtime_filter_specs: Vec::new(),
            distribution_mode: JoinDistributionMode::Broadcast,
            state,
            partition: 0,
            runtime_filter_hub,
            runtime_in_filter_merger: None,
            build_store_builder: BuildStoreBuilder::new(),
            build_input_chunks: Vec::new(),
            build_key_batches: Vec::new(),
            build_key_batches_retained_bytes: 0,
            build_key_batches_accounting: None,
            build_key_batches_mem_tracker: None,
            build_table: None,
            runtime_filters: None,
            runtime_in_filters: None,
            finished: false,
            build_row_count: 0,
            build_has_null_key: false,
            build_null_key_rows: None,
            logged_first_input: false,
            profile_initialized: false,
            profiles: None,
            input_rows: 0,
            input_chunks: 0,
            build_input_chunks_mem_tracker: None,
            build_table_mem_tracker: None,
        };

        operator.set_mem_tracker(Arc::clone(&root));
        let key_batches_tracker = root
            .children()
            .into_iter()
            .find(|child| child.label() == "BuildKeyBatches")
            .expect("BuildKeyBatches tracker");

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![1, 2, 3]))
            .expect("first push");
        let after_first = key_batches_tracker.current();
        assert!(after_first > 0);

        operator
            .push_chunk(&RuntimeState::default(), int32_chunk(vec![4, 5, 6]))
            .expect("second push");

        assert_eq!(
            key_batches_tracker.current(),
            operator.build_key_batches_retained_bytes as i64
        );
        assert_eq!(key_batches_tracker.peak(), key_batches_tracker.current());

        operator
            .set_finishing(&RuntimeState::default())
            .expect("finish");

        assert_eq!(key_batches_tracker.current(), 0);
    }
}
