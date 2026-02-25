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

use arrow::array::Array;

use super::build_artifact::JoinBuildArtifact;
use super::build_state::JoinBuildSinkState;
use super::join_hash_table::JoinHashTable;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::{JoinDistributionMode, JoinRuntimeFilterSpec, JoinType};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::runtime_filter::{
    LocalRuntimeFilterSet, LocalRuntimeInFilterSet, MAX_RUNTIME_IN_FILTER_CONDITIONS,
    PartialRuntimeInFilterMerger, RuntimeBloomFilter, RuntimeEmptyFilter, RuntimeInFilter,
    RuntimeMembershipFilter, RuntimeMembershipFilterBuildParam, RuntimeMinMaxFilter,
    data_type_to_tprimitive, encode_starrocks_bloom_filter, encode_starrocks_empty_filter,
};
use crate::metrics;
use crate::novarocks_logging::{debug, warn};
use crate::runtime::mem_tracker::MemTracker;
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
        debug!(
            "HashJoinBuildSink create: node_id={} driver_id={} partition={} join_type={} dist={} build_keys={} null_safe_keys={} runtime_filters={}",
            self.node_id,
            driver_id,
            partition,
            join_type_str(self.join_type),
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
            build_keys: self.build_keys.clone(),
            eq_null_safe: self.eq_null_safe.clone(),
            runtime_filter_specs: self.runtime_filters.clone(),
            distribution_mode: self.distribution_mode,
            state: Arc::clone(&self.state),
            partition,
            runtime_filter_hub: Arc::clone(&self.runtime_filter_hub),
            runtime_in_filter_merger: self.runtime_in_filter_merger.as_ref().map(Arc::clone),
            build_batches: Vec::new(),
            build_table: None,
            runtime_filters: None,
            runtime_in_filters: None,
            finished: false,
            build_row_count: 0,
            build_has_null_key: false,
            build_null_key_rows: if self.join_type == JoinType::NullAwareLeftAnti {
                Some(Vec::new())
            } else {
                None
            },
            logged_first_input: false,
            profile_initialized: false,
            profiles: None,
            input_rows: 0,
            input_chunks: 0,
            build_batches_mem_tracker: None,
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
    build_keys: Vec<ExprId>,
    eq_null_safe: Vec<bool>,
    runtime_filter_specs: Vec<JoinRuntimeFilterSpec>,
    distribution_mode: JoinDistributionMode,
    state: Arc<dyn JoinBuildSinkState>,
    partition: usize,
    runtime_filter_hub: Arc<RuntimeFilterHub>,
    runtime_in_filter_merger: Option<Arc<PartialRuntimeInFilterMerger>>,
    build_batches: Vec<Chunk>,
    build_table: Option<JoinHashTable>,
    runtime_filters: Option<LocalRuntimeFilterSet>,
    runtime_in_filters: Option<LocalRuntimeInFilterSet>,
    finished: bool,
    build_row_count: usize,
    build_has_null_key: bool,
    build_null_key_rows: Option<Vec<Vec<u32>>>,
    logged_first_input: bool,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    input_rows: u64,
    input_chunks: u64,
    build_batches_mem_tracker: Option<Arc<MemTracker>>,
    build_table_mem_tracker: Option<Arc<MemTracker>>,
}

impl Operator for HashJoinBuildSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let batches = MemTracker::new_child("BuildBatches", &tracker);
        self.build_batches_mem_tracker = Some(Arc::clone(&batches));
        for chunk in self.build_batches.iter_mut() {
            chunk.transfer_to(&batches);
        }

        let table = MemTracker::new_child("BuildHashTable", &tracker);
        self.build_table_mem_tracker = Some(Arc::clone(&table));
        if let Some(build_table) = self.build_table.as_mut() {
            build_table.set_mem_tracker(table);
        }
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
        self.build_row_count = self.build_row_count.saturating_add(chunk.len());

        let batch_index = u32::try_from(self.build_batches.len())
            .map_err(|_| "join build batch index overflow".to_string())?;
        if let Some(tracker) = self.build_batches_mem_tracker.as_ref() {
            chunk.transfer_to(tracker);
        }
        self.build_batches.push(chunk.clone());

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
        if let Some(null_rows_by_batch) = self.build_null_key_rows.as_mut() {
            let mut null_rows = Vec::new();
            for row in 0..chunk.len() {
                let has_forbidden_null =
                    key_arrays.iter().enumerate().any(|(key_idx, key_array)| {
                        !self.eq_null_safe.get(key_idx).copied().unwrap_or(false)
                            && !key_array.is_valid(row)
                    });
                if has_forbidden_null {
                    null_rows.push(row as u32);
                }
            }
            null_rows_by_batch.push(null_rows);
        }

        if self.build_table.is_none() {
            let key_types = key_arrays
                .iter()
                .map(|array| array.data_type().clone())
                .collect::<Vec<_>>();
            let mut table = JoinHashTable::new(key_types, self.eq_null_safe.clone())
                .map_err(|e| e.to_string())?;
            if let Some(tracker) = self.build_table_mem_tracker.as_ref() {
                table.set_mem_tracker(Arc::clone(tracker));
            }
            self.build_table = Some(table);
        }

        let table = self.build_table.as_mut().expect("join build table");
        table
            .add_build_batch(&key_arrays, chunk.len(), batch_index)
            .map_err(|e| e.to_string())?;
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
                self.runtime_filters = Some(LocalRuntimeFilterSet::new(
                    &self.runtime_filter_specs,
                    table.hash_seed(),
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

        if let Some(table) = self.build_table.as_mut() {
            table.finalize_groups().map_err(|e| e.to_string())?;
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

        let batch_count = self.build_batches.len();
        let table_present = self.build_table.is_some();
        let mut batches = std::mem::take(&mut self.build_batches);
        let mut table = self.build_table.take();

        if let Some(root) = state.mem_tracker() {
            let label = format!("JoinBuildArtifact: {}", self.state.dep_name(self.partition));
            let artifact = MemTracker::new_child(label, &root);
            let artifact_batches = MemTracker::new_child("BuildBatches", &artifact);
            for chunk in batches.iter_mut() {
                chunk.transfer_to(&artifact_batches);
            }
            let artifact_table = MemTracker::new_child("BuildHashTable", &artifact);
            if let Some(table) = table.as_mut() {
                table.set_mem_tracker(artifact_table);
            }
        }
        let runtime_filters = self.runtime_filters.take().map(Arc::new);
        let build_null_key_rows = self.build_null_key_rows.take().map(Arc::new);
        let artifact = Arc::new(JoinBuildArtifact::new(
            batches,
            table,
            self.build_row_count,
            self.build_has_null_key,
            build_null_key_rows,
            runtime_filters,
        ));
        self.state
            .set_build(self.partition, artifact)
            .map_err(|e| e.to_string())?;
        debug!(
            "HashJoinBuildSink finished: dep_key={} driver_id={} partition={} node_id={} join_type={} input_rows={} input_chunks={} build_row_count={} build_has_null_key={} build_batches={} build_table={} build_keys={}",
            self.state.dep_name(self.partition),
            self.driver_id,
            self.partition,
            self.node_id,
            join_type_str(self.join_type),
            self.input_rows,
            self.input_chunks,
            self.build_row_count,
            self.build_has_null_key,
            batch_count,
            table_present,
            self.build_keys.len()
        );
        Ok(())
    }
}

impl HashJoinBuildSinkOperator {
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
        if self.build_row_count > 0 && self.build_row_count <= MAX_RUNTIME_IN_FILTER_CONDITIONS {
            if let Some(filters) = self.runtime_in_filters.take() {
                in_filters = filters.into_filters();
            }
        }

        let membership_params = self.build_membership_filter_params()?;

        if let Some(merger) = self.runtime_in_filter_merger.as_ref() {
            let merged_in = merger.add_partial(self.partition, self.build_row_count, in_filters)?;
            let merged_membership =
                merger.add_partial_membership(self.partition, membership_params)?;
            if let (Some(in_filters), Some(membership_filters)) = (merged_in, merged_membership) {
                self.log_in_filters("publish", &in_filters);
                self.log_membership_filters("publish", &membership_filters);
                if let Some(profile) = self.profiles.as_ref() {
                    profile.common.counter_set(
                        "RuntimeFilterNum",
                        metrics::TUnit::UNIT,
                        membership_filters.len() as i64,
                    );
                    profile.common.counter_set(
                        "RuntimeInFilterNum",
                        metrics::TUnit::UNIT,
                        in_filters.len() as i64,
                    );
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
            )?;
            self.log_in_filters("publish", &in_filters);
            self.log_membership_filters("publish", &membership_filters);
            if let Some(profile) = self.profiles.as_ref() {
                profile.common.counter_set(
                    "RuntimeFilterNum",
                    metrics::TUnit::UNIT,
                    membership_filters.len() as i64,
                );
                profile.common.counter_set(
                    "RuntimeInFilterNum",
                    metrics::TUnit::UNIT,
                    in_filters.len() as i64,
                );
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
                RuntimeMembershipFilter::Bitset(_) => {
                    warn!(
                        "skip remote runtime filter encode: filter_id={} err=bitset not supported",
                        filter.filter_id()
                    );
                    continue;
                }
            };
            encoded_bytes = encoded_bytes.saturating_add(data.len() as i64);

            let mut seen_hosts = HashSet::new();
            if use_merge_nodes {
                for addr in &spec.merge_nodes {
                    if addr.hostname.is_empty() {
                        continue;
                    }
                    if !seen_hosts.insert(addr.hostname.clone()) {
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
                            finst_id: finst_id.clone(),
                            build_be_number: Some(build_be_number),
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
                            finst_id: finst_id.clone(),
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
        if encoded_bytes > 0 {
            if let Some(profile) = self.profiles.as_ref() {
                profile.common.counter_add(
                    "PartialRuntimeMembershipFilterBytes",
                    metrics::TUnit::BYTES,
                    encoded_bytes,
                );
            }
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

    fn build_membership_filter_params(
        &self,
    ) -> Result<Vec<RuntimeMembershipFilterBuildParam>, String> {
        if self.runtime_filter_specs.is_empty() {
            return Ok(Vec::new());
        }
        let join_mode: i8 = match self.distribution_mode {
            JoinDistributionMode::Broadcast => 1,
            JoinDistributionMode::Partitioned => 2,
        };
        let mut params = Vec::with_capacity(self.runtime_filter_specs.len());
        for spec in &self.runtime_filter_specs {
            let expr = self.build_keys.get(spec.expr_order).copied();
            if expr.is_none() {
                warn!(
                    "runtime membership filter missing build key: filter_id={}",
                    spec.filter_id
                );
            }
            let ltype = match expr.and_then(|id| self.arena.data_type(id)) {
                Some(data_type) => match data_type_to_tprimitive(data_type) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(
                            "runtime membership filter unsupported type: filter_id={} err={}",
                            spec.filter_id, e
                        );
                        crate::types::TPrimitiveType::INT
                    }
                },
                None => {
                    warn!(
                        "runtime membership filter missing build key type: filter_id={}",
                        spec.filter_id
                    );
                    crate::types::TPrimitiveType::INT
                }
            };
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

        for chunk in &self.build_batches {
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
            JoinDistributionMode::Broadcast => 1,
            JoinDistributionMode::Partitioned => 2,
        };
        let mut filters = Vec::new();
        for spec in specs {
            let expr = self.build_keys.get(spec.expr_order).copied();
            let ltype = match expr.and_then(|id| self.arena.data_type(id)) {
                Some(data_type) => match data_type_to_tprimitive(data_type) {
                    Ok(t) => t,
                    Err(_) => crate::types::TPrimitiveType::INT,
                },
                None => crate::types::TPrimitiveType::INT,
            };
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
        let temp_table =
            JoinHashTable::new(key_types, self.eq_null_safe.clone()).map_err(|e| e.to_string())?;
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
            profile
                .common
                .add_counter("RuntimeFilterNum", metrics::TUnit::UNIT);
            profile
                .common
                .add_counter("RuntimeInFilterNum", metrics::TUnit::UNIT);
            profile
                .common
                .add_counter("PartialRuntimeMembershipFilterBytes", metrics::TUnit::BYTES);
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
