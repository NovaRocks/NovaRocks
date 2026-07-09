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

//! Fragment scheduler: decides which backend each fragment instance lands on.
//!
//! This is a pure decision layer. The coordinator (PR-4) reads the produced
//! `SchedulingPlan` to build `TExecPlanFragmentParams` for each instance and
//! submits them through the dispatcher.
//!
//! # Instance-count policy (StarRocks-style "instance follows upstream")
//!
//! - A **scan fragment** (contains FILE_SCAN_NODE, HDFS_SCAN_NODE, or
//!   LAKE_SCAN_NODE) gets `N` instances, one per backend.
//! - A **non-scan fragment** gets `max(upstream_N)` over incoming
//!   `HashPartitioned` / `BucketShuffleHashPartitioned` edges, or 1 if no such
//!   edge exists.
//! - The **result root fragment** is forced to 1 instance (it holds the
//!   ResultSink; the FE fetches exactly one finst). Write-only DAGs may have
//!   multiple terminal write fragments; in that case one writer instance is
//!   selected as the execution anchor without changing writer parallelism.
//!
//! # Backend assignment
//!
//! - Multi-instance fragments: instance `i` lands on live backend slot `i`.
//!   The stored `backend_idx` is the backend id from the live snapshot, which
//!   may be sparse.
//! - Single-instance fragments (including the root): `backend_idx =
//!   live[(query_id.lo as usize) % N].0`.
//!
//! # Scan-split policy (Scheme C)
//!
//! D2 scan split policy: scheme C — partition the codegen-built per_node_scan_ranges
//! across instances. The scheduler never re-invokes to_thrift_scan, so the
//! min_max/cloud_props/change_op context (only known at codegen) is preserved.
//!
//! Round-robin: `range[i]` goes to `instance[i % count]`.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use crate::common::types::UniqueId;
use crate::runtime::endpoint::{
    FragmentDestination, RuntimeEndpoint, RuntimeFilterProberDestination,
};
use crate::runtime::scan_range::ScanRangeParams;
use crate::sql::codegen::{
    FragmentEdge, FragmentEdgeKind, FragmentId, FragmentSchedulingMetadata, FragmentStreamKind,
    RuntimeFilterPlanResult,
};
use crate::sql::planner::PartitionKind;

type LiveBackend = (usize, SocketAddr);

#[derive(Clone, Copy, Debug)]
struct IncomingEdge {
    source_fragment_id: FragmentId,
    is_native_hash_partitioned: bool,
    stream_kind: FragmentStreamKind,
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Placement information for one fragment instance.
#[derive(Clone, Debug)]
pub(crate) struct FragmentInstancePlacement {
    /// The fragment this instance belongs to. The coordinator iterates
    /// `SchedulingPlan::by_fragment` by key, so this is currently only used for
    /// diagnostics, but it keeps each placement self-describing.
    #[allow(dead_code)]
    pub(crate) fragment_id: FragmentId,
    pub(crate) instance_index: usize,
    pub(crate) finst_id: UniqueId,
    /// Backend id from the scheduler's live backend snapshot.
    pub(crate) backend_idx: usize,
    /// Native runtime endpoint for this fragment instance.
    pub(crate) endpoint: RuntimeEndpoint,
    /// Scan ranges for this instance, keyed by plan node id.
    pub(crate) scan_ranges: BTreeMap<i32, Vec<ScanRangeParams>>,
    /// Destinations this instance should push its output to.
    pub(crate) destinations: Vec<FragmentDestination>,
    /// Runtime filter prober destinations, keyed by filter_id.
    pub(crate) runtime_filter_prober_params: BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
    /// Number of upstream senders per exchange node id.
    pub(crate) per_exch_num_senders: BTreeMap<i32, i32>,
}

/// The result of scheduling a multi-fragment plan.
#[derive(Debug)]
pub(crate) struct SchedulingPlan {
    /// Fragment chosen as the execution anchor for fetch/write coordination.
    pub(crate) root_fragment_id: FragmentId,
    /// All instance placements, indexed by fragment id.
    pub(crate) by_fragment: BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
    /// The finst id of the root fragment's (single) instance.
    pub(crate) root_finst_id: UniqueId,
    /// Which backend index the root instance is assigned to.
    pub(crate) root_backend_idx: usize,
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// Decides which backend each fragment instance lands on.
pub(crate) struct FragmentScheduler {
    backends: Vec<SocketAddr>,
    live_backends: Vec<LiveBackend>,
}

impl FragmentScheduler {
    /// Create a new scheduler with the given backends.
    pub(crate) fn new(backends: Vec<SocketAddr>) -> Self {
        let live_backends = backends.iter().copied().enumerate().collect();
        Self {
            backends,
            live_backends,
        }
    }

    /// Create a scheduler from explicit backend ids and endpoints.
    pub(crate) fn new_with_backend_ids(backends: Vec<LiveBackend>) -> Self {
        let endpoints = backends.iter().map(|(_, endpoint)| *endpoint).collect();
        Self {
            backends: endpoints,
            live_backends: backends,
        }
    }

    /// Return the configured backends.
    pub(crate) fn backends(&self) -> &[SocketAddr] {
        &self.backends
    }

    /// Return the live backend-id/endpoint snapshot used by this scheduler.
    pub(crate) fn live_backend_entries(&self) -> &[LiveBackend] {
        &self.live_backends
    }

    /// Assign each fragment to one or more instances across the known backends.
    ///
    /// The returned `SchedulingPlan` contains backend index and finst id for
    /// each instance. `destinations`, `runtime_filter_prober_params`, and
    /// `per_exch_num_senders` are empty at this point; call the corresponding
    /// `fill_*` methods to populate them.
    pub(crate) fn assign(
        &self,
        fragments: &[FragmentSchedulingMetadata],
        edges: &[FragmentEdge],
        query_id: UniqueId,
    ) -> Result<SchedulingPlan, String> {
        let live = self.full_live_snapshot();
        self.assign_with_live(fragments, edges, query_id, &live)
    }

    pub(crate) fn assign_with_live(
        &self,
        fragments: &[FragmentSchedulingMetadata],
        edges: &[FragmentEdge],
        query_id: UniqueId,
        live: &[LiveBackend],
    ) -> Result<SchedulingPlan, String> {
        let n = live.len();
        if n == 0 {
            return Err("no live backend available".into());
        }

        // Step 1: topological sort (leaves first, root last).
        let topo = topological_sort_bottom_up(fragments, edges)?;

        // Step 2: identify the execution root fragment.
        let root_selection = select_execution_root_fragment(fragments, edges)?;
        let root_fragment_id = root_selection.fragment_id;

        // Build lookup from fragment_id -> FragmentSchedulingMetadata index.
        let fr_by_id: BTreeMap<FragmentId, &FragmentSchedulingMetadata> =
            fragments.iter().map(|fr| (fr.fragment_id, fr)).collect();

        // Step 3: compute instance counts in topo order.
        // Incoming edges are driven by planner-owned native partition semantics.
        let mut incoming: BTreeMap<FragmentId, Vec<IncomingEdge>> = BTreeMap::new();
        for e in edges {
            let stream_kind = match e.edge_kind {
                FragmentEdgeKind::Stream => e.stream_kind,
                FragmentEdgeKind::CteMulticast { .. } => FragmentStreamKind::Broadcast,
                FragmentEdgeKind::IcebergChangeStreamRouter { .. } => e.stream_kind,
            };
            incoming
                .entry(e.target_fragment_id)
                .or_default()
                .push(IncomingEdge {
                    source_fragment_id: e.source_fragment_id,
                    is_native_hash_partitioned: matches!(
                        e.output_partition.kind,
                        PartitionKind::Hash
                    ),
                    stream_kind,
                });
        }

        let mut instance_counts: BTreeMap<FragmentId, usize> = BTreeMap::new();
        for &fid in &topo {
            let fr = fr_by_id
                .get(&fid)
                .ok_or_else(|| format!("fragment {fid} missing from fragment list"))?;

            let has_gather_input = incoming
                .get(&fid)
                .map(|ins| {
                    ins.iter()
                        .any(|edge| edge.stream_kind == FragmentStreamKind::Gather)
                })
                .unwrap_or(false);

            let count = if has_gather_input {
                1
            } else if fr.has_scan_nodes {
                // Scan fragment: one instance per backend.
                n
            } else {
                // Non-scan: inherit max from upstream hash-partitioned edges.
                let hash_max = incoming
                    .get(&fid)
                    .map(|ins| {
                        ins.iter()
                            .filter_map(|edge| {
                                if edge.is_native_hash_partitioned {
                                    instance_counts.get(&edge.source_fragment_id).copied()
                                } else {
                                    None
                                }
                            })
                            .max()
                    })
                    .flatten();
                hash_max.unwrap_or(1)
            };
            instance_counts.insert(fid, count);
        }

        // Step 4: force only result roots to 1 instance. Write-only DAG
        // anchors keep their exchange-derived parallelism.
        if root_selection.force_single_instance {
            instance_counts.insert(root_fragment_id, 1);
        }

        // Step 5: determine root backend index.
        let preferred_root_backend_idx = live[(query_id.lo as usize) % n].0;

        // Step 6: build placements.
        let mut by_fragment: BTreeMap<FragmentId, Vec<FragmentInstancePlacement>> = BTreeMap::new();

        for (&fid, &count) in &instance_counts {
            let fr = fr_by_id
                .get(&fid)
                .ok_or_else(|| format!("fragment {fid} missing from fragment list"))?;

            let mut instances: Vec<FragmentInstancePlacement> = (0..count)
                .map(
                    |instance_index| -> Result<FragmentInstancePlacement, String> {
                        let backend_idx = if count == 1 {
                            preferred_root_backend_idx
                        } else {
                            live[instance_index].0
                        };
                        let addr = live_backend_addr(live, backend_idx)?;
                        // finst_id encoding: hi = query_id.hi, lo = (fragment_id << 16) | instance_index.
                        // Unique within a query as long as instance_index < 65536 (always true:
                        // instance_count <= backends.len(), far below 65536).
                        debug_assert!(
                            instance_index < (1 << 16),
                            "instance_index {instance_index} overflows finst_id encoding"
                        );
                        let finst_id = UniqueId {
                            hi: query_id.hi,
                            lo: ((fid as i64) << 16) | (instance_index as i64),
                        };
                        Ok(FragmentInstancePlacement {
                            fragment_id: fid,
                            instance_index,
                            finst_id,
                            backend_idx,
                            endpoint: RuntimeEndpoint::from_socket_addr(addr),
                            scan_ranges: BTreeMap::new(),
                            destinations: Vec::new(),
                            runtime_filter_prober_params: BTreeMap::new(),
                            per_exch_num_senders: BTreeMap::new(),
                        })
                    },
                )
                .collect::<Result<Vec<_>, _>>()?;

            // Step 7 (Scheme C): partition scan ranges round-robin.
            for (&node_id, all_ranges) in &fr.native_scan_ranges {
                for inst in instances.iter_mut() {
                    inst.scan_ranges.entry(node_id).or_default();
                }
                for (i, range) in all_ranges.iter().enumerate() {
                    instances[i % count]
                        .scan_ranges
                        .entry(node_id)
                        .or_default()
                        .push(range.clone());
                }
            }

            by_fragment.insert(fid, instances);
        }

        // Compute root_finst_id from the selected execution anchor. Result
        // roots have one instance; write-only anchors may have multiple, so the
        // first placement is the coordination anchor.
        let root_placement = by_fragment
            .get(&root_fragment_id)
            .and_then(|insts| insts.first())
            .ok_or_else(|| "root fragment has no instances".to_string())?;
        let root_finst_id = root_placement.finst_id.clone();
        let root_backend_idx = root_placement.backend_idx;

        Ok(SchedulingPlan {
            root_fragment_id,
            by_fragment,
            root_finst_id,
            root_backend_idx,
        })
    }

    /// Fill `destinations` on each source-fragment instance for each edge.
    ///
    /// For each edge, the target fragment's instances are collected, their
    /// `FragmentDestination` entries are built, and the full list is appended
    /// to every source-fragment instance's `destinations` vec.
    pub(crate) fn fill_destinations(&self, plan: &mut SchedulingPlan, edges: &[FragmentEdge]) {
        let live = self.full_live_snapshot();
        self.fill_destinations_with_live(plan, edges, &live)
            .expect("configured backend snapshot should resolve all placements");
    }

    pub(crate) fn fill_destinations_with_live(
        &self,
        plan: &mut SchedulingPlan,
        edges: &[FragmentEdge],
        live: &[LiveBackend],
    ) -> Result<(), String> {
        for e in edges {
            // Snapshot target placements to avoid borrow conflict.
            let target_placements: Vec<(UniqueId, usize)> = plan
                .by_fragment
                .get(&e.target_fragment_id)
                .map(|insts| {
                    insts
                        .iter()
                        .map(|inst| (inst.finst_id, inst.backend_idx))
                        .collect()
                })
                .unwrap_or_default();

            let dests: Vec<FragmentDestination> = target_placements
                .into_iter()
                .map(|(finst_id, backend_idx)| {
                    let addr = live_backend_addr(live, backend_idx)?;
                    Ok::<FragmentDestination, String>(FragmentDestination::new(
                        finst_id,
                        RuntimeEndpoint::from_socket_addr(addr),
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;

            if let Some(src_instances) = plan.by_fragment.get_mut(&e.source_fragment_id) {
                for inst in src_instances.iter_mut() {
                    inst.destinations.extend(dests.iter().cloned());
                }
            }
        }
        Ok(())
    }

    /// Fill `runtime_filter_prober_params` on each build-fragment instance.
    ///
    /// For each filter_id, all instances of every probe fragment are collected
    /// and attached to every build-fragment instance as its list of probers.
    pub(crate) fn fill_runtime_filter_params(
        &self,
        plan: &mut SchedulingPlan,
        rf_plan: &RuntimeFilterPlanResult,
    ) {
        let live = self.full_live_snapshot();
        self.fill_runtime_filter_params_with_live(plan, rf_plan, &live)
            .expect("configured backend snapshot should resolve all placements");
    }

    pub(crate) fn fill_runtime_filter_params_with_live(
        &self,
        plan: &mut SchedulingPlan,
        rf_plan: &RuntimeFilterPlanResult,
        live: &[LiveBackend],
    ) -> Result<(), String> {
        // Collect probe instances per filter_id.
        // probe_side_filters: HashMap<FragmentId, Vec<(filter_id, scan_node_id)>>
        let mut probe_instances_by_filter: BTreeMap<i32, Vec<(UniqueId, usize)>> = BTreeMap::new();
        for (frag_id, probes) in &rf_plan.probe_side_filters {
            if let Some(instances) = plan.by_fragment.get(frag_id) {
                let snapped: Vec<(UniqueId, usize)> = instances
                    .iter()
                    .map(|inst| (inst.finst_id, inst.backend_idx))
                    .collect();
                for (filter_id, _scan_node_id) in probes {
                    probe_instances_by_filter
                        .entry(*filter_id)
                        .or_default()
                        .extend(snapped.iter().cloned());
                }
            }
        }

        // For each build fragment, attach the probe instance list per filter.
        // build_side_filters: HashMap<FragmentId, Vec<filter_id>>
        for (build_frag_id, filter_ids) in &rf_plan.build_side_filters {
            if let Some(build_instances) = plan.by_fragment.get_mut(build_frag_id) {
                for filter_id in filter_ids {
                    if let Some(probe_list) = probe_instances_by_filter.get(filter_id) {
                        let probers: Vec<RuntimeFilterProberDestination> = probe_list
                            .iter()
                            .map(|(finst_id, backend_idx)| {
                                let addr = live_backend_addr(live, *backend_idx)?;
                                Ok(RuntimeFilterProberDestination::new(
                                    *finst_id,
                                    RuntimeEndpoint::from_socket_addr(addr),
                                ))
                            })
                            .collect::<Result<Vec<_>, String>>()?;
                        for inst in build_instances.iter_mut() {
                            inst.runtime_filter_prober_params
                                .insert(*filter_id, probers.clone());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Fill `per_exch_num_senders` on each target-fragment instance.
    ///
    /// For each edge, every target instance learns that `upstream_n` senders
    /// will push data to exchange node `edge.target_exchange_node_id`.
    pub(crate) fn fill_per_exch_num_senders(
        &self,
        plan: &mut SchedulingPlan,
        edges: &[FragmentEdge],
    ) {
        for e in edges {
            // Snapshot upstream count first.
            let upstream_n = plan
                .by_fragment
                .get(&e.source_fragment_id)
                .map(|insts| insts.len())
                .unwrap_or(0) as i32;

            if let Some(tgt_instances) = plan.by_fragment.get_mut(&e.target_fragment_id) {
                for inst in tgt_instances.iter_mut() {
                    *inst
                        .per_exch_num_senders
                        .entry(e.target_exchange_node_id)
                        .or_insert(0) += upstream_n;
                }
            }
        }
    }

    fn full_live_snapshot(&self) -> Vec<LiveBackend> {
        self.live_backends.clone()
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Return the fragment ids in topological order (leaves first, root last).
pub(crate) fn topological_sort_bottom_up(
    fragments: &[FragmentSchedulingMetadata],
    edges: &[FragmentEdge],
) -> Result<Vec<FragmentId>, String> {
    // In-degree is the number of incoming edges (i.e. number of upstream
    // producers that feed this fragment's exchange nodes).
    let mut in_degree: BTreeMap<FragmentId, usize> = BTreeMap::new();
    // Adjacency: source -> list of targets it produces for.
    let mut adj: BTreeMap<FragmentId, Vec<FragmentId>> = BTreeMap::new();

    for fr in fragments {
        in_degree.entry(fr.fragment_id).or_insert(0);
    }
    for e in edges {
        // target_fragment_id "depends on" source_fragment_id, so source has
        // lower in_degree in the dependency graph.  In the execution graph
        // source is a producer (upstream), target is a consumer (downstream).
        // We want bottom-up (producers first), so we treat the in-degree as:
        // how many upstream producers does this fragment have?
        *in_degree.entry(e.target_fragment_id).or_insert(0) += 1;
        adj.entry(e.source_fragment_id)
            .or_default()
            .push(e.target_fragment_id);
    }

    let mut queue: std::collections::VecDeque<FragmentId> = in_degree
        .iter()
        .filter_map(|(&id, &deg)| if deg == 0 { Some(id) } else { None })
        .collect();

    let mut order: Vec<FragmentId> = Vec::with_capacity(fragments.len());
    while let Some(fid) = queue.pop_front() {
        order.push(fid);
        if let Some(neighbors) = adj.get(&fid) {
            for &tgt in neighbors {
                let deg = in_degree.entry(tgt).or_insert(0);
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(tgt);
                }
            }
        }
    }

    if order.len() != fragments.len() {
        return Err("cycle detected in fragment graph".into());
    }
    Ok(order)
}

#[derive(Clone, Copy, Debug)]
struct ExecutionRootSelection {
    fragment_id: FragmentId,
    force_single_instance: bool,
}

fn select_execution_root_fragment(
    fragments: &[FragmentSchedulingMetadata],
    edges: &[FragmentEdge],
) -> Result<ExecutionRootSelection, String> {
    use std::collections::BTreeSet;

    let sources: BTreeSet<FragmentId> = edges.iter().map(|e| e.source_fragment_id).collect();
    let terminal_fragments: Vec<&FragmentSchedulingMetadata> = fragments
        .iter()
        .filter(|fr| !sources.contains(&fr.fragment_id))
        .collect();

    match terminal_fragments.len() {
        1 => Ok(ExecutionRootSelection {
            fragment_id: terminal_fragments[0].fragment_id,
            force_single_instance: !terminal_fragments[0].output_kind.is_terminal_write(),
        }),
        0 => Err("no root fragment found (every fragment has an outgoing edge)".into()),
        _ if terminal_fragments
            .iter()
            .all(|fr| fr.output_kind.is_terminal_write()) =>
        {
            let fragment_id = terminal_fragments
                .iter()
                .map(|fr| fr.fragment_id)
                .min()
                .expect("terminal fragments checked non-empty");
            Ok(ExecutionRootSelection {
                fragment_id,
                force_single_instance: false,
            })
        }
        _ => Err(format!(
            "multiple root fragments: {:?}",
            terminal_fragments
                .iter()
                .map(|fr| fr.fragment_id)
                .collect::<Vec<_>>()
        )),
    }
}

fn live_backend_addr(live: &[LiveBackend], backend_idx: usize) -> Result<SocketAddr, String> {
    live.iter()
        .find_map(|(idx, addr)| (*idx == backend_idx).then_some(*addr))
        .ok_or_else(|| format!("backend index {backend_idx} missing from live snapshot"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::str::FromStr;

    use crate::sql::codegen::RuntimeFilterPlanResult;
    use crate::sql::codegen::{
        FragmentEdge, FragmentEdgeKind, FragmentOutputKind, FragmentSchedulingMetadata,
        FragmentStreamKind,
    };
    use crate::sql::planner::{DataPartition, PartitionKind};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TestPartitionType {
        HashPartitioned,
        BucketShuffleHashPartitioned,
        Unpartitioned,
    }

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn be(addr: &str) -> SocketAddr {
        SocketAddr::from_str(addr).expect("valid socket addr")
    }

    fn three_backends() -> Vec<SocketAddr> {
        vec![
            be("10.0.0.1:9010"),
            be("10.0.0.2:9010"),
            be("10.0.0.3:9010"),
        ]
    }

    fn two_backends() -> Vec<SocketAddr> {
        vec![be("10.0.0.1:9010"), be("10.0.0.2:9010")]
    }

    fn make_query_id(hi: i64, lo: i64) -> UniqueId {
        UniqueId { hi, lo }
    }

    fn dummy_query_id() -> UniqueId {
        make_query_id(1, 0)
    }

    fn scan_range_params(marker: i32) -> crate::runtime::scan_range::ScanRangeParams {
        let mut params = crate::runtime::scan_range::ScanRangeParams::file(
            crate::runtime::scan_range::FileScanRange {
                file_format: crate::runtime::scan_range::FileFormat::Parquet,
                full_path: Some(format!("s3://bucket/file-{marker}.parquet")),
                relative_path: None,
                table_id: None,
                offset: 0,
                length: 1,
                file_length: 1,
                delete_files: Vec::new(),
                deletion_vector_descriptor: None,
                first_row_id: None,
                data_sequence_number: None,
                modification_time: None,
                datacache_options: None,
                included_positions: Vec::new(),
                serialized_split: None,
                use_iceberg_jni_metadata_reader: false,
                ivm_change_op: None,
                file_pruning_min_max_values: None,
                compat_change_op_slot_id: None,
            },
        );
        params.volume_id = Some(marker);
        params
    }

    fn fake_fragment(
        fid: FragmentId,
        scan_node_id: Option<i32>,
        n_ranges: usize,
    ) -> FragmentSchedulingMetadata {
        let native_scan_ranges = match scan_node_id {
            Some(node_id) => {
                let ranges: Vec<crate::runtime::scan_range::ScanRangeParams> =
                    (0..n_ranges as i32).map(scan_range_params).collect();
                BTreeMap::from([(node_id, ranges)])
            }
            None => BTreeMap::new(),
        };

        FragmentSchedulingMetadata {
            fragment_id: fid,
            has_scan_nodes: scan_node_id.is_some(),
            output_kind: FragmentOutputKind::NonTerminal,
            native_scan_ranges,
            output_columns: vec![],
            boundary_schemas: vec![],
            cte_id: None,
            cte_exchange_nodes: vec![],
        }
    }

    fn fake_write_fragment(
        fid: FragmentId,
        scan_node_id: Option<i32>,
        n_ranges: usize,
    ) -> FragmentSchedulingMetadata {
        let mut fragment = fake_fragment(fid, scan_node_id, n_ranges);
        fragment.output_kind = FragmentOutputKind::TerminalWrite;
        fragment
    }

    /// Build a `FragmentEdge` with the given partition type.
    fn fake_edge(
        src: FragmentId,
        tgt: FragmentId,
        ptype: TestPartitionType,
        exch_node_id: i32,
    ) -> FragmentEdge {
        let stream_kind = match ptype {
            TestPartitionType::HashPartitioned
            | TestPartitionType::BucketShuffleHashPartitioned => FragmentStreamKind::Partitioned,
            TestPartitionType::Unpartitioned => FragmentStreamKind::Gather,
            _ => FragmentStreamKind::Other,
        };
        fake_stream_edge(src, tgt, ptype, exch_node_id, stream_kind)
    }

    fn fake_broadcast_edge(src: FragmentId, tgt: FragmentId, exch_node_id: i32) -> FragmentEdge {
        fake_stream_edge(
            src,
            tgt,
            TestPartitionType::Unpartitioned,
            exch_node_id,
            FragmentStreamKind::Broadcast,
        )
    }

    fn fake_stream_edge(
        src: FragmentId,
        tgt: FragmentId,
        ptype: TestPartitionType,
        exch_node_id: i32,
        stream_kind: FragmentStreamKind,
    ) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: src,
            target_fragment_id: tgt,
            target_exchange_node_id: exch_node_id,
            output_partition: native_partition_for_test(ptype),
            stream_kind,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    fn native_partition_for_test(ptype: TestPartitionType) -> DataPartition {
        let kind = match ptype {
            TestPartitionType::HashPartitioned
            | TestPartitionType::BucketShuffleHashPartitioned => PartitionKind::Hash,
            _ => PartitionKind::Unpartitioned,
        };
        DataPartition {
            kind,
            exprs: Vec::new(),
        }
    }

    fn fake_router_edge(
        src: FragmentId,
        tgt: FragmentId,
        ptype: TestPartitionType,
        exch_node_id: i32,
        stream_kind: FragmentStreamKind,
    ) -> FragmentEdge {
        let mut edge = fake_stream_edge(src, tgt, ptype, exch_node_id, stream_kind);
        edge.edge_kind = FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id: 42,
            branch_id: 1,
            branch_kind: crate::sql::common::ChangeStreamBranchKind::DeleteDv,
        };
        edge
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    mod live_filter_tests {
        use super::*;

        #[test]
        fn assign_with_empty_live_snapshot_returns_explicit_error() {
            let scheduler = FragmentScheduler::new(three_backends());
            let result = scheduler.assign_with_live(&[], &[], dummy_query_id(), &[]);
            assert!(result.is_err());
            assert!(
                result.unwrap_err().contains("no live backend available"),
                "empty live snapshot should return explicit error"
            );
        }

        #[test]
        fn sparse_live_snapshot_preserves_original_backend_indices() {
            let scheduler = FragmentScheduler::new(three_backends());
            let fragments = vec![fake_fragment(0, Some(1), 2), fake_fragment(1, None, 0)];
            let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
            let live = vec![(0usize, be("10.0.0.1:9010")), (2usize, be("10.0.0.3:9010"))];

            let mut plan = scheduler
                .assign_with_live(&fragments, &edges, make_query_id(7, 1), &live)
                .expect("assign_with_live");
            scheduler
                .fill_destinations_with_live(&mut plan, &edges, &live)
                .expect("fill_destinations_with_live");

            let placements = &plan.by_fragment[&0];
            assert_eq!(placements.len(), 2, "live.len() controls instance count");
            assert_eq!(placements[0].backend_idx, 0);
            assert_eq!(placements[1].backend_idx, 2);
            assert_eq!(
                plan.root_backend_idx, 2,
                "query_id.lo=1 chooses live slot 1"
            );

            for inst in placements {
                let dest = inst.destinations.first().expect("root destination");
                let endpoint = dest.endpoint();
                assert_eq!(endpoint.host(), "10.0.0.3");
                assert_eq!(endpoint.port(), 9010);
            }
        }
    }

    #[test]
    fn scan_root_fragment_forced_to_one_instance() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        // Single scan fragment (is also the root: no outgoing edges).
        let fragments = vec![fake_fragment(0, Some(1), 3)];
        let edges: Vec<FragmentEdge> = vec![];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        // A lone scan fragment is also the root, so the root override (1 instance)
        // wins over the scan=N rule.
        assert_eq!(plan.by_fragment[&0].len(), 1);
    }

    #[test]
    fn scan_fragment_producer_gets_n_instances() {
        // Non-root scan fragment with 3 backends should get 3 instances.
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        // F0=scan producer, F1=root consumer (UNPARTITIONED gather)
        let fragments = vec![fake_fragment(0, Some(1), 3), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        assert_eq!(
            plan.by_fragment[&0].len(),
            3,
            "scan producer gets 3 instances"
        );
        assert_eq!(plan.by_fragment[&1].len(), 1, "root gets 1 instance");
    }

    #[test]
    fn change_stream_router_partitioned_edge_is_scheduled_like_stream_edge() {
        let scheduler = FragmentScheduler::new(three_backends());
        let fragments = vec![
            fake_fragment(0, Some(1), 3),
            fake_fragment(1, None, 0),
            fake_fragment(2, None, 0),
        ];
        let edges = vec![
            fake_router_edge(
                0,
                1,
                TestPartitionType::HashPartitioned,
                10,
                FragmentStreamKind::Partitioned,
            ),
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];

        let mut plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 1))
            .expect("router branch edge should schedule");
        assert_eq!(plan.by_fragment[&0].len(), 3, "scan source has 3 senders");
        assert_eq!(
            plan.by_fragment[&1].len(),
            3,
            "partitioned router target inherits upstream sender count"
        );

        scheduler.fill_destinations(&mut plan, &edges);
        scheduler.fill_per_exch_num_senders(&mut plan, &edges);

        for inst in &plan.by_fragment[&0] {
            assert_eq!(
                inst.destinations.len(),
                3,
                "router branch source sees every target writer instance"
            );
        }
        for inst in &plan.by_fragment[&1] {
            assert_eq!(
                inst.per_exch_num_senders.get(&10).copied(),
                Some(3),
                "router branch target sees same sender count as a stream edge"
            );
        }
    }

    #[test]
    fn hash_consumer_inherits_upstream_n() {
        // Topology: F0(scan) -> HASH -> F1(non-scan) -> UNPARTITIONED -> F2(root)
        // F0 has 2 backends, so F1 should inherit 2 instances.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4), // scan
            fake_fragment(1, None, 0),    // hash consumer (non-root)
            fake_fragment(2, None, 0),    // root
        ];
        let edges = vec![
            fake_edge(0, 1, TestPartitionType::HashPartitioned, 10),
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        assert_eq!(plan.by_fragment[&0].len(), 2, "scan: 2 instances");
        assert_eq!(
            plan.by_fragment[&1].len(),
            2,
            "hash consumer inherits 2 from upstream scan"
        );
        assert_eq!(plan.by_fragment[&2].len(), 1, "root: forced to 1");
    }

    #[test]
    fn scheduler_uses_native_partition_for_instance_count() {
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4),
            fake_fragment(1, None, 0),
            fake_fragment(2, None, 0),
        ];
        let mut hash_edge = fake_edge(0, 1, TestPartitionType::Unpartitioned, 10);
        hash_edge.output_partition = DataPartition::hash(Vec::new());
        hash_edge.stream_kind = FragmentStreamKind::Partitioned;
        let edges = vec![
            hash_edge,
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];

        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");

        assert_eq!(plan.by_fragment[&0].len(), 2, "scan: 2 instances");
        assert_eq!(
            plan.by_fragment[&1].len(),
            2,
            "scheduler must derive hash fanout from native edge.output_partition"
        );
        assert_eq!(plan.by_fragment[&2].len(), 1, "root: forced to 1");
    }

    #[test]
    fn bucket_shuffle_consumer_inherits_upstream_n() {
        // Topology: F0(scan) -> BUCKET_SHUFFLE_HASH -> F1(non-root consumer) -> UNPARTITIONED -> F2(root)
        // F0 has 2 backends so F1 should inherit 2 instances.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4), // scan producer
            fake_fragment(1, None, 0),    // bucket-shuffle consumer (non-root)
            fake_fragment(2, None, 0),    // root gather
        ];
        let edges = vec![
            fake_edge(0, 1, TestPartitionType::BucketShuffleHashPartitioned, 10),
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        assert_eq!(plan.by_fragment[&0].len(), 2, "scan: 2 instances");
        assert_eq!(
            plan.by_fragment[&1].len(),
            2,
            "bucket-shuffle consumer inherits 2 from upstream scan"
        );
        assert_eq!(plan.by_fragment[&2].len(), 1, "root: forced to 1");
    }

    #[test]
    fn mixed_partition_edges_hash_wins_over_unpartitioned() {
        // Topology:
        //   F0(scan, N=2) -> HASH_PARTITIONED     -> F2(consumer, non-root)
        //   F1(non-scan)  -> BROADCAST             -> F2
        //   F2            -> UNPARTITIONED         -> F3(root)
        // F2 should get N=2 instances (HASH edge determines count; broadcast is ignored).
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4), // scan producer: 2 instances
            fake_fragment(1, None, 0),    // non-scan producer: 1 instance (UNPARTITIONED into F2)
            fake_fragment(2, None, 0),    // mixed consumer (non-root)
            fake_fragment(3, None, 0),    // root gather
        ];
        let edges = vec![
            fake_edge(0, 2, TestPartitionType::HashPartitioned, 10),
            fake_broadcast_edge(1, 2, 20),
            fake_edge(2, 3, TestPartitionType::Unpartitioned, 30),
        ];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 1))
            .expect("assign");
        assert_eq!(plan.by_fragment[&0].len(), 2, "scan producer: 2 instances");
        assert_eq!(
            plan.by_fragment[&1].len(),
            1,
            "unpartitioned producer: 1 instance"
        );
        assert_eq!(
            plan.by_fragment[&2].len(),
            2,
            "HASH edge wins: consumer gets 2 instances"
        );
        assert_eq!(plan.by_fragment[&3].len(), 1, "root: forced to 1");
    }

    #[test]
    fn unpartitioned_gather_is_one_instance() {
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 4), // scan
            fake_fragment(1, None, 0),    // root (UNPARTITIONED gather)
        ];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 7))
            .expect("assign");
        assert_eq!(
            plan.by_fragment[&1].len(),
            1,
            "unpartitioned gather -> 1 instance"
        );
    }

    #[test]
    fn incoming_gather_forces_scan_consumer_to_one_instance() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, None, 0),    // gathered producer
            fake_fragment(1, Some(7), 6), // consumer also owns a scan
            fake_fragment(2, None, 0),    // root
        ];
        let edges = vec![
            fake_edge(0, 1, TestPartitionType::Unpartitioned, 10),
            fake_edge(1, 2, TestPartitionType::HashPartitioned, 20),
        ];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 7))
            .expect("assign");
        assert_eq!(
            plan.by_fragment[&1].len(),
            1,
            "a true Gather input must not be consumed by every scan instance"
        );
    }

    #[test]
    fn root_fragment_is_always_one_instance() {
        // Even if an edge into the root is HASH_PARTITIONED, root stays at 1.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 2), // scan
            fake_fragment(1, None, 0),    // root
        ];
        let edges = vec![fake_edge(0, 1, TestPartitionType::HashPartitioned, 10)];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(5, 5))
            .expect("assign");
        assert_eq!(plan.by_fragment[&1].len(), 1, "root always 1");
    }

    #[test]
    fn multi_terminal_write_dag_keeps_writer_parallelism() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 6),
            fake_write_fragment(10, None, 0),
            fake_write_fragment(11, None, 0),
        ];
        let edges = vec![
            fake_router_edge(
                0,
                10,
                TestPartitionType::HashPartitioned,
                100,
                FragmentStreamKind::Partitioned,
            ),
            fake_router_edge(
                0,
                11,
                TestPartitionType::HashPartitioned,
                101,
                FragmentStreamKind::Partitioned,
            ),
        ];

        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(5, 5))
            .expect("assign");

        assert_eq!(plan.root_fragment_id, 10);
        assert_eq!(
            plan.by_fragment[&0].len(),
            3,
            "scan source stays distributed"
        );
        assert_eq!(
            plan.by_fragment[&10].len(),
            3,
            "write anchor is not forced to a single instance"
        );
        assert_eq!(plan.by_fragment[&11].len(), 3);
        assert_eq!(plan.root_backend_idx, plan.by_fragment[&10][0].backend_idx);
        assert_eq!(plan.root_finst_id, plan.by_fragment[&10][0].finst_id);
    }

    #[test]
    fn single_terminal_write_dag_keeps_writer_parallelism() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 6),
            fake_write_fragment(10, None, 0),
        ];
        let edges = vec![fake_router_edge(
            0,
            10,
            TestPartitionType::HashPartitioned,
            100,
            FragmentStreamKind::Partitioned,
        )];

        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(5, 5))
            .expect("assign");

        assert_eq!(plan.root_fragment_id, 10);
        assert_eq!(
            plan.by_fragment[&0].len(),
            3,
            "scan source stays distributed"
        );
        assert_eq!(
            plan.by_fragment[&10].len(),
            3,
            "single terminal writer is not forced to a single instance"
        );
        assert_eq!(plan.root_backend_idx, plan.by_fragment[&10][0].backend_idx);
        assert_eq!(plan.root_finst_id, plan.by_fragment[&10][0].finst_id);
    }

    #[test]
    fn multi_instance_backend_idx_equals_instance_index() {
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 3), // scan: 3 instances
            fake_fragment(1, None, 0),    // root
        ];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 3);
        for inst in f0 {
            assert_eq!(
                inst.backend_idx, inst.instance_index,
                "multi-instance: backend_idx == instance_index"
            );
        }
    }

    #[test]
    fn single_instance_lands_on_query_id_hash() {
        // query_id.lo = 7, n = 3 -> root_backend_idx = 7 % 3 = 1
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![fake_fragment(0, None, 0)]; // non-scan root
        let edges: Vec<FragmentEdge> = vec![];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 7))
            .expect("assign");
        assert_eq!(plan.root_backend_idx, 1, "7 % 3 == 1");
        assert_eq!(plan.by_fragment[&0][0].backend_idx, 1);
    }

    #[test]
    fn finst_id_encodes_fragment_id_and_instance_index() {
        // fragment_id=3, instance 0 -> finst.lo == 0x30000; finst.hi == query_id.hi
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        // Use non-root scan so we get multi-instance (otherwise root override -> 1 inst)
        let fragments = vec![
            fake_fragment(3, Some(1), 3), // scan fragment, id=3
            fake_fragment(99, None, 0),   // root
        ];
        let edges = vec![fake_edge(3, 99, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(42, 0))
            .expect("assign");
        let inst0 = &plan.by_fragment[&3][0];
        assert_eq!(inst0.finst_id.hi, 42, "hi == query_id.hi");
        assert_eq!(
            inst0.finst_id.lo, 0x30000,
            "lo == (fid<<16)|idx == 3<<16 == 0x30000"
        );
    }

    #[test]
    fn scan_splits_round_robin_seven_ranges_three_instances() {
        // 7 ranges across 3 instances -> counts 3, 2, 2; total 7, no loss.
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        // Non-root scan with 3 backends
        let mut fr = fake_fragment(0, Some(1), 7);
        fr.fragment_id = 0;
        let root = fake_fragment(1, None, 0);
        let fragments = vec![fr, root];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 3);
        let counts: Vec<usize> = f0
            .iter()
            .map(|inst| inst.scan_ranges.get(&1).map(|r| r.len()).unwrap_or(0))
            .collect();
        assert_eq!(counts, vec![3, 2, 2], "round-robin 7 across 3: [3,2,2]");
        let total: usize = counts.iter().sum();
        assert_eq!(total, 7, "no ranges lost");
    }

    #[test]
    fn scan_preserves_empty_range_entry_for_each_instance() {
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![fake_fragment(0, Some(7), 0), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 2);
        for inst in f0 {
            let ranges = inst
                .scan_ranges
                .get(&7)
                .expect("empty scan range entry is preserved");
            assert!(ranges.is_empty());
        }
    }

    #[test]
    fn scan_splits_no_overlap_no_loss() {
        // 6 ranges, 2 instances -> each range appears exactly once.
        // Use volume_id markers 0..5 as distinguishable identity.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fr = fake_fragment(0, Some(5), 6); // node_id=5, 6 ranges with markers 0..5
        let root = fake_fragment(1, None, 0);
        let fragments = vec![fr, root];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        let f0 = &plan.by_fragment[&0];
        assert_eq!(f0.len(), 2);
        let empty: Vec<crate::runtime::scan_range::ScanRangeParams> = vec![];
        let mut all_markers: Vec<i32> = f0
            .iter()
            .flat_map(|inst| {
                inst.scan_ranges
                    .get(&5)
                    .unwrap_or(&empty)
                    .iter()
                    .map(|r| r.volume_id.expect("marker set"))
            })
            .collect();
        all_markers.sort();
        assert_eq!(
            all_markers,
            vec![0, 1, 2, 3, 4, 5],
            "each range appears exactly once"
        );
    }

    #[test]
    fn fill_destinations_source_gets_all_target_instances() {
        // F0 scan (3 inst) -> UNPARTITIONED -> F1 root (1 inst).
        // After fill_destinations, each F0 instance should have 1 destination.
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends.clone());
        let fragments = vec![fake_fragment(0, Some(1), 3), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let mut plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        scheduler.fill_destinations(&mut plan, &edges);

        // Each source instance (F0) should have exactly 1 destination (F1's 1 instance).
        for inst in &plan.by_fragment[&0] {
            assert_eq!(
                inst.destinations.len(),
                1,
                "1 destination per source instance"
            );
        }
    }

    #[test]
    fn fill_destinations_sets_runtime_endpoint() {
        // Verify hostname/port comes from backends[target.backend_idx].
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends.clone());
        let fragments = vec![fake_fragment(0, Some(1), 1), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let mut plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        scheduler.fill_destinations(&mut plan, &edges);

        // F1 root backend: query_id.lo=0, n=3 -> backend 0 -> "10.0.0.1:9010"
        let root_dest = &plan.by_fragment[&0][0].destinations[0];
        let endpoint = root_dest.endpoint();
        assert_eq!(endpoint.host(), "10.0.0.1");
        assert_eq!(endpoint.port(), 9010);
    }

    #[test]
    fn fill_runtime_filter_params_build_gets_all_probe_instances() {
        // 2 probe instances -> 2 probers with the right addresses.
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends.clone());
        // F0=scan (2 inst), F1=root (1 inst); F0 is probe, F1 is build (artificial scenario).
        let fragments = vec![fake_fragment(0, Some(1), 2), fake_fragment(1, None, 0)];
        let edges = vec![fake_edge(0, 1, TestPartitionType::Unpartitioned, 10)];
        let mut plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");

        let mut rf_plan = RuntimeFilterPlanResult {
            all_filters: Default::default(),
            build_side_filters: {
                let mut m = std::collections::HashMap::new();
                m.insert(1u32, vec![42i32]); // fragment 1 builds filter 42
                m
            },
            probe_side_filters: {
                let mut m = std::collections::HashMap::new();
                m.insert(0u32, vec![(42i32, 1i32)]); // fragment 0 probes filter 42 on scan node 1
                m
            },
        };
        // Suppress unused warning; all_filters is used by coordinator, not scheduler.
        let _ = &mut rf_plan;

        scheduler.fill_runtime_filter_params(&mut plan, &rf_plan);

        // F1 (build fragment) single instance should have 2 prober entries for filter 42.
        let build_inst = &plan.by_fragment[&1][0];
        let probers = build_inst
            .runtime_filter_prober_params
            .get(&42)
            .expect("filter 42 prober params");
        assert_eq!(probers.len(), 2, "2 probe instances -> 2 prober entries");

        // Verify addresses correspond to the 2 probe instances (F0: backends 0 and 1).
        let addrs: Vec<String> = probers
            .iter()
            .map(|p| p.endpoint().host().to_string())
            .collect();
        let ports: Vec<i32> = probers.iter().map(|p| p.endpoint().port()).collect();
        assert!(
            addrs.contains(&"10.0.0.1".to_string()),
            "probe instance 0 on backend 0"
        );
        assert!(
            addrs.contains(&"10.0.0.2".to_string()),
            "probe instance 1 on backend 1"
        );
        assert_eq!(
            ports,
            vec![9010, 9010],
            "both probe endpoints use port 9010"
        );
    }

    #[test]
    fn fill_per_exch_num_senders_accumulates_upstream_count() {
        // F0 (scan, 3 inst) -> exchange node 10 -> F2 (root)
        // F1 (scan, 3 inst) -> exchange node 20 -> F2 (root)
        // Each F2 instance should have per_exch_num_senders[10]=3, [20]=3.
        let backends = three_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![
            fake_fragment(0, Some(1), 3),
            fake_fragment(1, Some(2), 3),
            fake_fragment(2, None, 0), // root
        ];
        let edges = vec![
            fake_edge(0, 2, TestPartitionType::Unpartitioned, 10),
            fake_edge(1, 2, TestPartitionType::Unpartitioned, 20),
        ];
        let mut plan = scheduler
            .assign(&fragments, &edges, make_query_id(1, 0))
            .expect("assign");
        scheduler.fill_per_exch_num_senders(&mut plan, &edges);

        let root_inst = &plan.by_fragment[&2][0];
        assert_eq!(
            root_inst.per_exch_num_senders.get(&10).copied(),
            Some(3),
            "exch node 10 has 3 senders"
        );
        assert_eq!(
            root_inst.per_exch_num_senders.get(&20).copied(),
            Some(3),
            "exch node 20 has 3 senders"
        );
    }

    #[test]
    fn no_backends_returns_error() {
        let scheduler = FragmentScheduler::new(vec![]);
        let fragments = vec![fake_fragment(0, None, 0)];
        let edges: Vec<FragmentEdge> = vec![];
        let result = scheduler.assign(&fragments, &edges, make_query_id(1, 1));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no live backend available"));
    }

    #[test]
    fn cycle_detection_returns_error() {
        // Create a cycle: F0 -> F1 -> F0 (impossible in practice but scheduler should detect it).
        let backends = two_backends();
        let scheduler = FragmentScheduler::new(backends);
        let fragments = vec![fake_fragment(0, None, 0), fake_fragment(1, None, 0)];
        let edges = vec![
            fake_edge(0, 1, TestPartitionType::Unpartitioned, 10),
            fake_edge(1, 0, TestPartitionType::Unpartitioned, 20),
        ];
        let result = scheduler.assign(&fragments, &edges, make_query_id(1, 1));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cycle"));
    }
}
