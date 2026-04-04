//! Post-build runtime filter planning pass.
//!
//! Walks all fragment plan nodes, identifies hash join equi-conditions
//! eligible for runtime filters, finds target scan nodes, and produces
//! `TRuntimeFilterDescription` on join nodes.

use std::collections::{BTreeMap, HashMap};

use crate::exprs;
use crate::plan_nodes;
use crate::runtime_filter;
use crate::sql::cascades::operator::JoinDistribution;
use crate::sql::fragment::FragmentId;
use crate::sql::physical::FragmentBuildResult;

use super::fragment_builder::ScanTupleOwner;

/// Result of the runtime filter planning pass.
pub(crate) struct RuntimeFilterPlanResult {
    /// filter_id -> RF description.
    pub all_filters: HashMap<i32, runtime_filter::TRuntimeFilterDescription>,
    /// fragment_id -> build-side filter IDs in that fragment.
    pub build_side_filters: HashMap<FragmentId, Vec<i32>>,
    /// fragment_id -> (filter_id, scan_node_id) for probe-side targets.
    pub probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>>,
}

/// Plan runtime filters for all fragments.
///
/// Iterates plan nodes across fragments, finds hash join nodes with
/// equi-conditions, identifies target scan nodes via slot->tuple->scan
/// lookup, and populates `build_runtime_filters` on join nodes.
pub(crate) fn plan_runtime_filters(
    fragment_results: &mut [FragmentBuildResult],
    scan_tuple_owners: &HashMap<i32, ScanTupleOwner>,
    join_fragment_map: &HashMap<i32, FragmentId>,
    join_distributions: &HashMap<i32, JoinDistribution>,
    pipeline_dop: i32,
) -> RuntimeFilterPlanResult {
    let mut next_filter_id: i32 = 0;
    let mut all_filters: HashMap<i32, runtime_filter::TRuntimeFilterDescription> = HashMap::new();
    let mut build_side_filters: HashMap<FragmentId, Vec<i32>> = HashMap::new();
    let mut probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>> = HashMap::new();

    // Collect (fragment_id, node_index, join_node_id) for all eligible hash joins.
    let mut join_targets: Vec<(FragmentId, usize, i32)> = Vec::new();
    for fr in fragment_results.iter() {
        for (idx, node) in fr.plan.nodes.iter().enumerate() {
            if node.node_type == plan_nodes::TPlanNodeType::HASH_JOIN_NODE {
                if is_rf_eligible_join_op(node) {
                    join_targets.push((fr.fragment_id, idx, node.node_id));
                }
            }
        }
    }

    // For each eligible hash join, generate RF descriptions.
    for (join_frag_id, node_idx, join_node_id) in &join_targets {
        // Find the fragment and node (re-borrow immutably for reading).
        let fr = fragment_results
            .iter()
            .find(|f| f.fragment_id == *join_frag_id)
            .unwrap();
        let node = &fr.plan.nodes[*node_idx];
        let hash_join = match &node.hash_join_node {
            Some(hj) => hj,
            None => continue,
        };

        let distribution = join_distributions.get(join_node_id).cloned();

        let mut rf_descs: Vec<runtime_filter::TRuntimeFilterDescription> = Vec::new();

        for (expr_order, eq_cond) in hash_join.eq_join_conjuncts.iter().enumerate() {
            // The probe expr is `left` (probe side), build expr is `right`.
            let probe_expr = &eq_cond.left;
            let build_expr = &eq_cond.right;

            // v1: only support simple SlotRef probe expressions.
            let (_probe_slot_id, probe_tuple_id) = match extract_slot_ref(probe_expr) {
                Some(sr) => sr,
                None => continue,
            };

            // Find the scan node that owns this tuple.
            let scan_owner = match scan_tuple_owners.get(&probe_tuple_id) {
                Some(owner) => owner,
                None => continue,
            };

            let has_remote_targets = *join_frag_id != scan_owner.fragment_id;

            let filter_id = next_filter_id;
            next_filter_id += 1;

            let build_join_mode = match distribution {
                Some(JoinDistribution::Broadcast) => {
                    runtime_filter::TRuntimeFilterBuildJoinMode::BORADCAST
                }
                Some(JoinDistribution::Shuffle) => {
                    runtime_filter::TRuntimeFilterBuildJoinMode::PARTITIONED
                }
                Some(JoinDistribution::Colocate) => {
                    runtime_filter::TRuntimeFilterBuildJoinMode::COLOCATE
                }
                None => runtime_filter::TRuntimeFilterBuildJoinMode::BORADCAST,
            };

            let (local_layout, global_layout) = match &distribution {
                Some(JoinDistribution::Broadcast) | None => (
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                ),
                Some(JoinDistribution::Shuffle) => (
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                    runtime_filter::TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L,
                ),
                Some(JoinDistribution::Colocate) => (
                    runtime_filter::TRuntimeFilterLayoutMode::SINGLETON,
                    runtime_filter::TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L,
                ),
            };

            let layout = runtime_filter::TRuntimeFilterLayout::new(
                filter_id,
                local_layout,
                global_layout,
                false,                                               // pipeline_level_multi_partitioned
                1_i32,                                               // num_instances
                pipeline_dop,                                        // num_drivers_per_instance
                None::<Vec<i32>>,                                    // bucketseq_to_instance
                None::<Vec<i32>>,                                    // bucketseq_to_driverseq
                None::<Vec<i32>>,                                    // bucketseq_to_partition
                None::<Vec<crate::partitions::TBucketProperty>>,     // bucket_properties
            );

            let mut target_map = BTreeMap::new();
            target_map.insert(scan_owner.scan_node_id, probe_expr.clone());

            let desc = runtime_filter::TRuntimeFilterDescription::new(
                filter_id,                                                              // filter_id
                build_expr.clone(),                                                     // build_expr
                expr_order as i32,                                                      // expr_order
                target_map,                                                             // plan_node_id_to_target_expr
                has_remote_targets,                                                     // has_remote_targets
                None::<i64>,                                                            // bloom_filter_size
                None::<Vec<crate::types::TNetworkAddress>>,                             // runtime_filter_merge_nodes
                build_join_mode,                                                        // build_join_mode
                None::<crate::types::TUniqueId>,                                        // sender_finst_id
                *join_node_id,                                                          // build_plan_node_id
                None::<Vec<crate::types::TUniqueId>>,                                   // broadcast_grf_senders
                None::<Vec<runtime_filter::TRuntimeFilterDestination>>,                 // broadcast_grf_destinations
                None::<Vec<i32>>,                                                       // bucketseq_to_instance
                None::<BTreeMap<i32, Vec<exprs::TExpr>>>,                               // plan_node_id_to_partition_by_exprs
                runtime_filter::TRuntimeFilterBuildType::JOIN_FILTER,                   // filter_type
                layout,                                                                 // layout
                None::<bool>,                                                           // build_from_group_execution
                None::<bool>,                                                           // is_broad_cast_join_in_skew
                None::<i32>,                                                            // skew_shuffle_filter_id
            );

            rf_descs.push(desc.clone());
            all_filters.insert(filter_id, desc);
            build_side_filters
                .entry(*join_frag_id)
                .or_default()
                .push(filter_id);
            probe_side_filters
                .entry(scan_owner.fragment_id)
                .or_default()
                .push((filter_id, scan_owner.scan_node_id));
        }

        // Patch the join node in-place with build_runtime_filters.
        if !rf_descs.is_empty() {
            let fr_mut = fragment_results
                .iter_mut()
                .find(|f| f.fragment_id == *join_frag_id)
                .unwrap();
            if let Some(ref mut hj) = fr_mut.plan.nodes[*node_idx].hash_join_node {
                hj.build_runtime_filters = Some(rf_descs);
            }
        }
    }

    RuntimeFilterPlanResult {
        all_filters,
        build_side_filters,
        probe_side_filters,
    }
}

/// Check if the join operation type is eligible for runtime filter generation.
/// Aligned with StarRocks `JoinNode.buildRuntimeFilters()`.
fn is_rf_eligible_join_op(node: &plan_nodes::TPlanNode) -> bool {
    let hj = match &node.hash_join_node {
        Some(hj) => hj,
        None => return false,
    };
    matches!(
        hj.join_op,
        plan_nodes::TJoinOp::INNER_JOIN
            | plan_nodes::TJoinOp::LEFT_SEMI_JOIN
            | plan_nodes::TJoinOp::RIGHT_OUTER_JOIN
            | plan_nodes::TJoinOp::RIGHT_SEMI_JOIN
            | plan_nodes::TJoinOp::RIGHT_ANTI_JOIN
            | plan_nodes::TJoinOp::CROSS_JOIN
    )
}

/// Extract (slot_id, tuple_id) from a TExpr if it is a simple SlotRef.
/// Returns None for complex expressions (v1 limitation).
fn extract_slot_ref(expr: &exprs::TExpr) -> Option<(i32, i32)> {
    if expr.nodes.len() == 1 && expr.nodes[0].node_type == exprs::TExprNodeType::SLOT_REF {
        if let Some(ref sr) = expr.nodes[0].slot_ref {
            return Some((sr.slot_id, sr.tuple_id));
        }
    }
    None
}
