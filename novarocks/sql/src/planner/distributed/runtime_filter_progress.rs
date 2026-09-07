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

use std::collections::BTreeSet;

use crate::planner::physical::runtime_filter_placement::rf_sides_for_join;
use crate::planner::runtime_filter::contract::CompletionRequirement;
use crate::planner::runtime_filter::graph::{RuntimeFilterBindingRoleData, RuntimeFilterGraphData};
pub(crate) use crate::planner::runtime_filter::progress::{
    FrontierEdge, FrontierSkip, JoinBuildProgressCatalog, JoinBuildProgressProof,
    JoinBuildProgressSkip,
};

#[cfg(test)]
use super::FragmentId;
use super::{DistributedNode, DistributedNodeKind, PlanFragment};

/// Fragment-local input-closure audit.
///
/// A kind may appear on a frontier-collection path only when, inside one
/// fragment, it (a) consumes inputs only from its child subtrees, (b) shares
/// no cross-subtree state, and (c) completes without waiting on events outside
/// its subtree (runtime-filter waits are modeled separately as explicit wait
/// edges). The match is deliberately exhaustive with NO wildcard arm: adding a
/// `DistributedNodeKind` variant fails compilation here until the new kind is
/// audited against (a)-(c).
fn fragment_local_input_closed(kind: &DistributedNodeKind) -> bool {
    match kind {
        DistributedNodeKind::Scan(_)
        | DistributedNodeKind::Filter(_)
        | DistributedNodeKind::Project(_)
        | DistributedNodeKind::Unpivot(_)
        | DistributedNodeKind::Sort(_)
        | DistributedNodeKind::Values(_)
        | DistributedNodeKind::Repeat(_)
        | DistributedNodeKind::Window(_)
        | DistributedNodeKind::GenerateSeries(_)
        | DistributedNodeKind::TableFunction(_)
        | DistributedNodeKind::AssertOneRow(_)
        | DistributedNodeKind::TopN(_)
        | DistributedNodeKind::HashAggregate(_)
        | DistributedNodeKind::HashJoin(_)
        | DistributedNodeKind::NestLoopJoin(_)
        | DistributedNodeKind::SetOp(_)
        | DistributedNodeKind::ChangeEventExpand(_)
        | DistributedNodeKind::Exchange(_)
        // A `TableWriter` reads only its child subtree and hands each batch to
        // its own bound writer; a `TableFinish` reads only its Exchange
        // children (fragment-boundary leaves the walk stops at). Neither shares
        // cross-subtree state nor waits on an event outside its subtree, so both
        // are audited input-closed against (a)-(c).
        | DistributedNodeKind::TableWriter(_)
        | DistributedNodeKind::TableFinish(_) => true,
    }
}

/// Collect every Exchange in-edge underneath `node` (inclusive), stopping at
/// Exchange leaves (fragment boundaries). Fails on any unaudited node kind.
fn collect_exchange_inputs(
    node: &DistributedNode,
    out: &mut BTreeSet<FrontierEdge>,
) -> Result<(), FrontierSkip> {
    if !fragment_local_input_closed(&node.payload) {
        return Err(FrontierSkip::UnauditedNode {
            node_id: node.node_id,
        });
    }
    if let DistributedNodeKind::Exchange(exchange) = &node.payload {
        out.insert(FrontierEdge {
            source_fragment: exchange.source_fragment_id,
            target_exchange_node: node.node_id,
        });
        // Exchange is a fragment boundary: its subtree lives in the source
        // fragment, never descend past it.
        return Ok(());
    }
    for child in &node.children {
        collect_exchange_inputs(child, out)?;
    }
    Ok(())
}

/// The exact in-edge partition for one hash-join node inside its fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FrontierSplit {
    pub(crate) build_frontier: BTreeSet<FrontierEdge>,
    pub(crate) non_build_inputs: BTreeSet<FrontierEdge>,
}

/// Compute the build-frontier / non-build partition for `join_node` within
/// `fragment`. Pure structural layer: no RuntimeFilterGraph involvement.
pub(crate) fn split_join_inputs(
    fragment: &PlanFragment,
    join_node: &DistributedNode,
) -> Result<FrontierSplit, FrontierSkip> {
    let DistributedNodeKind::HashJoin(join) = &join_node.payload else {
        return Err(FrontierSkip::MissingChild);
    };
    let Some(sides) = rf_sides_for_join(join.join_type) else {
        return Err(FrontierSkip::NoRfSides);
    };
    let Some(build_child) = join_node.children.get(sides.build_child) else {
        return Err(FrontierSkip::MissingChild);
    };
    if join_node.children.get(sides.probe_child).is_none() {
        return Err(FrontierSkip::MissingChild);
    }
    let mut build_frontier = BTreeSet::new();
    collect_exchange_inputs(build_child, &mut build_frontier)?;
    let mut all_inputs = BTreeSet::new();
    collect_exchange_inputs(&fragment.root, &mut all_inputs)?;
    // The build frontier must be a subset of the fragment's inputs; anything
    // else means the traversal roots disagree (planner bug, skip loudly).
    if !build_frontier.is_subset(&all_inputs) {
        return Err(FrontierSkip::MissingChild);
    }
    let non_build_inputs = all_inputs.difference(&build_frontier).copied().collect();
    Ok(FrontierSplit {
        build_frontier,
        non_build_inputs,
    })
}

pub(super) fn build_join_progress_proof_catalog<A>(
    fragments: &[PlanFragment],
    graph: &RuntimeFilterGraphData<A>,
) -> JoinBuildProgressCatalog {
    fn visit<A>(
        fragment: &PlanFragment,
        node: &DistributedNode,
        graph: &RuntimeFilterGraphData<A>,
        catalog: &mut JoinBuildProgressCatalog,
    ) {
        if matches!(node.payload, DistributedNodeKind::HashJoin(_))
            && !node.runtime_filter_binding_ids.is_empty()
        {
            let split = split_join_inputs(fragment, node);
            for binding_id in &node.runtime_filter_binding_ids {
                let Some(binding) = graph.binding(*binding_id) else {
                    continue;
                };
                let RuntimeFilterBindingRoleData::Producer(requirement) = &binding.role else {
                    continue;
                };
                if requirement.completion_requirement != CompletionRequirement::ProducerClosed {
                    continue;
                }
                let key = (binding.channel_id, binding.binding_id, fragment.fragment_id);
                match &split {
                    Ok(split) => catalog.insert_proof(
                        key,
                        JoinBuildProgressProof {
                            channel: binding.channel_id,
                            producer_binding: binding.binding_id,
                            producer_fragment: fragment.fragment_id,
                            join_node_id: node.node_id,
                            build_frontier: split.build_frontier.iter().copied().collect(),
                            non_build_inputs: split.non_build_inputs.iter().copied().collect(),
                        },
                    ),
                    Err(rule) => catalog.insert_skip(
                        key,
                        JoinBuildProgressSkip {
                            join_node_id: node.node_id,
                            rule: *rule,
                        },
                    ),
                }
            }
        }
        for child in &node.children {
            visit(fragment, child, graph, catalog);
        }
    }
    let mut catalog = JoinBuildProgressCatalog::new();
    for fragment in fragments {
        visit(fragment, &fragment.root, graph, &mut catalog);
    }
    catalog
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use arrow::datatypes::DataType;

    use crate::analysis::{ExprKind, JoinKind, LiteralValue, TypedExpr};
    use crate::planner::distributed::{
        DataPartition, DataSink, ExchangeFlavor, ExchangeReceiver, FragmentEdge, FragmentEdgeKind,
        FragmentStreamKind, PartitionKind, PlanFragment,
    };
    use crate::planner::payload::PlanValuesNode;
    use crate::planner::physical::runtime_filter::JoinExecutionMode;
    use crate::planner::physical::{
        JoinDistribution, PhysicalHashJoinNode, PhysicalPlanStats, PlannerConfidence,
    };
    use crate::planner::runtime_filter::contract::{
        BindingId, ChannelId, ContributionKind, CoverageWitnessId, PlanFragmentId, PlanNodeId,
    };
    use crate::planner::runtime_filter::graph::{
        ApplyPoint, PlanLocation, ProducerRequirement, RuntimeFilterBindingRole,
        RuntimeFilterBindingSpec, RuntimeFilterGraph,
    };

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn node(
        node_id: i32,
        fragment_id: FragmentId,
        payload: DistributedNodeKind,
        children: Vec<DistributedNode>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![],
            nullable_tuple_ids: vec![],
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            stats: stats(),
            payload,
        }
    }

    fn values(node_id: i32, fragment_id: FragmentId) -> DistributedNode {
        node(
            node_id,
            fragment_id,
            DistributedNodeKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
        )
    }

    fn exchange(node_id: i32, fragment_id: FragmentId, source: FragmentId) -> DistributedNode {
        node(
            node_id,
            fragment_id,
            DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: DataPartition {
                    kind: PartitionKind::Unpartitioned,
                    exprs: vec![],
                },
                source_fragment_id: source,
                output_columns: vec![],
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            }),
            vec![],
        )
    }

    fn hash_join(
        node_id: i32,
        fragment_id: FragmentId,
        join_type: JoinKind,
        children: Vec<DistributedNode>,
    ) -> DistributedNode {
        node(
            node_id,
            fragment_id,
            DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type,
                eq_conditions: vec![],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
                execution_mode: Some(JoinExecutionMode::Partitioned),
                build_runtime_filters: vec![],
                output_columns: vec![],
            })),
            children,
        )
    }

    fn fragment(fragment_id: FragmentId, root: DistributedNode) -> PlanFragment {
        // Reuse the minimal-construction shape used across distributed build
        // tests: only fragment_id and root matter to this module.
        PlanFragment {
            fragment_id,
            root,
            data_partition: DataPartition {
                kind: PartitionKind::Unpartitioned,
                exprs: vec![],
            },
            output_partition: DataPartition {
                kind: PartitionKind::Unpartitioned,
                exprs: vec![],
            },
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: vec![],
            cte_id: None,
            cte_exchange_nodes: vec![],
        }
    }

    fn fe(source: FragmentId, exchange_node: i32) -> FrontierEdge {
        FrontierEdge {
            source_fragment: source,
            target_exchange_node: exchange_node,
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for runtime-filter progress fixture variants."
    )]
    fn producer_graph(fragment_id: FragmentId, join_node_id: i32) -> RuntimeFilterGraph {
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: BindingId::new(100),
                channel_id: ChannelId::new(7),
                coverage_witness_id: Some(CoverageWitnessId::new(1)),
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(fragment_id),
                    node_id: PlanNodeId::new(join_node_id),
                },
                expression: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                    contribution_kinds: BTreeSet::from([
                        ContributionKind::ValueDomainDelta,
                        ContributionKind::ProducerClosed,
                    ]),
                    completion_requirement: CompletionRequirement::ProducerClosed,
                    target:
                        crate::planner::runtime_filter::graph::ProducerBindingTarget::JoinBuildKey {
                            ordinal: 0,
                        },
                }),
            })
            .unwrap();
        graph
    }

    #[allow(
        dead_code,
        reason = "Retained for runtime-filter frontier fixture variants."
    )]
    fn fragment_edge(
        source_fragment_id: FragmentId,
        target_fragment_id: FragmentId,
        target_exchange_node_id: i32,
    ) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id,
            target_fragment_id,
            target_exchange_node_id,
            output_partition: DataPartition {
                kind: PartitionKind::Hash,
                exprs: vec![],
            },
            stream_kind: FragmentStreamKind::Partitioned,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![],
        }
    }

    // --- split_join_inputs: positive cases ---

    #[test]
    fn direct_two_exchange_join_partitions_inputs() {
        // fragment 1: HashJoin(probe=Exchange(20 <- frag 2), build=Exchange(30 <- frag 3))
        let join = hash_join(
            10,
            1,
            JoinKind::Inner,
            vec![exchange(20, 1, 2), exchange(30, 1, 3)],
        );
        let frag = fragment(1, join.clone());
        let split = split_join_inputs(&frag, &join).unwrap();
        assert_eq!(split.build_frontier, BTreeSet::from([fe(3, 30)]));
        assert_eq!(split.non_build_inputs, BTreeSet::from([fe(2, 20)]));
    }

    #[test]
    fn right_semi_join_swaps_probe_and_build() {
        // RightSemi: probe_child=1, build_child=0 (rf_sides_for_join).
        let join = hash_join(
            10,
            1,
            JoinKind::RightSemi,
            vec![exchange(20, 1, 2), exchange(30, 1, 3)],
        );
        let frag = fragment(1, join.clone());
        let split = split_join_inputs(&frag, &join).unwrap();
        assert_eq!(split.build_frontier, BTreeSet::from([fe(2, 20)]));
        assert_eq!(split.non_build_inputs, BTreeSet::from([fe(3, 30)]));
    }

    #[test]
    fn nested_join_in_build_subtree_contributes_both_sides() {
        // build child is itself a join; BOTH its inputs join the frontier.
        let inner = hash_join(
            11,
            1,
            JoinKind::Inner,
            vec![exchange(21, 1, 4), exchange(22, 1, 5)],
        );
        let join = hash_join(10, 1, JoinKind::Inner, vec![exchange(20, 1, 2), inner]);
        let frag = fragment(1, join.clone());
        let split = split_join_inputs(&frag, &join).unwrap();
        assert_eq!(split.build_frontier, BTreeSet::from([fe(4, 21), fe(5, 22)]));
        assert_eq!(split.non_build_inputs, BTreeSet::from([fe(2, 20)]));
    }

    #[test]
    fn local_build_subtree_yields_empty_frontier() {
        // Colocate-style: build side is fragment-local (Values leaf).
        let join = hash_join(
            10,
            1,
            JoinKind::Inner,
            vec![exchange(20, 1, 2), values(30, 1)],
        );
        let frag = fragment(1, join.clone());
        let split = split_join_inputs(&frag, &join).unwrap();
        assert!(split.build_frontier.is_empty());
        assert_eq!(split.non_build_inputs, BTreeSet::from([fe(2, 20)]));
    }

    #[test]
    fn inputs_outside_join_land_in_non_build() {
        // Join sits under another operator that has its own exchange input:
        // root=NestLoop-ish shape approximated with Values parent carrying
        // [join, exchange(40 <- frag 6)] children.
        let join = hash_join(
            10,
            1,
            JoinKind::Inner,
            vec![exchange(20, 1, 2), exchange(30, 1, 3)],
        );
        let root = node(
            9,
            1,
            DistributedNodeKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![join.clone(), exchange(40, 1, 6)],
        );
        let frag = fragment(1, root);
        let split = split_join_inputs(&frag, &join).unwrap();
        assert_eq!(split.build_frontier, BTreeSet::from([fe(3, 30)]));
        assert_eq!(
            split.non_build_inputs,
            BTreeSet::from([fe(2, 20), fe(6, 40)])
        );
    }

    // --- split_join_inputs: negative cases ---

    #[test]
    fn left_outer_join_has_no_rf_sides_and_is_skipped() {
        let join = hash_join(
            10,
            1,
            JoinKind::LeftOuter,
            vec![exchange(20, 1, 2), exchange(30, 1, 3)],
        );
        let frag = fragment(1, join.clone());
        assert_eq!(
            split_join_inputs(&frag, &join),
            Err(FrontierSkip::NoRfSides)
        );
    }

    #[test]
    fn missing_build_child_is_skipped() {
        let join = hash_join(10, 1, JoinKind::Inner, vec![exchange(20, 1, 2)]);
        let frag = fragment(1, join.clone());
        assert_eq!(
            split_join_inputs(&frag, &join),
            Err(FrontierSkip::MissingChild)
        );
    }

    #[test]
    fn audit_list_covers_every_current_kind() {
        // Existence guard: every current variant passed the (a)-(c) audit. New
        // variants are forced through the exhaustive match at compile time.
        assert!(fragment_local_input_closed(&DistributedNodeKind::Values(
            PlanValuesNode {
                rows: vec![],
                columns: vec![]
            }
        )));
    }
}
