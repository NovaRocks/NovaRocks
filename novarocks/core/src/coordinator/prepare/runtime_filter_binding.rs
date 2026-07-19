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
use std::num::NonZeroU32;
use std::sync::Arc;

use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, ComparatorDigest, CompletionRequirement,
    ConsumerActivation, ContributionKind, PlanNodeId, ReductionRequirement,
    RuntimeFilterLogicalDomain,
};
use crate::runtime_filter::model::graph::{
    ApplyPoint, ConsumerBindingTarget, RuntimeFilterBindingRole, RuntimeFilterBindingSpec,
    RuntimeFilterGraph,
};
use crate::runtime_filter::port::artifact::{ArtifactMembershipSchema, ArtifactSchemaDigest};
use crate::runtime_filter::port::ordered_bound::{
    OrderContractDigest, RuntimeOrderContract, RuntimeOrderKey,
};
use crate::runtime_filter::port::topk_summary::{
    RuntimeTopKSummaryContract, TopKSummaryContractDigest,
};
use crate::sql::analysis::TypedExpr;
use crate::sql::planner::distributed::{FragmentId, PlanFragment};

#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterBindingTable {
    fragment_id: FragmentId,
    bindings: BTreeMap<BindingId, PreparedRuntimeFilterBinding>,
}

impl RuntimeFilterBindingTable {
    pub(super) fn empty(fragment_id: FragmentId) -> Self {
        Self {
            fragment_id,
            bindings: BTreeMap::new(),
        }
    }

    pub(crate) const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub(crate) fn bindings(&self) -> impl ExactSizeIterator<Item = &PreparedRuntimeFilterBinding> {
        self.bindings.values()
    }

    pub(crate) fn binding(&self, binding_id: BindingId) -> Option<&PreparedRuntimeFilterBinding> {
        self.bindings.get(&binding_id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedRuntimeFilterBinding {
    binding_id: BindingId,
    channel_id: ChannelId,
    node_id: PlanNodeId,
    apply_point: ApplyPoint,
    expression: TypedExpr,
    contract: PreparedRuntimeFilterContract,
    reduction: PreparedReductionContract,
    role: PreparedRuntimeFilterBindingRole,
}

impl PreparedRuntimeFilterBinding {
    pub(crate) const fn binding_id(&self) -> BindingId {
        self.binding_id
    }

    pub(crate) const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    pub(crate) const fn node_id(&self) -> PlanNodeId {
        self.node_id
    }

    pub(crate) const fn apply_point(&self) -> ApplyPoint {
        self.apply_point
    }

    pub(crate) const fn expression(&self) -> &TypedExpr {
        &self.expression
    }

    pub(crate) const fn contract(&self) -> &PreparedRuntimeFilterContract {
        &self.contract
    }

    pub(crate) const fn reduction(&self) -> &PreparedReductionContract {
        &self.reduction
    }

    pub(crate) const fn role(&self) -> &PreparedRuntimeFilterBindingRole {
        &self.role
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PreparedRuntimeFilterContract {
    Membership {
        canonical_schema: Arc<[u8]>,
        schema_digest: ArtifactSchemaDigest,
    },
    Ordered {
        keys: Arc<[RuntimeOrderKey]>,
        comparator_digest: ComparatorDigest,
        order_contract_digest: OrderContractDigest,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PreparedReductionContract {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary {
        k: NonZeroU32,
        contract_digest: TopKSummaryContractDigest,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PreparedRuntimeFilterBindingRole {
    Producer {
        contribution_kinds: BTreeSet<ContributionKind>,
        completion_requirement: CompletionRequirement,
        join_key_ordinal: usize,
    },
    Consumer {
        capabilities: BTreeSet<ArtifactCapability>,
        activation: ConsumerActivation,
        target: ConsumerBindingTarget,
    },
}

pub(super) fn materialize_runtime_filter_binding_tables(
    graph: &RuntimeFilterGraph,
    fragments: &[PlanFragment],
) -> Result<BTreeMap<FragmentId, RuntimeFilterBindingTable>, String> {
    let mut tables = BTreeMap::new();
    for fragment in fragments {
        if tables
            .insert(
                fragment.fragment_id,
                RuntimeFilterBindingTable::empty(fragment.fragment_id),
            )
            .is_some()
        {
            return Err(format!(
                "runtime filter binding materialization found duplicate fragment id={}",
                fragment.fragment_id
            ));
        }
    }

    let mut pending = graph
        .bindings()
        .map(|binding| (binding.binding_id, binding))
        .collect::<BTreeMap<_, _>>();

    fn visit(
        graph: &RuntimeFilterGraph,
        pending: &mut BTreeMap<BindingId, &RuntimeFilterBindingSpec>,
        table: &mut RuntimeFilterBindingTable,
        node: &crate::sql::planner::distributed::DistributedNode,
    ) -> Result<(), String> {
        for binding_id in &node.runtime_filter_binding_ids {
            let Some(binding) = pending.remove(binding_id) else {
                return Err(if graph.binding(*binding_id).is_some() {
                    format!(
                        "runtime filter binding id={} is attached more than once",
                        binding_id.get()
                    )
                } else {
                    format!(
                        "runtime filter attachment references unknown binding id={}",
                        binding_id.get()
                    )
                });
            };
            if binding.location.fragment_id.get() != table.fragment_id
                || binding.location.node_id.get() != node.node_id
            {
                return Err(format!(
                    "runtime filter binding id={} location fragment_id={} node_id={} does not match attachment fragment_id={} node_id={}",
                    binding_id.get(),
                    binding.location.fragment_id.get(),
                    binding.location.node_id.get(),
                    table.fragment_id,
                    node.node_id
                ));
            }
            match (&binding.role, binding.apply_point) {
                (RuntimeFilterBindingRole::Producer(_), ApplyPoint::NodeOutput)
                | (RuntimeFilterBindingRole::Consumer(_), ApplyPoint::NodeInput) => {}
                (RuntimeFilterBindingRole::Producer(_), apply_point) => {
                    return Err(format!(
                        "runtime filter producer binding id={} must use NodeOutput, found {apply_point:?}",
                        binding_id.get()
                    ));
                }
                (RuntimeFilterBindingRole::Consumer(_), apply_point) => {
                    return Err(format!(
                        "runtime filter consumer binding id={} must use NodeInput, found {apply_point:?}",
                        binding_id.get()
                    ));
                }
            }
            let channel = graph.channel(binding.channel_id).ok_or_else(|| {
                format!(
                    "runtime filter binding id={} references unknown channel id={}",
                    binding_id.get(),
                    binding.channel_id.get()
                )
            })?;
            validate_expression_type(binding, &channel.logical_domain)?;
            let contract = materialize_contract(&channel.logical_domain).map_err(|error| {
                format!(
                    "runtime filter binding id={} has invalid canonical contract: {error}",
                    binding_id.get()
                )
            })?;
            let reduction =
                materialize_reduction(&channel.logical_domain, channel.reduction_requirement)
                    .map_err(|error| {
                        format!(
                            "runtime filter binding id={} has invalid reduction contract: {error}",
                            binding_id.get()
                        )
                    })?;
            let role = match &binding.role {
                RuntimeFilterBindingRole::Producer(requirement) => {
                    PreparedRuntimeFilterBindingRole::Producer {
                        contribution_kinds: requirement.contribution_kinds.clone(),
                        completion_requirement: requirement.completion_requirement,
                        join_key_ordinal: requirement.join_key_ordinal,
                    }
                }
                RuntimeFilterBindingRole::Consumer(requirement) => {
                    PreparedRuntimeFilterBindingRole::Consumer {
                        capabilities: requirement.capabilities.clone(),
                        activation: requirement.activation,
                        target: requirement.target,
                    }
                }
            };
            let prepared = PreparedRuntimeFilterBinding {
                binding_id: *binding_id,
                channel_id: binding.channel_id,
                node_id: binding.location.node_id,
                apply_point: binding.apply_point,
                expression: binding.expression.clone(),
                contract,
                reduction,
                role,
            };
            if table.bindings.insert(*binding_id, prepared).is_some() {
                return Err(format!(
                    "runtime filter binding id={} materialized more than once",
                    binding_id.get()
                ));
            }
        }
        for child in &node.children {
            visit(graph, pending, table, child)?;
        }
        Ok(())
    }

    for fragment in fragments {
        let table = tables
            .get_mut(&fragment.fragment_id)
            .expect("table was initialized from the same fragment list");
        visit(graph, &mut pending, table, &fragment.root)?;
    }
    if let Some((binding_id, binding)) = pending.first_key_value() {
        return Err(format!(
            "runtime filter graph binding id={} at fragment_id={} node_id={} has no node attachment",
            binding_id.get(),
            binding.location.fragment_id.get(),
            binding.location.node_id.get()
        ));
    }
    Ok(tables)
}

fn validate_expression_type(
    binding: &RuntimeFilterBindingSpec,
    domain: &RuntimeFilterLogicalDomain,
) -> Result<(), String> {
    let matches = match domain {
        RuntimeFilterLogicalDomain::Membership { value_type, .. } => {
            value_type == &binding.expression.data_type
        }
        RuntimeFilterLogicalDomain::OrderedBound(order) => {
            order.keys.len() == 1 && order.keys[0].data_type == binding.expression.data_type
        }
    };
    if matches {
        Ok(())
    } else {
        Err(format!(
            "runtime filter binding id={} expression type {:?} does not match channel domain",
            binding.binding_id.get(),
            binding.expression.data_type
        ))
    }
}

fn materialize_contract(
    domain: &RuntimeFilterLogicalDomain,
) -> Result<PreparedRuntimeFilterContract, String> {
    match domain {
        RuntimeFilterLogicalDomain::Membership {
            value_type,
            null_semantics,
        } => {
            let schema = ArtifactMembershipSchema::new(value_type, *null_semantics)
                .map_err(|error| format!("membership schema error: {error}"))?;
            Ok(PreparedRuntimeFilterContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest(),
            })
        }
        RuntimeFilterLogicalDomain::OrderedBound(plan) => {
            let contract = RuntimeOrderContract::try_from_plan(plan)
                .map_err(|error| format!("order contract error: {error:?}"))?;
            Ok(PreparedRuntimeFilterContract::Ordered {
                keys: Arc::from(contract.keys()),
                comparator_digest: contract.plan_comparator_digest(),
                order_contract_digest: contract.digest(),
            })
        }
    }
}

fn materialize_reduction(
    domain: &RuntimeFilterLogicalDomain,
    reduction: ReductionRequirement,
) -> Result<PreparedReductionContract, String> {
    match reduction {
        ReductionRequirement::SetUnion => Ok(PreparedReductionContract::SetUnion),
        ReductionRequirement::TightenOrderedBound => {
            Ok(PreparedReductionContract::TightenOrderedBound)
        }
        ReductionRequirement::MergeTopKSummary(requirement) => {
            let RuntimeFilterLogicalDomain::OrderedBound(order) = domain else {
                return Err("MergeTopKSummary requires an ordered domain".to_string());
            };
            let contract = RuntimeTopKSummaryContract::try_from_plan(order, requirement)
                .map_err(|error| format!("TopK summary contract error: {error:?}"))?;
            Ok(PreparedReductionContract::MergeTopKSummary {
                k: contract.k(),
                contract_digest: contract.digest(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder, NullSemantics,
        OrderContract, OrderKeyContract, PlanFragmentId, PlanNodeId, ReductionRequirement,
        RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
        SortDirection, TopKSummaryRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::model::graph::{
        ApplyPoint, ConsumerRequirement, PlanLocation, ProducerRequirement,
        RuntimeFilterBindingRole, RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
        RuntimeFilterGraph,
    };
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, RuntimeOrderContract, comparator_digest_for_test,
    };
    use crate::runtime_filter::port::topk_summary::RuntimeTopKSummaryContract;
    use crate::sql::analysis::{ExprKind, LiteralValue};
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    fn expression(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn node(fragment_id: FragmentId, node_id: i32, ids: Vec<BindingId>) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: ids,
            children: Vec::new(),
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
        }
    }

    fn node_with_children(
        fragment_id: FragmentId,
        node_id: i32,
        ids: Vec<BindingId>,
        children: Vec<DistributedNode>,
    ) -> DistributedNode {
        let mut node = node(fragment_id, node_id, ids);
        node.children = children;
        node
    }

    fn fragment(fragment_id: FragmentId, root: DistributedNode) -> PlanFragment {
        PlanFragment {
            fragment_id,
            root,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }
    }

    fn channel(
        channel_id: ChannelId,
        logical_domain: RuntimeFilterLogicalDomain,
        reduction_requirement: ReductionRequirement,
    ) -> RuntimeFilterChannelSpec {
        let (lifecycle, allowed_contribution_kinds, required_consumer_capabilities) =
            match (&logical_domain, reduction_requirement) {
                (RuntimeFilterLogicalDomain::Membership { .. }, ReductionRequirement::SetUnion) => {
                    (
                        RuntimeFilterLifecycle::CompleteOnce,
                        BTreeSet::from([
                            ContributionKind::ValueDomainDelta,
                            ContributionKind::ProducerClosed,
                        ]),
                        BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                    )
                }
                (
                    RuntimeFilterLogicalDomain::OrderedBound(_),
                    ReductionRequirement::TightenOrderedBound,
                ) => (
                    RuntimeFilterLifecycle::MonotonicUpdates,
                    BTreeSet::from([
                        ContributionKind::OrderedBoundUpdate,
                        ContributionKind::ProducerClosed,
                    ]),
                    BTreeSet::from([ArtifactCapability::OrderedRange]),
                ),
                (
                    RuntimeFilterLogicalDomain::OrderedBound(_),
                    ReductionRequirement::MergeTopKSummary(_),
                ) => (
                    RuntimeFilterLifecycle::MonotonicUpdates,
                    BTreeSet::from([
                        ContributionKind::TopKSummary,
                        ContributionKind::ProducerClosed,
                    ]),
                    BTreeSet::from([ArtifactCapability::OrderedRange]),
                ),
                _ => panic!("test channel contract must be semantically compatible"),
            };
        RuntimeFilterChannelSpec {
            channel_id,
            logical_domain,
            lifecycle,
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            reduction_requirement,
            allowed_contribution_kinds,
            required_consumer_capabilities,
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1,
                max_artifact_bytes: 1,
                deadline_ms: 1,
                max_retries: 0,
            },
        }
    }

    fn membership_channel(channel_id: ChannelId) -> RuntimeFilterChannelSpec {
        channel(
            channel_id,
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            ReductionRequirement::SetUnion,
        )
    }

    fn order_contract() -> OrderContract {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Descending,
            null_order: NullOrder::First,
        }];
        OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        }
    }

    fn producer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
        fragment_id: u32,
        node_id: i32,
    ) -> RuntimeFilterBindingSpec {
        producer_binding_with_kinds(
            binding_id,
            channel_id,
            fragment_id,
            node_id,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
        )
    }

    fn producer_binding_with_kinds(
        binding_id: BindingId,
        channel_id: ChannelId,
        fragment_id: u32,
        node_id: i32,
        contribution_kinds: BTreeSet<ContributionKind>,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id,
            channel_id,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment_id),
                node_id: PlanNodeId::new(node_id),
            },
            expression: expression(i64::from(binding_id.get())),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                contribution_kinds,
                completion_requirement: CompletionRequirement::ProducerClosed,
                join_key_ordinal: 0,
            }),
        }
    }

    fn consumer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
        fragment_id: u32,
        node_id: i32,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id,
            channel_id,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment_id),
                node_id: PlanNodeId::new(node_id),
            },
            expression: expression(i64::from(binding_id.get())),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                activation: ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                target: crate::runtime_filter::model::graph::ConsumerBindingTarget::SourceBoundary,
            }),
        }
    }

    fn graph_with(
        channels: Vec<RuntimeFilterChannelSpec>,
        bindings: Vec<RuntimeFilterBindingSpec>,
    ) -> RuntimeFilterGraph {
        let mut graph = RuntimeFilterGraph::default();
        for channel in channels {
            graph.insert_channel(channel).unwrap();
        }
        for binding in bindings {
            graph.insert_binding(binding).unwrap();
        }
        graph
    }

    #[test]
    fn preparation_projects_every_graph_binding_exactly_once() {
        let channel_id = ChannelId::new(9);
        let binding_id = BindingId::new(7);
        let consumer_id = BindingId::new(8);
        let graph = graph_with(
            vec![membership_channel(channel_id)],
            vec![
                producer_binding(binding_id, channel_id, 1, 10),
                consumer_binding(consumer_id, channel_id, 1, 11),
            ],
        );

        let tables = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(
                1,
                node_with_children(
                    1,
                    10,
                    vec![binding_id],
                    vec![node(1, 11, vec![consumer_id])],
                ),
            )],
        )
        .unwrap();

        assert_eq!(tables[&1].bindings().len(), 2);
        assert_eq!(
            tables[&1].bindings().next().unwrap().binding_id(),
            binding_id
        );
        let producer = tables[&1].binding(binding_id).unwrap();
        assert_eq!(producer.channel_id(), channel_id);
        assert_eq!(producer.node_id(), PlanNodeId::new(10));
        assert_eq!(producer.apply_point(), ApplyPoint::NodeOutput);
        assert!(matches!(
            producer.role(),
            PreparedRuntimeFilterBindingRole::Producer {
                completion_requirement: CompletionRequirement::ProducerClosed,
                ..
            }
        ));
        let consumer = tables[&1].binding(consumer_id).unwrap();
        assert_eq!(consumer.node_id(), PlanNodeId::new(11));
        assert_eq!(consumer.apply_point(), ApplyPoint::NodeInput);
        assert!(matches!(
            consumer.role(),
            PreparedRuntimeFilterBindingRole::Consumer {
                activation: ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                ..
            }
        ));
        assert_eq!(consumer.expression().data_type, DataType::Int64);

        let duplicate_error = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(
                1,
                node_with_children(1, 10, vec![binding_id], vec![node(1, 11, vec![binding_id])]),
            )],
        )
        .unwrap_err();
        assert!(
            duplicate_error.contains("binding id=7 is attached more than once"),
            "{duplicate_error}"
        );
    }

    #[test]
    fn preparation_partitions_bindings_by_fragment_and_sorts_by_binding_id() {
        let channel_id = ChannelId::new(9);
        let ids = [BindingId::new(30), BindingId::new(10), BindingId::new(20)];
        let graph = graph_with(
            vec![membership_channel(channel_id)],
            vec![
                producer_binding(ids[0], channel_id, 2, 20),
                producer_binding(ids[1], channel_id, 1, 11),
                producer_binding(ids[2], channel_id, 1, 12),
            ],
        );
        let fragments = vec![
            fragment(
                2,
                node_with_children(2, 20, vec![ids[0]], vec![node(2, 21, Vec::new())]),
            ),
            fragment(
                1,
                node_with_children(
                    1,
                    10,
                    Vec::new(),
                    vec![node(1, 12, vec![ids[2]]), node(1, 11, vec![ids[1]])],
                ),
            ),
            fragment(3, node(3, 30, Vec::new())),
        ];

        let tables = materialize_runtime_filter_binding_tables(&graph, &fragments).unwrap();

        assert_eq!(tables.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(
            tables[&1]
                .bindings()
                .map(PreparedRuntimeFilterBinding::binding_id)
                .collect::<Vec<_>>(),
            vec![ids[1], ids[2]]
        );
        assert_eq!(tables[&2].bindings().next().unwrap().binding_id(), ids[0]);
        assert_eq!(tables[&3].fragment_id(), 3);
        assert!(tables[&3].is_empty());
    }

    #[test]
    fn preparation_rejects_attachment_without_graph_binding() {
        let error = materialize_runtime_filter_binding_tables(
            &RuntimeFilterGraph::default(),
            &[fragment(1, node(1, 10, vec![BindingId::new(7)]))],
        )
        .unwrap_err();
        assert!(error.contains("unknown binding id=7"), "{error}");
    }

    #[test]
    fn preparation_rejects_graph_binding_without_attachment() {
        let channel_id = ChannelId::new(9);
        let graph = graph_with(
            vec![membership_channel(channel_id)],
            vec![producer_binding(BindingId::new(7), channel_id, 1, 10)],
        );
        let error = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(1, node(1, 10, Vec::new()))],
        )
        .unwrap_err();
        assert!(error.contains("binding id=7"), "{error}");
        assert!(error.contains("has no node attachment"), "{error}");
    }

    #[test]
    fn preparation_rejects_wrong_fragment_node_role_and_apply_point() {
        let channel_id = ChannelId::new(9);
        let binding_id = BindingId::new(7);
        let cases = [
            (
                producer_binding(binding_id, channel_id, 2, 10),
                "does not match attachment fragment_id=1 node_id=10",
            ),
            (
                producer_binding(binding_id, channel_id, 1, 11),
                "does not match attachment fragment_id=1 node_id=10",
            ),
            (
                {
                    let mut binding = producer_binding(binding_id, channel_id, 1, 10);
                    binding.apply_point = ApplyPoint::NodeInput;
                    binding
                },
                "producer binding id=7 must use NodeOutput",
            ),
            (
                {
                    let mut binding = consumer_binding(binding_id, channel_id, 1, 10);
                    binding.apply_point = ApplyPoint::NodeOutput;
                    binding
                },
                "consumer binding id=7 must use NodeInput",
            ),
        ];
        for (binding, expected) in cases {
            let graph = graph_with(vec![membership_channel(channel_id)], vec![binding]);
            let error = materialize_runtime_filter_binding_tables(
                &graph,
                &[fragment(1, node(1, 10, vec![binding_id]))],
            )
            .unwrap_err();
            assert!(
                error.contains(expected),
                "expected {expected:?}, got {error:?}"
            );
        }
    }

    #[test]
    fn preparation_materializes_membership_canonical_schema_and_digest() {
        let channel_id = ChannelId::new(9);
        let binding_id = BindingId::new(7);
        let graph = graph_with(
            vec![membership_channel(channel_id)],
            vec![producer_binding(binding_id, channel_id, 1, 10)],
        );
        let tables = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(1, node(1, 10, vec![binding_id]))],
        )
        .unwrap();
        let expected =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let PreparedRuntimeFilterContract::Membership {
            canonical_schema,
            schema_digest,
        } = tables[&1].binding(binding_id).unwrap().contract()
        else {
            panic!("membership channel must materialize a membership contract");
        };
        assert_eq!(canonical_schema.as_ref(), expected.canonical_bytes());
        assert_eq!(*schema_digest, expected.digest());
    }

    #[test]
    fn preparation_materializes_order_keys_comparator_and_order_digest() {
        let channel_id = ChannelId::new(9);
        let binding_id = BindingId::new(7);
        let order = order_contract();
        let expected = RuntimeOrderContract::try_from_plan(&order).unwrap();
        let graph = graph_with(
            vec![channel(
                channel_id,
                RuntimeFilterLogicalDomain::OrderedBound(order),
                ReductionRequirement::TightenOrderedBound,
            )],
            vec![producer_binding(binding_id, channel_id, 1, 10)],
        );
        let tables = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(1, node(1, 10, vec![binding_id]))],
        )
        .unwrap();
        let PreparedRuntimeFilterContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        } = tables[&1].binding(binding_id).unwrap().contract()
        else {
            panic!("ordered channel must materialize an ordered contract");
        };
        assert_eq!(keys.as_ref(), expected.keys());
        assert_eq!(*comparator_digest, expected.plan_comparator_digest());
        assert_eq!(*order_contract_digest, expected.digest());
    }

    #[test]
    fn preparation_preserves_set_union_tighten_and_topk_k_plus_digest() {
        let order = order_contract();
        let topk_requirement = TopKSummaryRequirement::try_new(13).unwrap();
        let expected_topk =
            RuntimeTopKSummaryContract::try_from_plan(&order, topk_requirement).unwrap();
        let channel_ids = [ChannelId::new(1), ChannelId::new(2), ChannelId::new(3)];
        let binding_ids = [BindingId::new(1), BindingId::new(2), BindingId::new(3)];
        let graph = graph_with(
            vec![
                membership_channel(channel_ids[0]),
                channel(
                    channel_ids[1],
                    RuntimeFilterLogicalDomain::OrderedBound(order.clone()),
                    ReductionRequirement::TightenOrderedBound,
                ),
                channel(
                    channel_ids[2],
                    RuntimeFilterLogicalDomain::OrderedBound(order),
                    ReductionRequirement::MergeTopKSummary(topk_requirement),
                ),
            ],
            vec![
                producer_binding(binding_ids[0], channel_ids[0], 1, 10),
                producer_binding_with_kinds(
                    binding_ids[1],
                    channel_ids[1],
                    1,
                    10,
                    BTreeSet::from([
                        ContributionKind::OrderedBoundUpdate,
                        ContributionKind::ProducerClosed,
                    ]),
                ),
                producer_binding_with_kinds(
                    binding_ids[2],
                    channel_ids[2],
                    1,
                    10,
                    BTreeSet::from([
                        ContributionKind::TopKSummary,
                        ContributionKind::ProducerClosed,
                    ]),
                ),
            ],
        );
        let tables = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(1, node(1, 10, binding_ids.to_vec()))],
        )
        .unwrap();
        assert_eq!(
            tables[&1].binding(binding_ids[0]).unwrap().reduction(),
            &PreparedReductionContract::SetUnion
        );
        assert_eq!(
            tables[&1].binding(binding_ids[1]).unwrap().reduction(),
            &PreparedReductionContract::TightenOrderedBound
        );
        assert_eq!(
            tables[&1].binding(binding_ids[2]).unwrap().reduction(),
            &PreparedReductionContract::MergeTopKSummary {
                k: topk_requirement.k(),
                contract_digest: expected_topk.digest(),
            }
        );
    }
}
