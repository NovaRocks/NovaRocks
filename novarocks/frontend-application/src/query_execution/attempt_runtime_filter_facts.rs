// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! What deploying one plan's runtime filters reads about it.
//!
//! Three tables: the channels an artifact can be built for, where each
//! binding sits and what role it plays there, and which exchange edges must
//! close before a producer is done. Both plan representations state them, so
//! deployment stops depending on which one built the execution.

use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    EdgeId, FragmentId as PhysicalFragmentId, PhysicalPlan, RuntimeFilter,
    RuntimeFilterApplyPoint as PhysicalApplyPoint, RuntimeFilterCoverage,
    RuntimeFilterCoverageNode, RuntimeFilterProducer,
    RuntimeFilterProducerTarget as PhysicalProducerTarget,
};
use novarocks_plan_codec::{
    PhysicalV1RuntimeFilterBindingRole, physical_v1_runtime_filter_bindings,
    physical_v1_runtime_filter_comparator_digest,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterApplyPoint {
    NodeInput,
    NodeOutput,
}
#[derive(Clone, Debug)]
pub(crate) enum AttemptRuntimeFilterLogicalDomainFacts {
    Membership {
        value_type: DataType,
        null_semantics: AttemptRuntimeFilterNullSemantics,
    },
    Ordered {
        keys: Vec<AttemptRuntimeFilterOrderKeyFacts>,
        inclusive: bool,
        comparator_digest: [u8; 32],
    },
}
#[derive(Clone, Debug)]
pub(crate) struct AttemptRuntimeFilterOrderKeyFacts {
    pub data_type: DataType,
    pub direction: AttemptRuntimeFilterSortDirection,
    pub null_order: AttemptRuntimeFilterNullOrder,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterNullSemantics {
    NeverMatches,
    NullSafeEqual,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterSortDirection {
    Ascending,
    Descending,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterNullOrder {
    First,
    Last,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterReductionFacts {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: u32 },
}
#[derive(Clone, Debug)]
pub(crate) enum AttemptRuntimeFilterBindingRoleFacts {
    Producer {
        contribution_kinds: Vec<AttemptRuntimeFilterContributionKind>,
        completion_requirement: AttemptRuntimeFilterCompletionRequirement,
        target: AttemptRuntimeFilterProducerTarget,
    },
    Consumer {
        capabilities: Vec<AttemptRuntimeFilterArtifactCapability>,
        activation: AttemptRuntimeFilterConsumerActivation,
        target: AttemptRuntimeFilterConsumerTarget,
        feedback_scan_type: Option<novarocks_physical_plan::ValueType>,
    },
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterContributionKind {
    ValueDomainDelta,
    FinalDomainShard,
    OrderedBoundUpdate,
    TopKSummary,
    ProducerClosed,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterCompletionRequirement {
    ProducerClosed,
    FencedCommittedDomainFrozen,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterArtifactCapability {
    Membership,
    OrderedRange,
    EmptyDomain,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive(AttemptRuntimeFilterLateApplyGranularity),
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterLateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterProducerTarget {
    JoinBuildKey { ordinal: u32 },
    AggregateTopNKey { group_key_ordinal: u32, limit: u32 },
}
#[derive(Clone, Debug)]
pub(crate) enum AttemptRuntimeFilterConsumerTarget {
    DirectInput { input_ordinal: u32 },
    SourceBoundary,
}

#[derive(Clone, Debug)]
pub(crate) struct AttemptRuntimeFilterChannelFacts {
    pub channel_id: u32,
    pub logical_domain: AttemptRuntimeFilterLogicalDomainFacts,
    pub lifecycle: AttemptRuntimeFilterLifecycleFacts,
    pub availability_coverage: AttemptRuntimeFilterCoverageFacts,
    pub terminal_coverage: AttemptRuntimeFilterCoverageFacts,
    pub reduction: AttemptRuntimeFilterReductionFacts,
    pub allowed_contribution_kinds: Vec<AttemptRuntimeFilterContributionKind>,
    pub required_consumer_capabilities: Vec<AttemptRuntimeFilterArtifactCapability>,
    pub policy: AttemptRuntimeFilterPolicyFacts,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterLifecycleFacts {
    CompleteOnce,
    MonotonicUpdates,
}
#[derive(Clone, Debug)]
pub(crate) enum AttemptRuntimeFilterCoverageFacts {
    LeafWitnessId(u32),
    AllOf(Vec<AttemptRuntimeFilterCoverageFacts>),
    AnyOf(Vec<AttemptRuntimeFilterCoverageFacts>),
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct AttemptRuntimeFilterPolicyFacts {
    pub max_contribution_bytes: u64,
    pub max_artifact_bytes: u64,
    pub deadline_ms: u64,
    pub max_retries: u32,
}
#[derive(Clone, Debug)]
pub(crate) struct AttemptRuntimeFilterDeploymentBindingFacts {
    pub binding_id: u32,
    pub channel_id: u32,
    pub fragment_id: u32,
    pub node_id: i32,
    pub coverage_witness_id: Option<u32>,
    pub role: AttemptRuntimeFilterBindingRoleFacts,
}
#[derive(Clone, Debug)]
pub(crate) enum AttemptRuntimeFilterJoinProgressFacts {
    Proven {
        channel_id: u32,
        producer_binding_id: u32,
        producer_fragment_id: u32,
        join_node_id: i32,
        build_frontier: Vec<AttemptRuntimeFilterFrontierEdgeFacts>,
        non_build_inputs: Vec<AttemptRuntimeFilterFrontierEdgeFacts>,
    },
    Skipped {
        channel_id: u32,
        producer_binding_id: u32,
        producer_fragment_id: u32,
        join_node_id: i32,
        reason: AttemptRuntimeFilterJoinProgressSkipReason,
    },
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct AttemptRuntimeFilterFrontierEdgeFacts {
    pub source_fragment_id: u32,
    pub target_exchange_node_id: i32,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttemptRuntimeFilterJoinProgressSkipReason {
    NoRfSides,
    MissingChild,
    UnauditedNode { node_id: i32 },
}

/// One plan's runtime filters, as the attempt that deploys them reads them.
#[derive(Clone, Debug, Default)]
pub(crate) struct AttemptRuntimeFilterFacts {
    channels: Vec<AttemptRuntimeFilterChannelFacts>,
    deployment_bindings: Vec<AttemptRuntimeFilterDeploymentBindingFacts>,
    join_progress: Vec<AttemptRuntimeFilterJoinProgressFacts>,
}

impl AttemptRuntimeFilterFacts {
    /// The same facts, from a completed plan's own runtime-filter contract.
    ///
    /// The per-fragment binding table is deliberately absent: a completed
    /// plan's fragments already carry their encoded bindings, written by the
    /// same numbering this projection joins on, so nothing needs to encode
    /// them a second time from these facts.
    pub(crate) fn from_completed(plan: &PhysicalPlan) -> Result<Self, String> {
        let mut channels = Vec::with_capacity(plan.runtime_filters().len());
        for filter in plan.runtime_filters().values() {
            channels.push(completed_channel(filter)?);
        }
        let mut deployment_bindings = Vec::new();
        let mut join_progress = Vec::new();
        for binding in physical_v1_runtime_filter_bindings(plan)? {
            let filter = &plan.runtime_filters()[&binding.filter];
            let (role, witness) = match binding.role {
                PhysicalV1RuntimeFilterBindingRole::Producer(index) => {
                    let producer = &filter.producers[index];
                    join_progress.push(completed_join_progress(
                        plan,
                        filter,
                        producer,
                        binding.binding_id,
                        binding.fragment,
                    )?);
                    (
                        completed_producer_role(filter, producer)?,
                        Some(producer.witness.get()),
                    )
                }
                PhysicalV1RuntimeFilterBindingRole::Consumer(index) => {
                    let consumer = &filter.consumers[index];
                    let scan_type = match consumer.target {
                        novarocks_physical_plan::RuntimeFilterConsumerTarget::ScanField { .. }
                        | novarocks_physical_plan::RuntimeFilterConsumerTarget::AggregateTopNScanField { .. } => {
                            let value = consumer.endpoint.values.first().ok_or_else(|| {
                                format!("runtime filter {} scan consumer has no value", filter.id.get())
                            })?;
                            let fragment = plan.fragments().get(&consumer.endpoint.fragment).ok_or_else(|| {
                                format!("runtime filter {} scan consumer names an absent fragment", filter.id.get())
                            })?;
                            let value = fragment.values().get(value).ok_or_else(|| {
                                format!("runtime filter {} scan consumer names an absent value", filter.id.get())
                            })?;
                            Some(&value.ty)
                        }
                        novarocks_physical_plan::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => None,
                    };
                    (completed_consumer_role(consumer, scan_type)?, None)
                }
            };
            deployment_bindings.push(AttemptRuntimeFilterDeploymentBindingFacts {
                binding_id: binding.binding_id,
                channel_id: filter.id.get(),
                fragment_id: binding.fragment.get(),
                node_id: wire_node_id(binding.node.get())?,
                coverage_witness_id: witness,
                role,
            });
        }
        Ok(Self {
            channels,
            deployment_bindings,
            join_progress,
        })
    }

    pub(crate) fn channels(&self) -> &[AttemptRuntimeFilterChannelFacts] {
        &self.channels
    }

    pub(crate) fn deployment_bindings(&self) -> &[AttemptRuntimeFilterDeploymentBindingFacts] {
        &self.deployment_bindings
    }

    pub(crate) fn join_progress(&self) -> &[AttemptRuntimeFilterJoinProgressFacts] {
        &self.join_progress
    }

    pub(crate) fn has_channels(&self) -> bool {
        !self.channels.is_empty()
    }
}

fn wire_node_id(node: u32) -> Result<i32, String> {
    i32::try_from(node).map_err(|_| format!("runtime filter node {node} exceeds the wire identity"))
}

fn completed_channel(filter: &RuntimeFilter) -> Result<AttemptRuntimeFilterChannelFacts, String> {
    use novarocks_physical_plan::{
        RuntimeFilterDomain, RuntimeFilterLifecycle, RuntimeFilterNullSemantics,
    };

    let logical_domain = match &filter.domain {
        RuntimeFilterDomain::Membership { ty, null_semantics } => {
            AttemptRuntimeFilterLogicalDomainFacts::Membership {
                value_type: ty.data_type.clone(),
                null_semantics: match null_semantics {
                    RuntimeFilterNullSemantics::NeverMatches => {
                        AttemptRuntimeFilterNullSemantics::NeverMatches
                    }
                    RuntimeFilterNullSemantics::NullSafeEqual => {
                        AttemptRuntimeFilterNullSemantics::NullSafeEqual
                    }
                },
            }
        }
        RuntimeFilterDomain::Ordered { key, inclusive, .. } => {
            let comparator_digest = physical_v1_runtime_filter_comparator_digest(filter)?
                .ok_or_else(|| {
                    format!(
                        "ordered runtime filter {} has no comparator digest",
                        filter.id.get()
                    )
                })?;
            AttemptRuntimeFilterLogicalDomainFacts::Ordered {
                keys: vec![AttemptRuntimeFilterOrderKeyFacts {
                    data_type: key.ty.data_type.clone(),
                    direction: match key.direction {
                        novarocks_physical_plan::SortDirection::Ascending => {
                            AttemptRuntimeFilterSortDirection::Ascending
                        }
                        novarocks_physical_plan::SortDirection::Descending => {
                            AttemptRuntimeFilterSortDirection::Descending
                        }
                    },
                    null_order: match key.null_ordering {
                        novarocks_physical_plan::NullOrdering::First => {
                            AttemptRuntimeFilterNullOrder::First
                        }
                        novarocks_physical_plan::NullOrdering::Last => {
                            AttemptRuntimeFilterNullOrder::Last
                        }
                    },
                }],
                inclusive: *inclusive,
                comparator_digest,
            }
        }
    };
    Ok(AttemptRuntimeFilterChannelFacts {
        channel_id: filter.id.get(),
        logical_domain,
        lifecycle: match filter.lifecycle {
            RuntimeFilterLifecycle::CompleteOnce => {
                AttemptRuntimeFilterLifecycleFacts::CompleteOnce
            }
            RuntimeFilterLifecycle::MonotonicUpdates => {
                AttemptRuntimeFilterLifecycleFacts::MonotonicUpdates
            }
        },
        availability_coverage: coverage_tree(&filter.availability_coverage)?,
        terminal_coverage: coverage_tree(&filter.terminal_coverage)?,
        reduction: completed_reduction(filter)?,
        allowed_contribution_kinds: completed_contribution_kinds(filter),
        required_consumer_capabilities: completed_consumer_capabilities(filter),
        policy: AttemptRuntimeFilterPolicyFacts {
            max_contribution_bytes: filter.policy.max_contribution_bytes,
            max_artifact_bytes: filter.policy.max_artifact_bytes,
            deadline_ms: filter.policy.deadline_ms,
            max_retries: filter.policy.max_retries,
        },
    })
}

/// Every contribution kind some producer of this filter may send, once each.
///
/// The channel states what it accepts; each producer states what it sends.
/// A completed plan says the second, so the first is their union -- listed in
/// one fixed order so two plans with the same producers describe the same
/// channel.
fn completed_contribution_kinds(
    filter: &RuntimeFilter,
) -> Vec<AttemptRuntimeFilterContributionKind> {
    use novarocks_physical_plan::RuntimeFilterContributionKind as Physical;

    let mut seen = [false; 5];
    for producer in &filter.producers {
        for kind in &producer.contribution_kinds {
            seen[match kind {
                Physical::ValueDomainDelta => 0,
                Physical::FinalDomainShard => 1,
                Physical::OrderedBoundUpdate => 2,
                // The plan names the shape of what a TopN producer sends; the
                // runtime that receives it has always called that a top-k
                // summary.
                Physical::FinalOrderedHullShard => 3,
                Physical::ProducerClosed => 4,
            }] = true;
        }
    }
    [
        AttemptRuntimeFilterContributionKind::ValueDomainDelta,
        AttemptRuntimeFilterContributionKind::FinalDomainShard,
        AttemptRuntimeFilterContributionKind::OrderedBoundUpdate,
        AttemptRuntimeFilterContributionKind::TopKSummary,
        AttemptRuntimeFilterContributionKind::ProducerClosed,
    ]
    .into_iter()
    .enumerate()
    .filter_map(|(index, kind)| seen[index].then_some(kind))
    .collect()
}

/// Every artifact capability some consumer of this filter needs, once each.
fn completed_consumer_capabilities(
    filter: &RuntimeFilter,
) -> Vec<AttemptRuntimeFilterArtifactCapability> {
    use novarocks_physical_plan::RuntimeFilterArtifactCapability as Physical;

    let mut seen = [false; 3];
    for consumer in &filter.consumers {
        for capability in &consumer.capabilities {
            seen[match capability {
                Physical::Membership => 0,
                Physical::OrderedRange => 1,
                Physical::EmptyDomain => 2,
            }] = true;
        }
    }
    [
        AttemptRuntimeFilterArtifactCapability::Membership,
        AttemptRuntimeFilterArtifactCapability::OrderedRange,
        AttemptRuntimeFilterArtifactCapability::EmptyDomain,
    ]
    .into_iter()
    .enumerate()
    .filter_map(|(index, capability)| seen[index].then_some(capability))
    .collect()
}

/// How contributions to this filter are combined.
///
/// An ordered hull is the shape a TopN producer contributes, and the runtime
/// merges it as a top-k summary whose k is that producer's own limit. The
/// plan states the shape and the limit separately rather than repeating the
/// limit in the reduction, so it is read back from the producer here -- the
/// same derivation the wire encoder makes.
fn completed_reduction(
    filter: &RuntimeFilter,
) -> Result<AttemptRuntimeFilterReductionFacts, String> {
    use novarocks_physical_plan::RuntimeFilterReduction as Physical;

    Ok(match filter.reduction {
        Physical::SetUnion => AttemptRuntimeFilterReductionFacts::SetUnion,
        Physical::TightenOrderedBound => AttemptRuntimeFilterReductionFacts::TightenOrderedBound,
        Physical::UnionOrderedHull => {
            let limit = filter
                .producers
                .iter()
                .find_map(|producer| match producer.target {
                    PhysicalProducerTarget::AggregateTopNKey { limit, .. } => Some(limit),
                    PhysicalProducerTarget::JoinBuildKey { .. } => None,
                })
                .ok_or_else(|| {
                    format!(
                        "ordered-hull runtime filter {} has no Aggregate TopN producer",
                        filter.id.get()
                    )
                })?;
            let k = u32::try_from(limit).map_err(|_| {
                format!(
                    "runtime filter {} top-k limit exceeds the deployed identity",
                    filter.id.get()
                )
            })?;
            AttemptRuntimeFilterReductionFacts::MergeTopKSummary { k }
        }
    })
}

/// Expand one coverage arena into the tree its consumer reads.
///
/// The arena is bounded and non-recursive, and every composite child precedes
/// its parent, so this walk terminates without a visited set.
fn coverage_tree(
    coverage: &RuntimeFilterCoverage,
) -> Result<AttemptRuntimeFilterCoverageFacts, String> {
    fn expand(
        coverage: &RuntimeFilterCoverage,
        index: u32,
    ) -> Result<AttemptRuntimeFilterCoverageFacts, String> {
        let node = coverage
            .nodes
            .get(index as usize)
            .ok_or_else(|| format!("runtime filter coverage names absent node {index}"))?;
        Ok(match node {
            RuntimeFilterCoverageNode::Witness(witness) => {
                AttemptRuntimeFilterCoverageFacts::LeafWitnessId(witness.get())
            }
            RuntimeFilterCoverageNode::AllOf { children } => AttemptRuntimeFilterCoverageFacts::AllOf(
                children
                    .iter()
                    .map(|child| {
                        if *child >= index {
                            return Err(format!(
                                "runtime filter coverage node {index} names child {child} that does not precede it"
                            ));
                        }
                        expand(coverage, *child)
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            ),
            RuntimeFilterCoverageNode::AnyOf { children } => AttemptRuntimeFilterCoverageFacts::AnyOf(
                children
                    .iter()
                    .map(|child| {
                        if *child >= index {
                            return Err(format!(
                                "runtime filter coverage node {index} names child {child} that does not precede it"
                            ));
                        }
                        expand(coverage, *child)
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            ),
        })
    }

    expand(coverage, coverage.root)
}

fn apply_point(point: PhysicalApplyPoint) -> AttemptRuntimeFilterApplyPoint {
    match point {
        // A scan-source filter is applied to what the scan produces, which is
        // the node's output; the provider-side pruning it also enables is
        // named by the scan's own binding rather than by this apply point.
        PhysicalApplyPoint::NodeOutput | PhysicalApplyPoint::ScanSource => {
            AttemptRuntimeFilterApplyPoint::NodeOutput
        }
        PhysicalApplyPoint::NodeInput { .. } => AttemptRuntimeFilterApplyPoint::NodeInput,
    }
}

fn completed_producer_role(
    filter: &RuntimeFilter,
    producer: &RuntimeFilterProducer,
) -> Result<AttemptRuntimeFilterBindingRoleFacts, String> {
    use novarocks_physical_plan::RuntimeFilterCompletion;

    let target = match producer.target {
        PhysicalProducerTarget::JoinBuildKey { equality } => {
            let witness = filter
                .equality_witnesses
                .iter()
                .find(|candidate| candidate.id == equality)
                .ok_or_else(|| {
                    format!(
                        "runtime filter {} producer names absent equality witness {}",
                        filter.id.get(),
                        equality.get()
                    )
                })?;
            AttemptRuntimeFilterProducerTarget::JoinBuildKey {
                ordinal: witness.key_ordinal,
            }
        }
        PhysicalProducerTarget::AggregateTopNKey {
            group_key_ordinal,
            limit,
            ..
        } => AttemptRuntimeFilterProducerTarget::AggregateTopNKey {
            group_key_ordinal,
            limit: u32::try_from(limit).map_err(|_| {
                format!(
                    "runtime filter {} top-k limit exceeds the deployed identity",
                    filter.id.get()
                )
            })?,
        },
    };
    Ok(AttemptRuntimeFilterBindingRoleFacts::Producer {
        contribution_kinds: completed_contribution_kinds_of(producer),
        completion_requirement: match producer.completion {
            RuntimeFilterCompletion::ProducerClosed => {
                AttemptRuntimeFilterCompletionRequirement::ProducerClosed
            }
            RuntimeFilterCompletion::FencedCommittedDomain => {
                AttemptRuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen
            }
        },
        target,
    })
}

fn completed_contribution_kinds_of(
    producer: &RuntimeFilterProducer,
) -> Vec<AttemptRuntimeFilterContributionKind> {
    use novarocks_physical_plan::RuntimeFilterContributionKind as Physical;

    producer
        .contribution_kinds
        .iter()
        .map(|kind| match kind {
            Physical::ValueDomainDelta => AttemptRuntimeFilterContributionKind::ValueDomainDelta,
            Physical::FinalDomainShard => AttemptRuntimeFilterContributionKind::FinalDomainShard,
            Physical::OrderedBoundUpdate => {
                AttemptRuntimeFilterContributionKind::OrderedBoundUpdate
            }
            Physical::FinalOrderedHullShard => AttemptRuntimeFilterContributionKind::TopKSummary,
            Physical::ProducerClosed => AttemptRuntimeFilterContributionKind::ProducerClosed,
        })
        .collect()
}

fn completed_consumer_role(
    consumer: &novarocks_physical_plan::RuntimeFilterConsumer,
    scan_type: Option<&novarocks_physical_plan::ValueType>,
) -> Result<AttemptRuntimeFilterBindingRoleFacts, String> {
    use novarocks_physical_plan::{
        LateApplyGranularity, RuntimeFilterArtifactCapability as PhysicalCapability,
        RuntimeFilterConsumerActivation as PhysicalActivation,
        RuntimeFilterConsumerTarget as PhysicalConsumerTarget,
    };

    let activation = match consumer.activation {
        PhysicalActivation::BlockingSnapshot => {
            AttemptRuntimeFilterConsumerActivation::BlockingSnapshot
        }
        PhysicalActivation::NonBlockingLive { late_apply }
        | PhysicalActivation::StartUnfilteredThenApplyComplete { late_apply } => {
            AttemptRuntimeFilterConsumerActivation::NonBlockingLive(match late_apply {
                LateApplyGranularity::Row => AttemptRuntimeFilterLateApplyGranularity::Row,
                LateApplyGranularity::Batch => AttemptRuntimeFilterLateApplyGranularity::Batch,
                LateApplyGranularity::RowGroup => {
                    AttemptRuntimeFilterLateApplyGranularity::RowGroup
                }
                LateApplyGranularity::Split => AttemptRuntimeFilterLateApplyGranularity::Split,
                LateApplyGranularity::File => AttemptRuntimeFilterLateApplyGranularity::File,
            })
        }
    };
    let target = match &consumer.target {
        PhysicalConsumerTarget::JoinProbeKey { .. } => {
            AttemptRuntimeFilterConsumerTarget::DirectInput {
                input_ordinal: match consumer.apply_point {
                    PhysicalApplyPoint::NodeInput { input_ordinal } => input_ordinal,
                    PhysicalApplyPoint::NodeOutput | PhysicalApplyPoint::ScanSource => {
                        return Err(
                            "a join probe runtime filter applies to a named join input".to_string()
                        );
                    }
                },
            }
        }
        PhysicalConsumerTarget::ScanField { .. }
        | PhysicalConsumerTarget::AggregateTopNScanField { .. } => {
            scan_type.ok_or_else(|| {
                "a completed scan runtime filter has no pinned output type".to_string()
            })?;
            AttemptRuntimeFilterConsumerTarget::SourceBoundary
        }
    };
    Ok(AttemptRuntimeFilterBindingRoleFacts::Consumer {
        capabilities: consumer
            .capabilities
            .iter()
            .map(|capability| match capability {
                PhysicalCapability::Membership => {
                    AttemptRuntimeFilterArtifactCapability::Membership
                }
                PhysicalCapability::OrderedRange => {
                    AttemptRuntimeFilterArtifactCapability::OrderedRange
                }
                PhysicalCapability::EmptyDomain => {
                    AttemptRuntimeFilterArtifactCapability::EmptyDomain
                }
            })
            .collect(),
        activation,
        target,
        feedback_scan_type: scan_type.cloned(),
    })
}

/// Which exchange edges must close before this producer is done.
///
/// The plan names them as edge identities; deployment names them the way it
/// watches them, by the fragment that sends and the exchange node that
/// receives.
fn completed_join_progress(
    plan: &PhysicalPlan,
    filter: &RuntimeFilter,
    producer: &RuntimeFilterProducer,
    binding_id: u32,
    fragment: PhysicalFragmentId,
) -> Result<AttemptRuntimeFilterJoinProgressFacts, String> {
    Ok(AttemptRuntimeFilterJoinProgressFacts::Proven {
        channel_id: filter.id.get(),
        producer_binding_id: binding_id,
        producer_fragment_id: fragment.get(),
        join_node_id: wire_node_id(producer.endpoint.node.get())?,
        build_frontier: frontier_edges(plan, &producer.progress.build_edges)?,
        non_build_inputs: frontier_edges(plan, &producer.progress.non_build_edges)?,
    })
}

fn frontier_edges(
    plan: &PhysicalPlan,
    edges: &[EdgeId],
) -> Result<Vec<AttemptRuntimeFilterFrontierEdgeFacts>, String> {
    edges
        .iter()
        .map(|edge_id| {
            let edge = plan
                .edges()
                .get(edge_id)
                .ok_or_else(|| format!("runtime filter names absent edge {}", edge_id.get()))?;
            Ok(AttemptRuntimeFilterFrontierEdgeFacts {
                source_fragment_id: edge.source.fragment.get(),
                target_exchange_node_id: wire_node_id(edge.destination.node.get())?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{
        JoinSide, NodeId, RuntimeFilterCompletion, RuntimeFilterConsumer,
        RuntimeFilterContributionKind as PhysicalContributionKind, RuntimeFilterDomain,
        RuntimeFilterEndpoint, RuntimeFilterEqualityWitness, RuntimeFilterEqualityWitnessId,
        RuntimeFilterId, RuntimeFilterKind, RuntimeFilterLifecycle, RuntimeFilterNullSemantics,
        RuntimeFilterPolicy, RuntimeFilterProducerProgress, RuntimeFilterReduction,
        RuntimeFilterWitnessId, TopNPhase, ValueId, ValueType,
    };

    use super::*;

    fn witness_coverage(witness: u32) -> RuntimeFilterCoverage {
        RuntimeFilterCoverage {
            nodes: Box::from([RuntimeFilterCoverageNode::Witness(
                RuntimeFilterWitnessId::new(witness),
            )]),
            root: 0,
        }
    }

    fn join_build_producer() -> RuntimeFilterProducer {
        RuntimeFilterProducer {
            witness: RuntimeFilterWitnessId::new(1),
            endpoint: RuntimeFilterEndpoint {
                fragment: PhysicalFragmentId::new(1),
                node: NodeId::new(7),
                values: Box::from([ValueId::new(1)]),
            },
            apply_point: PhysicalApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from([
                PhysicalContributionKind::FinalDomainShard,
                PhysicalContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::FencedCommittedDomain,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::from([]),
                non_build_edges: Box::from([]),
            },
            target: PhysicalProducerTarget::JoinBuildKey {
                equality: RuntimeFilterEqualityWitnessId::new(1),
            },
        }
    }

    fn membership_filter(producers: Vec<RuntimeFilterProducer>) -> RuntimeFilter {
        RuntimeFilter {
            id: RuntimeFilterId::new(3),
            kind: RuntimeFilterKind::InList,
            domain: RuntimeFilterDomain::Membership {
                ty: ValueType::new(DataType::Int64, false),
                null_semantics: RuntimeFilterNullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            reduction: RuntimeFilterReduction::SetUnion,
            availability_coverage: witness_coverage(1),
            terminal_coverage: witness_coverage(1),
            equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
                id: RuntimeFilterEqualityWitnessId::new(1),
                fragment: PhysicalFragmentId::new(1),
                join: NodeId::new(7),
                key_ordinal: 2,
                domain_side: JoinSide::Right,
            }]),
            producers: producers.into_boxed_slice(),
            consumers: Box::from([]),
            policy: RuntimeFilterPolicy {
                max_contribution_bytes: 1 << 20,
                max_artifact_bytes: 1 << 22,
                deadline_ms: 250,
                max_retries: 2,
            },
        }
    }

    /// The plan states the shape of an ordered-hull reduction and the TopN
    /// producer's limit separately. Deployment needs both as one value, and
    /// reads the limit from the producer rather than from a second copy.
    #[test]
    fn an_ordered_hull_reduction_takes_its_k_from_its_top_n_producer() {
        let mut producer = join_build_producer();
        producer.target = PhysicalProducerTarget::AggregateTopNKey {
            group_key_ordinal: 0,
            topn: NodeId::new(9),
            phase: TopNPhase::Single,
            order_key_ordinal: 0,
            limit: 25,
            offset: 0,
            direction: novarocks_physical_plan::SortDirection::Ascending,
            null_ordering: novarocks_physical_plan::NullOrdering::First,
        };
        let mut filter = membership_filter(vec![producer]);
        filter.reduction = RuntimeFilterReduction::UnionOrderedHull;

        let reduction = completed_reduction(&filter).expect("a top-n producer states the limit");
        assert!(matches!(
            reduction,
            AttemptRuntimeFilterReductionFacts::MergeTopKSummary { k: 25 }
        ));
    }

    /// Without a TopN producer there is no k, and inventing one would deploy
    /// a summary the runtime would merge against the wrong bound.
    #[test]
    fn an_ordered_hull_reduction_without_a_top_n_producer_is_refused() {
        let mut filter = membership_filter(vec![join_build_producer()]);
        filter.reduction = RuntimeFilterReduction::UnionOrderedHull;

        let error = completed_reduction(&filter).expect_err("no producer states a limit");
        assert!(error.contains("no Aggregate TopN producer"), "{error}");
    }

    /// The channel accepts the union of what its producers send, once each
    /// and in one fixed order, so two plans with the same producers describe
    /// the same channel.
    #[test]
    fn a_channel_accepts_each_contribution_kind_its_producers_send_once() {
        let mut second = join_build_producer();
        second.contribution_kinds = Box::from([
            PhysicalContributionKind::ProducerClosed,
            PhysicalContributionKind::ValueDomainDelta,
            PhysicalContributionKind::FinalOrderedHullShard,
        ]);
        let filter = membership_filter(vec![join_build_producer(), second]);

        let kinds = completed_contribution_kinds(&filter)
            .into_iter()
            .map(|kind| format!("{kind:?}"))
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                "ValueDomainDelta".to_string(),
                "FinalDomainShard".to_string(),
                "TopKSummary".to_string(),
                "ProducerClosed".to_string(),
            ],
            "an ordered-hull shard is what the runtime calls a top-k summary"
        );
    }

    /// A join build key names an equality witness; deployment names the key
    /// ordinal that witness proves, because that is what it matches against
    /// the join it is attached to.
    #[test]
    fn a_join_build_producer_deploys_the_key_ordinal_its_witness_proves() {
        let filter = membership_filter(vec![join_build_producer()]);
        let role = completed_producer_role(&filter, &filter.producers[0])
            .expect("the witness this producer names is in the filter");
        let AttemptRuntimeFilterBindingRoleFacts::Producer { target, .. } = role else {
            panic!("a producer binding has a producer role");
        };
        assert!(matches!(
            target,
            AttemptRuntimeFilterProducerTarget::JoinBuildKey { ordinal: 2 }
        ));
    }

    /// A producer naming a witness this filter does not have is refused: the
    /// key ordinal would otherwise be read off whichever witness happened to
    /// be first.
    #[test]
    fn a_producer_naming_an_absent_equality_witness_is_refused() {
        let mut filter = membership_filter(vec![join_build_producer()]);
        filter.equality_witnesses = Box::from([]);
        let error = completed_producer_role(&filter, &filter.producers[0])
            .expect_err("an absent witness has no key ordinal");
        assert!(error.contains("absent equality witness"), "{error}");
    }

    /// A probe-side consumer applies to one named join input. Applying it
    /// anywhere else would filter rows the join never compares.
    #[test]
    fn a_join_probe_consumer_deploys_the_input_it_applies_to() {
        let consumer = RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: PhysicalFragmentId::new(1),
                node: NodeId::new(7),
                values: Box::from([ValueId::new(2)]),
            },
            apply_point: PhysicalApplyPoint::NodeInput { input_ordinal: 0 },
            capabilities: Box::from([
                novarocks_physical_plan::RuntimeFilterArtifactCapability::Membership,
            ]),
            activation: novarocks_physical_plan::RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: novarocks_physical_plan::RuntimeFilterConsumerTarget::JoinProbeKey {
                equality: RuntimeFilterEqualityWitnessId::new(1),
            },
        };
        let role = completed_consumer_role(&consumer, None).expect("a probe key names its input");
        let AttemptRuntimeFilterBindingRoleFacts::Consumer {
            target,
            feedback_scan_type,
            ..
        } = role
        else {
            panic!("a consumer binding has a consumer role");
        };
        assert!(matches!(
            target,
            AttemptRuntimeFilterConsumerTarget::DirectInput { input_ordinal: 0 }
        ));
        assert!(feedback_scan_type.is_none());
    }

    #[test]
    fn a_scan_consumer_deploys_its_pinned_value_type_for_feedback() {
        let consumer = RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: PhysicalFragmentId::new(1),
                node: NodeId::new(0),
                values: Box::from([ValueId::new(2)]),
            },
            apply_point: PhysicalApplyPoint::ScanSource,
            capabilities: Box::from([
                novarocks_physical_plan::RuntimeFilterArtifactCapability::Membership,
            ]),
            activation: novarocks_physical_plan::RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: novarocks_physical_plan::RuntimeFilterConsumerTarget::ScanField {
                equality: RuntimeFilterEqualityWitnessId::new(1),
                lineage: Box::from([]),
            },
        };
        let ty = novarocks_physical_plan::ValueType::new(DataType::Int32, true);
        let role = completed_consumer_role(&consumer, Some(&ty)).expect("scan type is pinned");
        let AttemptRuntimeFilterBindingRoleFacts::Consumer {
            target,
            feedback_scan_type,
            ..
        } = role
        else {
            panic!("a consumer binding has a consumer role");
        };
        assert!(matches!(
            target,
            AttemptRuntimeFilterConsumerTarget::SourceBoundary
        ));
        assert_eq!(feedback_scan_type, Some(ty));
    }

    /// Coverage is carried as an arena and read as a tree.
    #[test]
    fn coverage_expands_from_its_arena_in_the_order_it_was_written() {
        let coverage = RuntimeFilterCoverage {
            nodes: Box::from([
                RuntimeFilterCoverageNode::Witness(RuntimeFilterWitnessId::new(4)),
                RuntimeFilterCoverageNode::Witness(RuntimeFilterWitnessId::new(5)),
                RuntimeFilterCoverageNode::AnyOf {
                    children: Box::from([0, 1]),
                },
            ]),
            root: 2,
        };
        let tree = coverage_tree(&coverage).expect("children precede their parent");
        let AttemptRuntimeFilterCoverageFacts::AnyOf(children) = tree else {
            panic!("the root is an any-of");
        };
        assert_eq!(children.len(), 2);
        assert!(matches!(
            children[0],
            AttemptRuntimeFilterCoverageFacts::LeafWitnessId(4)
        ));
        assert!(matches!(
            children[1],
            AttemptRuntimeFilterCoverageFacts::LeafWitnessId(5)
        ));
    }

    /// A composite that names a node at or after itself is refused rather
    /// than walked, because walking it need not terminate.
    #[test]
    fn coverage_that_names_a_child_after_its_parent_is_refused() {
        let coverage = RuntimeFilterCoverage {
            nodes: Box::from([
                RuntimeFilterCoverageNode::AllOf {
                    children: Box::from([1]),
                },
                RuntimeFilterCoverageNode::Witness(RuntimeFilterWitnessId::new(6)),
            ]),
            root: 0,
        };
        let error = coverage_tree(&coverage).expect_err("a forward child is not walkable");
        assert!(error.contains("does not precede it"), "{error}");
    }
}
