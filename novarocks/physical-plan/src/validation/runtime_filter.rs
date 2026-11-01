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

use super::*;

use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::DataType;

use crate::{
    EdgeId, Fragment, FragmentId, FragmentSink, NodeId, NodeKind, PhysicalPlan,
    RuntimeFilterEndpoint, ValueId,
};

pub(crate) fn validate_runtime_filters(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let mut lineage_indexes = RuntimeFilterLineageIndexes::default();
    let mut parent_proof_work = SemanticTraceWorkBudget::new(errors.limits());
    let attachments = runtime_filter_attachment_index(plan);
    let inbound_edges = runtime_filter_inbound_edge_index(plan);
    for filter in plan.runtime_filters().values() {
        let path = format!("runtime_filters[{}]", filter.id.get());
        if !validate_runtime_filter_shape(filter, &path, errors) {
            continue;
        }
        let witnesses = runtime_filter_witness_index(&filter.equality_witnesses);
        for witness in &filter.equality_witnesses {
            match plan.fragments().get(&witness.fragment) {
                Some(fragment) => validate_runtime_filter_equality_witness(
                    fragment,
                    witness,
                    &filter.domain,
                    &path,
                    errors,
                ),
                None => errors.push(ValidationError::new(
                    &path,
                    "runtime filter equality witness fragment is not defined",
                )),
            }
        }
        for producer in &filter.producers {
            validate_runtime_filter_attachment(
                plan,
                &attachments,
                filter.id,
                &producer.endpoint,
                &path,
                errors,
            );
            validate_runtime_filter_endpoint(
                plan,
                &producer.endpoint,
                &filter.domain,
                &path,
                errors,
            );
            validate_apply_point(
                plan,
                &mut lineage_indexes,
                &producer.endpoint,
                producer.apply_point,
                &path,
                errors,
            );
            if let Some(fragment) = plan.fragments().get(&producer.endpoint.fragment) {
                validate_runtime_filter_producer_target(
                    fragment,
                    &witnesses,
                    producer,
                    &filter.domain,
                    filter.reduction,
                    &path,
                    errors,
                    &mut parent_proof_work,
                );
                validate_runtime_filter_join_coverage(fragment, filter, producer, &path, errors);
                validate_runtime_filter_producer_progress(
                    fragment,
                    producer,
                    inbound_edges
                        .get(&fragment.id())
                        .unwrap_or(&BTreeSet::new()),
                    &mut lineage_indexes,
                    &path,
                    errors,
                );
            }
        }
        for consumer in &filter.consumers {
            validate_runtime_filter_attachment(
                plan,
                &attachments,
                filter.id,
                &consumer.endpoint,
                &path,
                errors,
            );
            validate_runtime_filter_endpoint(
                plan,
                &consumer.endpoint,
                &filter.domain,
                &path,
                errors,
            );
            validate_apply_point(
                plan,
                &mut lineage_indexes,
                &consumer.endpoint,
                consumer.apply_point,
                &path,
                errors,
            );
            if let Some(fragment) = plan.fragments().get(&consumer.endpoint.fragment) {
                validate_runtime_filter_consumer_semantics(
                    fragment,
                    &witnesses,
                    &filter.producers,
                    &filter.domain,
                    consumer,
                    &mut lineage_indexes,
                    &path,
                    errors,
                );
            }
            validate_runtime_filter_consumer_lineage(
                plan,
                &witnesses,
                &filter.producers,
                consumer,
                &path,
                &mut lineage_indexes,
                errors,
            );
        }
    }
    for fragment in plan.fragments().values() {
        for id in fragment.runtime_filters() {
            if !plan.runtime_filters().contains_key(id) {
                errors.push(ValidationError::new(
                    format!("fragments[{}].runtime_filters", fragment.id().get()),
                    format!("runtime filter {} is not defined", id.get()),
                ));
            }
        }
    }
}

/// Check the actual frozen activations using the same dependency author as
/// planning. Every active cycle is a rejection here; no planning downgrade runs.
pub(crate) fn validate_runtime_filter_wait_graph_in(
    plan: &PhysicalPlan,
    errors: &mut ValidationContext,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), novarocks_type_contract::CompileControlError> {
    use crate::wait_graph::{WaitGraph, WaitGraphError};
    let result = WaitGraph::build_observed(
        plan.fragments(),
        plan.edges(),
        plan.runtime_filters().values(),
        work,
    )
    .and_then(|graph| graph.active_cycles_observed(work));
    match result {
        Err(WaitGraphError::Control(cause)) => return Err(cause),
        Err(cause) => errors.push(ValidationError::new("runtime_filters", cause.to_string())),
        Ok(cycles) => {
            if cycles.filter {
                errors.push(ValidationError::new(
                    "runtime_filters",
                    "blocking runtime-filter waits form a cycle with physical execution dependencies",
                ));
            }
            if cycles.data {
                errors.push(ValidationError::new(
                    "fragments.wait_graph",
                    "required physical input waits form a structural cycle",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_runtime_filter_attachment(
    plan: &PhysicalPlan,
    attachments: &BTreeMap<FragmentId, BTreeSet<crate::RuntimeFilterId>>,
    id: crate::RuntimeFilterId,
    endpoint: &RuntimeFilterEndpoint,
    path: &str,
    errors: &mut ValidationContext,
) {
    if let Some(fragment) = plan.fragments().get(&endpoint.fragment)
        && !attachments
            .get(&fragment.id())
            .is_some_and(|filters| filters.contains(&id))
    {
        errors.push(ValidationError::new(
            path,
            "runtime filter endpoint fragment does not attach the filter",
        ));
    }
}

pub(crate) type RuntimeFilterWitnessIndex<'a> = BTreeMap<
    crate::RuntimeFilterEqualityWitnessId,
    Option<&'a crate::RuntimeFilterEqualityWitness>,
>;

pub(crate) fn runtime_filter_witness_index(
    witnesses: &[crate::RuntimeFilterEqualityWitness],
) -> RuntimeFilterWitnessIndex<'_> {
    let mut index = BTreeMap::new();
    for witness in witnesses {
        index
            .entry(witness.id)
            .and_modify(|existing| *existing = None)
            .or_insert(Some(witness));
    }
    index
}

pub(crate) fn runtime_filter_attachment_index(
    plan: &PhysicalPlan,
) -> BTreeMap<FragmentId, BTreeSet<crate::RuntimeFilterId>> {
    plan.fragments()
        .values()
        .map(|fragment| {
            (
                fragment.id(),
                fragment.runtime_filters().iter().copied().collect(),
            )
        })
        .collect()
}

pub(crate) fn runtime_filter_inbound_edge_index(
    plan: &PhysicalPlan,
) -> BTreeMap<FragmentId, BTreeSet<EdgeId>> {
    let mut index = BTreeMap::<FragmentId, BTreeSet<EdgeId>>::new();
    for edge in plan.edges().values() {
        index
            .entry(edge.destination.fragment)
            .or_default()
            .insert(edge.id);
    }
    index
}

pub(crate) fn validate_runtime_filter_shape(
    filter: &crate::RuntimeFilter,
    path: &str,
    errors: &mut ValidationContext,
) -> bool {
    // Zero is reserved: a runtime filter's identity is the channel a
    // deployment addresses, and an absent wire field must not read back as a
    // real channel.
    if filter.id.get() == 0 {
        errors.push(ValidationError::new(
            path,
            "runtime filter identity zero is reserved",
        ));
        return false;
    }
    if filter.producers.len() > errors.limits().runtime_filter_endpoints
        || filter.consumers.len() > errors.limits().runtime_filter_endpoints
        || filter.equality_witnesses.len() > errors.limits().runtime_filter_endpoints
        || filter
            .producers
            .len()
            .saturating_add(filter.consumers.len())
            > errors.limits().runtime_filter_endpoints
    {
        errors.push(ValidationError::resource_limit(
            path,
            "runtime filter endpoint count exceeds the contract maximum",
        ));
        return false;
    }
    let lineage_steps = filter.consumers.iter().fold(0_usize, |total, consumer| {
        total.saturating_add(match &consumer.target {
            crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => 0,
            crate::RuntimeFilterConsumerTarget::ScanField { lineage, .. }
            | crate::RuntimeFilterConsumerTarget::AggregateTopNScanField { lineage, .. } => {
                lineage.len()
            }
        })
    });
    if lineage_steps > errors.limits().runtime_filter_lineage_steps {
        errors.push(ValidationError::resource_limit(
            path,
            "runtime filter lineage exceeds the contract maximum",
        ));
        return false;
    }
    if !matches!(
        (filter.kind, &filter.domain),
        (
            crate::RuntimeFilterKind::Bloom | crate::RuntimeFilterKind::InList,
            crate::RuntimeFilterDomain::Membership { .. }
        ) | (
            crate::RuntimeFilterKind::MinMax,
            crate::RuntimeFilterDomain::Ordered { .. }
        )
    ) {
        errors.push(ValidationError::new(
            path,
            "runtime filter kind differs from its logical domain",
        ));
    }
    if let crate::RuntimeFilterDomain::Ordered {
        key, comparator, ..
    } = &filter.domain
    {
        if *key.ty.data_type() == DataType::Null {
            errors.push(ValidationError::new(
                path,
                "ordered runtime-filter key has Null type",
            ));
        }
        if !comparator.supports_order_key(key.ty.data_type()) {
            errors.push(ValidationError::new(
                path,
                "ordered runtime-filter key type is unsupported by its comparison algorithm",
            ));
        }
    }
    if filter.producers.is_empty() {
        errors.push(ValidationError::new(
            path,
            "runtime filter has no producers",
        ));
    }
    if filter.consumers.is_empty() {
        errors.push(ValidationError::new(
            path,
            "runtime filter has no consumers",
        ));
    }
    let equality_witnesses = filter
        .equality_witnesses
        .iter()
        .map(|witness| witness.id)
        .collect::<BTreeSet<_>>();
    if equality_witnesses.len() != filter.equality_witnesses.len() {
        errors.push(ValidationError::new(
            path,
            "runtime filter equality witness identities are not unique",
        ));
    }
    let equality_anchors = filter
        .equality_witnesses
        .iter()
        .map(|witness| {
            (
                witness.fragment,
                witness.join,
                witness.key_ordinal,
                witness.domain_side,
            )
        })
        .collect::<BTreeSet<_>>();
    if equality_anchors.len() != filter.equality_witnesses.len() {
        errors.push(ValidationError::new(
            path,
            "runtime filter equality witnesses duplicate one hash-join key direction",
        ));
    }
    let producer_equalities = filter
        .producers
        .iter()
        .filter_map(|producer| match &producer.target {
            crate::RuntimeFilterProducerTarget::JoinBuildKey { equality } => Some(*equality),
            crate::RuntimeFilterProducerTarget::AggregateTopNKey { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    let consumer_equalities = filter
        .consumers
        .iter()
        .filter_map(|consumer| match &consumer.target {
            crate::RuntimeFilterConsumerTarget::JoinProbeKey { equality }
            | crate::RuntimeFilterConsumerTarget::ScanField { equality, .. } => Some(*equality),
            crate::RuntimeFilterConsumerTarget::AggregateTopNScanField { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    if !consumer_equalities.is_subset(&producer_equalities) {
        errors.push(ValidationError::new(
            path,
            "runtime filter consumer equality witness has no producer for the same join key",
        ));
    }
    let referenced_equalities = producer_equalities
        .union(&consumer_equalities)
        .copied()
        .collect::<BTreeSet<_>>();
    if equality_witnesses != referenced_equalities {
        errors.push(ValidationError::new(
            path,
            "runtime filter equality witness references differ from their definitions",
        ));
    }
    if filter.policy.max_contribution_bytes == 0
        || filter.policy.max_artifact_bytes == 0
        || filter.policy.deadline_ms == 0
        || filter.policy.max_retries == 0
    {
        errors.push(ValidationError::new(
            path,
            "runtime filter policy has a zero byte, time, or retry bound",
        ));
    }
    if filter.policy.max_contribution_bytes > filter.policy.max_artifact_bytes
        || filter.policy.max_artifact_bytes > MAX_RUNTIME_FILTER_ARTIFACT_BYTES
        || filter.policy.deadline_ms > MAX_RUNTIME_FILTER_DEADLINE_MS
        || filter.policy.max_retries > MAX_RUNTIME_FILTER_RETRIES
    {
        errors.push(ValidationError::resource_limit(
            path,
            "runtime filter policy exceeds its resource bounds",
        ));
    }
    match (&filter.domain, filter.reduction) {
        (
            crate::RuntimeFilterDomain::Membership { .. },
            crate::RuntimeFilterReduction::SetUnion,
        )
        | (
            crate::RuntimeFilterDomain::Ordered { .. },
            crate::RuntimeFilterReduction::UnionOrderedHull
            | crate::RuntimeFilterReduction::TightenOrderedBound,
        ) => {}
        _ => errors.push(ValidationError::new(
            path,
            "runtime filter domain and reduction are inconsistent",
        )),
    }
    let producer_witnesses = filter
        .producers
        .iter()
        .map(|producer| producer.witness)
        .collect::<BTreeSet<_>>();
    if producer_witnesses.len() != filter.producers.len() {
        errors.push(ValidationError::new(
            path,
            "runtime filter producer witnesses are not unique",
        ));
    }
    let (availability, availability_safe) = validate_runtime_filter_coverage(
        &filter.availability_coverage,
        "availability",
        path,
        errors,
    );
    if availability != producer_witnesses {
        errors.push(ValidationError::new(
            path,
            "runtime filter availability coverage differs from its producer witnesses",
        ));
    }
    let (terminal, terminal_safe) =
        validate_runtime_filter_coverage(&filter.terminal_coverage, "terminal", path, errors);
    if !terminal.is_subset(&producer_witnesses) {
        errors.push(ValidationError::new(
            path,
            "runtime filter terminal coverage references an unknown producer witness",
        ));
    }
    for producer in &filter.producers {
        if producer.contribution_kinds.is_empty() {
            errors.push(ValidationError::new(
                path,
                "runtime filter producer has no contribution kinds",
            ));
        }
    }
    for consumer in &filter.consumers {
        if consumer.capabilities.is_empty() {
            errors.push(ValidationError::new(
                path,
                "runtime filter consumer has no artifact capabilities",
            ));
        }
    }
    validate_runtime_filter_matrix(filter, availability_safe && terminal_safe, path, errors);
    true
}

pub(crate) fn validate_runtime_filter_matrix(
    filter: &crate::RuntimeFilter,
    coverage_comparison_safe: bool,
    path: &str,
    errors: &mut ValidationContext,
) {
    if coverage_comparison_safe
        && filter.lifecycle == crate::RuntimeFilterLifecycle::CompleteOnce
        && filter.availability_coverage != filter.terminal_coverage
    {
        errors.push(ValidationError::new(
            path,
            "complete-once runtime filter has different availability and terminal coverage",
        ));
    }

    let (expected_contributions, expected_completion, required_capabilities, exact_capabilities) =
        match (&filter.domain, filter.reduction) {
            (
                crate::RuntimeFilterDomain::Membership { null_semantics, .. },
                crate::RuntimeFilterReduction::SetUnion,
            ) => {
                if filter.lifecycle != crate::RuntimeFilterLifecycle::CompleteOnce
                    || (coverage_comparison_safe
                        && filter.availability_coverage != filter.terminal_coverage)
                {
                    errors.push(ValidationError::new(
                        path,
                        "membership runtime filter requires complete-once identical coverage",
                    ));
                }
                let has_final_shard = filter.producers.iter().any(|producer| {
                    producer
                        .contribution_kinds
                        .contains(&crate::RuntimeFilterContributionKind::FinalDomainShard)
                });
                if has_final_shard
                    && (*null_semantics != crate::RuntimeFilterNullSemantics::NullSafeEqual
                        || !coverage_is_all_of_only(&filter.availability_coverage))
                {
                    errors.push(ValidationError::new(
                        path,
                        "fenced final-domain runtime filter requires null-safe AllOf coverage",
                    ));
                }
                let contributions = if has_final_shard {
                    BTreeSet::from([
                        crate::RuntimeFilterContributionKind::FinalDomainShard,
                        crate::RuntimeFilterContributionKind::ProducerClosed,
                    ])
                } else {
                    BTreeSet::from([
                        crate::RuntimeFilterContributionKind::ValueDomainDelta,
                        crate::RuntimeFilterContributionKind::ProducerClosed,
                    ])
                };
                (
                    contributions,
                    if has_final_shard {
                        crate::RuntimeFilterCompletion::FencedCommittedDomain
                    } else {
                        crate::RuntimeFilterCompletion::ProducerClosed
                    },
                    BTreeSet::from([
                        crate::RuntimeFilterArtifactCapability::Membership,
                        crate::RuntimeFilterArtifactCapability::EmptyDomain,
                    ]),
                    false,
                )
            }
            (
                crate::RuntimeFilterDomain::Ordered { .. },
                crate::RuntimeFilterReduction::UnionOrderedHull,
            ) => {
                if filter.lifecycle != crate::RuntimeFilterLifecycle::CompleteOnce
                    || !coverage_is_all_of_only(&filter.availability_coverage)
                    || (coverage_comparison_safe
                        && filter.availability_coverage != filter.terminal_coverage)
                {
                    errors.push(ValidationError::new(
                        path,
                        "ordered hull runtime filter requires complete-once identical AllOf coverage",
                    ));
                }
                (
                    BTreeSet::from([
                        crate::RuntimeFilterContributionKind::FinalOrderedHullShard,
                        crate::RuntimeFilterContributionKind::ProducerClosed,
                    ]),
                    crate::RuntimeFilterCompletion::FencedCommittedDomain,
                    BTreeSet::from([
                        crate::RuntimeFilterArtifactCapability::OrderedRange,
                        crate::RuntimeFilterArtifactCapability::EmptyDomain,
                    ]),
                    true,
                )
            }
            (
                crate::RuntimeFilterDomain::Ordered { .. },
                crate::RuntimeFilterReduction::TightenOrderedBound,
            ) => {
                if filter.lifecycle != crate::RuntimeFilterLifecycle::MonotonicUpdates {
                    errors.push(ValidationError::new(
                        path,
                        "ordered-bound runtime filter requires monotonic-update lifecycle",
                    ));
                }
                (
                    BTreeSet::from([
                        crate::RuntimeFilterContributionKind::OrderedBoundUpdate,
                        crate::RuntimeFilterContributionKind::ProducerClosed,
                    ]),
                    crate::RuntimeFilterCompletion::ProducerClosed,
                    BTreeSet::from([crate::RuntimeFilterArtifactCapability::OrderedRange]),
                    true,
                )
            }
            _ => return,
        };

    for producer in &filter.producers {
        let contributions = producer
            .contribution_kinds
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if contributions.len() != producer.contribution_kinds.len()
            || producer
                .contribution_kinds
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || contributions != expected_contributions
        {
            errors.push(ValidationError::new(
                path,
                "runtime filter producer contributions differ from the channel matrix",
            ));
        }
        if producer.completion != expected_completion {
            errors.push(ValidationError::new(
                path,
                "runtime filter producer completion differs from the channel matrix",
            ));
        }
    }
    for consumer in &filter.consumers {
        let capabilities = consumer
            .capabilities
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let invalid = capabilities.len() != consumer.capabilities.len()
            || consumer
                .capabilities
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || if exact_capabilities {
                capabilities != required_capabilities
            } else {
                !required_capabilities.is_subset(&capabilities)
            };
        if invalid {
            errors.push(ValidationError::new(
                path,
                "runtime filter consumer capabilities differ from the channel matrix",
            ));
        }
        if filter.lifecycle == crate::RuntimeFilterLifecycle::MonotonicUpdates
            && !matches!(
                consumer.activation,
                crate::RuntimeFilterConsumerActivation::NonBlockingLive { .. }
            )
        {
            errors.push(ValidationError::new(
                path,
                "monotonic runtime-filter consumer requires non-blocking live-update activation",
            ));
        }
    }
}

pub(crate) fn coverage_is_all_of_only(coverage: &crate::RuntimeFilterCoverage) -> bool {
    let Some(crate::RuntimeFilterCoverageNode::AllOf { .. }) = usize::try_from(coverage.root)
        .ok()
        .and_then(|root| coverage.nodes.get(root))
    else {
        return false;
    };
    let mut pending = vec![coverage.root];
    let mut visited = BTreeSet::new();
    while let Some(current) = pending.pop() {
        if !visited.insert(current) {
            continue;
        }
        match usize::try_from(current)
            .ok()
            .and_then(|index| coverage.nodes.get(index))
        {
            Some(crate::RuntimeFilterCoverageNode::Witness(_)) => {}
            Some(crate::RuntimeFilterCoverageNode::AllOf { children }) => {
                pending.extend(children.iter().copied());
            }
            Some(crate::RuntimeFilterCoverageNode::AnyOf { .. }) | None => return false,
        }
    }
    true
}

pub(crate) fn validate_runtime_filter_coverage(
    coverage: &crate::RuntimeFilterCoverage,
    label: &str,
    path: &str,
    errors: &mut ValidationContext,
) -> (BTreeSet<crate::RuntimeFilterWitnessId>, bool) {
    if coverage.nodes.is_empty()
        || coverage.nodes.len() > errors.limits().runtime_filter_coverage_nodes
    {
        errors.push(ValidationError::new(
            path,
            format!(
                "runtime filter {label} coverage requires 1..={} arena nodes",
                errors.limits().runtime_filter_coverage_nodes
            ),
        ));
        return (BTreeSet::new(), false);
    }
    let Ok(root) = usize::try_from(coverage.root) else {
        errors.push(ValidationError::new(
            path,
            format!("runtime filter {label} coverage root is outside its arena"),
        ));
        return (BTreeSet::new(), false);
    };
    if root >= coverage.nodes.len() {
        errors.push(ValidationError::new(
            path,
            format!("runtime filter {label} coverage root is outside its arena"),
        ));
        return (BTreeSet::new(), false);
    }

    let mut witnesses = Vec::new();
    let mut safe = true;
    let mut depths = Vec::with_capacity(coverage.nodes.len());
    let mut child_references = 0_usize;
    for (index, node) in coverage.nodes.iter().enumerate() {
        match node {
            crate::RuntimeFilterCoverageNode::Witness(_) => depths.push(1_usize),
            crate::RuntimeFilterCoverageNode::AllOf { children }
            | crate::RuntimeFilterCoverageNode::AnyOf { children } => {
                if children.is_empty() {
                    errors.push(ValidationError::new(
                        path,
                        format!("runtime filter {label} coverage has an empty composite"),
                    ));
                }
                child_references = child_references.saturating_add(children.len());
                if child_references > errors.limits().runtime_filter_coverage_nodes {
                    safe = false;
                    errors.push(ValidationError::resource_limit(
                        path,
                        format!(
                            "runtime filter {label} coverage exceeds {} child references",
                            errors.limits().runtime_filter_coverage_nodes
                        ),
                    ));
                    break;
                }
                if children.windows(2).any(|pair| pair[0] >= pair[1]) {
                    errors.push(ValidationError::new(
                        path,
                        format!(
                            "runtime filter {label} coverage children are not strictly ordered"
                        ),
                    ));
                }
                let child_depth = children
                    .iter()
                    .filter_map(|child| usize::try_from(*child).ok())
                    .filter_map(|child| depths.get(child))
                    .copied()
                    .max()
                    .unwrap_or(0);
                if children
                    .iter()
                    .any(|child| usize::try_from(*child).map_or(true, |child| child >= index))
                {
                    safe = false;
                    errors.push(ValidationError::new(
                        path,
                        format!(
                            "runtime filter {label} coverage child does not precede its parent"
                        ),
                    ));
                }
                let depth = child_depth.saturating_add(1);
                if depth > errors.limits().runtime_filter_coverage_depth {
                    safe = false;
                    errors.push(ValidationError::resource_limit(
                        path,
                        format!(
                            "runtime filter {label} coverage exceeds semantic depth {}",
                            errors.limits().runtime_filter_coverage_depth
                        ),
                    ));
                }
                depths.push(depth);
            }
        }
    }
    let mut reachable = BTreeSet::new();
    let mut pending = vec![coverage.root];
    while let Some(current) = pending.pop() {
        if !reachable.insert(current) {
            continue;
        }
        match usize::try_from(current)
            .ok()
            .and_then(|index| coverage.nodes.get(index))
        {
            Some(crate::RuntimeFilterCoverageNode::Witness(witness)) => {
                witnesses.push(*witness);
            }
            Some(crate::RuntimeFilterCoverageNode::AllOf { children })
            | Some(crate::RuntimeFilterCoverageNode::AnyOf { children }) => {
                pending.extend(children.iter().copied());
            }
            None => safe = false,
        }
    }
    if reachable.len() != coverage.nodes.len() {
        safe = false;
        errors.push(ValidationError::new(
            path,
            format!("runtime filter {label} coverage arena contains unreachable nodes"),
        ));
    }
    let unique = witnesses.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != witnesses.len() {
        errors.push(ValidationError::new(
            path,
            format!("runtime filter {label} coverage repeats a producer witness"),
        ));
    }
    (unique, safe)
}

pub(crate) fn validate_runtime_filter_producer_progress(
    fragment: &Fragment,
    producer: &crate::RuntimeFilterProducer,
    inbound_edges: &BTreeSet<EdgeId>,
    indexes: &mut RuntimeFilterLineageIndexes,
    path: &str,
    errors: &mut ValidationContext,
) {
    let crate::RuntimeFilterProducerProgress {
        build_edges,
        non_build_edges,
    } = &producer.progress;
    let Some(node) = fragment.nodes().get(&producer.endpoint.node) else {
        return;
    };
    let producer_input = match (&producer.target, &node.kind) {
        (
            crate::RuntimeFilterProducerTarget::JoinBuildKey { .. },
            NodeKind::HashJoin { build_side, .. },
        ) if node.inputs.len() == 2 => {
            node.inputs[usize::try_from(build_side.input_ordinal()).unwrap()]
        }
        (
            crate::RuntimeFilterProducerTarget::AggregateTopNKey { .. },
            NodeKind::Aggregate { .. },
        ) if node.inputs.len() == 1 && non_build_edges.is_empty() => node.inputs[0],
        _ => {
            errors.push(ValidationError::new(
                path,
                "runtime filter producer progress does not belong to its declared producer input",
            ));
            return;
        }
    };
    for (label, edges) in [("build", build_edges), ("non-build", non_build_edges)] {
        if edges.windows(2).any(|pair| pair[0] >= pair[1]) {
            errors.push(ValidationError::new(
                path,
                format!("runtime filter {label} frontier edges are not strictly increasing"),
            ));
        }
        if edges.iter().any(|edge| !inbound_edges.contains(edge)) {
            errors.push(ValidationError::new(
                path,
                format!("runtime filter {label} frontier names a non-inbound edge"),
            ));
        }
    }
    let declared_build = build_edges.iter().copied().collect::<BTreeSet<_>>();
    let declared_non_build = non_build_edges.iter().copied().collect::<BTreeSet<_>>();
    if !declared_build.is_disjoint(&declared_non_build) {
        errors.push(ValidationError::new(
            path,
            "runtime filter build and non-build frontier edges overlap",
        ));
    }
    let expected = indexes.build_frontier(fragment, producer_input, inbound_edges);
    let expected_non_build = if matches!(
        producer.target,
        crate::RuntimeFilterProducerTarget::AggregateTopNKey { .. }
    ) {
        BTreeSet::new()
    } else {
        expected.non_build.clone()
    };
    if declared_build != expected.build || declared_non_build != expected_non_build {
        let detail = match producer.target {
            crate::RuntimeFilterProducerTarget::JoinBuildKey { .. } => {
                "runtime filter join-build frontier differs from the exact join input exchange cuts"
            }
            crate::RuntimeFilterProducerTarget::AggregateTopNKey { .. } => {
                "runtime filter Aggregate TopN frontier differs from the exact aggregate input exchange cuts"
            }
        };
        errors.push(ValidationError::new(path, detail));
    }
}

pub(crate) fn collect_subtree_exchange_edges(
    fragment: &Fragment,
    root: NodeId,
) -> BTreeSet<EdgeId> {
    let mut edges = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut pending = vec![root];
    while let Some(node_id) = pending.pop() {
        if !visited.insert(node_id) {
            continue;
        }
        let Some(node) = fragment.nodes().get(&node_id) else {
            continue;
        };
        if let NodeKind::ExchangeSource { edge, .. } = node.kind {
            edges.insert(edge);
        }
        pending.extend(node.inputs.iter().copied());
    }
    edges
}

pub(crate) fn validate_runtime_filter_endpoint(
    plan: &PhysicalPlan,
    endpoint: &RuntimeFilterEndpoint,
    domain: &crate::RuntimeFilterDomain,
    path: &str,
    errors: &mut ValidationContext,
) {
    match plan.fragments().get(&endpoint.fragment) {
        Some(fragment) => {
            validate_runtime_filter_endpoint_in_fragment(fragment, endpoint, domain, path, errors)
        }
        None => errors.push(ValidationError::new(
            path,
            "runtime filter fragment is not defined",
        )),
    }
}

pub(crate) fn validate_runtime_filter_endpoint_in_fragment(
    fragment: &Fragment,
    endpoint: &RuntimeFilterEndpoint,
    domain: &crate::RuntimeFilterDomain,
    path: &str,
    errors: &mut ValidationContext,
) {
    require_node(fragment, endpoint.node, path, errors);
    let expected = match domain {
        crate::RuntimeFilterDomain::Membership { ty, .. } => std::slice::from_ref(ty),
        // Checked below because the ordered domain wraps its scalar type.
        crate::RuntimeFilterDomain::Ordered { .. } => &[],
    };
    let expected_len = match domain {
        crate::RuntimeFilterDomain::Membership { .. } => expected.len(),
        crate::RuntimeFilterDomain::Ordered { .. } => 1,
    };
    if endpoint.values.len() != expected_len {
        errors.push(ValidationError::new(
            path,
            "runtime filter endpoint width differs from its logical domain",
        ));
    }
    for (ordinal, value_id) in endpoint.values.iter().enumerate() {
        match fragment.values().get(value_id) {
            Some(value) => {
                let expected_type = match domain {
                    crate::RuntimeFilterDomain::Membership { ty, .. } => Some(ty),
                    crate::RuntimeFilterDomain::Ordered { key, .. } => {
                        (ordinal == 0).then_some(&key.ty)
                    }
                };
                // The domain names the type the filter's values have. Whether
                // a given endpoint's column admits null is that column's own
                // fact -- a build side may never write one where the probe
                // side may read one -- and whether a null matches is said
                // once, by the domain's null semantics.
                if expected_type.is_some_and(|expected| !expected.same_value_domain(&value.ty)) {
                    errors.push(ValidationError::new(
                        path,
                        "runtime filter endpoint type differs from its domain",
                    ));
                }
            }
            None => require_value(fragment, *value_id, path, errors),
        }
    }
}

pub(crate) fn validate_apply_point(
    plan: &PhysicalPlan,
    indexes: &mut RuntimeFilterLineageIndexes,
    endpoint: &RuntimeFilterEndpoint,
    apply_point: crate::RuntimeFilterApplyPoint,
    path: &str,
    errors: &mut ValidationContext,
) {
    if let Some(fragment) = plan.fragments().get(&endpoint.fragment) {
        validate_apply_point_in_fragment(fragment, indexes, endpoint, apply_point, path, errors);
    }
}

pub(crate) fn validate_apply_point_in_fragment(
    fragment: &Fragment,
    indexes: &mut RuntimeFilterLineageIndexes,
    endpoint: &RuntimeFilterEndpoint,
    apply_point: crate::RuntimeFilterApplyPoint,
    path: &str,
    errors: &mut ValidationContext,
) {
    if let Some(node) = fragment.nodes().get(&endpoint.node) {
        match indexes.apply_port_contains_all(fragment, node, apply_point, &endpoint.values) {
            Some(true) => {}
            Some(false) => errors.push(ValidationError::new(
                path,
                "runtime filter endpoint value is absent from its exact apply port",
            )),
            None => errors.push(ValidationError::new(
                path,
                "runtime filter apply point is invalid for its node",
            )),
        }
    }
}

pub(crate) fn runtime_filter_equality_witness<'a>(
    witnesses: &RuntimeFilterWitnessIndex<'a>,
    id: crate::RuntimeFilterEqualityWitnessId,
) -> Option<&'a crate::RuntimeFilterEqualityWitness> {
    witnesses.get(&id).copied().flatten()
}

pub(crate) fn equality_key_value(
    fragment: &Fragment,
    witness: &crate::RuntimeFilterEqualityWitness,
    side: crate::JoinSide,
) -> Option<ValueId> {
    let node = fragment.nodes().get(&witness.join)?;
    let NodeKind::HashJoin { keys, .. } = &node.kind else {
        return None;
    };
    let key = keys.get(usize::try_from(witness.key_ordinal).ok()?)?;
    crate::expression_value(
        fragment.expressions(),
        match side {
            crate::JoinSide::Left => key.left,
            crate::JoinSide::Right => key.right,
        },
    )
}

pub(crate) fn validate_runtime_filter_equality_witness(
    fragment: &Fragment,
    witness: &crate::RuntimeFilterEqualityWitness,
    domain: &crate::RuntimeFilterDomain,
    path: &str,
    errors: &mut ValidationContext,
) {
    let valid = fragment.nodes().get(&witness.join).is_some_and(|node| {
        let NodeKind::HashJoin {
            kind,
            keys,
            build_side,
            ..
        } = &node.kind
        else {
            return false;
        };
        if witness.fragment != fragment.id() || *build_side != witness.domain_side {
            return false;
        }
        let Some(key) = usize::try_from(witness.key_ordinal)
            .ok()
            .and_then(|ordinal| keys.get(ordinal))
        else {
            return false;
        };
        let domain_type = match witness.domain_side {
            crate::JoinSide::Left => fragment.expressions().get(key.left).map(|expr| &expr.ty),
            crate::JoinSide::Right => fragment.expressions().get(key.right).map(|expr| &expr.ty),
        };
        let domain_matches = match (domain, domain_type) {
            (crate::RuntimeFilterDomain::Membership { ty, null_semantics }, Some(actual)) => {
                ty == actual
                    && *null_semantics
                        == if key.null_safe {
                            crate::RuntimeFilterNullSemantics::NullSafeEqual
                        } else {
                            crate::RuntimeFilterNullSemantics::NeverMatches
                        }
            }
            (
                crate::RuntimeFilterDomain::Ordered {
                    key: domain_key, ..
                },
                Some(actual),
            ) => !key.null_safe && domain_key.ty == *actual,
            (_, None) => false,
        };
        domain_matches
            && matches!(
                (kind, witness.domain_side),
                (crate::JoinKind::Inner, _)
                    | (crate::JoinKind::LeftOuter, crate::JoinSide::Left)
                    | (crate::JoinKind::RightOuter, crate::JoinSide::Right)
                    | (crate::JoinKind::LeftSemi, crate::JoinSide::Right)
                    | (crate::JoinKind::RightSemi, crate::JoinSide::Left)
            )
    });
    if !valid {
        errors.push(ValidationError::new(
            path,
            "runtime filter equality witness does not prove a safe hash-join key direction",
        ));
    }
}

pub(crate) fn validate_runtime_filter_producer_target(
    fragment: &Fragment,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    producer: &crate::RuntimeFilterProducer,
    domain: &crate::RuntimeFilterDomain,
    reduction: crate::RuntimeFilterReduction,
    path: &str,
    errors: &mut ValidationContext,
    parent_proof_work: &mut SemanticTraceWorkBudget,
) {
    if matches!(
        producer.target,
        crate::RuntimeFilterProducerTarget::AggregateTopNKey { limit: 0, .. }
    ) {
        errors.push(ValidationError::new(
            path,
            "runtime filter Aggregate TopN producer has a zero limit",
        ));
        return;
    }
    let valid = match producer.target {
        crate::RuntimeFilterProducerTarget::JoinBuildKey { equality }
            if matches!(
                reduction,
                crate::RuntimeFilterReduction::SetUnion
                    | crate::RuntimeFilterReduction::UnionOrderedHull
            ) =>
        {
            runtime_filter_equality_witness(witnesses, equality).is_some_and(|witness| {
                witness.fragment == fragment.id()
                    && witness.join == producer.endpoint.node
                    && producer.apply_point
                        == (crate::RuntimeFilterApplyPoint::NodeInput {
                            input_ordinal: witness.domain_side.input_ordinal(),
                        })
                    && equality_key_value(fragment, witness, witness.domain_side)
                        .is_some_and(|value| producer.endpoint.values.as_ref() == [value])
            })
        }
        crate::RuntimeFilterProducerTarget::AggregateTopNKey {
            group_key_ordinal,
            topn,
            phase,
            order_key_ordinal,
            limit,
            offset,
            direction,
            null_ordering,
        } if matches!(
            reduction,
            crate::RuntimeFilterReduction::TightenOrderedBound
        ) && limit > 0 =>
        {
            let Some(aggregate) = fragment.nodes().get(&producer.endpoint.node) else {
                return;
            };
            let NodeKind::Aggregate { group_by, .. } = &aggregate.kind else {
                return errors.push(ValidationError::new(
                    path,
                    "runtime filter Aggregate TopN producer does not belong to an aggregate",
                ));
            };
            let Some(topn_node) = fragment.nodes().get(&topn) else {
                return errors.push(ValidationError::new(
                    path,
                    "runtime filter Aggregate TopN producer references an absent TopN node",
                ));
            };
            let NodeKind::TopN {
                order_by,
                limit: actual_limit,
                offset: actual_offset,
                phase: actual_phase,
                reduction: topn_reduction,
            } = &topn_node.kind
            else {
                return errors.push(ValidationError::new(
                    path,
                    "runtime filter Aggregate TopN producer witness is not a TopN node",
                ));
            };
            let group_key = group_by.get(usize::try_from(group_key_ordinal).unwrap_or(usize::MAX));
            let order_key = order_by.get(usize::try_from(order_key_ordinal).unwrap_or(usize::MAX));
            let ordered_domain_matches = matches!(
                domain,
                crate::RuntimeFilterDomain::Ordered { key, .. }
                    if key.direction == direction && key.null_ordering == null_ordering
            );
            let parent_matches = match topn_reduction {
                crate::TopNReduction::Rows => true,
                crate::TopNReduction::GroupedStates { .. } => {
                    aggregate::aggregate_topn_grouped_parent_matches(
                        fragment,
                        aggregate,
                        topn_node,
                        parent_proof_work,
                    )
                }
            };
            parent_matches
                && producer.apply_point
                    == (crate::RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 })
                && aggregate.inputs.len() == 1
                && group_by.len() == 1
                && topn_node.inputs.as_ref() == [aggregate.id]
                && order_by.len() == 1
                && phase == *actual_phase
                && matches!(
                    phase,
                    crate::TopNPhase::Single | crate::TopNPhase::Partial { .. }
                )
                && limit == *actual_limit
                && offset == *actual_offset
                && offset == 0
                && order_key.is_some_and(|item| {
                    item.direction == direction
                        && item.null_ordering == null_ordering
                        && group_key.is_some_and(|(_, output)| {
                            crate::expression_value(fragment.expressions(), item.expr)
                                == Some(*output)
                        })
                })
                && ordered_domain_matches
                && group_key
                    .and_then(|(expression, _)| {
                        crate::expression_value(fragment.expressions(), *expression)
                    })
                    .is_some_and(|value| producer.endpoint.values.as_ref() == [value])
        }
        _ => false,
    };
    if !valid {
        let detail = match producer.target {
            crate::RuntimeFilterProducerTarget::JoinBuildKey { .. } => {
                "runtime filter producer target does not match its equality witness and exact build key"
            }
            crate::RuntimeFilterProducerTarget::AggregateTopNKey { .. } => {
                "runtime filter Aggregate TopN producer target does not match its exact group key"
            }
        };
        errors.push(ValidationError::new(path, detail));
    }
}

pub(crate) fn validate_runtime_filter_consumer_semantics(
    fragment: &Fragment,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    producers: &[crate::RuntimeFilterProducer],
    domain: &crate::RuntimeFilterDomain,
    consumer: &crate::RuntimeFilterConsumer,
    indexes: &mut RuntimeFilterLineageIndexes,
    path: &str,
    errors: &mut ValidationContext,
) {
    if let crate::RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { late_apply }
    | crate::RuntimeFilterConsumerActivation::NonBlockingLive { late_apply } =
        consumer.activation
    {
        // What a filter can be applied at follows from what it decides, not
        // from where it is applied. A membership set decides which rows
        // survive, so it is applied to the rows as they come, a batch at a
        // time, by the scan reading them just as by the join probing them. An
        // ordered bound decides what is worth reading at all, so at a scan it
        // reaches the units a provider reads in.
        let supported = match domain {
            crate::RuntimeFilterDomain::Membership { .. } => matches!(
                late_apply,
                crate::LateApplyGranularity::Row | crate::LateApplyGranularity::Batch
            ),
            crate::RuntimeFilterDomain::Ordered { .. } => match consumer.target {
                crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => matches!(
                    late_apply,
                    crate::LateApplyGranularity::Row | crate::LateApplyGranularity::Batch
                ),
                crate::RuntimeFilterConsumerTarget::ScanField { .. }
                | crate::RuntimeFilterConsumerTarget::AggregateTopNScanField { .. } => matches!(
                    late_apply,
                    crate::LateApplyGranularity::Batch
                        | crate::LateApplyGranularity::RowGroup
                        | crate::LateApplyGranularity::Split
                        | crate::LateApplyGranularity::File
                ),
            },
        };
        if !supported {
            errors.push(ValidationError::new(
                path,
                "runtime filter late-apply granularity is unsupported at its consumer target",
            ));
        }
    }

    let valid = match &consumer.target {
        crate::RuntimeFilterConsumerTarget::JoinProbeKey { equality } => {
            runtime_filter_equality_witness(witnesses, *equality).is_some_and(|witness| {
                let probe_side = witness.domain_side.opposite();
                witness.fragment == fragment.id()
                    && witness.join == consumer.endpoint.node
                    && consumer.apply_point
                        == (crate::RuntimeFilterApplyPoint::NodeInput {
                            input_ordinal: probe_side.input_ordinal(),
                        })
                    && equality_key_value(fragment, witness, probe_side)
                        .is_some_and(|value| consumer.endpoint.values.as_ref() == [value])
            })
        }
        crate::RuntimeFilterConsumerTarget::ScanField { equality, .. } => {
            runtime_filter_equality_witness(witnesses, *equality).is_some()
                && consumer.apply_point == crate::RuntimeFilterApplyPoint::ScanSource
                && fragment
                    .nodes()
                    .get(&consumer.endpoint.node)
                    .is_some_and(|node| {
                        consumer.endpoint.values.len() == 1
                            && indexes.scan_provider_contains(
                                fragment,
                                node,
                                consumer.endpoint.values[0],
                            )
                    })
        }
        crate::RuntimeFilterConsumerTarget::AggregateTopNScanField { producer, .. } => {
            producers.iter().any(|candidate| {
                candidate.witness == *producer
                    && matches!(
                        candidate.target,
                        crate::RuntimeFilterProducerTarget::AggregateTopNKey { .. }
                    )
            }) && consumer.apply_point == crate::RuntimeFilterApplyPoint::ScanSource
                && fragment
                    .nodes()
                    .get(&consumer.endpoint.node)
                    .is_some_and(|node| {
                        consumer.endpoint.values.len() == 1
                            && indexes.scan_provider_contains(
                                fragment,
                                node,
                                consumer.endpoint.values[0],
                            )
                    })
        }
    };
    if !valid {
        let detail = match &consumer.target {
            crate::RuntimeFilterConsumerTarget::AggregateTopNScanField { .. } => {
                "runtime filter Aggregate TopN consumer has no exact producer witness or scan field"
            }
            crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. }
            | crate::RuntimeFilterConsumerTarget::ScanField { .. } => {
                "runtime filter consumer target is not locally valid for its equality witness"
            }
        };
        errors.push(ValidationError::new(path, detail));
    }
}

pub(crate) fn validate_runtime_filter_consumer_lineage(
    plan: &PhysicalPlan,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    producers: &[crate::RuntimeFilterProducer],
    consumer: &crate::RuntimeFilterConsumer,
    path: &str,
    indexes: &mut RuntimeFilterLineageIndexes,
    errors: &mut ValidationContext,
) {
    let (origin, lineage) = match &consumer.target {
        crate::RuntimeFilterConsumerTarget::ScanField { equality, lineage } => (
            RuntimeFilterLineageOrigin::Join(*equality),
            lineage.as_ref(),
        ),
        crate::RuntimeFilterConsumerTarget::AggregateTopNScanField { producer, lineage } => (
            RuntimeFilterLineageOrigin::AggregateTopN(*producer),
            lineage.as_ref(),
        ),
        crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => return,
    };
    if lineage.len() > errors.limits().runtime_filter_lineage_steps
        || runtime_filter_scan_lineage_is_valid(
            |id| plan.fragments().get(&id),
            |id| plan.edges().get(&id),
            witnesses,
            producers,
            consumer,
            origin,
            lineage,
            indexes,
        )
        .is_none()
    {
        errors.push(ValidationError::new(
            path,
            "runtime filter scan consumer is not connected to its exact probe key by a safe lineage",
        ));
    }
}

/// Recheck every locally addressable lineage segment using the whole-plan
/// rules. Cross-fragment connectivity is FE-owned; no executable peer graph
/// is materialized to reconstruct it here.
pub(crate) fn validate_local_runtime_filter_consumer_lineage(
    fragment: &Fragment,
    inbound: &BTreeMap<EdgeId, &crate::InboundFragmentCut>,
    outbound: &BTreeMap<EdgeId, &crate::OutboundFragmentCut>,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    producers: &[crate::RuntimeFilterProducer],
    consumer: &crate::RuntimeFilterConsumer,
    path: &str,
    indexes: &mut RuntimeFilterLineageIndexes,
    errors: &mut ValidationContext,
) {
    let (origin, lineage, origin_fragment) = match &consumer.target {
        crate::RuntimeFilterConsumerTarget::ScanField { equality, lineage } => {
            let Some(witness) = runtime_filter_equality_witness(witnesses, *equality) else {
                return;
            };
            (
                RuntimeFilterLineageOrigin::Join(*equality),
                lineage.as_ref(),
                witness.fragment,
            )
        }
        crate::RuntimeFilterConsumerTarget::AggregateTopNScanField { producer, lineage } => {
            let Some(producer) = producers.iter().find(|item| item.witness == *producer) else {
                return;
            };
            (
                RuntimeFilterLineageOrigin::AggregateTopN(producer.witness),
                lineage.as_ref(),
                producer.endpoint.fragment,
            )
        }
        crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => return,
    };
    if lineage.len() > errors.limits().runtime_filter_lineage_steps {
        errors.push(ValidationError::resource_limit(
            path,
            "runtime-filter lineage exceeds its local work bound",
        ));
        return;
    }
    let valid = validate_runtime_filter_local_segments(
        fragment,
        inbound,
        outbound,
        witnesses,
        producers,
        consumer,
        origin,
        origin_fragment,
        lineage,
        indexes,
    );
    if valid.is_none() {
        errors.push(ValidationError::new(path,
            "runtime filter scan consumer is not connected to its exact probe key by a safe lineage"));
    }
}

/// State changes between local and remote only at an exact exchange mapping.
/// Remote executable facts stay FE-owned; every locally addressable segment
/// uses the same node/occurrence/adjacency rules as the complete-plan walk.
fn validate_runtime_filter_local_segments(
    fragment: &Fragment,
    inbound: &BTreeMap<EdgeId, &crate::InboundFragmentCut>,
    outbound: &BTreeMap<EdgeId, &crate::OutboundFragmentCut>,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    producers: &[crate::RuntimeFilterProducer],
    consumer: &crate::RuntimeFilterConsumer,
    origin: RuntimeFilterLineageOrigin,
    origin_fragment: FragmentId,
    lineage: &[crate::RuntimeFilterLineageStep],
    indexes: &mut RuntimeFilterLineageIndexes,
) -> Option<()> {
    let lookup = |id| (id == fragment.id()).then_some(fragment);
    let mut local = if origin_fragment == fragment.id() {
        Some(runtime_filter_lineage_start(
            lookup, witnesses, producers, origin, indexes,
        )?)
    } else {
        None
    };
    // A cut fixes the immediate peer identity even though its executable
    // nodes remain FE-owned. An unobserved remote exchange loses that fact.
    let mut known_remote = (origin_fragment != fragment.id()).then_some(origin_fragment);
    let mut visited = BTreeSet::new();
    if let Some(position) = local {
        visited.insert(position);
    }
    for step in lineage {
        match step {
            crate::RuntimeFilterLineageStep::ExchangeMapping {
                edge,
                mapping_ordinal,
            } => {
                let ordinal = usize::try_from(*mapping_ordinal).ok()?;
                if let Some(position) = local {
                    let cut = inbound.get(edge)?;
                    let mapping = cut.imports.get(ordinal)?;
                    let node = fragment.nodes().get(&position.1)?;
                    let NodeKind::ExchangeSource {
                        edge: node_edge,
                        imports,
                    } = &node.kind
                    else {
                        return None;
                    };
                    if cut.kind != crate::EdgeKind::Stream
                        || cut.destination_node != position.1
                        || mapping.destination != position.2
                        || *node_edge != *edge
                        || imports.get(ordinal).copied()
                            != Some((mapping.source.value, mapping.destination))
                    {
                        return None;
                    }
                    local = None;
                    known_remote = Some(cut.source_fragment);
                } else if let Some(cut) = outbound.get(edge) {
                    let mapping = cut.destination_imports.get(ordinal)?;
                    if cut.kind != crate::EdgeKind::Stream
                        || cut.projection.get(ordinal) != Some(&mapping.source)
                        || known_remote.is_some_and(|peer| peer != cut.destination_fragment)
                    {
                        return None;
                    }
                    let position = (fragment.id(), fragment.root(), mapping.source.value);
                    if !visited.insert(position) {
                        return None;
                    }
                    local = Some(position);
                    known_remote = None;
                } else if inbound.contains_key(edge) {
                    return None;
                } else {
                    known_remote = None;
                }
            }
            crate::RuntimeFilterLineageStep::FilterPassThrough { fragment: id, .. }
            | crate::RuntimeFilterLineageStep::SortPassThrough { fragment: id, .. }
            | crate::RuntimeFilterLineageStep::ProjectIdentity { fragment: id, .. }
            | crate::RuntimeFilterLineageStep::JoinEquality { fragment: id, .. }
            | crate::RuntimeFilterLineageStep::JoinOutputPassThrough { fragment: id, .. }
            | crate::RuntimeFilterLineageStep::AggregateGroupKey { fragment: id, .. }
            | crate::RuntimeFilterLineageStep::UnionAllBranch { fragment: id, .. } => {
                if *id == fragment.id() {
                    let position = local?;
                    let next = advance_runtime_filter_lineage_step(
                        lookup,
                        |_| None,
                        position,
                        step,
                        indexes,
                    )?;
                    if !visited.insert(next) {
                        return None;
                    }
                    local = Some(next);
                } else if local.is_some() || known_remote.is_some_and(|peer| peer != *id) {
                    return None;
                }
            }
        }
    }
    if consumer.endpoint.fragment == fragment.id() {
        let position = local?;
        if position
            != (
                fragment.id(),
                consumer.endpoint.node,
                *consumer.endpoint.values.first()?,
            )
            || consumer.endpoint.values.len() != 1
            || consumer.apply_point != crate::RuntimeFilterApplyPoint::ScanSource
        {
            return None;
        }
        let node = fragment.nodes().get(&position.1)?;
        indexes
            .scan_provider_contains(fragment, node, position.2)
            .then_some(())
    } else {
        (local.is_none() && known_remote.is_none_or(|peer| peer == consumer.endpoint.fragment))
            .then_some(())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RuntimeFilterLineageOrigin {
    Join(crate::RuntimeFilterEqualityWitnessId),
    AggregateTopN(crate::RuntimeFilterWitnessId),
}

pub(crate) fn runtime_filter_scan_lineage_is_valid<'a>(
    get_fragment: impl Fn(FragmentId) -> Option<&'a Fragment>,
    get_edge: impl Fn(EdgeId) -> Option<&'a crate::Edge>,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    producers: &[crate::RuntimeFilterProducer],
    consumer: &crate::RuntimeFilterConsumer,
    origin: RuntimeFilterLineageOrigin,
    lineage: &[crate::RuntimeFilterLineageStep],
    indexes: &mut RuntimeFilterLineageIndexes,
) -> Option<()> {
    let position =
        runtime_filter_lineage_start(&get_fragment, witnesses, producers, origin, indexes)?;
    runtime_filter_scan_lineage_from(get_fragment, get_edge, consumer, position, lineage, indexes)
}

fn runtime_filter_lineage_start<'a>(
    get_fragment: impl Fn(FragmentId) -> Option<&'a Fragment>,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    producers: &[crate::RuntimeFilterProducer],
    origin: RuntimeFilterLineageOrigin,
    indexes: &mut RuntimeFilterLineageIndexes,
) -> Option<(FragmentId, NodeId, ValueId)> {
    let position = match origin {
        RuntimeFilterLineageOrigin::Join(equality) => {
            let witness = runtime_filter_equality_witness(witnesses, equality)?;
            let fragment = get_fragment(witness.fragment)?;
            let join = fragment.nodes().get(&witness.join)?;
            let probe_side = witness.domain_side.opposite();
            let probe_value = equality_key_value(fragment, witness, probe_side)?;
            let probe_input = join
                .inputs
                .get(usize::try_from(probe_side.input_ordinal()).ok()?)
                .copied()?;
            if !node_has_exact_parent(&mut indexes.parents, fragment, probe_input, witness.join) {
                return None;
            }
            (witness.fragment, probe_input, probe_value)
        }
        RuntimeFilterLineageOrigin::AggregateTopN(producer_witness) => {
            let producer = producers
                .iter()
                .find(|producer| producer.witness == producer_witness)?;
            let crate::RuntimeFilterProducerTarget::AggregateTopNKey { .. } = producer.target
            else {
                return None;
            };
            let fragment = get_fragment(producer.endpoint.fragment)?;
            let aggregate = fragment.nodes().get(&producer.endpoint.node)?;
            let input = *aggregate.inputs.first()?;
            if aggregate.inputs.len() != 1
                || !node_has_exact_parent(&mut indexes.parents, fragment, input, aggregate.id)
            {
                return None;
            }
            (fragment.id(), input, *producer.endpoint.values.first()?)
        }
    };
    Some(position)
}

fn runtime_filter_scan_lineage_from<'a>(
    get_fragment: impl Fn(FragmentId) -> Option<&'a Fragment>,
    get_edge: impl Fn(EdgeId) -> Option<&'a crate::Edge>,
    consumer: &crate::RuntimeFilterConsumer,
    mut position: (FragmentId, NodeId, ValueId),
    lineage: &[crate::RuntimeFilterLineageStep],
    indexes: &mut RuntimeFilterLineageIndexes,
) -> Option<()> {
    let mut visited = BTreeSet::from([position]);
    for step in lineage {
        let next =
            advance_runtime_filter_lineage_step(&get_fragment, &get_edge, position, step, indexes)?;
        if !visited.insert(next) {
            return None;
        }
        position = next;
    }

    if position
        != (
            consumer.endpoint.fragment,
            consumer.endpoint.node,
            *consumer.endpoint.values.first()?,
        )
        || consumer.endpoint.values.len() != 1
        || consumer.apply_point != crate::RuntimeFilterApplyPoint::ScanSource
    {
        return None;
    }
    let fragment = get_fragment(position.0)?;
    let node = fragment.nodes().get(&position.1)?;
    indexes
        .scan_provider_contains(fragment, node, position.2)
        .then_some(())
}

fn advance_runtime_filter_lineage_step<'a>(
    get_fragment: impl Fn(FragmentId) -> Option<&'a Fragment>,
    get_edge: impl Fn(EdgeId) -> Option<&'a crate::Edge>,
    position: (FragmentId, NodeId, ValueId),
    step: &crate::RuntimeFilterLineageStep,
    indexes: &mut RuntimeFilterLineageIndexes,
) -> Option<(FragmentId, NodeId, ValueId)> {
    let next = match *step {
        crate::RuntimeFilterLineageStep::FilterPassThrough {
            fragment,
            node,
            input_ordinal,
        } => {
            if (fragment, node) != (position.0, position.1) {
                return None;
            }
            let fragment = get_fragment(fragment)?;
            let node = fragment.nodes().get(&node)?;
            if !matches!(node.kind, NodeKind::Filter { .. })
                || node.inputs.len() != 1
                || input_ordinal != 0
                || !indexes.port_contains(fragment, node, position.2)
            {
                return None;
            }
            let child = node.inputs[0];
            let child_node = fragment.nodes().get(&child)?;
            if !indexes.port_contains(fragment, child_node, position.2)
                || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
            {
                return None;
            }
            (fragment.id(), child, position.2)
        }
        crate::RuntimeFilterLineageStep::SortPassThrough {
            fragment,
            node,
            input_ordinal,
        } => {
            if (fragment, node) != (position.0, position.1) {
                return None;
            }
            let fragment = get_fragment(fragment)?;
            let node = fragment.nodes().get(&node)?;
            let safe_sort = matches!(
                &node.kind,
                NodeKind::Sort {
                    order_by,
                    mode: crate::SortMode::Global | crate::SortMode::Analytic { .. },
                } if order_by
                    .iter()
                    .all(|item| crate::expression_value(fragment.expressions(), item.expr).is_some())
            );
            let child = node
                .inputs
                .get(usize::try_from(input_ordinal).ok()?)
                .copied()?;
            let child_node = fragment.nodes().get(&child)?;
            if !safe_sort
                || node.inputs.len() != 1
                || input_ordinal != 0
                || !indexes.port_contains(fragment, node, position.2)
                || !indexes.port_contains(fragment, child_node, position.2)
                || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
            {
                return None;
            }
            (fragment.id(), child, position.2)
        }
        crate::RuntimeFilterLineageStep::ProjectIdentity {
            fragment,
            node,
            output_ordinal,
        } => {
            if (fragment, node) != (position.0, position.1) {
                return None;
            }
            let fragment = get_fragment(fragment)?;
            let node = fragment.nodes().get(&node)?;
            let NodeKind::Project { expressions } = &node.kind else {
                return None;
            };
            if node.inputs.len() != 1
                || node
                    .output
                    .columns
                    .get(usize::try_from(output_ordinal).ok()?)
                    .copied()
                    != Some(position.2)
            {
                return None;
            }
            let child = node.inputs[0];
            let child_node = fragment.nodes().get(&child)?;
            let (expression, output) = expressions
                .get(usize::try_from(output_ordinal).ok()?)
                .copied()?;
            if output != position.2 {
                return None;
            }
            let source = crate::expression_value(fragment.expressions(), expression)?;
            if !indexes.port_contains(fragment, child_node, source)
                || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
            {
                return None;
            }
            (fragment.id(), child, source)
        }
        crate::RuntimeFilterLineageStep::JoinEquality {
            fragment,
            node,
            key_ordinal,
            source_side,
            target_side,
        } => {
            if (fragment, node) != (position.0, position.1) {
                return None;
            }
            let fragment = get_fragment(fragment)?;
            let node = fragment.nodes().get(&node)?;
            let NodeKind::HashJoin { kind, keys, .. } = &node.kind else {
                return None;
            };
            let key = keys.get(usize::try_from(key_ordinal).ok()?)?;
            if !kind.key_filter_reaches_side(target_side) || key.null_safe || node.inputs.len() != 2
            {
                return None;
            }
            let key_value = |side: crate::JoinSide| match side {
                crate::JoinSide::Left => {
                    crate::join_key_source_value(fragment.expressions(), key.left)
                }
                crate::JoinSide::Right => {
                    crate::join_key_source_value(fragment.expressions(), key.right)
                }
            };
            if key_value(source_side) != Some(position.2)
                || !indexes.port_contains(fragment, node, position.2)
            {
                return None;
            }
            let target_value = key_value(target_side)?;
            let target_input = node.inputs[usize::try_from(target_side.input_ordinal()).ok()?];
            let child_node = fragment.nodes().get(&target_input)?;
            if !indexes.port_contains(fragment, child_node, target_value)
                || !node_has_exact_parent(&mut indexes.parents, fragment, target_input, node.id)
            {
                return None;
            }
            (fragment.id(), target_input, target_value)
        }
        crate::RuntimeFilterLineageStep::JoinOutputPassThrough {
            fragment,
            node,
            input_ordinal,
        } => {
            if (fragment, node) != (position.0, position.1) {
                return None;
            }
            let fragment = get_fragment(fragment)?;
            let node = fragment.nodes().get(&node)?;
            let kind = match &node.kind {
                NodeKind::HashJoin { kind, .. } | NodeKind::NestLoopJoin { kind, .. } => *kind,
                _ => return None,
            };
            let side = match input_ordinal {
                0 => crate::JoinSide::Left,
                1 => crate::JoinSide::Right,
                _ => return None,
            };
            if node.inputs.len() != 2
                || !kind.side_only_loses_rows(side)
                || !indexes.port_contains(fragment, node, position.2)
            {
                return None;
            }
            let child = node.inputs[usize::from(input_ordinal != 0)];
            let child_node = fragment.nodes().get(&child)?;
            // The value has to be the child's own, republished unchanged:
            // a null-extended copy is a different value and stops here.
            if !indexes.port_contains(fragment, child_node, position.2)
                || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
            {
                return None;
            }
            (fragment.id(), child, position.2)
        }
        crate::RuntimeFilterLineageStep::AggregateGroupKey {
            fragment,
            node,
            group_key_ordinal,
        } => {
            if (fragment, node) != (position.0, position.1) {
                return None;
            }
            let fragment = get_fragment(fragment)?;
            let node = fragment.nodes().get(&node)?;
            let NodeKind::Aggregate { group_by, .. } = &node.kind else {
                return None;
            };
            let (expression, output) = group_by
                .get(usize::try_from(group_key_ordinal).ok()?)
                .copied()?;
            let source = crate::expression_value(fragment.expressions(), expression)?;
            let child = *node.inputs.first()?;
            let child_node = fragment.nodes().get(&child)?;
            if node.inputs.len() != 1
                || output != position.2
                || !indexes.port_contains(fragment, child_node, source)
                || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
            {
                return None;
            }
            (fragment.id(), child, source)
        }
        crate::RuntimeFilterLineageStep::UnionAllBranch {
            fragment,
            node,
            input_ordinal,
            output_ordinal,
        } => {
            if (fragment, node) != (position.0, position.1) {
                return None;
            }
            let fragment = get_fragment(fragment)?;
            let node = fragment.nodes().get(&node)?;
            let NodeKind::SetOp {
                kind: crate::SetOperationKind::UnionAll,
                input_mappings,
            } = &node.kind
            else {
                return None;
            };
            let input_ordinal = usize::try_from(input_ordinal).ok()?;
            let output_ordinal = usize::try_from(output_ordinal).ok()?;
            if node.output.columns.get(output_ordinal).copied() != Some(position.2) {
                return None;
            }
            let child = *node.inputs.get(input_ordinal)?;
            let source = *input_mappings.get(input_ordinal)?.get(output_ordinal)?;
            let child_node = fragment.nodes().get(&child)?;
            if !indexes.port_contains(fragment, child_node, source)
                || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
            {
                return None;
            }
            (fragment.id(), child, source)
        }
        crate::RuntimeFilterLineageStep::ExchangeMapping {
            edge,
            mapping_ordinal,
        } => {
            let fragment = get_fragment(position.0)?;
            let node = fragment.nodes().get(&position.1)?;
            let NodeKind::ExchangeSource {
                edge: node_edge,
                imports,
            } = &node.kind
            else {
                return None;
            };
            let edge_contract = get_edge(edge)?;
            let ordinal = usize::try_from(mapping_ordinal).ok()?;
            let (source, destination) = *edge_contract.destination.receive_mapping.get(ordinal)?;
            if *node_edge != edge
                || edge_contract.kind != crate::EdgeKind::Stream
                || edge_contract.destination.fragment != position.0
                || edge_contract.destination.node != position.1
                || destination != position.2
                || imports.get(ordinal).copied() != Some((source, destination))
                || edge_contract.source.projection.get(ordinal).copied() != Some(source)
            {
                return None;
            }
            let source_fragment = get_fragment(edge_contract.source.fragment)?;
            let source_root = source_fragment.nodes().get(&source_fragment.root())?;
            if !indexes.port_contains(source_fragment, source_root, source)
                || !matches!(source_fragment.sink(), FragmentSink::Stream { edge: sink_edge }
                        if *sink_edge == edge)
            {
                return None;
            }
            (source_fragment.id(), source_fragment.root(), source)
        }
    };
    Some(next)
}

pub(crate) fn node_has_exact_parent(
    indexes: &mut BTreeMap<FragmentId, BTreeMap<NodeId, Option<NodeId>>>,
    fragment: &Fragment,
    child: NodeId,
    expected_parent: NodeId,
) -> bool {
    let parents = indexes.entry(fragment.id()).or_insert_with(|| {
        let mut parents = BTreeMap::new();
        for node in fragment.nodes().values() {
            for child in &node.inputs {
                parents
                    .entry(*child)
                    .and_modify(|parent| *parent = None)
                    .or_insert(Some(node.id));
            }
        }
        parents
    });
    parents.get(&child).copied().flatten() == Some(expected_parent)
}
