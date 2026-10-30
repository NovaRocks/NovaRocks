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

//! Admit and lower one fragment's runtime-filter endpoints into their local
//! program sites.
//!
//! The package's binding table is the only source of binding identities: one
//! entry per local endpoint, plan-global and never recomputed here. The
//! channel identity of every site is its physical runtime filter, which is
//! also the `filter_id` a scan's frozen dynamic filter names.
//!
//! Complete-once membership sites reuse existing hash-join build/probe keys
//! and blocking scan consumers. The first monotonic ordered route reuses an
//! original Partial Aggregate group key and its direct Scan's live Batch key.
//! Every endpoint retains its one original binding requirement. Shapes outside
//! these installed declarations are refused by name before expressions lower.

use crate::{
    channels::{NodeChannels, ResolvedInput, UnionChannelSource},
    creation::FragmentCreationError,
    lowering::FragmentCompileError,
    union_flow::RuntimeFilterKeyRoot,
};
use novarocks_connector_contract::ConnectorReadProgramRecipe;
use novarocks_local_program::{
    BindingRequirement, FilterConsumerActivation, FilterConsumerAtExpr, FilterMembershipSchema,
    FilterMembershipSchemaError, FilterNullSemantics, FilterProducerKind, FilterReduction,
    ProgramChannelLayoutRole, ProgramChannelSite, ProgramExprId, ProgramNodeId,
    StaticFilterConsumer, StaticFilterContract, StaticFilterProducer,
};
use novarocks_physical_plan::{
    FragmentPackage, NodeId, NodeKind, RuntimeFilter, RuntimeFilterApplyPoint,
    RuntimeFilterArtifactCapability, RuntimeFilterBindingCut, RuntimeFilterBindingRole,
    RuntimeFilterCompletion, RuntimeFilterConsumerActivation, RuntimeFilterConsumerTarget,
    RuntimeFilterContributionKind, RuntimeFilterDomain, RuntimeFilterLifecycle,
    RuntimeFilterNullSemantics, RuntimeFilterProducerTarget, RuntimeFilterReduction, ValueType,
};
use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ValueSchemaCreationFactory, ValueSchemaCreationScope,
};
use std::collections::{BTreeMap, BTreeSet};

#[path = "runtime_filter/ordered.rs"]
mod ordered;

/// One admitted hash-join build-key membership producer.
struct PlannedProducer {
    requirement: i32,
    key_ordinal: usize,
    producer: StaticFilterProducer,
}

/// One admitted scan-source consumer of output occurrence `ordinal`, with
/// the exact declared key and separately checked membership/ordered contract.
struct PlannedConsumer {
    requirement: i32,
    ordinal: usize,
    ty: ValueType,
    consumer: StaticFilterConsumer,
}

fn has_ordered_consumer(
    consumers: &[PlannedConsumer],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, CompileControlError> {
    for site in consumers {
        work.step()?;
        if matches!(
            site.consumer.contract(),
            StaticFilterContract::Ordered { .. }
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

struct PlannedJoinConsumer {
    requirement: i32,
    key_ordinal: usize,
    key: novarocks_physical_plan::ExprId,
    value: novarocks_physical_plan::ValueId,
    consumer: StaticFilterConsumer,
}

/// Every local runtime-filter endpoint of one fragment, admitted and keyed by
/// its physical node, each node's sites in binding-table order. Lowering takes
/// each node's sites exactly once.
pub(crate) struct PlannedRuntimeFilters {
    producers: BTreeMap<NodeId, Vec<PlannedProducer>>,
    aggregate_producers: BTreeMap<NodeId, Vec<ordered::PlannedAggregateProducer>>,
    join_consumers: BTreeMap<NodeId, Vec<PlannedJoinConsumer>>,
    consumers: BTreeMap<NodeId, Vec<PlannedConsumer>>,
}

/// The sites one lowered scan owns.
pub(crate) struct ScanFilterSites {
    pub ordered: bool,
    pub filters: novarocks_local_program::ScanFilterConsumers,
    pub requirements: Vec<BindingRequirement>,
    pub roots: Vec<RuntimeFilterKeyRoot>,
}

/// The producers one lowered hash join owns: each build-key ordinal with its
/// static producer, and their requirements.
pub(crate) struct JoinFilterSites {
    pub producers: Vec<(usize, StaticFilterProducer)>,
    pub consumers: Vec<(usize, ProgramExprId, StaticFilterConsumer)>,
    pub roots: Vec<RuntimeFilterKeyRoot>,
    pub requirements: Vec<BindingRequirement>,
}

fn unsupported(node: NodeId, feature: &'static str) -> FragmentCompileError {
    FragmentCompileError::Unsupported {
        node: Some(node),
        feature,
    }
}

fn static_filter(
    error: impl std::error::Error
    + novarocks_failure_contract::FailureProjection
    + Send
    + Sync
    + 'static,
) -> FragmentCompileError {
    FragmentCompileError::Owner {
        phase: "runtime filter",
        error: novarocks_failure_contract::SharedFailureCause::new(error),
    }
}

enum MembershipCreationFailure<H> {
    Schema,
    Control(CompileControlError),
    Creator(H),
    Resource(LayoutResourceError),
}
impl<H> From<FilterMembershipSchemaError> for MembershipCreationFailure<H> {
    fn from(_error: FilterMembershipSchemaError) -> Self {
        Self::Schema
    }
}
impl<H> From<LayoutResourceError> for MembershipCreationFailure<H> {
    fn from(error: LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}

/// The complete-once membership contract every lowered site of `filter`
/// shares. `node` names the endpoint a refusal reports.
fn membership_contract<H, F: ValueSchemaCreationFactory<H>>(
    filter: &RuntimeFilter,
    node: NodeId,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(StaticFilterContract, FilterNullSemantics), FragmentCreationError<H>> {
    let RuntimeFilterDomain::Membership { ty, null_semantics } = &filter.domain else {
        return Err(unsupported(node, "ordered-domain runtime filter").into());
    };
    if filter.lifecycle != RuntimeFilterLifecycle::CompleteOnce {
        return Err(unsupported(node, "monotonic-update runtime-filter lifecycle").into());
    }
    if filter.reduction != RuntimeFilterReduction::SetUnion {
        return Err(unsupported(node, "runtime-filter reduction other than set union").into());
    }
    let null_semantics = match null_semantics {
        RuntimeFilterNullSemantics::NeverMatches => FilterNullSemantics::NeverMatches,
        RuntimeFilterNullSemantics::NullSafeEqual => FilterNullSemantics::NullSafeEqual,
    };
    let contract = match factory {
        Some(factory) => {
            // One canonical entity keeps its own actual creator and borrowed
            // carrier backing. It never retains the compiler or its factory.
            work.flush()?;
            let mut scope = factory
                .begin_scope()
                .map_err(FragmentCreationError::Creator)?;
            work.flush()?;
            let storage = scope.storage_lifetime().retaining_observed(
                &ty.typed_binding().layout().arrow_backing_storage_lifetime(),
                |layout| {
                    scope
                        .request_layout(layout)
                        .map_err(FragmentCreationError::Creator)
                },
            )?;
            let canonical = FilterMembershipSchema::try_new_with_storage_observed(
                ty.data_type(),
                null_semantics,
                storage,
                |layout| {
                    scope
                        .request_layout(layout)
                        .map_err(MembershipCreationFailure::Creator)
                },
                |units| {
                    for _ in 0..units {
                        work.step().map_err(MembershipCreationFailure::Control)?;
                    }
                    // The sole canonical author observes work before both
                    // actual requests; flush it before the request callback.
                    work.flush().map_err(MembershipCreationFailure::Control)
                },
            )
            .map_err(|error| match error {
                MembershipCreationFailure::Schema => {
                    unsupported(node, "runtime-filter membership key type").into()
                }
                MembershipCreationFailure::Control(cause) => FragmentCreationError::Control(cause),
                MembershipCreationFailure::Creator(cause) => FragmentCreationError::Creator(cause),
                MembershipCreationFailure::Resource(cause) => {
                    FragmentCreationError::Resource(cause)
                }
            })?;
            StaticFilterContract::membership_from_canonical(canonical, ty.clone())
                .map_err(static_filter)?
        }
        None => StaticFilterContract::membership(ty, null_semantics)
            .map_err(|_| unsupported(node, "runtime-filter membership key type"))?,
    };
    Ok((contract, null_semantics))
}

fn contract_type(contract: &StaticFilterContract) -> Result<&ValueType, FragmentCompileError> {
    match contract {
        StaticFilterContract::Membership { value_type, .. } => Ok(value_type),
        StaticFilterContract::Ordered { .. } => Err(FragmentCompileError::Invalid(
            "membership runtime filter has an ordered contract",
        )),
    }
}

/// Admit every local endpoint the package's binding table names, and check
/// that each scan's frozen dynamic filters are exactly its admitted consumers.
pub(crate) fn plan_runtime_filters<H, F: ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    reads: &BTreeMap<NodeId, ConnectorReadProgramRecipe>,
    factory: Option<&F>,
    mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PlannedRuntimeFilters, FragmentCreationError<H>> {
    let fragment = package.fragment();
    let cuts = package.cuts();
    let mut filters = BTreeMap::new();
    for filter in cuts.runtime_filters.iter() {
        if matches!(filter.domain, RuntimeFilterDomain::Ordered { .. }) {
            crate::creation::before_tree_insert::<
                novarocks_physical_plan::RuntimeFilterId,
                &RuntimeFilter,
                H,
            >(
                filters.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
        }
        filters.insert(filter.id, filter);
        work.step()?;
    }
    for id in fragment.runtime_filters() {
        let supplied = filters.contains_key(id);
        work.step()?;
        if !supplied {
            return Err(FragmentCompileError::Invalid(
                "attached runtime filter has no supplied contract",
            )
            .into());
        }
    }
    let mut planned = PlannedRuntimeFilters {
        producers: BTreeMap::new(),
        aggregate_producers: BTreeMap::new(),
        join_consumers: BTreeMap::new(),
        consumers: BTreeMap::new(),
    };
    let mut bound = BTreeSet::new();
    for binding in cuts.runtime_filter_bindings.iter() {
        work.step()?;
        let filter = *filters
            .get(&binding.filter)
            .ok_or(FragmentCompileError::Invalid(
                "runtime-filter binding names no supplied runtime filter",
            ))?;
        if matches!(filter.domain, RuntimeFilterDomain::Ordered { .. })
            && !bound.contains(&binding.binding_id)
        {
            crate::creation::before_tree_insert::<u32, (), H>(
                bound.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
        }
        if !bound.insert(binding.binding_id) {
            return Err(FragmentCompileError::Invalid(
                "runtime-filter binding identity is not unique",
            )
            .into());
        }
        match binding.role {
            RuntimeFilterBindingRole::Producer(index) => plan_producer(
                package,
                filter,
                binding,
                index,
                &mut planned,
                factory,
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?,
            RuntimeFilterBindingRole::Consumer(index) => plan_consumer(
                package,
                filter,
                binding,
                index,
                &mut planned,
                factory,
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?,
        }
    }
    // A scan's frozen dynamic filters name exactly the filters its admitted
    // consumers read, each over the provider column its consumer keys.
    for node in fragment.nodes().values() {
        let NodeKind::Scan {
            provider_outputs, ..
        } = &node.kind
        else {
            continue;
        };
        work.step()?;
        let Some(recipe) = reads.get(&node.id) else {
            continue;
        };
        let scan = recipe.frozen().scan();
        let consumers = planned
            .consumers
            .get(&node.id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if consumers.is_empty() && scan.dynamic_filters().is_empty() {
            continue;
        }
        let ordered = has_ordered_consumer(consumers, work)?;
        let mut expected = BTreeMap::new();
        for consumer in consumers {
            let value = node.output.columns[consumer.ordinal];
            if ordered && !expected.contains_key(&consumer.consumer.channel_id()) {
                crate::creation::before_tree_insert::<u32, novarocks_physical_plan::ValueId, H>(
                    expected.len(),
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
            }
            if expected
                .insert(consumer.consumer.channel_id(), value)
                .is_some_and(|other| other != value)
            {
                return Err(FragmentCompileError::Invalid(
                    "one runtime filter keys two scan columns",
                )
                .into());
            }
            work.step()?;
        }
        // Assignment `i` names provider output `i`.
        let mut assigned = BTreeMap::new();
        for (ordinal, assignment) in scan.assignments().iter().enumerate() {
            if ordered && !assigned.contains_key(assignment.variable()) {
                crate::creation::before_tree_insert::<&str, usize, H>(
                    assigned.len(),
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
            }
            assigned.entry(assignment.variable()).or_insert(ordinal);
            work.step()?;
        }
        let mut actual = BTreeSet::new();
        for dynamic in scan.dynamic_filters() {
            let value = assigned
                .get(dynamic.variable())
                .and_then(|ordinal| provider_outputs.get(*ordinal))
                .map(|(_, value)| *value);
            work.step()?;
            if value.is_none() || expected.get(&dynamic.filter_id()) != value.as_ref() {
                return Err(FragmentCompileError::Invalid(
                    "scan dynamic filter binds no admitted runtime-filter consumer column",
                )
                .into());
            }
            if ordered && !actual.contains(&dynamic.filter_id()) {
                crate::creation::before_tree_insert::<u32, (), H>(
                    actual.len(),
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
            }
            actual.insert(dynamic.filter_id());
        }
        if actual.len() != expected.len() {
            return Err(FragmentCompileError::Invalid(
                "scan dynamic filters differ from its admitted runtime-filter consumers",
            )
            .into());
        }
    }
    Ok(planned)
}

fn requirement_id(
    binding: &RuntimeFilterBindingCut,
    node: NodeId,
) -> Result<i32, FragmentCompileError> {
    i32::try_from(binding.binding_id).map_err(|_| {
        unsupported(
            node,
            "runtime-filter binding identity outside the program requirement range",
        )
    })
}

fn plan_producer<H, F: ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    filter: &RuntimeFilter,
    binding: &RuntimeFilterBindingCut,
    index: usize,
    planned: &mut PlannedRuntimeFilters,
    factory: Option<&F>,
    mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCreationError<H>> {
    let fragment = package.fragment();
    let producer = filter
        .producers
        .get(index)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter binding names an absent producer",
        ))?;
    let node = producer.endpoint.node;
    if producer.endpoint.fragment != fragment.id() {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter binding names a remote producer",
        )
        .into());
    }
    let RuntimeFilterProducerTarget::JoinBuildKey { equality } = producer.target else {
        return ordered::plan_producer(
            package,
            filter,
            binding,
            index,
            planned,
            factory,
            crate::creation::borrow_scope(&mut temporary),
            work,
        );
    };
    let (contract, null_semantics) = membership_contract(filter, node, factory, work)?;
    // A fenced final domain is published by a separate completion owner; only
    // the delta-and-close membership producer is lowered.
    if producer
        .contribution_kinds
        .contains(&RuntimeFilterContributionKind::FinalDomainShard)
        || producer.completion == RuntimeFilterCompletion::FencedCommittedDomain
    {
        return Err(unsupported(node, "fenced final-domain runtime-filter producer").into());
    }
    if producer.contribution_kinds.as_ref()
        != [
            RuntimeFilterContributionKind::ValueDomainDelta,
            RuntimeFilterContributionKind::ProducerClosed,
        ]
        || producer.completion != RuntimeFilterCompletion::ProducerClosed
    {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter producer contributions differ from a membership producer",
        )
        .into());
    }
    let witness = filter
        .equality_witnesses
        .iter()
        .find(|witness| witness.id == equality)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter producer names an absent equality witness",
        ))?;
    work.step()?;
    if witness.fragment != fragment.id() || witness.join != node {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter producer witness names another join",
        )
        .into());
    }
    let join = fragment
        .nodes()
        .get(&node)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter producer names an absent node",
        ))?;
    let NodeKind::HashJoin {
        condition,
        build_side,
        ..
    } = &join.kind
    else {
        return Err(unsupported(node, "runtime-filter producer outside a hash join").into());
    };
    // The local build input is the physical build side, so only a witness
    // whose domain is the build side names a local build key.
    if witness.domain_side != *build_side {
        return Err(unsupported(
            node,
            "runtime-filter witness domain side differs from the join build side",
        )
        .into());
    }
    if producer.apply_point
        != (RuntimeFilterApplyPoint::NodeInput {
            input_ordinal: build_side.input_ordinal(),
        })
    {
        return Err(unsupported(
            node,
            "runtime-filter producer apply point other than its join build input",
        )
        .into());
    }
    let key_ordinal =
        usize::try_from(witness.key_ordinal).map_err(|_| CompileControlError::ResourceExhausted)?;
    let key = condition
        .keys()
        .get(key_ordinal)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter witness names an absent join key",
        ))?;
    let build_key = match build_side {
        novarocks_physical_plan::JoinSide::Left => key.left,
        novarocks_physical_plan::JoinSide::Right => key.right,
    };
    let key_type = &fragment
        .expressions()
        .get(build_key)
        .ok_or(FragmentCompileError::Invalid("missing join key definition"))?
        .ty;
    if !key_type.same_physical_binding_ignoring_root_nullability_observed(
        contract_type(&contract)?,
        || work.step(),
    )? {
        return Err(unsupported(
            node,
            "runtime-filter build key type differs from its membership type",
        )
        .into());
    }
    let key_semantics = if key.null_safe {
        FilterNullSemantics::NullSafeEqual
    } else {
        FilterNullSemantics::NeverMatches
    };
    if key_semantics != null_semantics {
        return Err(unsupported(
            node,
            "runtime-filter NULL semantics differ from the join key equality",
        )
        .into());
    }
    let requirement = requirement_id(binding, node)?;
    let producer = StaticFilterProducer::try_new(
        binding.binding_id,
        filter.id.get(),
        FilterProducerKind::Membership,
        contract,
        FilterReduction::SetUnion,
    )
    .map_err(static_filter)?;
    planned
        .producers
        .entry(node)
        .or_default()
        .push(PlannedProducer {
            requirement,
            key_ordinal,
            producer,
        });
    work.step()?;
    Ok(())
}

fn plan_consumer<H, F: ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    filter: &RuntimeFilter,
    binding: &RuntimeFilterBindingCut,
    index: usize,
    planned: &mut PlannedRuntimeFilters,
    factory: Option<&F>,
    mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCreationError<H>> {
    let fragment = package.fragment();
    let consumer = filter
        .consumers
        .get(index)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter binding names an absent consumer",
        ))?;
    let node = consumer.endpoint.node;
    if consumer.endpoint.fragment != fragment.id() {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter binding names a remote consumer",
        )
        .into());
    }
    match consumer.target {
        RuntimeFilterConsumerTarget::ScanField { .. } => {}
        RuntimeFilterConsumerTarget::JoinProbeKey { .. } => {
            return plan_join_consumer(package, filter, binding, index, planned, factory, work);
        }
        RuntimeFilterConsumerTarget::AggregateTopNScanField { .. } => {
            return ordered::plan_consumer(
                package,
                filter,
                binding,
                index,
                planned,
                factory,
                crate::creation::borrow_scope(&mut temporary),
                work,
            );
        }
    }
    let (contract, _) = membership_contract(filter, node, factory, work)?;
    if consumer.apply_point != RuntimeFilterApplyPoint::ScanSource {
        return Err(unsupported(node, "runtime-filter consumer at a node input or output").into());
    }
    match consumer.activation {
        RuntimeFilterConsumerActivation::BlockingSnapshot => {}
        RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { .. } => {
            return Err(
                unsupported(node, "start-unfiltered runtime-filter consumer activation").into(),
            );
        }
        RuntimeFilterConsumerActivation::NonBlockingLive { .. } => {
            return Err(
                unsupported(node, "non-blocking live runtime-filter consumer activation").into(),
            );
        }
    }
    if consumer.capabilities.as_ref()
        != [
            RuntimeFilterArtifactCapability::Membership,
            RuntimeFilterArtifactCapability::EmptyDomain,
        ]
    {
        return Err(unsupported(
            node,
            "runtime-filter consumer capabilities other than membership and empty domain",
        )
        .into());
    }
    let scan = fragment
        .nodes()
        .get(&node)
        .ok_or(FragmentCompileError::Invalid(
            "runtime-filter consumer names an absent node",
        ))?;
    if !matches!(scan.kind, NodeKind::Scan { .. }) {
        return Err(unsupported(node, "runtime-filter consumer outside a scan source").into());
    }
    let [value] = consumer.endpoint.values.as_ref() else {
        return Err(FragmentCompileError::Invalid(
            "runtime-filter consumer endpoint is not one value",
        )
        .into());
    };
    let mut ordinal = None;
    for (index, output) in scan.output.columns.iter().enumerate() {
        work.step()?;
        if output == value {
            ordinal = Some(index);
            break;
        }
    }
    let ordinal = ordinal
        .ok_or_else(|| unsupported(node, "runtime-filter consumer value is not a scan output"))?;
    let ty = fragment
        .values()
        .get(value)
        .ok_or(FragmentCompileError::Invalid(
            "missing runtime-filter consumer value",
        ))?
        .ty
        .clone();
    if !ty.same_physical_binding_ignoring_root_nullability_observed(
        contract_type(&contract)?,
        || work.step(),
    )? {
        return Err(unsupported(
            node,
            "runtime-filter scan key type differs from its membership type",
        )
        .into());
    }
    let requirement = requirement_id(binding, node)?;
    let consumer = StaticFilterConsumer::try_new(
        binding.binding_id,
        filter.id.get(),
        FilterConsumerActivation::BlockingSnapshot,
        contract,
        FilterReduction::SetUnion,
    )
    .map_err(static_filter)?;
    planned
        .consumers
        .entry(node)
        .or_default()
        .push(PlannedConsumer {
            requirement,
            ordinal,
            ty,
            consumer,
        });
    work.step()?;
    Ok(())
}

fn plan_join_consumer<H, F: ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    filter: &RuntimeFilter,
    binding: &RuntimeFilterBindingCut,
    index: usize,
    planned: &mut PlannedRuntimeFilters,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCreationError<H>> {
    let fragment = package.fragment();
    let consumer = &filter.consumers[index];
    let node = consumer.endpoint.node;
    let RuntimeFilterConsumerTarget::JoinProbeKey { equality } = consumer.target else {
        return Err(FragmentCompileError::Invalid("expected join probe-key target").into());
    };
    let witness = filter
        .equality_witnesses
        .iter()
        .find(|w| w.id == equality)
        .ok_or(FragmentCompileError::Invalid(
            "join probe consumer has no equality witness",
        ))?;
    let join = fragment
        .nodes()
        .get(&node)
        .ok_or(FragmentCompileError::Invalid(
            "join probe consumer has no join",
        ))?;
    let NodeKind::HashJoin {
        condition,
        build_side,
        ..
    } = &join.kind
    else {
        return Err(unsupported(node, "runtime-filter probe consumer outside a hash join").into());
    };
    if witness.fragment != fragment.id()
        || witness.join != node
        || witness.domain_side != *build_side
    {
        return Err(FragmentCompileError::Invalid(
            "join probe consumer equality witness differs from its join",
        )
        .into());
    }
    if consumer.apply_point
        != (RuntimeFilterApplyPoint::NodeInput {
            input_ordinal: build_side.opposite().input_ordinal(),
        })
    {
        return Err(unsupported(
            node,
            "runtime-filter probe consumer apply point differs from its probe input",
        )
        .into());
    }
    if consumer.activation != RuntimeFilterConsumerActivation::BlockingSnapshot {
        return Err(unsupported(
            node,
            "non-blocking join probe-key runtime-filter consumer activation",
        )
        .into());
    }
    if consumer.capabilities.as_ref()
        != [
            RuntimeFilterArtifactCapability::Membership,
            RuntimeFilterArtifactCapability::EmptyDomain,
        ]
    {
        return Err(unsupported(
            node,
            "runtime-filter probe consumer capabilities other than membership and empty domain",
        )
        .into());
    }
    let (contract, null_semantics) = membership_contract(filter, node, factory, work)?;
    let key_ordinal =
        usize::try_from(witness.key_ordinal).map_err(|_| CompileControlError::ResourceExhausted)?;
    let pair = condition
        .keys()
        .get(key_ordinal)
        .ok_or(FragmentCompileError::Invalid(
            "join probe consumer names an absent key",
        ))?;
    let key = match build_side {
        novarocks_physical_plan::JoinSide::Left => pair.right,
        novarocks_physical_plan::JoinSide::Right => pair.left,
    };
    let definition = fragment
        .expressions()
        .get(key)
        .ok_or(FragmentCompileError::Invalid(
            "missing join probe key definition",
        ))?;
    // The checked Physical endpoint author requires this exact direct Value.
    // No expression is inferred from a matching datatype or from SQL text.
    let novarocks_physical_plan::ExprKind::Value(value) = &definition.kind else {
        return Err(FragmentCompileError::Invalid(
            "join probe endpoint is not its checked direct value key",
        )
        .into());
    };
    if consumer.endpoint.values.as_ref() != [*value] {
        return Err(FragmentCompileError::Invalid(
            "join probe endpoint differs from its equality key",
        )
        .into());
    }
    if !definition
        .ty
        .same_physical_binding_ignoring_root_nullability_observed(
            contract_type(&contract)?,
            || work.step(),
        )?
    {
        return Err(unsupported(
            node,
            "runtime-filter probe key type differs from its membership type",
        )
        .into());
    }
    if null_semantics
        != if pair.null_safe {
            FilterNullSemantics::NullSafeEqual
        } else {
            FilterNullSemantics::NeverMatches
        }
    {
        return Err(unsupported(
            node,
            "runtime-filter NULL semantics differ from the join probe equality",
        )
        .into());
    }
    let requirement = requirement_id(binding, node)?;
    let consumer = StaticFilterConsumer::try_new(
        binding.binding_id,
        filter.id.get(),
        FilterConsumerActivation::BlockingSnapshot,
        contract,
        FilterReduction::SetUnion,
    )
    .map_err(static_filter)?;
    planned
        .join_consumers
        .entry(node)
        .or_default()
        .push(PlannedJoinConsumer {
            requirement,
            key_ordinal,
            key,
            value: *value,
            consumer,
        });
    work.step()?;
    Ok(())
}

impl PlannedRuntimeFilters {
    /// The number of compiler-authored consumer key definitions and roots.
    pub(crate) fn consumer_count(&self) -> Result<usize, FragmentCompileError> {
        let mut count = 0usize;
        for consumers in self.consumers.values() {
            count = count
                .checked_add(consumers.len())
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
        Ok(count)
    }

    pub(crate) fn join_consumer_count(&self) -> Result<usize, FragmentCompileError> {
        let mut count = 0usize;
        for consumers in self.join_consumers.values() {
            count = count
                .checked_add(consumers.len())
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
        Ok(count)
    }

    /// Every scan consumer's key read: the planned slot and channel of the
    /// scan output occurrence it keys, typed exactly as that occurrence.
    pub(crate) fn consumer_keys<H>(
        &self,
        nodes: &BTreeMap<NodeId, NodeChannels>,
        mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<BTreeMap<NodeId, Vec<UnionChannelSource>>, FragmentCreationError<H>> {
        let mut keys = BTreeMap::new();
        for (&scan, consumers) in &self.consumers {
            let planned = nodes.get(&scan).ok_or(FragmentCompileError::Invalid(
                "runtime-filter consumer scan has no planned channels",
            ))?;
            let mut reads = Vec::new();
            let ordered = has_ordered_consumer(consumers, work)?;
            if ordered {
                crate::creation::reserve_vec(
                    &mut reads,
                    consumers.len(),
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
                crate::creation::before_tree_insert::<NodeId, Vec<UnionChannelSource>, H>(
                    keys.len(),
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
            } else {
                crate::assert_rows::reserve_vec(&mut reads, consumers.len(), work)?;
            }
            for consumer in consumers {
                let slot =
                    *planned
                        .slots
                        .get(consumer.ordinal)
                        .ok_or(FragmentCompileError::Invalid(
                            "runtime-filter consumer occurrence has no slot",
                        ))?;
                reads.push(UnionChannelSource {
                    input: ResolvedInput {
                        slot,
                        source: ProgramChannelSite::Layout {
                            node: planned.local,
                            role: ProgramChannelLayoutRole::NodeOutput,
                            ordinal: u32::try_from(consumer.ordinal)
                                .map_err(|_| CompileControlError::ResourceExhausted)?,
                        },
                    },
                    ty: consumer.ty.clone(),
                });
                work.step()?;
            }
            keys.insert(scan, reads);
        }
        Ok(keys)
    }

    /// Take one lowered scan's consumer sites. `definitions` are its key
    /// reads, as expression lowering authored them in binding order.
    pub(crate) fn take_scan<H, F: ValueSchemaCreationFactory<H>>(
        &mut self,
        scan: NodeId,
        local: ProgramNodeId,
        definitions: Option<&Vec<ProgramExprId>>,
        factory: Option<&F>,
        mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ScanFilterSites, FragmentCreationError<H>> {
        let consumers = self.consumers.remove(&scan).unwrap_or_default();
        let definitions = definitions.map(Vec::as_slice).unwrap_or_default();
        if definitions.len() != consumers.len() {
            return Err(FragmentCompileError::Invalid(
                "runtime-filter consumer keys differ from their scan's consumers",
            )
            .into());
        }
        let ordered = has_ordered_consumer(&consumers, work)?;
        let mut scope = match factory {
            Some(factory) if ordered => {
                work.flush()?;
                Some(
                    factory
                        .begin_scope()
                        .map_err(FragmentCreationError::Creator)?,
                )
            }
            _ => None,
        };
        let mut filters = Vec::new();
        let mut requirements = Vec::new();
        let mut roots = Vec::new();
        crate::creation::reserve_vec(
            &mut filters,
            consumers.len(),
            scope
                .as_mut()
                .map(|scope| scope as &mut dyn ValueSchemaCreationScope<H>),
            work,
        )?;
        if ordered {
            crate::creation::reserve_vec(
                &mut requirements,
                consumers.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            crate::creation::reserve_vec(
                &mut roots,
                consumers.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
        } else {
            crate::assert_rows::reserve_vec(&mut requirements, consumers.len(), work)?;
            crate::assert_rows::reserve_vec(&mut roots, consumers.len(), work)?;
        }
        for (binding, (consumer, &definition)) in consumers.into_iter().zip(definitions).enumerate()
        {
            roots.push(RuntimeFilterKeyRoot {
                node: local,
                binding: u32::try_from(binding)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
                definition,
                source: ProgramChannelSite::Layout {
                    node: local,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: u32::try_from(consumer.ordinal)
                        .map_err(|_| CompileControlError::ResourceExhausted)?,
                },
            });
            requirements.push(BindingRequirement::RuntimeFilter {
                binding_id: consumer.requirement,
            });
            filters.push(FilterConsumerAtExpr {
                expr_id: definition,
                consumer: consumer.consumer,
            });
            work.step()?;
        }
        let filters = match scope.as_mut() {
            Some(scope) => novarocks_local_program::ScanFilterConsumers::try_from_vec_observed(
                filters,
                scope.storage_lifetime(),
                |layout| crate::creation::request(layout, scope, work),
            )?,
            None => filters.into(),
        };
        Ok(ScanFilterSites {
            ordered,
            filters,
            requirements,
            roots,
        })
    }

    /// Take one lowered hash join's producer sites.
    pub(crate) fn take_join(
        &mut self,
        join: NodeId,
        package: &FragmentPackage,
        planned_join: &crate::join::PlannedJoin,
        definitions: &BTreeMap<novarocks_physical_plan::ExprId, ProgramExprId>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<JoinFilterSites, FragmentCompileError> {
        let planned = self.producers.remove(&join).unwrap_or_default();
        let mut sites = JoinFilterSites {
            producers: Vec::new(),
            consumers: Vec::new(),
            roots: Vec::new(),
            requirements: Vec::new(),
        };
        crate::assert_rows::reserve_vec(&mut sites.producers, planned.len(), work)?;
        crate::assert_rows::reserve_vec(&mut sites.requirements, planned.len(), work)?;
        for producer in planned {
            sites.requirements.push(BindingRequirement::RuntimeFilter {
                binding_id: producer.requirement,
            });
            sites
                .producers
                .push((producer.key_ordinal, producer.producer));
            work.step()?;
        }
        let consumers = self.join_consumers.remove(&join).unwrap_or_default();
        crate::assert_rows::reserve_vec(&mut sites.consumers, consumers.len(), work)?;
        crate::assert_rows::reserve_vec(&mut sites.roots, consumers.len(), work)?;
        crate::assert_rows::reserve_vec(&mut sites.requirements, consumers.len(), work)?;
        let probe = &package.fragment().nodes()[&planned_join.probe];
        for (binding, consumer) in consumers.into_iter().enumerate() {
            let definition =
                *definitions
                    .get(&consumer.key)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing lowered probe consumer key",
                    ))?;
            let ordinal = probe
                .output
                .columns
                .iter()
                .position(|v| *v == consumer.value)
                .ok_or(FragmentCompileError::Invalid(
                    "probe consumer value is absent from its probe input",
                ))?;
            sites.roots.push(RuntimeFilterKeyRoot {
                node: planned_join.join,
                binding: u32::try_from(binding)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
                definition,
                source: ProgramChannelSite::Layout {
                    node: planned_join.join,
                    role: ProgramChannelLayoutRole::JoinLeft,
                    ordinal: u32::try_from(ordinal)
                        .map_err(|_| CompileControlError::ResourceExhausted)?,
                },
            });
            sites.requirements.push(BindingRequirement::RuntimeFilter {
                binding_id: consumer.requirement,
            });
            sites
                .consumers
                .push((consumer.key_ordinal, definition, consumer.consumer));
            work.step()?;
        }
        Ok(sites)
    }

    /// Every admitted endpoint was taken by exactly its lowered owner.
    pub(crate) fn finish(&self) -> Result<(), FragmentCompileError> {
        if !self.aggregate_producers.is_empty()
            || !self.producers.is_empty()
            || !self.consumers.is_empty()
            || !self.join_consumers.is_empty()
        {
            return Err(FragmentCompileError::Invalid(
                "runtime-filter endpoint has no lowered site",
            ));
        }
        Ok(())
    }
}
