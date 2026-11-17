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

//! Install validated ordered endpoints without reconstructing remote graphs.

use super::*;
use novarocks_local_program::{
    AggregateTopNFilter, FilterLateApplyGranularity, FilterNullOrder, FilterOrderKey,
    FilterOrderKeys, FilterSortDirection, is_aggregate_topn_key_type,
    is_ordered_filter_flat_payload_type, single_key_ordered_filter_digests_observed,
    visit_filter_key_type,
};
use novarocks_physical_plan::{
    AggregateGrouping, LateApplyGranularity, NullOrdering, SortDirection,
};
use novarocks_type_contract::OrderedComparisonAlgorithm;
use std::num::NonZeroU32;

pub(super) struct PlannedAggregateProducer {
    requirement: i32,
    key_ordinal: usize,
    key: novarocks_expression_contract::RootOrdinal,
    limit: NonZeroU32,
    producer: StaticFilterProducer,
}

pub(crate) struct AggregateFilterSites {
    pub filters: novarocks_local_program::AggregateTopNFilters,
    pub requirements: Vec<BindingRequirement>,
}

fn contract<H, F: ValueSchemaCreationFactory<H>>(
    filter: &RuntimeFilter,
    node: NodeId,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<StaticFilterContract, FragmentCreationError<H>> {
    let RuntimeFilterDomain::Ordered {
        key,
        inclusive: true,
        comparator,
    } = &filter.domain
    else {
        return Err(unsupported(node, "inclusive ordered runtime-filter domain").into());
    };
    if *comparator != OrderedComparisonAlgorithm::NativeScalarOrderV1
        || !is_aggregate_topn_key_type(&key.ty)
    {
        return Err(unsupported(node, "Aggregate TopN runtime-filter order key type").into());
    }
    if filter.lifecycle != RuntimeFilterLifecycle::MonotonicUpdates
        || filter.reduction != RuntimeFilterReduction::TightenOrderedBound
    {
        return Err(unsupported(node, "monotonic tightening ordered runtime filter").into());
    }
    let direction = match key.direction {
        SortDirection::Ascending => FilterSortDirection::Ascending,
        SortDirection::Descending => FilterSortDirection::Descending,
    };
    let null_order = match key.null_ordering {
        NullOrdering::First => FilterNullOrder::First,
        NullOrdering::Last => FilterNullOrder::Last,
    };
    let (comparator_digest, contract_digest) = single_key_ordered_filter_digests_observed(
        direction,
        null_order,
        |emit| visit_filter_key_type::<MembershipCreationFailure<H>>(key.ty.data_type(), emit),
        || work.step().map_err(MembershipCreationFailure::Control),
    )
    .map_err(|cause| match cause {
        MembershipCreationFailure::Schema => {
            unsupported(node, "Aggregate TopN runtime-filter key encoding").into()
        }
        MembershipCreationFailure::Control(cause) => FragmentCreationError::Control(cause),
        MembershipCreationFailure::Creator(cause) => FragmentCreationError::Creator(cause),
        MembershipCreationFailure::Resource(cause) => FragmentCreationError::Resource(cause),
    })?;
    let declared_key = FilterOrderKey {
        value_type: key.ty.clone_observed(|| work.step())?,
        direction,
        null_order,
    };
    let keys = match factory {
        Some(factory) => {
            work.flush()?;
            let mut scope = factory
                .begin_scope()
                .map_err(FragmentCreationError::Creator)?;
            work.flush()?;
            FilterOrderKeys::try_single_observed(
                declared_key,
                scope.storage_lifetime(),
                |layout| {
                    work.flush()?;
                    scope
                        .request_layout(layout)
                        .map_err(FragmentCreationError::Creator)
                },
            )?
        }
        None => vec![declared_key].into(),
    };
    Ok(StaticFilterContract::Ordered {
        keys,
        comparator_digest,
        contract_digest,
    })
}

/// The package already checked parent/order/state witnesses. This is the
/// narrower installed route, not another physical proof or lineage decoder.
fn route<H>(
    package: &FragmentPackage,
    producer: &novarocks_physical_plan::RuntimeFilterProducer,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(usize, NonZeroU32), FragmentCreationError<H>> {
    let node = producer.endpoint.node;
    let fragment = package.fragment();
    let aggregate = &fragment.nodes()[&node];
    let NodeKind::Aggregate {
        group_by,
        calls,
        grouping,
    } = &aggregate.kind
    else {
        return Err(unsupported(node, "Aggregate TopN runtime-filter producer").into());
    };
    if *grouping == AggregateGrouping::Partial && calls.is_empty() {
        return Err(unsupported(node, "Aggregate TopN runtime-filter producer phase").into());
    }
    for call in calls.iter() {
        work.step()?;
        if *grouping == AggregateGrouping::Partial && call.distinct {
            return Err(
                unsupported(node, "Aggregate TopN runtime-filter DISTINCT producer").into(),
            );
        }
        if *grouping == AggregateGrouping::Partial
            && !call.binding.phase.consumes_logical_arguments()
        {
            return Err(unsupported(node, "Aggregate TopN runtime-filter producer phase").into());
        }
    }
    let RuntimeFilterProducerTarget::AggregateTopNKey {
        group_key_ordinal,
        topn,
        phase,
        limit,
        offset: 0,
        ..
    } = producer.target
    else {
        return Err(
            unsupported(node, "Aggregate TopN runtime-filter phase, limit or offset").into(),
        );
    };
    work.step()?;
    let installed_parent = match (grouping, fragment.nodes().get(&topn).map(|node| &node.kind)) {
        (
            AggregateGrouping::Partial,
            Some(NodeKind::TopN {
                phase: novarocks_physical_plan::TopNPhase::Partial { .. },
                reduction: novarocks_physical_plan::TopNReduction::GroupedStates { .. },
                ..
            }),
        ) => true,
        (
            AggregateGrouping::Complete,
            Some(NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::Rows,
                ..
            }),
        ) => matches!(
            phase,
            novarocks_physical_plan::TopNPhase::Partial { .. }
                | novarocks_physical_plan::TopNPhase::Single
        ),
        _ => false,
    };
    if !installed_parent {
        return Err(unsupported(
            node,
            "Aggregate TopN runtime-filter aggregate and TopN phases",
        )
        .into());
    }
    let limit = u32::try_from(limit)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| unsupported(node, "Aggregate TopN runtime-filter positive u32 limit"))?;
    let ordinal =
        usize::try_from(group_key_ordinal).map_err(|_| CompileControlError::ResourceExhausted)?;
    let (key, _) = group_by.get(ordinal).ok_or(FragmentCompileError::Invalid(
        "missing Aggregate TopN group key",
    ))?;
    let key_type = &crate::expressions::bound_root(package, node, *key)?.ty;
    if !is_aggregate_topn_key_type(key_type) {
        return Err(unsupported(node, "Aggregate TopN runtime-filter order key type").into());
    }
    Ok((ordinal, limit))
}

pub(super) fn plan_producer<H, F: ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    filter: &RuntimeFilter,
    binding: &RuntimeFilterBindingCut,
    index: usize,
    planned: &mut PlannedRuntimeFilters,
    factory: Option<&F>,
    mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCreationError<H>> {
    let producer = &filter.producers[index];
    let node = producer.endpoint.node;
    let (key_ordinal, limit) = route(package, producer, work)?;
    if producer.contribution_kinds.as_ref()
        != [
            RuntimeFilterContributionKind::OrderedBoundUpdate,
            RuntimeFilterContributionKind::ProducerClosed,
        ]
        || producer.completion != RuntimeFilterCompletion::ProducerClosed
    {
        return Err(unsupported(
            node,
            "Aggregate TopN runtime-filter ordered update and close",
        )
        .into());
    }
    let key = match &package.fragment().nodes()[&node].kind {
        NodeKind::Aggregate { group_by, .. } => group_by[key_ordinal].0,
        _ => unreachable!("route checked aggregate kind"),
    };
    let contract = contract(filter, node, factory, work)?;
    let StaticFilterContract::Ordered { keys, .. } = &contract else {
        unreachable!()
    };
    if !keys[0]
        .value_type
        .same_value_domain_observed::<FragmentCreationError<H>>(
            &crate::expressions::bound_root(package, node, key)?.ty,
            || work.step().map_err(Into::into),
        )?
    {
        return Err(unsupported(
            node,
            "Aggregate TopN runtime-filter complete group key type",
        )
        .into());
    }
    let producer = StaticFilterProducer::try_new(
        binding.binding_id,
        filter.id.get(),
        FilterProducerKind::OrderedBound,
        contract,
        FilterReduction::TightenOrderedBound,
    )
    .map_err(static_filter)?;
    if !planned.aggregate_producers.contains_key(&node) {
        crate::creation::before_tree_insert::<NodeId, Vec<PlannedAggregateProducer>, H>(
            planned.aggregate_producers.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
    }
    let producers = planned.aggregate_producers.entry(node).or_default();
    crate::creation::before_vec_push(
        producers,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    producers.push(PlannedAggregateProducer {
        requirement: requirement_id(binding, node)?,
        key_ordinal,
        key,
        limit,
        producer,
    });
    work.step()?;
    Ok(())
}

pub(super) fn plan_consumer<H, F: ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    filter: &RuntimeFilter,
    binding: &RuntimeFilterBindingCut,
    index: usize,
    planned: &mut PlannedRuntimeFilters,
    factory: Option<&F>,
    mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCreationError<H>> {
    let consumer = &filter.consumers[index];
    let node = consumer.endpoint.node;
    let RuntimeFilterConsumerTarget::AggregateTopNScanField {
        producer: witness, ..
    } = &consumer.target
    else {
        unreachable!()
    };
    let mut producer = None;
    for candidate in filter.producers.iter() {
        work.step()?;
        if candidate.witness == *witness {
            producer = Some(candidate);
            break;
        }
    }
    let producer = producer.ok_or(FragmentCompileError::Invalid(
        "missing ordered consumer producer witness",
    ))?;
    // Full-graph and local-cut validation already checked this witness and
    // its lineage. A remote producer is deliberately absent from this graph.
    if !matches!(
        producer.target,
        RuntimeFilterProducerTarget::AggregateTopNKey { .. }
    ) {
        return Err(FragmentCompileError::Invalid(
            "ordered consumer witness is not Aggregate TopN",
        )
        .into());
    }
    let scan = package
        .fragment()
        .nodes()
        .get(&node)
        .ok_or(FragmentCompileError::Invalid(
            "missing ordered scan consumer",
        ))?;
    if !matches!(scan.kind, NodeKind::Scan { .. })
        || !scan.inputs.is_empty()
        || consumer.apply_point != RuntimeFilterApplyPoint::ScanSource
        || consumer.activation
            != (RuntimeFilterConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            })
        || consumer.capabilities.as_ref() != [RuntimeFilterArtifactCapability::OrderedRange]
    {
        return Err(unsupported(
            node,
            "Aggregate TopN runtime-filter live Batch scan consumer",
        )
        .into());
    }
    for output in scan.output.columns.iter() {
        work.step()?;
        if !is_ordered_filter_flat_payload_type(&package.fragment().values()[output].ty) {
            return Err(
                unsupported(node, "Aggregate TopN runtime-filter flat Scan payload").into(),
            );
        }
    }
    let [value] = consumer.endpoint.values.as_ref() else {
        return Err(FragmentCompileError::Invalid("ordered scan consumer is not one value").into());
    };
    let mut ordinal = None;
    for (index, output) in scan.output.columns.iter().enumerate() {
        work.step()?;
        if output == value {
            ordinal = Some(index);
            break;
        }
    }
    let ordinal = ordinal.ok_or(FragmentCompileError::Invalid(
        "ordered scan key is not a published output",
    ))?;
    let ty = package.fragment().values()[value]
        .ty
        .clone_observed(|| work.step())?;
    let contract = contract(filter, node, factory, work)?;
    let StaticFilterContract::Ordered { keys, .. } = &contract else {
        unreachable!()
    };
    if !ty.same_value_domain_observed::<FragmentCreationError<H>>(&keys[0].value_type, || {
        work.step().map_err(Into::into)
    })? {
        return Err(
            unsupported(node, "Aggregate TopN runtime-filter complete scan key type").into(),
        );
    }
    let consumer = StaticFilterConsumer::try_new(
        binding.binding_id,
        filter.id.get(),
        FilterConsumerActivation::NonBlockingLive {
            late_apply: FilterLateApplyGranularity::Batch,
        },
        contract,
        FilterReduction::TightenOrderedBound,
    )
    .map_err(static_filter)?;
    if !planned.consumers.contains_key(&node) {
        crate::creation::before_tree_insert::<NodeId, Vec<PlannedConsumer>, H>(
            planned.consumers.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
    }
    let consumers = planned.consumers.entry(node).or_default();
    crate::creation::before_vec_push(
        consumers,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    consumers.push(PlannedConsumer {
        requirement: requirement_id(binding, node)?,
        ordinal,
        ty,
        consumer,
    });
    work.step()?;
    Ok(())
}

impl PlannedRuntimeFilters {
    pub(crate) fn take_aggregate<H, F: ValueSchemaCreationFactory<H>>(
        &mut self,
        node: NodeId,
        definitions: &crate::expressions::RootMap,
        factory: Option<&F>,
        mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<AggregateFilterSites, FragmentCreationError<H>> {
        let planned = self.aggregate_producers.remove(&node).unwrap_or_default();
        let mut scope = match factory {
            Some(factory) if !planned.is_empty() => {
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
        crate::creation::reserve_vec(
            &mut filters,
            planned.len(),
            scope
                .as_mut()
                .map(|scope| scope as &mut dyn ValueSchemaCreationScope<H>),
            work,
        )?;
        crate::creation::reserve_vec(
            &mut requirements,
            planned.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        for producer in planned {
            let group_key_expr =
                *definitions
                    .get(&producer.key)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing lowered ordered group key",
                    ))?;
            filters.push(AggregateTopNFilter {
                group_key_expr,
                group_key_ordinal: producer.key_ordinal,
                limit: producer.limit,
                producer: producer.producer,
            });
            requirements.push(BindingRequirement::RuntimeFilter {
                binding_id: producer.requirement,
            });
            work.step()?;
        }
        let filters = match scope.as_mut() {
            Some(scope) => novarocks_local_program::AggregateTopNFilters::try_from_vec_observed(
                filters,
                scope.storage_lifetime(),
                |layout| crate::creation::request(layout, scope, work),
            )?,
            None => filters.into(),
        };
        Ok(AggregateFilterSites {
            filters,
            requirements,
        })
    }
}
