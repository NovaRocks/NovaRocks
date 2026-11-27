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
#[cfg(test)]
use crate::tests::unit_fixture::DefinitionFixture;

fn broadcast() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Broadcast,
        row_multiplicity: RowMultiplicity::Replicated,
        ordering: Box::default(),
    }
}

fn single_copy() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn any_of(witnesses: &[RuntimeFilterWitnessId]) -> RuntimeFilterCoverage {
    let mut nodes = witnesses
        .iter()
        .copied()
        .map(RuntimeFilterCoverageNode::Witness)
        .collect::<Vec<_>>();
    nodes.push(RuntimeFilterCoverageNode::AnyOf {
        children: (0..u32::try_from(witnesses.len()).unwrap())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    });
    RuntimeFilterCoverage {
        root: u32::try_from(nodes.len() - 1).unwrap(),
        nodes: nodes.into_boxed_slice(),
    }
}

fn append_single_copy_literal(
    builder: &mut FragmentBuilder,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value_type = ty(DataType::Int64, false);
    let expression = builder
        .define(
            node,
            (value_type.clone()).clone(),
            fixture_constants.i64(11, &(value_type.clone())),
        )
        .unwrap();
    let value = builder
        .add_value(
            value_type,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let value_field_root_0 = builder.root_definition(
        node,
        expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: single_copy(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0])]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_exchange(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    source_value: ValueId,
    properties: PhysicalProperties,
) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, value)]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_cte_exchange(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    producer_fragment: FragmentId,
    source_value: ValueId,
) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::CteImport {
                edge,
                producer_fragment,
                producer_value: source_value,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: single_copy(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, value)]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_broadcast_join(
    builder: &mut FragmentBuilder,
    left: (NodeId, ValueId),
    right: (NodeId, ValueId),
) -> NodeId {
    let node = builder.reserve_node_id().unwrap();
    let left_key = builder
        .define(node, ty(DataType::Int64, false), ExprKind::input(left.1))
        .unwrap();
    let right_key = builder
        .define(node, ty(DataType::Int64, false), ExprKind::input(right.1))
        .unwrap();
    let left_key = builder.root_definition(
        node,
        left_key,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    let right_key = builder.root_definition(
        node,
        right_key,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(1),
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([left.0, right.0]),
            required_inputs: Box::from([single_copy(), broadcast()]),
            output_properties: single_copy(),
            output: OutputPort {
                node,
                columns: Box::from([left.1, right.1]),
            },
            kind: NodeKind::HashJoin {
                kind: JoinKind::Inner,
                build_side: JoinSide::Right,
                condition: crate::HashJoinCondition::Ordinary {
                    keys: Box::from([JoinKey {
                        left: left_key,
                        right: right_key,
                        null_safe: false,
                        match_relation: novarocks_type_contract::JoinKeyMatch::Grouping,
                    }]),
                    residual: None,
                },
                distribution: JoinDistribution::BroadcastBuild,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    node
}

fn equality(id: u32, fragment: FragmentId, join: NodeId) -> RuntimeFilterEqualityWitness {
    RuntimeFilterEqualityWitness {
        id: RuntimeFilterEqualityWitnessId::new(id),
        fragment,
        join,
        key_ordinal: 0,
        domain_side: JoinSide::Right,
    }
}

fn producer(
    witness: u32,
    equality: u32,
    fragment: FragmentId,
    join: NodeId,
    build_value: ValueId,
    build_edges: Box<[EdgeId]>,
) -> RuntimeFilterProducer {
    RuntimeFilterProducer {
        witness: RuntimeFilterWitnessId::new(witness),
        endpoint: RuntimeFilterEndpoint {
            fragment,
            node: join,
            values: Box::from([build_value]),
        },
        apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
        contribution_kinds: Box::from([
            RuntimeFilterContributionKind::ValueDomainDelta,
            RuntimeFilterContributionKind::ProducerClosed,
        ]),
        completion: RuntimeFilterCompletion::ProducerClosed,
        progress: RuntimeFilterProducerProgress {
            build_edges,
            non_build_edges: Box::default(),
        },
        target: RuntimeFilterProducerTarget::JoinBuildKey {
            equality: RuntimeFilterEqualityWitnessId::new(equality),
        },
    }
}

fn blocking_consumer(
    equality: u32,
    fragment: FragmentId,
    join: NodeId,
    probe_value: ValueId,
) -> RuntimeFilterConsumer {
    RuntimeFilterConsumer {
        endpoint: RuntimeFilterEndpoint {
            fragment,
            node: join,
            values: Box::from([probe_value]),
        },
        apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
        capabilities: Box::from([
            RuntimeFilterArtifactCapability::Membership,
            RuntimeFilterArtifactCapability::EmptyDomain,
        ]),
        activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
        target: RuntimeFilterConsumerTarget::JoinProbeKey {
            equality: RuntimeFilterEqualityWitnessId::new(equality),
        },
    }
}

fn filter(
    id: RuntimeFilterId,
    equalities: Box<[RuntimeFilterEqualityWitness]>,
    producers: Box<[RuntimeFilterProducer]>,
    consumers: Box<[RuntimeFilterConsumer]>,
) -> RuntimeFilter {
    let witnesses = producers
        .iter()
        .map(|producer| producer.witness)
        .collect::<Vec<_>>();
    let coverage = any_of(&witnesses);
    RuntimeFilter {
        id,
        kind: RuntimeFilterKind::InList,
        domain: RuntimeFilterDomain::Membership {
            ty: ty(DataType::Int64, false),
            null_semantics: RuntimeFilterNullSemantics::NeverMatches,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction: RuntimeFilterReduction::SetUnion,
        availability_coverage: coverage.clone(),
        terminal_coverage: coverage,
        equality_witnesses: equalities,
        producers,
        consumers,
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    }
}

struct EdgeEndpoints {
    source_fragment: FragmentId,
    source_value: ValueId,
    destination_fragment: FragmentId,
    destination_node: NodeId,
    destination_value: ValueId,
}

fn add_edge(plan: &mut PlanBuilder, id: EdgeId, endpoints: EdgeEndpoints, broadcast_edge: bool) {
    add_edge_with_kind(plan, id, endpoints, broadcast_edge, EdgeKind::Stream);
}

fn add_edge_with_kind(
    plan: &mut PlanBuilder,
    id: EdgeId,
    endpoints: EdgeEndpoints,
    broadcast_edge: bool,
    kind: EdgeKind,
) {
    plan.add_edge(Edge {
        role: crate::OutputRole::Relational,
        delivery: crate::DeliveryRequirement::DemandDriven,
        id,
        kind,
        source: EdgeSource {
            port: crate::FragmentOutputPort::Root,
            fragment: endpoints.source_fragment,
            projection: Box::from([endpoints.source_value]),
        },
        destination: EdgeDestination {
            fragment: endpoints.destination_fragment,
            node: endpoints.destination_node,
            receive_mapping: Box::from([(endpoints.source_value, endpoints.destination_value)]),
        },
        partitioning: EdgePartitioning {
            source: if broadcast_edge {
                Distribution::Broadcast
            } else {
                Distribution::Singleton
            },
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: if broadcast_edge {
                Distribution::Broadcast
            } else {
                Distribution::Singleton
            },
            destination_multiplicity: if broadcast_edge {
                RowMultiplicity::Replicated
            } else {
                RowMultiplicity::SingleCopy
            },
            routing_only: Box::default(),
        },
    })
    .unwrap();
}

#[test]
fn blocking_runtime_filter_rejects_multicast_backpressure_cycle() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let source_fragment_id = FragmentId::new(420);
    let build_fragment_id = FragmentId::new(421);
    let probe_fragment_id = FragmentId::new(422);
    let build_branch = EdgeId::new(420);
    let probe_branch = EdgeId::new(421);
    let join_build_edge = EdgeId::new(422);
    let filter_id = RuntimeFilterId::new(420);

    let mut source_builder = FragmentBuilder::new(source_fragment_id);
    let (source_root, source_value) =
        append_single_copy_literal(&mut source_builder, &mut fixture_constants);
    let source_fragment = source_builder
        .finish_fixture_definition(
            source_root,
            FragmentSink::Multicast {
                edges: Box::from([build_branch, probe_branch]),
            },
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut build_builder = FragmentBuilder::new(build_fragment_id);
    let (build_root, build_value) = append_cte_exchange(
        &mut build_builder,
        build_branch,
        source_fragment_id,
        source_value,
    );
    let build_fragment = build_builder
        .finish_fixture_definition(
            build_root,
            FragmentSink::Stream {
                edge: join_build_edge,
            },
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut probe_builder = FragmentBuilder::new(probe_fragment_id);
    let probe = append_cte_exchange(
        &mut probe_builder,
        probe_branch,
        source_fragment_id,
        source_value,
    );
    let build = append_exchange(
        &mut probe_builder,
        join_build_edge,
        build_value,
        broadcast(),
    );
    let join = append_broadcast_join(&mut probe_builder, probe, build);
    probe_builder.attach_runtime_filter(filter_id).unwrap();
    let probe_fragment = probe_builder
        .finish_fixture_definition(
            join,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut filter = filter(
        filter_id,
        Box::from([equality(420, probe_fragment_id, join)]),
        Box::from([producer(
            420,
            420,
            probe_fragment_id,
            join,
            build.1,
            Box::from([join_build_edge]),
        )]),
        Box::from([blocking_consumer(420, probe_fragment_id, join, probe.1)]),
    );
    filter.producers[0].progress.non_build_edges = Box::from([probe_branch]);

    let finish = |filter: RuntimeFilter| {
        let mut plan = PlanBuilder::new(version());
        for fragment in [
            source_fragment.clone(),
            build_fragment.clone(),
            probe_fragment.clone(),
        ] {
            plan.add_fragment(fragment).unwrap();
        }
        for (id, destination_fragment, destination_node, destination_value) in [
            (build_branch, build_fragment_id, build_root, build_value),
            (probe_branch, probe_fragment_id, probe.0, probe.1),
        ] {
            add_edge_with_kind(
                &mut plan,
                id,
                EdgeEndpoints {
                    source_fragment: source_fragment_id,
                    source_value,
                    destination_fragment,
                    destination_node,
                    destination_value,
                },
                false,
                EdgeKind::CteMulticast,
            );
        }
        add_edge(
            &mut plan,
            join_build_edge,
            EdgeEndpoints {
                source_fragment: build_fragment_id,
                source_value: build_value,
                destination_fragment: probe_fragment_id,
                destination_node: build.0,
                destination_value: build.1,
            },
            true,
        );
        plan.add_runtime_filter(filter).unwrap();
        crate::tests::constant_fixture::publish_plan(plan, &fixture_constants)
    };

    let mut non_blocking = filter.clone();
    non_blocking.consumers[0].activation =
        RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
            late_apply: LateApplyGranularity::Batch,
        };
    // Both direct branches still feed opposite ordered join inputs. Removing
    // the optional RF wait cannot remove that mandatory shared-data cycle.
    let data_cycle = finish(non_blocking)
        .expect_err("late application cannot break the required multicast input-order cycle");
    assert!(
        data_cycle
            .errors()
            .iter()
            .any(|error| error.path() == "fragments.wait_graph")
    );

    let error = finish(filter).unwrap_err().to_string();
    assert!(error.contains(
        "blocking runtime-filter waits form a cycle with physical execution dependencies"
    ));
}

#[derive(Clone, Copy)]
struct NestedJoinFacts {
    equality: u32,
    fragment: FragmentId,
    join: NodeId,
    probe: ValueId,
    build: ValueId,
    build_edge: EdgeId,
}

fn nested_unicast_plan(
    make_filters: impl FnOnce(NestedJoinFacts, NestedJoinFacts) -> Vec<RuntimeFilter>,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Result<PhysicalPlan, ValidationErrors> {
    let build_fragment_id = FragmentId::new(400);
    let inner_fragment_id = FragmentId::new(401);
    let outer_fragment_id = FragmentId::new(402);
    let build_edge = EdgeId::new(400);
    let nested_build_edge = EdgeId::new(401);

    let mut build_builder = FragmentBuilder::new(build_fragment_id);
    let (build_root, build_value) =
        append_single_copy_literal(&mut build_builder, fixture_constants);
    let build_fragment = build_builder
        .finish_fixture_definition(
            build_root,
            FragmentSink::Stream { edge: build_edge },
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut inner_builder = FragmentBuilder::new(inner_fragment_id);
    let inner_probe = append_single_copy_literal(&mut inner_builder, fixture_constants);
    let inner_build = append_exchange(&mut inner_builder, build_edge, build_value, broadcast());
    let inner_join = append_broadcast_join(&mut inner_builder, inner_probe, inner_build);

    let mut outer_builder = FragmentBuilder::new(outer_fragment_id);
    let outer_probe = append_single_copy_literal(&mut outer_builder, fixture_constants);
    let outer_build = append_exchange(
        &mut outer_builder,
        nested_build_edge,
        inner_build.1,
        broadcast(),
    );
    let outer_join = append_broadcast_join(&mut outer_builder, outer_probe, outer_build);
    let filters = make_filters(
        NestedJoinFacts {
            equality: 400,
            fragment: inner_fragment_id,
            join: inner_join,
            probe: inner_probe.1,
            build: inner_build.1,
            build_edge,
        },
        NestedJoinFacts {
            equality: 401,
            fragment: outer_fragment_id,
            join: outer_join,
            probe: outer_probe.1,
            build: outer_build.1,
            build_edge: nested_build_edge,
        },
    );
    for filter in &filters {
        for (fragment, builder) in [
            (inner_fragment_id, &mut inner_builder),
            (outer_fragment_id, &mut outer_builder),
        ] {
            if filter
                .producers
                .iter()
                .any(|producer| producer.endpoint.fragment == fragment)
                || filter
                    .consumers
                    .iter()
                    .any(|consumer| consumer.endpoint.fragment == fragment)
            {
                builder.attach_runtime_filter(filter.id).unwrap();
            }
        }
    }
    let inner_fragment = inner_builder
        .finish_fixture_definition(
            inner_join,
            FragmentSink::Stream {
                edge: nested_build_edge,
            },
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let outer_fragment = outer_builder
        .finish_fixture_definition(
            outer_join,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(build_fragment).unwrap();
    plan.add_fragment(inner_fragment).unwrap();
    plan.add_fragment(outer_fragment).unwrap();
    add_edge(
        &mut plan,
        build_edge,
        EdgeEndpoints {
            source_fragment: build_fragment_id,
            source_value: build_value,
            destination_fragment: inner_fragment_id,
            destination_node: inner_build.0,
            destination_value: inner_build.1,
        },
        true,
    );
    add_edge(
        &mut plan,
        nested_build_edge,
        EdgeEndpoints {
            source_fragment: inner_fragment_id,
            source_value: inner_build.1,
            destination_fragment: outer_fragment_id,
            destination_node: outer_build.0,
            destination_value: outer_build.1,
        },
        true,
    );
    for filter in filters {
        plan.add_runtime_filter(filter).unwrap();
    }
    crate::tests::constant_fixture::publish_plan(plan, fixture_constants)
}

#[test]
fn nested_runtime_filter_consumer_cannot_borrow_another_joins_equality_witness() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let error = nested_unicast_plan(
        |inner, outer| {
            let mut filter = filter(
                RuntimeFilterId::new(400),
                Box::from([
                    equality(inner.equality, inner.fragment, inner.join),
                    equality(outer.equality, outer.fragment, outer.join),
                ]),
                Box::from([producer(
                    outer.equality,
                    outer.equality,
                    outer.fragment,
                    outer.join,
                    outer.build,
                    Box::from([outer.build_edge]),
                )]),
                Box::from([blocking_consumer(
                    inner.equality,
                    inner.fragment,
                    inner.join,
                    inner.probe,
                )]),
            );
            // This is a same-key contract negative, not an executable RF cycle.
            // Disable the optional wait so no dependency diagnostic can mask it.
            filter.consumers[0].activation =
                RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
                    late_apply: LateApplyGranularity::Batch,
                };
            vec![filter]
        },
        &mut fixture_constants,
    )
    .expect_err("an outer producer cannot authorize an inner join's consumer equality");
    assert_eq!(error.errors().len(), 1, "{error:?}");
    let diagnostic = &error.errors()[0];
    assert_eq!(
        diagnostic.category(),
        ValidationErrorCategory::StructuralInvariant
    );
    assert_eq!(diagnostic.path(), "runtime_filters[400]");
    assert_eq!(
        diagnostic.message(),
        "runtime filter consumer equality witness has no producer for the same join key"
    );
}

#[test]
fn nested_unicast_joins_keep_their_own_blocking_or_late_runtime_filter_contracts() {
    let make = |activation| {
        let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
        nested_unicast_plan(
            |inner, outer| {
                [inner, outer]
                    .into_iter()
                    .map(|join| {
                        let mut filter = filter(
                            RuntimeFilterId::new(join.equality),
                            Box::from([equality(join.equality, join.fragment, join.join)]),
                            Box::from([producer(
                                join.equality,
                                join.equality,
                                join.fragment,
                                join.join,
                                join.build,
                                Box::from([join.build_edge]),
                            )]),
                            Box::from([blocking_consumer(
                                join.equality,
                                join.fragment,
                                join.join,
                                join.probe,
                            )]),
                        );
                        filter.consumers[0].activation = activation;
                        filter
                    })
                    .collect()
            },
            &mut fixture_constants,
        )
        .expect("each nested join owns one exact producer, consumer, and coverage witness")
    };
    let blocking = make(RuntimeFilterConsumerActivation::BlockingSnapshot);
    let late = make(
        RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
            late_apply: LateApplyGranularity::Batch,
        },
    );
    assert_eq!(blocking.fragments(), late.fragments());
    assert_eq!(blocking.edges(), late.edges());
    assert_eq!(blocking.runtime_filters().len(), 2);
    for plan in [&blocking, &late] {
        validate_plan(plan).unwrap();
        for fragment in plan.fragments().values() {
            let cuts = fragment_cuts(plan, fragment.id()).unwrap();
            validate_fragment(fragment, &cuts).unwrap();
        }
        for filter in plan.runtime_filters().values() {
            assert_eq!(filter.producers.len(), 1);
            assert_eq!(filter.consumers.len(), 1);
            let producer = &filter.producers[0];
            let consumer = &filter.consumers[0];
            assert_eq!(producer.endpoint.fragment, consumer.endpoint.fragment);
            assert_eq!(producer.endpoint.node, consumer.endpoint.node);
            assert_eq!(filter.equality_witnesses.len(), 1);
            let witness = filter.equality_witnesses[0];
            assert_eq!(witness.fragment, producer.endpoint.fragment);
            assert_eq!(witness.join, producer.endpoint.node);
            assert_eq!(
                producer.target,
                RuntimeFilterProducerTarget::JoinBuildKey {
                    equality: witness.id,
                }
            );
            assert_eq!(
                consumer.target,
                RuntimeFilterConsumerTarget::JoinProbeKey {
                    equality: witness.id,
                }
            );
        }
    }
    assert!(
        blocking
            .runtime_filters()
            .values()
            .all(|filter| filter.consumers[0].activation
                == RuntimeFilterConsumerActivation::BlockingSnapshot)
    );
    assert!(
        late.runtime_filters()
            .values()
            .all(|filter| filter.consumers[0].activation
                == RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
                    late_apply: LateApplyGranularity::Batch,
                })
    );
}

pub(super) struct AcyclicFixture {
    pub(super) plan: PhysicalPlan,
    target_fragment: FragmentId,
    recursive_build_edge: EdgeId,
    recursive_source_fragment: FragmentId,
}

pub(super) fn acyclic_cross_fragment_fixture(
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> AcyclicFixture {
    let source_fragment = FragmentId::new(410);
    let relay_fragment = FragmentId::new(411);
    let target_fragment = FragmentId::new(412);
    let recursive_build_edge = EdgeId::new(410);
    let direct_build_edge = EdgeId::new(411);
    let filter_id = RuntimeFilterId::new(410);

    let mut source_builder = FragmentBuilder::new(source_fragment);
    let (source_root, source_value) =
        append_single_copy_literal(&mut source_builder, fixture_constants);
    let source = source_builder
        .finish_fixture_definition(
            source_root,
            FragmentSink::Stream {
                edge: recursive_build_edge,
            },
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut relay_builder = FragmentBuilder::new(relay_fragment);
    let (relay_root, relay_value) = append_exchange(
        &mut relay_builder,
        recursive_build_edge,
        source_value,
        single_copy(),
    );
    let relay = relay_builder
        .finish_fixture_definition(
            relay_root,
            FragmentSink::Stream {
                edge: direct_build_edge,
            },
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut target_builder = FragmentBuilder::new(target_fragment);
    let probe = append_single_copy_literal(&mut target_builder, fixture_constants);
    let build = append_exchange(
        &mut target_builder,
        direct_build_edge,
        relay_value,
        broadcast(),
    );
    let join = append_broadcast_join(&mut target_builder, probe, build);
    target_builder.attach_runtime_filter(filter_id).unwrap();
    let target = target_builder
        .finish_fixture_definition(
            join,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let filter = filter(
        filter_id,
        Box::from([equality(410, target_fragment, join)]),
        Box::from([producer(
            410,
            410,
            target_fragment,
            join,
            build.1,
            Box::from([direct_build_edge]),
        )]),
        Box::from([blocking_consumer(410, target_fragment, join, probe.1)]),
    );

    let mut plan = PlanBuilder::new(version());
    for fragment in [source, relay, target] {
        plan.add_fragment(fragment).unwrap();
    }
    add_edge(
        &mut plan,
        recursive_build_edge,
        EdgeEndpoints {
            source_fragment,
            source_value,
            destination_fragment: relay_fragment,
            destination_node: relay_root,
            destination_value: relay_value,
        },
        false,
    );
    add_edge(
        &mut plan,
        direct_build_edge,
        EdgeEndpoints {
            source_fragment: relay_fragment,
            source_value: relay_value,
            destination_fragment: target_fragment,
            destination_node: build.0,
            destination_value: build.1,
        },
        true,
    );
    plan.add_runtime_filter(filter).unwrap();

    AcyclicFixture {
        plan: crate::tests::constant_fixture::publish_plan(plan, fixture_constants).unwrap(),
        target_fragment,
        recursive_build_edge,
        recursive_source_fragment: source_fragment,
    }
}

fn replace_source_fragment(
    plan: &PhysicalPlan,
    source_id: FragmentId,
    change: impl FnOnce(&mut crate::plan::FragmentParts),
) -> PhysicalPlan {
    let source = &plan.fragments()[&source_id];
    let mut parts = crate::plan::FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: source.id(),
        root: source.root(),
        values: source.values().clone(),

        nodes: source.nodes().clone(),
        sink: source.sink().clone(),
        dop_domain: source.dop_domain(),
        runtime_filters: source.runtime_filters().to_vec().into_boxed_slice(),
        call_requests: source.call_requests().clone(),
        calls: source.calls().clone(),
    };
    change(&mut parts);
    let mut fragments = plan.fragments().clone();
    fragments.insert(source_id, Fragment::from(parts));
    PhysicalPlan::from(crate::PhysicalPlanParts {
        constants: plan.constants().clone(),
        parameters: plan.parameters().clone(),

        version: plan.version(),
        fragments,
        edges: plan.edges().clone(),
        runtime_filters: plan.runtime_filters().clone(),
        result_port: plan.result_port().cloned(),
        required: plan.required(),
        annotations: plan.annotations().to_vec().into_boxed_slice(),
        write_layouts: Default::default(),
    })
}

#[test]
fn blocking_runtime_filter_accepts_an_acyclic_recursive_build_dependency() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = acyclic_cross_fragment_fixture(&mut fixture_constants);
    let target = &fixture.plan.fragments()[&fixture.target_fragment];
    let cuts = fragment_cuts(&fixture.plan, fixture.target_fragment).unwrap();
    validate_fragment(target, &cuts).unwrap();
}

#[test]
fn remote_execution_growth_does_not_expand_local_boundary_facts() {
    use novarocks_expression_contract::{BoundExprSet, BoundExprSetBuilder, UnitLimits};
    use novarocks_type_contract::{
        CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand,
        PureCompileControl, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::Arc;
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    struct Source {
        _unit: Arc<BoundExprSet>,
    }
    impl ValueTypeStorageHold for Source {}
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = acyclic_cross_fragment_fixture(&mut fixture_constants);
    let cuts = fragment_cuts(&fixture.plan, fixture.target_fragment).unwrap();
    for count in [2, 64, 1024] {
        let enlarged =
            replace_source_fragment(&fixture.plan, fixture.recursive_source_fragment, |parts| {
                let node = parts.nodes.get_mut(&parts.root).unwrap();
                let NodeKind::Values { rows } = &node.kind else {
                    panic!("fixture source must contain literal rows")
                };
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 1);
                let source = node.expressions.take().unwrap();
                assert_eq!(source.definitions().len(), 1);
                let definition = &source.definitions()[0];
                let ExprKind::Constant(reference) = definition.kind else {
                    panic!("the actual source fixture is one checked constant")
                };
                let ty = definition.ty.clone();
                let ports = source.ports().to_vec();
                let mut draft = BoundExprSetBuilder::new(UnitLimits {
                    expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
                })
                .with_storage_lifetime(ValueTypeStorageLifetime::held(Arc::new(Source {
                    _unit: source,
                })))
                .unwrap();
                let shared_definition = draft.add(None, ty, ExprKind::Constant(reference)).unwrap();
                let mut rows = Vec::with_capacity(count);
                for _ in 0..count {
                    // A new field occurrence owns its own root/use even when the
                    // original checked constant definition is genuinely shared.
                    let root = draft
                        .root(
                            shared_definition,
                            EvaluationDemand::Value,
                            crate::RootScope::AllPorts,
                        )
                        .unwrap();
                    rows.push(Box::from([root]));
                }
                let mut work =
                    CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
                let unit = draft
                    .seal(ports, &mut work)
                    .unwrap()
                    .finish(std::collections::BTreeMap::new(), &mut work)
                    .unwrap();
                work.finish().unwrap();
                assert_eq!(unit.definitions().len(), 1);
                assert_eq!(unit.roots().len(), count);
                assert_eq!(unit.root_use_ids().len(), count);
                assert!(
                    unit.roots()
                        .iter()
                        .all(|root| root.definition == shared_definition)
                );
                node.kind = NodeKind::Values {
                    rows: rows.into_boxed_slice(),
                };
                node.expressions = Some(Arc::new(unit));
            });
        assert_eq!(
            enlarged.constants().entries().keys().collect::<Vec<_>>(),
            fixture
                .plan
                .constants()
                .entries()
                .keys()
                .collect::<Vec<_>>()
        );
        for (id, pool) in fixture.plan.constants().entries() {
            assert_eq!(
                enlarged.constants().entries()[id].backing_identity(),
                pool.backing_identity()
            );
        }
        validate_plan(&enlarged).unwrap();
        let local = fragment_cuts(&enlarged, fixture.target_fragment).unwrap();
        assert_eq!(cuts, local);
        validate_fragment(&enlarged.fragments()[&fixture.target_fragment], &local).unwrap();
    }
}

#[test]
fn global_validation_rejects_remote_sink_ownership_that_local_facts_cannot_prove() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = acyclic_cross_fragment_fixture(&mut fixture_constants);
    let cuts = fragment_cuts(&fixture.plan, fixture.target_fragment).unwrap();
    let direct_edge = cuts.inbound[0].edge;
    for sink in [
        FragmentSink::Noop,
        FragmentSink::Stream { edge: direct_edge },
        FragmentSink::Multicast {
            edges: Box::from([fixture.recursive_build_edge]),
        },
    ] {
        let malformed =
            replace_source_fragment(&fixture.plan, fixture.recursive_source_fragment, |parts| {
                parts.sink = sink
            });
        assert!(validate_plan(&malformed).is_err());
        // The receiver cannot reconstruct remote executable dependencies. Its
        // own self-consistent input boundary remains independently valid.
        validate_fragment(&malformed.fragments()[&fixture.target_fragment], &cuts).unwrap();
    }
}

#[test]
fn local_validation_rejects_an_unattached_runtime_filter() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = acyclic_cross_fragment_fixture(&mut fixture_constants);
    let mut cuts = fragment_cuts(&fixture.plan, fixture.target_fragment).unwrap();
    cuts.runtime_filters = Box::default();
    let error = validate_fragment(&fixture.plan.fragments()[&fixture.target_fragment], &cuts)
        .unwrap_err()
        .to_string();
    assert!(error.contains("differ from fragment attachments"));
}

#[test]
fn caller_observed_complete_plan_waits_keep_the_original_three_control_causes() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    use novarocks_type_contract::{
        CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RefuseGraphWork {
        cause: CompileControlError,
        calls: AtomicUsize,
    }
    impl PureCompileControl for RefuseGraphWork {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Validate);
            self.calls.fetch_add(1, Ordering::Relaxed);
            if units == 0 {
                Ok(())
            } else {
                Err(self.cause.clone())
            }
        }
    }
    let fixture = acyclic_cross_fragment_fixture(&mut fixture_constants);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let owner = RefuseGraphWork {
            cause: cause.clone(),
            calls: AtomicUsize::new(0),
        };
        let mut work = CompileCheckpoints::try_new(&owner, CompilePhase::Validate).unwrap();
        // Enter the complete-plan core with the caller's existing scope, not
        // the graph convenience API or the plain validation adapter.
        let error = crate::validation::validate_plan_with_limits_in(
            &fixture.plan,
            PlanLimits::FROZEN,
            &mut work,
        )
        .unwrap_err();
        assert!(matches!(
            &error,
            PlanConstructionError::Constants(ConstantReferenceError::Control(original))
                if original == &cause
        ));
        let calls = owner.calls.load(Ordering::Relaxed);
        let finished = work
            .finish_result::<(), PlanConstructionError>(Err(error.clone()), |cause| {
                ConstantReferenceError::from(cause).into()
            });
        assert_eq!(finished, Err(error));
        assert_eq!(owner.calls.load(Ordering::Relaxed), calls);
        assert!(
            fixture.plan.runtime_filters().values().all(|filter| filter
                .consumers
                .iter()
                .all(|consumer| consumer.activation
                    == RuntimeFilterConsumerActivation::BlockingSnapshot))
        );
    }
}

#[test]
fn observed_complete_plan_validation_retains_acyclic_frozen_blocking_activations() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ObserveWork(AtomicUsize);
    impl PureCompileControl for ObserveWork {
        fn checkpoint(&self, _phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.0.fetch_add(units as usize, Ordering::Relaxed);
            Ok(())
        }
    }
    let fixture = acyclic_cross_fragment_fixture(&mut fixture_constants);
    let owner = ObserveWork(AtomicUsize::new(0));
    validate_plan_observed(&fixture.plan, &owner).unwrap();
    assert!(owner.0.load(Ordering::Relaxed) > 1);
    assert!(fixture.plan.runtime_filters().values().all(|filter| {
        filter.consumers.iter().all(|consumer| {
            consumer.activation == RuntimeFilterConsumerActivation::BlockingSnapshot
        })
    }));
}
