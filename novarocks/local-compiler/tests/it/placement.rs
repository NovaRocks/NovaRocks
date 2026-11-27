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

use super::fragment_fixture::{
    ADMISSION, DOP, compile, empty_values, numeric_catalogue, output_value, seal_units,
};
use arrow_schema::DataType;
use novarocks_expression::testkit::HarnessControl;
use novarocks_expression_contract::{ExprKind, RootOrdinal, RootScope};
use novarocks_local_compiler::FragmentCompileError;
use novarocks_local_program::{LocalProgram, ProgramNodeKind};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    BucketLayoutAlgorithm, EvaluationDemand, PartitionCountParameterId, PartitionHashAlgorithm,
    PartitionSpaceId,
};
use std::{collections::BTreeMap, sync::Arc};

const CONSUMER: FragmentId = FragmentId::new(10);

#[derive(Clone, Copy, Debug)]
enum Route {
    Unknown,
    Gather,
    RoundRobin,
    Hash,
    ConditionalKey,
    BucketShuffle,
    Broadcast,
}

impl Route {
    fn distribution(self, key: ValueId) -> Distribution {
        let keys = || Box::<[ValueId]>::from([key]);
        let hash_scheme = || HashPartitionScheme {
            space: PartitionSpaceId::try_new([31; 32]).unwrap(),
            count: PartitionCountParameter {
                id: PartitionCountParameterId::try_new([32; 32]).unwrap(),
                admissible: PartitionCountDomain {
                    min: 1,
                    max: 16,
                    requires_power_of_two: true,
                },
            },
            definition: HashDefinition {
                algorithm: PartitionHashAlgorithm::NativeHierarchicalV1,
            },
        };
        match self {
            Self::Unknown => Distribution::Unconstrained,
            Self::Gather => Distribution::Singleton,
            Self::RoundRobin => Distribution::RoundRobin,
            Self::Hash => Distribution::Hash {
                keys: keys(),
                scheme: hash_scheme(),
            },
            Self::ConditionalKey => Distribution::ConditionalKey {
                applies: key,
                keys: keys(),
                scheme: hash_scheme(),
            },
            Self::BucketShuffle => Distribution::BucketShuffle {
                keys: keys(),
                scheme: BucketPartitionScheme {
                    space: PartitionSpaceId::try_new([33; 32]).unwrap(),
                    bucket_count: 16,
                    hash: PartitionHashAlgorithm::NativeBucketCrc32V1,
                    layout: BucketLayoutAlgorithm::DenseZeroBasedV1,
                    ordinal_domain: BucketOrdinalDomainProof {
                        first_ordinal: 0,
                        ordinal_count: 16,
                        evidence_digest: [34; 32],
                    },
                },
            },
            Self::Broadcast => Distribution::Broadcast,
        }
    }
}

fn boolean() -> ValueType {
    ValueType::new(DataType::Boolean, false)
}

fn stream(
    plan: &mut PlanBuilder,
    consumer: &mut FragmentBuilder,
    producer_id: u32,
    route: Route,
) -> (NodeId, ValueId) {
    let edge = EdgeId::new(producer_id);
    let producer_id = FragmentId::new(producer_id);
    let mut producer = FragmentBuilder::new(producer_id);
    let (source, values) = empty_values(&mut producer, &[boolean()]);
    let source_value = values[0];
    plan.add_fragment(
        producer
            .finish_definition(
                source,
                FragmentSink::Stream { edge },
                DOP,
                StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    )
    .unwrap();
    let (node, values) = receive(plan, consumer, producer_id, edge, &[source_value], route);
    (node, values[0])
}

fn receive(
    plan: &mut PlanBuilder,
    consumer: &mut FragmentBuilder,
    producer: FragmentId,
    edge: EdgeId,
    produced: &[ValueId],
    route: Route,
) -> (NodeId, Vec<ValueId>) {
    let node = consumer.reserve_node_id().unwrap();
    let imported: Vec<_> = produced
        .iter()
        .map(|&source_value| {
            consumer
                .add_value(
                    boolean(),
                    ValueOrigin::ExchangeImport { edge, source_value },
                )
                .unwrap()
        })
        .collect();
    let mapping: Box<[_]> = produced
        .iter()
        .copied()
        .zip(imported.iter().copied())
        .collect();
    let destination = route.distribution(imported[0]);
    let destination_multiplicity = if matches!(route, Route::Broadcast) {
        RowMultiplicity::Replicated
    } else {
        RowMultiplicity::SingleCopy
    };
    consumer
        .add_exchange_source(
            node,
            edge,
            mapping.clone(),
            imported.clone().into(),
            destination.clone(),
            destination_multiplicity,
        )
        .unwrap();
    plan.add_edge(Edge {
        role: OutputRole::Relational,
        delivery: DeliveryRequirement::DemandDriven,
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: FragmentOutputPort::Root,
            fragment: producer,
            projection: produced.into(),
        },
        destination: EdgeDestination {
            fragment: consumer.id(),
            node,
            receive_mapping: mapping,
        },
        partitioning: EdgePartitioning {
            source: route.distribution(produced[0]),
            destination,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination_multiplicity,
            routing_only: Box::default(),
        },
    })
    .unwrap();
    (node, imported)
}

fn compile_consumer(
    mut plan: PlanBuilder,
    mut consumer: FragmentBuilder,
    root: NodeId,
    placement: StagePlacementPolicy,
) -> Result<Arc<LocalProgram>, FragmentCompileError> {
    seal_units(&mut consumer);
    let fragment = if placement == StagePlacementPolicy::Singleton {
        consumer
            .finish_definition(root, FragmentSink::Result, DOP, placement)
            .unwrap()
    } else {
        let columns = consumer
            .construction_node(root)
            .unwrap()
            .output
            .columns
            .clone();
        let edge = EdgeId::new(3);
        plan.add_fragment(
            consumer
                .finish_definition(root, FragmentSink::Stream { edge }, DOP, placement)
                .unwrap(),
        )
        .unwrap();
        let mut anchor = FragmentBuilder::new(FragmentId::new(11));
        let (receiver, _) = receive(
            &mut plan,
            &mut anchor,
            CONSUMER,
            edge,
            &columns,
            Route::Gather,
        );
        anchor
            .finish_definition(
                receiver,
                FragmentSink::Result,
                DOP,
                StagePlacementPolicy::Singleton,
            )
            .unwrap()
    };
    let output = fragment.nodes()[&fragment.root()].output.clone();
    let fields = output
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, &value)| ResultField {
            domain: ResultValueDomain::Plain,
            name: format!("column_{ordinal}").into_boxed_str(),
            alias: None,
            value,
            ty: fragment.values()[&value].ty.clone(),
        })
        .collect::<Vec<_>>();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output,
        fields: fields.into_boxed_slice(),
    })
    .unwrap();
    plan.add_fragment(fragment).unwrap();
    let plan = plan.finish_observed(&HarnessControl).unwrap();
    let pruning = plan
        .fragments()
        .keys()
        .map(|&id| {
            (
                id,
                FrozenFragmentPruning::try_new(id, vec![], &HarnessControl).unwrap(),
            )
        })
        .collect();
    let admissions = plan.fragments().keys().map(|&id| (id, ADMISSION)).collect();
    let mut packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &pruning,
        &admissions,
        &HarnessControl,
    )
    .unwrap();
    compile(
        &numeric_catalogue(),
        Arc::new(packages.remove(&CONSUMER).unwrap()),
        &HarnessControl,
    )
}

fn builder() -> (PlanBuilder, FragmentBuilder) {
    (
        PlanBuilder::new(PlanVersionId::try_new([100; 16]).unwrap()),
        FragmentBuilder::new(CONSUMER),
    )
}

fn spread() -> StagePlacementPolicy {
    StagePlacementPolicy::Spread {
        max_instances: None,
    }
}

#[test]
fn union_all_accepts_routed_single_copy_receivers_in_a_multi_instance_stage() {
    for route in [
        Route::RoundRobin,
        Route::Hash,
        Route::ConditionalKey,
        Route::BucketShuffle,
    ] {
        let (mut plan, mut consumer) = builder();
        let (left, left_value) = stream(&mut plan, &mut consumer, 1, Route::RoundRobin);
        let (right, right_value) = stream(&mut plan, &mut consumer, 2, route);
        let union = consumer.reserve_node_id().unwrap();
        let output = output_value(&mut consumer, union, 0, boolean());
        consumer
            .add_row_consuming(
                union,
                Box::new([left, right]),
                RequiredInputs::AsProduced,
                Distribution::Unconstrained,
                Box::new([output]),
                NodeKind::SetOp {
                    kind: SetOperationKind::UnionAll,
                    input_mappings: Box::new([Box::new([left_value]), Box::new([right_value])]),
                },
            )
            .unwrap();
        let program = compile_consumer(plan, consumer, union, spread())
            .unwrap_or_else(|error| panic!("{route:?} placement: {error}"));
        assert!(matches!(
            program.graph().nodes()[program.graph().root().index()].kind(),
            ProgramNodeKind::UnionAll { inputs } if inputs.len() == 2
        ));
    }
}

#[test]
fn singleton_stage_itself_proves_placement_of_unconstrained_single_copy_rows() {
    let (mut plan, mut consumer) = builder();
    let (receiver, _) = stream(&mut plan, &mut consumer, 1, Route::Unknown);
    let program =
        compile_consumer(plan, consumer, receiver, StagePlacementPolicy::Singleton).unwrap();
    assert_eq!(program.exchange_inputs().len(), 1);
}

#[test]
fn unconstrained_exchange_input_remains_unsupported_without_instance_placement() {
    let (mut plan, mut consumer) = builder();
    let (receiver, _) = stream(&mut plan, &mut consumer, 1, Route::Unknown);
    assert!(matches!(
        compile_consumer(plan, consumer, receiver, spread()),
        Err(FragmentCompileError::Unsupported {
            node: Some(node),
            feature: "unconstrained distribution without a runtime-split scan placement",
        }) if node == receiver
    ));
}

fn read(
    builder: &mut FragmentBuilder,
    node: NodeId,
    value: ValueId,
    demand: EvaluationDemand,
) -> RootOrdinal {
    let unit = builder.unit(node).unwrap();
    let definition = unit.add(None, boolean(), ExprKind::input(value)).unwrap();
    unit.root(definition, demand, RootScope::Port(0)).unwrap()
}

fn broadcast_build(
    cross_region_boundary: bool,
) -> (Result<Arc<LocalProgram>, FragmentCompileError>, NodeId) {
    let (mut plan, mut consumer) = builder();
    let (probe, probe_value) = stream(&mut plan, &mut consumer, 1, Route::RoundRobin);
    let (build, build_value) = stream(&mut plan, &mut consumer, 2, Route::Broadcast);
    let filter = consumer.reserve_node_id().unwrap();
    let predicate = read(
        &mut consumer,
        filter,
        build_value,
        EvaluationDemand::TruthOnly,
    );
    consumer
        .add_filter(filter, build, Box::new([predicate]))
        .unwrap();
    let project = consumer.reserve_node_id().unwrap();
    let expression = read(&mut consumer, project, build_value, EvaluationDemand::Value);
    consumer
        .add_project(
            project,
            filter,
            Box::new([(expression, build_value)]),
            Box::new([build_value]),
        )
        .unwrap();
    let build = if cross_region_boundary {
        // This node preserves the frozen broadcast properties, but repeats are
        // outside the approved replicated subtree family.
        let repeat = consumer.reserve_node_id().unwrap();
        consumer
            .add_repeat(
                repeat,
                project,
                Box::default(),
                Box::new([Box::default(), Box::default()]),
                Box::default(),
                Box::default(),
                Box::new([build_value]),
            )
            .unwrap();
        repeat
    } else {
        project
    };
    let join = consumer.reserve_node_id().unwrap();
    let required =
        [probe, build].map(|input| consumer.node_output_properties(input).unwrap().clone());
    consumer
        .add_join(
            join,
            [probe, build],
            Box::new(required),
            Box::new([probe_value, build_value]),
            Distribution::RoundRobin,
            NodeKind::NestLoopJoin {
                kind: JoinKind::Cross,
                distribution: NestLoopJoinDistribution::BroadcastRight,
                predicate: None,
                null_extended: Box::default(),
            },
        )
        .unwrap();
    (compile_consumer(plan, consumer, join, spread()), build)
}

#[test]
fn broadcast_build_admits_the_complete_deterministic_projection_and_filter_chain() {
    let (program, _) = broadcast_build(false);
    let program = program.unwrap();
    let nodes = program.graph().nodes();
    assert!(
        nodes
            .iter()
            .any(|node| matches!(node.kind(), ProgramNodeKind::NestedLoopJoin { .. }))
    );
    assert!(
        nodes
            .iter()
            .any(|node| matches!(node.kind(), ProgramNodeKind::Filter { .. }))
    );
}

#[test]
fn broadcast_build_cannot_consume_a_replicated_repeat() {
    let (result, repeat) = broadcast_build(true);
    assert!(matches!(
        result,
        Err(FragmentCompileError::Unsupported {
            node: Some(node),
            feature: "replicated or broadcast source-tree properties",
        }) if node == repeat
    ));
}
