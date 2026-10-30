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

//! Pure physical-plan facts shared by runtime-filter codec tests.

use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_functions::FunctionValueType;
use novarocks_physical_plan::{
    Distribution, Edge, EdgeId, EdgeKind, ExprKind, FragmentBuilder, FragmentId, FragmentSink,
    JoinDistribution, JoinKey, JoinKind, JoinSide, NodeId, NodeKind, OutputPort, PhysicalNode,
    PhysicalPlan, PhysicalProperties, PipelineDopDomain, PlanBuilder, PlanVersionId, ResultField,
    ResultPort, RowMultiplicity, ValueId, ValueOrigin, ValueType,
};

pub(crate) struct CodecTestControl;
impl novarocks_type_contract::PureCompileControl for CodecTestControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}

pub(crate) fn codec_constant_policy() -> novarocks_constant_contract::ConstantPolicy {
    novarocks_constant_contract::ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 8 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 8 * 1024 * 1024,
        max_library_validation_bytes: 32 * 1024 * 1024,
    }
}

pub(crate) fn fixture_i64_constants() -> novarocks_physical_plan::ConstantPools {
    use arrow::array::{Array, Int64Array};
    let pool = novarocks_constant_contract::ConstantPool::try_new(
        Arc::new(arrow::datatypes::Field::new(
            "source",
            DataType::Int64,
            false,
        )),
        FunctionValueType::new(DataType::Int64, false),
        Int64Array::from(vec![1, 2, 7, 9]).to_data(),
        codec_constant_policy(),
        novarocks_type_contract::CompilePhase::Encode,
        &CodecTestControl,
    )
    .unwrap();
    let mut constants = novarocks_physical_plan::ConstantPools::empty();
    constants
        .insert(novarocks_physical_plan::ConstantPoolId::new(0), pool)
        .unwrap();
    constants
}

pub(crate) fn fixture_i64_reference(value: i64) -> ExprKind {
    let ordinal = [1, 2, 7, 9]
        .iter()
        .position(|candidate| *candidate == value)
        .expect("the fixture must explicitly author its source value");
    ExprKind::Constant(novarocks_physical_plan::ConstantReference {
        pool: novarocks_physical_plan::ConstantPoolId::new(0),
        ordinal: u32::try_from(ordinal).unwrap(),
    })
}

pub(crate) fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

pub(crate) fn append_test_i64_values(
    builder: &mut FragmentBuilder,
    literal_value: i64,
) -> (NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let literal = builder
        .add_expression(node, ty.clone(), fixture_i64_reference(literal_value))
        .unwrap();
    let value = builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([literal])]),
            },
        })
        .unwrap();
    (node, value)
}

pub(crate) fn test_hash_scheme(seed: u8) -> novarocks_physical_plan::HashPartitionScheme {
    novarocks_physical_plan::HashPartitionScheme {
        space: novarocks_physical_plan::PartitionSpaceId::try_new([seed; 32]).unwrap(),
        count: novarocks_physical_plan::PartitionCountParameter {
            id: novarocks_physical_plan::PartitionCountParameterId::try_new(
                [seed.wrapping_add(1); 32],
            )
            .unwrap(),
            admissible: novarocks_physical_plan::PartitionCountDomain {
                min: 1,
                max: 64,
                requires_power_of_two: true,
            },
        },
        definition: novarocks_physical_plan::HashDefinition::native_exchange(),
    }
}

pub(crate) fn test_runtime_filter_coverage(
    witness: novarocks_physical_plan::RuntimeFilterWitnessId,
) -> novarocks_physical_plan::RuntimeFilterCoverage {
    use novarocks_physical_plan::RuntimeFilterCoverageNode;

    novarocks_physical_plan::RuntimeFilterCoverage {
        nodes: Box::from([
            RuntimeFilterCoverageNode::Witness(witness),
            RuntimeFilterCoverageNode::AllOf {
                children: Box::from([0]),
            },
        ]),
        root: 1,
    }
}

pub(crate) fn finish_ordered_join_build_filter_plan() -> PhysicalPlan {
    use novarocks_physical_plan::{
        EdgeDestination, EdgePartitioning, EdgeSource, NullOrdering, OrderedComparisonAlgorithm,
        RuntimeFilter, RuntimeFilterApplyPoint, RuntimeFilterArtifactCapability,
        RuntimeFilterCompletion, RuntimeFilterConsumer, RuntimeFilterConsumerActivation,
        RuntimeFilterConsumerTarget, RuntimeFilterContributionKind, RuntimeFilterDomain,
        RuntimeFilterEqualityWitness, RuntimeFilterEqualityWitnessId, RuntimeFilterId,
        RuntimeFilterKind, RuntimeFilterLifecycle, RuntimeFilterOrderKey, RuntimeFilterPolicy,
        RuntimeFilterProducer, RuntimeFilterProducerProgress, RuntimeFilterProducerTarget,
        RuntimeFilterReduction, RuntimeFilterWitnessId, SortDirection,
    };

    let left_fragment = FragmentId::new(40);
    let right_fragment = FragmentId::new(41);
    let join_fragment = FragmentId::new(42);
    let left_edge = EdgeId::new(40);
    let right_edge = EdgeId::new(41);
    let scheme = test_hash_scheme(40);
    let ty = ValueType::new(DataType::Int64, false);

    let mut left_builder = FragmentBuilder::new(left_fragment);
    let (left_source_node, left_source_value) = append_test_i64_values(&mut left_builder, 1);
    let left_source = left_builder
        .finish_definition(
            left_source_node,
            FragmentSink::Stream { edge: left_edge },
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut right_builder = FragmentBuilder::new(right_fragment);
    let (right_source_node, right_source_value) = append_test_i64_values(&mut right_builder, 2);
    let right_source = right_builder
        .finish_definition(
            right_source_node,
            FragmentSink::Stream { edge: right_edge },
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut join_builder = FragmentBuilder::new(join_fragment);
    let left = join_builder.reserve_node_id().unwrap();
    let left_value = join_builder
        .add_value(
            ty.clone(),
            ValueOrigin::ExchangeImport {
                edge: left_edge,
                source_value: left_source_value,
            },
        )
        .unwrap();
    let left_properties = PhysicalProperties {
        distribution: Distribution::Hash {
            keys: Box::from([left_value]),
            scheme: scheme.clone(),
        },
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    join_builder
        .insert_node_unchecked(PhysicalNode {
            id: left,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: left_properties.clone(),
            output: OutputPort {
                node: left,
                columns: Box::from([left_value]),
            },
            kind: NodeKind::ExchangeSource {
                edge: left_edge,
                imports: Box::from([(left_source_value, left_value)]),
            },
        })
        .unwrap();

    let right = join_builder.reserve_node_id().unwrap();
    let right_value = join_builder
        .add_value(
            ty.clone(),
            ValueOrigin::ExchangeImport {
                edge: right_edge,
                source_value: right_source_value,
            },
        )
        .unwrap();
    let right_properties = PhysicalProperties {
        distribution: Distribution::Hash {
            keys: Box::from([right_value]),
            scheme: scheme.clone(),
        },
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    join_builder
        .insert_node_unchecked(PhysicalNode {
            id: right,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: right_properties.clone(),
            output: OutputPort {
                node: right,
                columns: Box::from([right_value]),
            },
            kind: NodeKind::ExchangeSource {
                edge: right_edge,
                imports: Box::from([(right_source_value, right_value)]),
            },
        })
        .unwrap();
    let aggregate = join_builder.reserve_node_id().unwrap();
    let group_key = join_builder
        .add_expression(aggregate, ty.clone(), ExprKind::Value(right_value))
        .unwrap();
    join_builder
        .insert_node_unchecked(PhysicalNode {
            id: aggregate,
            inputs: Box::from([right]),
            required_inputs: Box::from([right_properties.clone()]),
            output_properties: right_properties.clone(),
            output: OutputPort {
                node: aggregate,
                columns: Box::from([right_value]),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::from([(group_key, right_value)]),
                calls: Box::default(),
                grouping: novarocks_physical_plan::AggregateGrouping::Complete,
            },
        })
        .unwrap();
    let join = join_builder.reserve_node_id().unwrap();
    let left_key = join_builder
        .add_expression(join, ty.clone(), ExprKind::Value(left_value))
        .unwrap();
    let right_key = join_builder
        .add_expression(join, ty.clone(), ExprKind::Value(right_value))
        .unwrap();
    join_builder
        .insert_node_unchecked(PhysicalNode {
            id: join,
            inputs: Box::from([left, aggregate]),
            required_inputs: Box::from([left_properties.clone(), right_properties]),
            output_properties: left_properties,
            output: OutputPort {
                node: join,
                columns: Box::from([left_value, right_value]),
            },
            kind: NodeKind::HashJoin {
                kind: JoinKind::Inner,
                condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                    keys: Box::from([JoinKey {
                        left: left_key,
                        right: right_key,
                        null_safe: false,
                    }]),
                    residual: None,
                },
                build_side: JoinSide::Right,
                distribution: JoinDistribution::Partitioned,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    let filter_id = RuntimeFilterId::new(40);
    join_builder.attach_runtime_filter(filter_id).unwrap();
    let join_stage = join_builder
        .finish_definition(
            join,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let witness = RuntimeFilterWitnessId::new(40);
    let equality = RuntimeFilterEqualityWitnessId::new(40);
    let filter = RuntimeFilter {
        id: filter_id,
        kind: RuntimeFilterKind::MinMax,
        domain: RuntimeFilterDomain::Ordered {
            key: RuntimeFilterOrderKey {
                ty: ty.clone(),
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            },
            inclusive: true,
            comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction: RuntimeFilterReduction::UnionOrderedHull,
        availability_coverage: test_runtime_filter_coverage(witness),
        terminal_coverage: test_runtime_filter_coverage(witness),
        equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
            id: equality,
            fragment: join_fragment,
            join,
            key_ordinal: 0,
            domain_side: JoinSide::Right,
        }]),
        producers: Box::from([RuntimeFilterProducer {
            witness,
            endpoint: novarocks_physical_plan::RuntimeFilterEndpoint {
                fragment: join_fragment,
                node: join,
                values: Box::from([right_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from([
                RuntimeFilterContributionKind::FinalOrderedHullShard,
                RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::FencedCommittedDomain,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::from([right_edge]),
                non_build_edges: Box::from([left_edge]),
            },
            target: RuntimeFilterProducerTarget::JoinBuildKey { equality },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: novarocks_physical_plan::RuntimeFilterEndpoint {
                fragment: join_fragment,
                node: join,
                values: Box::from([left_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
            capabilities: Box::from([
                RuntimeFilterArtifactCapability::OrderedRange,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ]),
            activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: RuntimeFilterConsumerTarget::JoinProbeKey { equality },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    };

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([40; 16]).unwrap())
        .with_constant_pools(fixture_i64_constants());
    plan.add_fragment(left_source).unwrap();
    plan.add_fragment(right_source).unwrap();
    plan.add_fragment(join_stage).unwrap();
    plan.add_edge(Edge {
        role: novarocks_physical_plan::OutputRole::Relational,
        delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
        id: left_edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: left_fragment,
            projection: Box::from([left_source_value]),
        },
        destination: EdgeDestination {
            fragment: join_fragment,
            node: left,
            receive_mapping: Box::from([(left_source_value, left_value)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Hash {
                keys: Box::from([left_source_value]),
                scheme: scheme.clone(),
            },
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Hash {
                keys: Box::from([left_value]),
                scheme: scheme.clone(),
            },
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    plan.add_edge(Edge {
        role: novarocks_physical_plan::OutputRole::Relational,
        delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
        id: right_edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: right_fragment,
            projection: Box::from([right_source_value]),
        },
        destination: EdgeDestination {
            fragment: join_fragment,
            node: right,
            receive_mapping: Box::from([(right_source_value, right_value)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Hash {
                keys: Box::from([right_source_value]),
                scheme: scheme.clone(),
            },
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Hash {
                keys: Box::from([right_value]),
                scheme,
            },
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    plan.add_runtime_filter(filter).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: join_fragment,
        output: OutputPort {
            node: join,
            columns: Box::from([left_value, right_value]),
        },
        fields: Box::from([
            ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "left".into(),
                alias: None,
                value: left_value,
                ty: ty.clone(),
            },
            ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "right".into(),
                alias: None,
                value: right_value,
                ty,
            },
        ]),
    })
    .unwrap();
    plan.finish_observed(&CodecTestControl).unwrap()
}
