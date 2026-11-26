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

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use novarocks_expression::testkit::HarnessControl;
use novarocks_local_compiler::FragmentCompileError;
use novarocks_local_program::{
    BindingRequirement, CompiledExchangeRouting, DataStreamPartitionType, ProgramNodeKind,
    StaticSinkProgram, StaticStreamRouting,
};
use novarocks_physical_plan::{
    CutImport, CutValue, DeliveryRequirement, Distribution, EdgeId, EdgeKind, EdgePartitioning,
    ExchangePayloadContract, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentOutputPort, FragmentPackage, FragmentPackageError, FragmentPackageInput, FragmentSink,
    FrozenFragmentPruning, HashDefinition, HashPartitionScheme, InboundFragmentCut, NodeId,
    OutboundFragmentCut, PartitionCountDomain, PartitionCountParameter, PlanVersionId, ResultField,
    ResultPort, ResultValueDomain, RowMultiplicity, StagePlacementPolicy, ValueDef, ValueId,
    ValueOrigin, ValueType, WriterResultCut,
};
use novarocks_type_contract::{
    PartitionCountParameterId, PartitionHashAlgorithm, PartitionSpaceId,
};

use super::fragment_fixture::{ADMISSION, DOP, compile, empty_values, numeric_catalogue, options};

const PRODUCER: FragmentId = FragmentId::new(1);
const RECEIVER: NodeId = NodeId::new(7);

fn package_input(
    fragment: Fragment,
    cuts: FragmentCuts,
    result: Option<ResultPort>,
) -> FragmentPackageInput {
    FragmentPackageInput {
        version: PlanVersionId::try_new([1; 16]).unwrap(),
        required: Default::default(),
        constants: novarocks_constant_contract::ConstantPools::empty(),

        pruning: FrozenFragmentPruning::try_new(fragment.id(), vec![], &HarnessControl).unwrap(),
        fragment,
        cuts,
        result,
        parameters: Default::default(),
        scans: Default::default(),
        writes: Default::default(),
        layouts: Default::default(),
        annotations: Box::default(),
    }
}

fn hash(keys: Box<[ValueId]>, algorithm: PartitionHashAlgorithm) -> Distribution {
    Distribution::Hash {
        keys,
        scheme: HashPartitionScheme {
            space: PartitionSpaceId::try_new([8; 32]).unwrap(),
            count: PartitionCountParameter {
                id: PartitionCountParameterId::try_new([9; 32]).unwrap(),
                admissible: PartitionCountDomain {
                    min: 1,
                    max: 4,
                    requires_power_of_two: false,
                },
            },
            definition: HashDefinition { algorithm },
        },
    }
}

fn producer_input() -> FragmentPackageInput {
    producer_input_with_hash(
        ValueType::new(DataType::Int64, false),
        PartitionHashAlgorithm::NativeExchangeV1,
    )
}

fn producer_input_with_hash(
    a_type: ValueType,
    algorithm: PartitionHashAlgorithm,
) -> FragmentPackageInput {
    let mut builder = FragmentBuilder::new(PRODUCER);
    let b_type = ValueType::new(DataType::Boolean, false);
    let (root, values) = empty_values(&mut builder, &[a_type.clone(), b_type.clone()]);
    let [a, b] = values[..] else {
        panic!("fixture root width")
    };
    let a = CutValue {
        value: a,
        ty: a_type,
    };
    let b = CutValue {
        value: b,
        ty: b_type,
    };

    // Each cut has a different consumer and the same fragment-local node
    // number. Its edge ID alone identifies the declared branch ordinal.
    let branch = |edge, consumer, projection: &[&CutValue], source, destination, multiplicity| {
        let projection: Box<[_]> = projection.iter().copied().cloned().collect();
        let destination_imports = projection
            .iter()
            .enumerate()
            .map(|(ordinal, source)| CutImport {
                source: source.clone(),
                destination: ValueId::new(10 + ordinal as u32),
            })
            .collect();
        OutboundFragmentCut {
            edge: EdgeId::new(edge),
            kind: EdgeKind::CteMulticast,
            destination_fragment: FragmentId::new(consumer),
            destination_node: RECEIVER,
            projection,
            destination_imports,
            partitioning: EdgePartitioning {
                source,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination,
                destination_multiplicity: multiplicity,
                routing_only: Box::default(),
            },
            source_port: FragmentOutputPort::Root,
            payload: ExchangePayloadContract::Relational,
            delivery: DeliveryRequirement::DemandDriven,
        }
    };
    let cuts = Box::from([
        branch(
            2,
            2,
            &[&b, &a],
            hash(Box::from([a.value]), algorithm),
            hash(Box::from([ValueId::new(11)]), algorithm),
            RowMultiplicity::SingleCopy,
        ),
        branch(
            5,
            3,
            &[],
            Distribution::Broadcast,
            Distribution::Broadcast,
            RowMultiplicity::Replicated,
        ),
        branch(
            7,
            4,
            &[&b, &b, &b],
            Distribution::RoundRobin,
            Distribution::RoundRobin,
            RowMultiplicity::SingleCopy,
        ),
        branch(
            9,
            5,
            &[&b, &a, &b],
            Distribution::Singleton,
            Distribution::Singleton,
            RowMultiplicity::SingleCopy,
        ),
    ]);
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Multicast {
                edges: Box::from([
                    EdgeId::new(9),
                    EdgeId::new(2),
                    EdgeId::new(7),
                    EdgeId::new(5),
                ]),
            },
            DOP,
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    package_input(
        fragment,
        FragmentCuts {
            outbound: cuts,
            ..FragmentCuts::default()
        },
        None,
    )
}

fn consumer_input(cut: &OutboundFragmentCut) -> FragmentPackageInput {
    let mut builder = FragmentBuilder::new(cut.destination_fragment);
    let mut fields = Vec::new();
    let mut import_mapping = Vec::new();
    let mut imports = Vec::new();
    let mut output_values = Vec::new();
    for (ordinal, import) in cut.destination_imports.iter().enumerate() {
        let value = import.destination;
        builder
            .insert_value(ValueDef {
                id: value,
                ty: import.source.ty.clone(),
                origin: ValueOrigin::CteImport {
                    edge: cut.edge,
                    producer_fragment: PRODUCER,
                    producer_value: import.source.value,
                },
            })
            .unwrap();
        output_values.push(value);
        import_mapping.push((import.source.value, value));
        imports.push(CutImport {
            source: import.source.clone(),
            destination: value,
        });
        fields.push(ResultField {
            domain: ResultValueDomain::Plain,
            name: format!("column_{ordinal}").into(),
            alias: None,
            value,
            ty: import.source.ty.clone(),
        });
    }
    let partitioning = cut.partitioning.clone();
    builder
        .add_exchange_source(
            RECEIVER,
            cut.edge,
            import_mapping.into(),
            output_values.into(),
            partitioning.destination.clone(),
            partitioning.destination_multiplicity,
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: cut.destination_fragment,
        output: builder.construction_node(RECEIVER).unwrap().output.clone(),
        fields: fields.into(),
    };
    let fragment = builder
        .finish_definition(
            RECEIVER,
            FragmentSink::Result,
            DOP,
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    package_input(
        fragment,
        FragmentCuts {
            inbound: Box::from([InboundFragmentCut {
                edge: cut.edge,
                kind: cut.kind,
                source_fragment: PRODUCER,
                destination_node: RECEIVER,
                imports: imports.into(),
                partitioning,
                source_port: cut.source_port,
                payload: cut.payload.clone(),
                delivery: cut.delivery,
            }]),
            ..FragmentCuts::default()
        },
        Some(result),
    )
}

#[test]
fn multicast_lowers_exact_branch_order_shared_aliases_and_hash_key_sources() {
    let input = producer_input();
    let package =
        Arc::new(FragmentPackage::try_new(input.clone(), ADMISSION, &HarnessControl).unwrap());
    assert!(package.result().is_none());
    let functions = numeric_catalogue();
    let program = compile(&functions, package, &HarnessControl).unwrap();
    let graph = program.graph();
    let root = &graph.nodes()[graph.root().index()];
    let ProgramNodeKind::Project { exprs, .. } = root.kind() else {
        panic!("missing occurrence aliases");
    };
    let slots = root.output_layout().slots();
    // Both repeated branches share the aliases for b; the more demanding
    // branch needs two new occurrences beyond the one already in the root.
    assert_eq!(slots.len(), 4);
    assert_eq!(exprs.len(), 4);
    for (root, ordinal) in exprs.iter().zip([0, 1, 1, 1]) {
        let unit = program.unit(root.unit).unwrap();
        assert_eq!(
            unit.input(unit.root(root.root).unwrap().use_id)
                .unwrap()
                .ordinal,
            ordinal
        );
    }
    let Some(StaticSinkProgram::MultiCastDataStream { branches }) = graph.sink() else {
        panic!("expected multicast sink");
    };
    let expected = [
        (
            DataStreamPartitionType::Unpartitioned,
            vec![slots[1], slots[0], slots[2]],
        ),
        (
            DataStreamPartitionType::HashPartitioned,
            vec![slots[1], slots[0]],
        ),
        (
            DataStreamPartitionType::Random,
            vec![slots[1], slots[2], slots[3]],
        ),
        (DataStreamPartitionType::Unpartitioned, vec![]),
    ];
    assert_eq!(branches.len(), expected.len());
    for (ordinal, (branch, (partitioning, projection))) in branches.iter().zip(expected).enumerate()
    {
        assert_eq!(branch.dest_node_id(), RECEIVER.get() as i32);
        assert_eq!(branch.partition_type(), partitioning);
        assert_eq!(branch.output_columns(), projection.as_slice());
        assert!(
            graph
                .requirements()
                .entries()
                .iter()
                .any(|requirement| matches!(requirement,
                    BindingRequirement::ExchangeOutput { branch, layout, .. }
                        if *branch == ordinal && layout.slots() == projection.as_slice()
                ))
        );
    }
    let key = branches[1].partition_exprs()[0];
    let unit = program.unit(key.unit).unwrap();
    assert_eq!(
        unit.input(unit.root(key.root).unwrap().use_id)
            .unwrap()
            .ordinal,
        0
    );
}

#[test]
fn multicast_receivers_lower_to_exact_exchange_channels() {
    let functions = numeric_catalogue();
    let input = producer_input();
    for (index, expected) in [
        (
            0,
            CompiledExchangeRouting::Hash {
                algorithm: PartitionHashAlgorithm::NativeExchangeV1,
            },
        ),
        (2, CompiledExchangeRouting::RoundRobin),
        (3, CompiledExchangeRouting::Unpartitioned),
    ] {
        let cut = &input.cuts.outbound[index];
        let package = Arc::new(
            FragmentPackage::try_new(consumer_input(cut), ADMISSION, &HarnessControl).unwrap(),
        );
        let program = compile(&functions, package, &HarnessControl).unwrap();
        let root = program.graph().root();
        let receiver = &program.exchange_inputs()[&root];
        let layout = program.graph().nodes()[root.index()].output_layout();
        assert_eq!(receiver.routing, expected);
        assert_eq!(receiver.wire_layout.slots(), layout.slots());
        for (ordinal, (field, import)) in layout
            .schema()
            .fields()
            .iter()
            .zip(cut.destination_imports.iter())
            .enumerate()
        {
            assert_eq!(field.name(), &format!("column_{ordinal}"));
            assert_eq!(field.data_type(), import.source.ty.data_type());
        }
        if index == 0 {
            assert_eq!(receiver.hash_partition_slots.as_ref(), &[layout.slots()[1]]);
        } else {
            assert!(receiver.hash_partition_slots.is_empty());
        }
        if index == 3 {
            assert_ne!(layout.slots()[0], layout.slots()[2]);
        }
    }
}

#[test]
fn multicast_preserves_the_frozen_map_hash_on_both_ends() {
    let map = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int64, false),
                    Field::new("value", DataType::Int64, false),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let algorithm = PartitionHashAlgorithm::NativeExchangeV2;
    let input = producer_input_with_hash(ValueType::new(map.clone(), false), algorithm);
    let consumer = consumer_input(&input.cuts.outbound[0]);
    let functions = numeric_catalogue();
    let producer_package = FragmentPackage::try_new(input, ADMISSION, &HarnessControl).unwrap();
    let producer = compile(&functions, Arc::new(producer_package), &HarnessControl).unwrap();
    let Some(StaticSinkProgram::MultiCastDataStream { branches }) = producer.graph().sink() else {
        panic!("expected multicast sink");
    };
    assert_eq!(
        branches[1].contract().routing,
        StaticStreamRouting::OrdinaryHash { algorithm }
    );
    assert_eq!(branches[1].partition_exprs().len(), 1);

    let consumer_package = FragmentPackage::try_new(consumer, ADMISSION, &HarnessControl).unwrap();
    let consumer = compile(&functions, Arc::new(consumer_package), &HarnessControl).unwrap();
    let root = consumer.graph().root();
    let receiver = &consumer.exchange_inputs()[&root];
    assert_eq!(
        receiver.routing,
        CompiledExchangeRouting::Hash { algorithm }
    );
    assert_eq!(
        receiver.hash_partition_slots.as_ref(),
        &[receiver.wire_layout.slots()[1]]
    );
    assert_eq!(receiver.wire_layout.schema().field(1).data_type(), &map);
}

#[test]
fn multicast_rejects_wrong_kind_role_delivery_or_distribution() {
    let functions = numeric_catalogue();
    let cases: [fn(&mut OutboundFragmentCut); 4] = [
        |cut: &mut OutboundFragmentCut| cut.kind = EdgeKind::Stream,
        |cut: &mut OutboundFragmentCut| cut.delivery = DeliveryRequirement::MustDrain,
        |cut: &mut OutboundFragmentCut| {
            cut.payload = ExchangePayloadContract::WriterResults(WriterResultCut {
                write_target_ordinal: novarocks_physical_plan::WriteTargetOrdinal::try_new(1)
                    .unwrap(),
                schema_revision: 1,
                fields: Box::default(),
            })
        },
        |cut: &mut OutboundFragmentCut| {
            cut.partitioning.source = Distribution::Unconstrained;
            cut.partitioning.destination = Distribution::Unconstrained;
        },
    ];
    for make_invalid in cases {
        let mut input = producer_input();
        make_invalid(&mut input.cuts.outbound[0]);
        match FragmentPackage::try_new(input, ADMISSION, &HarnessControl) {
            Ok(package) => assert!(matches!(
                compile(&functions, Arc::new(package), &HarnessControl),
                Err(FragmentCompileError::Invalid(_))
            )),
            Err(FragmentPackageError::Structure(_)) => {}
            Err(error) => panic!("unexpected package rejection: {error}"),
        }
    }
}
