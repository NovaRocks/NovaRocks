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

//! Two-fragment exchange plans built through the real physical builders and
//! package extraction: a Values producer streaming over one edge and an
//! ExchangeSource consumer. Both sides compile independently.

use crate::{
    FragmentCompileError, LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use arrow_schema::DataType;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    BindingRequirement, CompiledExchangeInput, DataStreamPartitionType, KernelAbiVersion,
    LocalProgram, ProgramChannelLayoutRole, ProgramChannelSite, ProgramExprId,
    ProgramExpressionArena, ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeId,
    ProgramNodeKind, ProgramUseRef, StaticExprKind, StaticSinkProgram,
};
use novarocks_physical_plan::{
    Distribution, Edge, EdgeDestination, EdgeId, EdgeKind, EdgePartitioning, EdgeSource, ExprId,
    ExprKind, Fragment, FragmentBuilder, FragmentId, FragmentPackage, FragmentPackageAdmission,
    FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning, HashDefinition, HashPartitionScheme,
    LiteralValue, NodeId, OutputPort, PartitionCountDomain, PartitionCountParameter,
    PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanBuilder, PlanLimits,
    PlanVersionId, PropertyProofProjectionLimits, ResultField, ResultPort, RowMultiplicity,
    ValueOrigin, ValueType, extract_fragment_packages,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    PartitionCountParameterId, PartitionSpaceId, PureCompileControl,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

const PRODUCER: FragmentId = FragmentId::new(1);
const CONSUMER: FragmentId = FragmentId::new(2);
const EDGE: EdgeId = EdgeId::new(5);
const VALUES: NodeId = NodeId::new(10);
const RECEIVER: NodeId = NodeId::new(20);

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    /// Stream edge, Singleton to Singleton, consumer publishes the result.
    Gather,
    Hierarchical,
    Conditional,
    RepeatedConditional,
    RoundRobin,
    /// Stream edge hashed on both produced columns; consumer is a Noop sink.
    Hash,
    /// Hash keys `[a, b]` with the received value `b` published twice.
    RepeatedHash,
    /// Stream edge without a partitioning contract; consumer is a Noop sink.
    Unconstrained,
    /// CTE multicast producer; consumer is a Noop sink.
    Multicast,
}

fn hash(keys: [novarocks_physical_plan::ValueId; 2]) -> Distribution {
    Distribution::Hash {
        keys: Box::from(keys),
        scheme: HashPartitionScheme {
            space: PartitionSpaceId::try_new([41; 32]).unwrap(),
            count: PartitionCountParameter {
                id: PartitionCountParameterId::try_new([42; 32]).unwrap(),
                admissible: PartitionCountDomain {
                    min: 1,
                    max: 64,
                    requires_power_of_two: true,
                },
            },
            definition: HashDefinition::native_exchange(),
        },
    }
}

fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}

/// Producer `Values(a = 11, b = 22)` streams the reversed projection `[b, a]`;
/// the consumer receives `[ib, ia]` positionally at `RECEIVER`.
fn packages(shape: Shape) -> BTreeMap<FragmentId, FragmentPackage> {
    let ty = ValueType::new(DataType::Int64, false);
    let conditional = matches!(shape, Shape::Conditional | Shape::RepeatedConditional);
    let repeated = matches!(shape, Shape::RepeatedHash | Shape::RepeatedConditional);
    let b_type = if conditional {
        ValueType::new(DataType::Boolean, true)
    } else {
        ty.clone()
    };
    let mut producer = FragmentBuilder::new(PRODUCER);
    let mut outputs = Vec::new();
    let mut cells = Vec::new();
    for (ordinal, literal) in [11, 22].into_iter().enumerate() {
        let field_type = if ordinal == 1 { &b_type } else { &ty };
        let literal = if conditional && ordinal == 1 {
            LiteralValue::Null
        } else {
            LiteralValue::Int64(literal)
        };
        outputs.push(
            producer
                .add_value(
                    field_type.clone(),
                    ValueOrigin::NodeOutput {
                        node: VALUES,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
        );
        cells.push(
            producer
                .add_expression(VALUES, field_type.clone(), ExprKind::Literal(literal))
                .unwrap(),
        );
    }
    let (a, b) = (outputs[0], outputs[1]);
    producer
        .add_values(
            VALUES,
            Box::from([cells.into_boxed_slice()]),
            outputs.into_boxed_slice(),
        )
        .unwrap();
    let root = if repeated {
        let project = NodeId::new(11);
        let read_a = producer
            .add_expression(project, ty.clone(), ExprKind::Value(a))
            .unwrap();
        let read_b = producer
            .add_expression(project, b_type.clone(), ExprKind::Value(b))
            .unwrap();
        producer
            .add_project(
                project,
                VALUES,
                Box::from([(read_b, b), (read_a, a), (read_b, b)]),
                Box::from([b, a, b]),
            )
            .unwrap();
        project
    } else {
        VALUES
    };
    let multicast = shape == Shape::Multicast;
    let sink = if multicast {
        FragmentSink::Multicast {
            edges: Box::from([EDGE]),
        }
    } else {
        FragmentSink::Stream { edge: EDGE }
    };
    let producer = producer
        .finish_definition(
            root,
            sink,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut consumer = FragmentBuilder::new(CONSUMER);
    let import = |builder: &mut FragmentBuilder, source_value| {
        let origin = if multicast {
            ValueOrigin::CteImport {
                edge: EDGE,
                producer_fragment: PRODUCER,
                producer_value: source_value,
            }
        } else {
            ValueOrigin::ExchangeImport {
                edge: EDGE,
                source_value,
            }
        };
        let field_type = if source_value == b { &b_type } else { &ty };
        builder.add_value(field_type.clone(), origin).unwrap()
    };
    let ib = import(&mut consumer, b);
    let ia = import(&mut consumer, a);
    let mapping: Box<[_]> = if repeated {
        Box::from([(b, ib), (a, ia), (b, ib)])
    } else {
        Box::from([(b, ib), (a, ia)])
    };
    let received: Box<[_]> = if repeated {
        Box::from([ib, ia, ib])
    } else {
        Box::from([ib, ia])
    };
    let projected: Box<[_]> = if repeated {
        Box::from([b, a, b])
    } else {
        Box::from([b, a])
    };
    let (source, destination) = match shape {
        Shape::Gather => (Distribution::Singleton, Distribution::Singleton),
        Shape::Hash => (hash([b, a]), hash([ib, ia])),
        Shape::RepeatedHash => (hash([a, b]), hash([ia, ib])),
        Shape::Hierarchical => {
            let hierarchical = |keys| {
                let Distribution::Hash { keys, mut scheme } = hash(keys) else {
                    unreachable!()
                };
                scheme.definition.algorithm =
                    novarocks_type_contract::PartitionHashAlgorithm::NativeHierarchicalV1;
                Distribution::Hash { keys, scheme }
            };
            (hierarchical([b, a]), hierarchical([ib, ia]))
        }
        Shape::Conditional | Shape::RepeatedConditional => {
            let conditional = |keys, applies| {
                let Distribution::Hash { keys, mut scheme } = hash(keys) else {
                    unreachable!()
                };
                scheme.definition.algorithm =
                    novarocks_type_contract::PartitionHashAlgorithm::NativeHierarchicalV1;
                Distribution::ConditionalKey {
                    keys: if repeated { keys } else { Box::from([keys[1]]) },
                    applies,
                    scheme,
                }
            };
            (conditional([b, a], b), conditional([ib, ia], ib))
        }
        Shape::RoundRobin => (Distribution::RoundRobin, Distribution::RoundRobin),
        Shape::Unconstrained | Shape::Multicast => {
            (Distribution::Unconstrained, Distribution::Unconstrained)
        }
    };
    consumer
        .add_exchange_source(
            RECEIVER,
            EDGE,
            mapping.clone(),
            received.clone(),
            destination.clone(),
            RowMultiplicity::SingleCopy,
        )
        .unwrap();
    let gather = matches!(
        shape,
        Shape::Gather
            | Shape::RepeatedHash
            | Shape::Hierarchical
            | Shape::Conditional
            | Shape::RepeatedConditional
            | Shape::RoundRobin
    );
    let consumer_sink = if gather {
        FragmentSink::Result
    } else {
        FragmentSink::Noop
    };
    let consumer = consumer
        .finish_definition(
            RECEIVER,
            consumer_sink,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(Edge {
        role: novarocks_physical_plan::OutputRole::Relational,
        delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
        id: EDGE,
        kind: if multicast {
            EdgeKind::CteMulticast
        } else {
            EdgeKind::Stream
        },
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: PRODUCER,
            projection: projected,
        },
        destination: EdgeDestination {
            fragment: CONSUMER,
            node: RECEIVER,
            receive_mapping: mapping,
        },
        partitioning: EdgePartitioning {
            source,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    if gather {
        let mut fields = vec![
            ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "b".into(),
                alias: Some("bee".into()),
                value: ib,
                ty: b_type.clone(),
            },
            ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "a".into(),
                alias: None,
                value: ia,
                ty: ty.clone(),
            },
        ];
        if repeated {
            fields.push(ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "b".into(),
                alias: Some("bee_again".into()),
                value: ib,
                ty: b_type.clone(),
            });
        }
        plan.set_result_port(ResultPort {
            scalar_schema: None,
            fragment: CONSUMER,
            output: OutputPort {
                node: RECEIVER,
                columns: received,
            },
            fields: fields.into_boxed_slice(),
        })
        .unwrap();
    }
    let plan = plan.finish().unwrap();

    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let root_uses = root_uses(fragment);
        calls.insert(
            id,
            FrozenFragmentCalls::try_new(fragment, &root_uses, vec![], &FixtureControl).unwrap(),
        );
        uses.insert(id, root_uses);
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
        );
        admissions.insert(id, package_admission());
    }
    extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap()
}

/// One eager root occurrence per physical root site in one root domain.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut invocations = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(ordinal, (&site, root))| {
            let use_id = ExpressionUseId::new(ordinal as u32);
            invocations.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain,
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, use_id)
        })
        .collect();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap()
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
fn package_admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

// A real RAND-only sealed subset; the fixtures call no function.
fn functions() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.scalar/rand/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}

fn options(root_sink_dop: Option<usize>) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: root_sink_dop.map(|dop| NonZeroUsize::new(dop).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        // Explicit fixture admission; these values are not production defaults.
        constants: ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 128,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
    }
}

fn compile(
    package: FragmentPackage,
    root_sink_dop: Option<usize>,
) -> Result<LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated =
        validate_fragment_providers(Arc::new(package), &providers, &FixtureControl).unwrap();
    compile_fragment(
        validated,
        &functions(),
        options(root_sink_dop),
        &FixtureControl,
    )
}

fn refused(result: Result<LocalProgram, FragmentCompileError>, expected: &str) {
    match result {
        Err(FragmentCompileError::Unsupported { feature, .. }) => assert_eq!(feature, expected),
        other => panic!("expected refusal {expected:?}, got {other:?}"),
    }
}

#[test]
fn gather_producer_streams_and_consumer_receives_one_addressed_edge() {
    let mut packages = packages(Shape::Gather);
    let producer = packages.remove(&PRODUCER).unwrap();
    // A stream producer has no result port and borrows no result labels.
    assert!(producer.result().is_none());
    let producer = compile(producer, None).unwrap();
    let root_slots = producer.graph().nodes()[0].output_layout().slots().to_vec();
    let Some(StaticSinkProgram::DataStream { branch, arena }) = producer.graph().sink() else {
        panic!("stream sink");
    };
    assert_eq!(branch.dest_node_id(), RECEIVER.get() as i32);
    assert_eq!(
        branch.partition_type(),
        DataStreamPartitionType::Unpartitioned
    );
    assert!(branch.partition_exprs().is_empty());
    // The reversed cut projection `[b, a]` names root slots in that order.
    assert_eq!(branch.output_columns(), &[root_slots[1], root_slots[0]]);
    assert!(branch.limit().is_none());
    assert!(arena.nodes().is_empty());
    let requirements = producer.graph().requirements().entries();
    assert_eq!(requirements.len(), 1);
    assert!(matches!(
        &requirements[0],
        BindingRequirement::ExchangeOutput { branch: 0, layout , ..}
            if layout.slots() == [root_slots[1], root_slots[0]]
    ));
    // The empty Sink arena still owns its (empty) flow.
    let snapshot = producer
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    assert!(
        snapshot.flows()[&ProgramExpressionArena::Sink]
            .uses()
            .is_empty()
    );
    assert!(producer.exchange_inputs().is_empty());

    let consumer = compile(packages.remove(&CONSUMER).unwrap(), Some(1)).unwrap();
    let receiver = &consumer.graph().nodes()[0];
    let ProgramNodeKind::ExchangeSource {
        runtime_filters,
        hash_partition_exprs,
    } = receiver.kind()
    else {
        panic!("ExchangeSource");
    };
    assert!(runtime_filters.is_empty() && hash_partition_exprs.is_empty());
    let layout = receiver.output_layout();
    let names = layout
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["bee", "a"]);
    assert_eq!(
        consumer.exchange_inputs(),
        &BTreeMap::from([(
            ProgramNodeId::new(0),
            CompiledExchangeInput {
                role: novarocks_local_program::ProgramOutputRole::Relational,
                delivery: novarocks_local_program::ExchangeDeliveryRequirement::DemandDriven,
                routing: novarocks_local_program::CompiledExchangeRouting::Unpartitioned,
                receiver_node: RECEIVER.get(),
                edge: EDGE.get(),
                source_fragment: PRODUCER.get(),
                hash_partition_slots: Box::default(),
            },
        )])
    );
    let requirements = consumer.graph().requirements().entries();
    assert_eq!(requirements.len(), 2);
    assert!(requirements.iter().any(|requirement| matches!(
        requirement,
        BindingRequirement::ExchangeInput { node, layout: required , ..}
            if *node == ProgramNodeId::new(0) && required.slots() == layout.slots()
    )));
    assert!(
        requirements
            .iter()
            .any(|requirement| matches!(requirement, BindingRequirement::ResultSink { .. }))
    );
    assert!(matches!(
        consumer.graph().sink(),
        Some(StaticSinkProgram::Result)
    ));
}

#[test]
fn hash_producer_authors_one_sink_partition_root_per_key() {
    let producer = compile(packages(Shape::Hash).remove(&PRODUCER).unwrap(), None).unwrap();
    let root_slots = producer.graph().nodes()[0].output_layout().slots().to_vec();
    let Some(StaticSinkProgram::DataStream { branch, arena }) = producer.graph().sink() else {
        panic!("stream sink");
    };
    assert_eq!(
        branch.partition_type(),
        DataStreamPartitionType::HashPartitioned
    );
    assert_eq!(
        branch.partition_exprs(),
        &[ProgramExprId::new(0), ProgramExprId::new(1)]
    );
    // Keys `[b, a]` read the root occurrences 1 and 0 in a separate arena.
    for (key, ordinal) in [(0, 1u32), (1, 0u32)] {
        assert!(matches!(
            arena.nodes()[key].kind(),
            StaticExprKind::SlotId(slot) if *slot == root_slots[ordinal as usize]
        ));
    }
    assert!(!Arc::ptr_eq(arena, producer.graph().expressions()));
    let checked = producer.checked();
    let snapshot = checked.channels().expressions().resolved_calls().snapshot();
    let sites = snapshot
        .bindings()
        .iter()
        .filter(|(site, _)| matches!(site, ProgramExpressionRootSite::SinkPartition { .. }))
        .map(|(site, use_id)| (*site, *use_id))
        .collect::<Vec<_>>();
    assert_eq!(sites.len(), 2);
    assert_eq!(
        snapshot.flows()[&ProgramExpressionArena::Sink].uses().len(),
        2
    );
    for (key, (site, use_id)) in sites.into_iter().enumerate() {
        assert_eq!(
            site,
            ProgramExpressionRootSite::SinkPartition {
                branch: 0,
                key: key as u32,
            }
        );
        let expected = ProgramLexicalSource::Input(ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: if key == 0 { 1 } else { 0 },
        });
        assert_eq!(
            checked.slots()[&ProgramUseRef {
                arena: ProgramExpressionArena::Sink,
                use_id,
            }],
            expected
        );
    }
}

#[test]
fn repeated_hash_imports_keep_each_channel_and_the_ordered_key_representative() {
    let mut packages = packages(Shape::RepeatedHash);
    let producer = compile(packages.remove(&PRODUCER).unwrap(), None).unwrap();
    let source_slots = producer.graph().nodes()[producer.graph().root().index()]
        .output_layout()
        .slots();
    let Some(StaticSinkProgram::DataStream { branch, .. }) = producer.graph().sink() else {
        panic!("stream sink");
    };
    assert_eq!(source_slots.len(), 3);
    assert_ne!(source_slots[0], source_slots[2]);
    assert_eq!(branch.output_columns(), source_slots);

    let consumer = compile(packages.remove(&CONSUMER).unwrap(), Some(1)).unwrap();
    let (id, input) = consumer.exchange_inputs().iter().next().unwrap();
    let layout = consumer.graph().nodes()[id.index()].output_layout();
    let slots = layout.slots();
    assert_eq!(slots.len(), 3);
    assert_ne!(slots[0], slots[2], "wire occurrences keep fresh channels");
    assert_eq!(input.hash_partition_slots.as_ref(), &[slots[1], slots[0]]);
    assert_eq!(layout.schema().field(2).name(), "bee_again");
}

#[test]
fn unsupported_sinks_and_partitionings_are_refused() {
    refused(
        compile(packages(Shape::Multicast).remove(&PRODUCER).unwrap(), None),
        "multicast or noop sink",
    );
    let mut unconstrained = packages(Shape::Unconstrained);
    refused(
        compile(unconstrained.remove(&PRODUCER).unwrap(), None),
        "unconstrained stream partitioning",
    );
    refused(
        compile(unconstrained.remove(&CONSUMER).unwrap(), None),
        "multicast or noop sink",
    );
    // A hash receiver is not a singleton source tree yet.
    let hash_consumer = packages(Shape::Hash).remove(&CONSUMER).unwrap();
    assert!(matches!(
        compile(hash_consumer, None),
        Err(FragmentCompileError::Unsupported { .. })
    ));
}

#[test]
fn result_sink_without_result_port_never_reaches_lowering() {
    // The checked package owns sink/result-port correspondence; the compiler's
    // own `missing result port` refusal is defense behind that boundary.
    let mut input = packages(Shape::Gather)
        .remove(&CONSUMER)
        .unwrap()
        .into_input();
    assert!(input.result.take().is_some());
    assert!(FragmentPackage::try_new(input, package_admission(), &FixtureControl).is_err());
}

#[test]
fn hierarchical_conditional_and_round_robin_keep_their_exact_sender_and_receiver_facts() {
    use novarocks_local_program::{CompiledExchangeRouting, StaticStreamRouting};
    use novarocks_type_contract::{EvaluationDemand, PartitionHashAlgorithm};
    for shape in [Shape::Hierarchical, Shape::Conditional, Shape::RoundRobin] {
        let mut packages = packages(shape);
        let producer = compile(packages.remove(&PRODUCER).unwrap(), None)
            .expect("the exact sender contract compiles");
        let Some(StaticSinkProgram::DataStream { branch, arena }) = producer.graph().sink() else {
            panic!("stream")
        };
        let consumer = compile(packages.remove(&CONSUMER).unwrap(), Some(1))
            .expect("the exact receiver contract compiles");
        let facts = &consumer.exchange_inputs()[&ProgramNodeId::new(0)];
        let slots = consumer.graph().nodes()[0].output_layout().slots();
        match shape {
            Shape::Hierarchical => {
                assert_eq!(
                    branch.contract().routing,
                    StaticStreamRouting::HierarchicalHashV1
                );
                assert_eq!(
                    facts.routing,
                    CompiledExchangeRouting::Hash {
                        algorithm: PartitionHashAlgorithm::NativeHierarchicalV1
                    }
                );
                assert_eq!(facts.hash_partition_slots.as_ref(), slots);
            }
            Shape::Conditional => {
                let StaticStreamRouting::ConditionalKeyV1 { applies } = branch.contract().routing
                else {
                    panic!("conditional routing must not become ordinary Hash")
                };
                assert_eq!(arena.node(applies).unwrap().data_type(), &DataType::Boolean);
                assert_eq!(
                    facts.routing,
                    CompiledExchangeRouting::ConditionalKey {
                        applies: slots[0],
                        algorithm: PartitionHashAlgorithm::NativeHierarchicalV1
                    }
                );
                assert_eq!(facts.hash_partition_slots.as_ref(), &slots[1..]);
                let snapshot = producer
                    .checked()
                    .channels()
                    .expressions()
                    .resolved_calls()
                    .snapshot();
                let id = snapshot.bindings()
                    [&ProgramExpressionRootSite::SinkApplicability { branch: 0 }];
                assert_eq!(
                    snapshot.flows()[&ProgramExpressionArena::Sink].uses()[&id]
                        .context
                        .demand,
                    EvaluationDemand::TruthOnly
                );
                assert_eq!(
                    snapshot.flows()[&ProgramExpressionArena::Sink].uses().len(),
                    2
                );
            }
            Shape::RoundRobin => {
                assert_eq!(branch.partition_type(), DataStreamPartitionType::Random);
                assert_eq!(branch.contract().routing, StaticStreamRouting::Ordinary);
                assert_eq!(facts.routing, CompiledExchangeRouting::RoundRobin);
                assert!(facts.hash_partition_slots.is_empty());
            }
            _ => unreachable!(),
        }
        assert_eq!(
            facts.role,
            novarocks_local_program::ProgramOutputRole::Relational
        );
    }
}

#[test]
fn repeated_conditional_imports_use_the_same_first_mask_and_hash_key_occurrence() {
    use novarocks_local_program::{CompiledExchangeRouting, StaticStreamRouting};
    use novarocks_type_contract::PartitionHashAlgorithm;
    let mut packages = packages(Shape::RepeatedConditional);
    let producer = compile(packages.remove(&PRODUCER).unwrap(), None).unwrap();
    let root = &producer.graph().nodes()[producer.graph().root().index()];
    let Some(StaticSinkProgram::DataStream { branch, arena }) = producer.graph().sink() else {
        panic!("stream sink");
    };
    let StaticStreamRouting::ConditionalKeyV1 { applies } = branch.contract().routing else {
        panic!("conditional stream");
    };
    let slots = root.output_layout().slots();
    assert_eq!(slots.len(), 3);
    assert_ne!(slots[0], slots[2]);
    assert!(
        matches!(arena.node(applies).unwrap().kind(), StaticExprKind::SlotId(slot) if *slot == slots[0])
    );
    assert!(
        matches!(arena.node(branch.partition_exprs()[0]).unwrap().kind(), StaticExprKind::SlotId(slot) if *slot == slots[0])
    );
    assert_eq!(branch.output_columns(), slots);

    let consumer = compile(packages.remove(&CONSUMER).unwrap(), Some(1)).unwrap();
    let (id, input) = consumer.exchange_inputs().iter().next().unwrap();
    let slots = consumer.graph().nodes()[id.index()].output_layout().slots();
    assert_eq!(slots.len(), 3);
    assert_ne!(slots[0], slots[2]);
    assert_eq!(
        input.routing,
        CompiledExchangeRouting::ConditionalKey {
            applies: slots[0],
            algorithm: PartitionHashAlgorithm::NativeHierarchicalV1,
        }
    );
    assert_eq!(input.hash_partition_slots.as_ref(), &[slots[0], slots[1]]);
}
