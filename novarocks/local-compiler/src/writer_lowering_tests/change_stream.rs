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

//! An independently frozen Router -> two writers -> UnionAll -> finish graph.
use super::*;
use novarocks_local_program::{
    DataStreamPartitionType, ExchangeDeliveryRequirement, ProgramOutputRole,
    ProgramWriteLayoutPreference, StaticStreamRouting,
};
use novarocks_type_contract::{
    PartitionCountParameterId, PartitionHashAlgorithm, PartitionSpaceId,
};

const ROUTER: FragmentId = FragmentId::new(31);
const WRITERS: [FragmentId; 2] = [FragmentId::new(32), FragmentId::new(33)];
const FINISH: FragmentId = FragmentId::new(34);
const ROUTES: [EdgeId; 2] = [EdgeId::new(41), EdgeId::new(42)];
const RESULTS: [EdgeId; 2] = [EdgeId::new(43), EdgeId::new(44)];
const SUMMARY: EdgeId = EdgeId::new(45);

#[derive(Clone, Copy)]
enum Colocation {
    None,
    Singleton,
    ByKey,
}
fn distribution(mode: Colocation, key: ValueId) -> Distribution {
    match mode {
        Colocation::None => Distribution::RoundRobin,
        Colocation::Singleton => Distribution::Singleton,
        Colocation::ByKey => Distribution::Hash {
            keys: Box::from([key]),
            scheme: HashPartitionScheme {
                space: PartitionSpaceId::try_new([11; 32]).unwrap(),
                count: PartitionCountParameter {
                    id: PartitionCountParameterId::try_new([12; 32]).unwrap(),
                    admissible: PartitionCountDomain {
                        min: 1,
                        max: 64,
                        requires_power_of_two: false,
                    },
                },
                definition: HashDefinition {
                    algorithm: PartitionHashAlgorithm::NativeHierarchicalV1,
                },
            },
        },
    }
}
fn insert(
    builder: &mut FragmentBuilder,
    id: NodeId,
    inputs: Vec<NodeId>,
    values: Vec<ValueId>,
    kind: NodeKind,
    provided: Distribution,
    required: Vec<Distribution>,
) {
    builder
        .insert_node_unchecked(PhysicalNode {
            id,
            inputs: inputs.into_boxed_slice(),
            required_inputs: required.into_iter().map(property).collect(),
            output_properties: property(provided),
            output: OutputPort {
                node: id,
                columns: values.into_boxed_slice(),
            },
            kind,
        })
        .unwrap();
}
fn receive(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    sources: &[(ValueId, ValueType)],
    provided: impl FnOnce(&[ValueId]) -> Distribution,
) -> (NodeId, Vec<ValueId>) {
    let node = builder.reserve_node_id().unwrap();
    let imports = sources
        .iter()
        .map(|(source_value, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::ExchangeImport {
                        edge,
                        source_value: *source_value,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    insert(
        builder,
        node,
        vec![],
        imports.clone(),
        NodeKind::ExchangeSource {
            edge,
            imports: sources
                .iter()
                .zip(&imports)
                .map(|((source, _), destination)| (*source, *destination))
                .collect(),
        },
        provided(&imports),
        vec![],
    );
    (node, imports)
}
fn edge(
    id: EdgeId,
    role: OutputRole,
    from: FragmentId,
    port: FragmentOutputPort,
    source: &[ValueId],
    to: FragmentId,
    node: NodeId,
    destination: &[ValueId],
    source_distribution: Distribution,
    destination_distribution: Distribution,
) -> Edge {
    Edge {
        id,
        role,
        delivery: DeliveryRequirement::MustDrain,
        kind: if matches!(role, OutputRole::ProviderRoute { .. }) {
            EdgeKind::ChangeStreamRouter
        } else {
            EdgeKind::Stream
        },
        source: EdgeSource {
            fragment: from,
            port,
            projection: source.into(),
        },
        destination: EdgeDestination {
            fragment: to,
            node,
            receive_mapping: source
                .iter()
                .copied()
                .zip(destination.iter().copied())
                .collect(),
        },
        partitioning: EdgePartitioning {
            source: source_distribution,
            destination: destination_distribution,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    }
}

fn packages(mode: Colocation, summary_first: bool) -> BTreeMap<FragmentId, FragmentPackage> {
    let mut builder = FragmentBuilder::new(ROUTER);
    let source = builder.reserve_node_id().unwrap();
    let source_type = ty(DataType::Int64, false);
    let source_value = builder
        .add_value(
            source_type.clone(),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let source_expression = builder
        .add_expression(
            source,
            source_type.clone(),
            ExprKind::Literal(LiteralValue::Int64(17)),
        )
        .unwrap();
    insert(
        &mut builder,
        source,
        vec![],
        vec![source_value],
        NodeKind::Values {
            rows: Box::from([Box::from([source_expression])]),
        },
        Distribution::Singleton,
        vec![],
    );
    let root = builder.reserve_node_id().unwrap();
    let effect = builder
        .add_value(
            ty(DataType::Int8, false),
            ValueOrigin::NodeOutput {
                node: root,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let data = builder
        .add_value(
            source_type.clone(),
            ValueOrigin::NodeOutput {
                node: root,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let expression = builder
        .add_expression(root, source_type, ExprKind::Value(source_value))
        .unwrap();
    insert(
        &mut builder,
        root,
        vec![source],
        vec![effect, data],
        NodeKind::ChangeEventExpand {
            effect_output: effect,
            events: [
                ConnectorRowMutationEffect::Insert,
                ConnectorRowMutationEffect::Replace,
                ConnectorRowMutationEffect::Delete,
            ]
            .map(|effect| ChangeEventSpec {
                predicate: None,
                effect,
                assignments: Box::from([(data, Some(expression))]),
            })
            .into(),
        },
        Distribution::Unconstrained,
        vec![Distribution::Unconstrained],
    );
    let mut counter = |field| {
        builder
            .add_value(
                ty(DataType::Int64, false),
                ValueOrigin::LogicalSummary { field },
            )
            .unwrap()
    };
    let summary = LogicalSummaryOutput {
        edge: SUMMARY,
        columns: LogicalSummaryColumns {
            inserted: counter(LogicalChangeCountField::Inserted),
            replaced: counter(LogicalChangeCountField::Replaced),
            deleted: counter(LogicalChangeCountField::Deleted),
        },
    };
    let drafts = [
        draft_with_types([ty(DataType::Int64, false), ty(DataType::Int64, true)]),
        draft_with_types([ty(DataType::Int64, false), ty(DataType::Int64, true)]),
    ];
    let ordinals = [
        WriteTargetOrdinal::try_new(0).unwrap(),
        WriteTargetOrdinal::try_new(1).unwrap(),
    ];
    let routes = ordinals
        .iter()
        .enumerate()
        .map(|(index, ordinal)| ChangeStreamRoute {
            route_id: ConnectorWriteRouteId::from_bytes([index as u8 + 1; 32]),
            write_target_ordinal: *ordinal,
            accepted_effects: if index == 0 {
                Box::from([
                    ConnectorRowMutationEffect::Insert,
                    ConnectorRowMutationEffect::Replace,
                ])
            } else {
                Box::from([
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Delete,
                ])
            },
            // Two distinct provider targets intentionally read one root value.
            input_mapping: drafts[index]
                .input()
                .fields_iter()
                .map(|binding| (binding.token(), data))
                .collect(),
            requirements: WriteRouteRequirements {
                colocation: match mode {
                    Colocation::None => WriteColocation::None,
                    Colocation::Singleton => WriteColocation::Singleton,
                    Colocation::ByKey => WriteColocation::ByKey(Box::from([data])),
                },
                layout: WriteLayoutPreference::ClusterBy(Box::from([data])),
            },
            edge: ROUTES[index],
        })
        .collect();
    let router = builder
        .finish_definition(
            root,
            FragmentSink::Router {
                effect,
                routes,
                summary,
            },
            dop_domain(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([31; 16]).unwrap());
    plan.add_fragment(router).unwrap();
    let mut writer_fields = Vec::new();
    let mut edges = Vec::new();
    for index in 0..2 {
        let mut builder = FragmentBuilder::new(WRITERS[index]);
        // Equal source values still mint independent destination occurrences.
        let (receiver, inputs) = receive(
            &mut builder,
            ROUTES[index],
            &[
                (data, ty(DataType::Int64, false)),
                (data, ty(DataType::Int64, false)),
            ],
            |inputs| distribution(mode, inputs[0]),
        );
        let receiver_distribution = distribution(mode, inputs[0]);
        let writer = builder.reserve_node_id().unwrap();
        let fields = relation_fields(&mut builder, writer, false);
        insert(
            &mut builder,
            writer,
            vec![receiver],
            fields.iter().map(|field| field.value).collect(),
            NodeKind::TableWriter {
                target: WriterTarget {
                    handle: drafts[index].payload().clone(),
                    write_target_ordinal: ordinals[index],
                    input: inputs.clone().into(),
                    required_distribution: receiver_distribution.clone(),
                    target_fields: drafts[index]
                        .input()
                        .fields_iter()
                        .zip(&inputs)
                        .map(|(binding, value)| WriterTargetField {
                            provider_name: binding.field().name().clone().into(),
                            token: binding.token(),
                            input: *value,
                            ty: binding.execution_type().unwrap().clone(),
                            hidden: false,
                        })
                        .collect(),
                    output_schema: WriterRelationSchema {
                        revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: fields.clone(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
            Distribution::Unconstrained,
            vec![receiver_distribution.clone()],
        );
        let fragment = builder
            .finish_definition(
                writer,
                FragmentSink::Stream {
                    edge: RESULTS[index],
                },
                dop_domain(),
                if matches!(mode, Colocation::Singleton) {
                    StagePlacementPolicy::Singleton
                } else {
                    StagePlacementPolicy::Spread {
                        max_instances: None,
                    }
                },
            )
            .unwrap();
        plan.add_fragment(fragment).unwrap();
        edges.push(edge(
            ROUTES[index],
            OutputRole::ProviderRoute {
                target: ordinals[index],
            },
            ROUTER,
            FragmentOutputPort::Root,
            &[data, data],
            WRITERS[index],
            receiver,
            &inputs,
            distribution(mode, data),
            receiver_distribution,
        ));
        writer_fields.push(fields);
    }
    let mut builder = FragmentBuilder::new(FINISH);
    let mut receivers = Vec::new();
    let mut received_fields = Vec::new();
    for index in 0..2 {
        let sources = writer_fields[index]
            .iter()
            .map(|field| (field.value, field.ty.clone()))
            .collect::<Vec<_>>();
        let (receiver, inputs) = receive(&mut builder, RESULTS[index], &sources, |_| {
            Distribution::Singleton
        });
        let fields = writer_fields[index]
            .iter()
            .zip(&inputs)
            .map(|(field, value)| WriterRelationField {
                value: *value,
                ..field.clone()
            })
            .collect::<Box<[_]>>();
        edges.push(edge(
            RESULTS[index],
            OutputRole::WriterResults,
            WRITERS[index],
            FragmentOutputPort::Root,
            &sources.iter().map(|(value, _)| *value).collect::<Vec<_>>(),
            FINISH,
            receiver,
            &inputs,
            Distribution::Singleton,
            Distribution::Singleton,
        ));
        receivers.push(receiver);
        received_fields.push(fields);
    }
    let union = builder.reserve_node_id().unwrap();
    let multiplex = received_fields[0]
        .iter()
        .enumerate()
        .map(|(ordinal, field)| WriterRelationField {
            value: builder
                .add_value(
                    field.ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: union,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
            ..field.clone()
        })
        .collect::<Box<[_]>>();
    insert(
        &mut builder,
        union,
        receivers,
        multiplex.iter().map(|field| field.value).collect(),
        NodeKind::SetOp {
            kind: SetOperationKind::UnionAll,
            input_mappings: received_fields
                .iter()
                .map(|fields| fields.iter().map(|field| field.value).collect())
                .collect(),
        },
        Distribution::Singleton,
        vec![Distribution::Singleton, Distribution::Singleton],
    );
    let summary_sources = summary
        .columns
        .values()
        .map(|value| (value, ty(DataType::Int64, false)));
    let (summary_receiver, summary_values) =
        receive(&mut builder, SUMMARY, &summary_sources, |_| {
            Distribution::Singleton
        });
    edges.push(edge(
        SUMMARY,
        OutputRole::LogicalSummary,
        ROUTER,
        FragmentOutputPort::LogicalSummary,
        &summary.columns.values(),
        FINISH,
        summary_receiver,
        &summary_values,
        Distribution::Singleton,
        Distribution::Singleton,
    ));
    let finish = builder.reserve_node_id().unwrap();
    let output = relation_fields(&mut builder, finish, true);
    let (inputs, roles) = if summary_first {
        (
            vec![summary_receiver, union],
            FinishInputs {
                writer_results: 1,
                logical_summary: Some(0),
            },
        )
    } else {
        (
            vec![union, summary_receiver],
            FinishInputs {
                writer_results: 0,
                logical_summary: Some(1),
            },
        )
    };
    insert(
        &mut builder,
        finish,
        inputs,
        output.iter().map(|field| field.value).collect(),
        NodeKind::TableFinish(WriterFinishSpec {
            count_mode: FinishCountMode::LogicalChanges,
            inputs: roles,
            logical_summary: Some(LogicalSummaryColumns {
                inserted: summary_values[0],
                replaced: summary_values[1],
                deleted: summary_values[2],
            }),
            expected_target_ordinals: Box::from(ordinals),
            input_schema: WriterRelationSchema {
                revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                fields: multiplex,
            },
            output_schema: WriterRelationSchema {
                revision: ROOT_WRITE_RESULT_SCHEMA_REVISION,
                fields: output.clone(),
            },
            final_aggregates: Box::default(),
            grouped_unpivot: None,
        }),
        Distribution::Singleton,
        vec![Distribution::Singleton, Distribution::Singleton],
    );
    plan.add_fragment(
        builder
            .finish_definition(
                finish,
                FragmentSink::Result,
                dop_domain(),
                StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    )
    .unwrap();
    for edge in edges {
        plan.add_edge(edge).unwrap();
    }
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: FINISH,
        output: OutputPort {
            node: finish,
            columns: output.iter().map(|field| field.value).collect(),
        },
        fields: output
            .iter()
            .map(|field| ResultField {
                domain: ResultValueDomain::Plain,
                name: field.name.clone(),
                alias: None,
                value: field.value,
                ty: field.ty.clone(),
            })
            .collect(),
    })
    .unwrap();
    let plan = plan.finish().unwrap();
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let roots = root_uses(fragment);
        calls.insert(
            id,
            FrozenFragmentCalls::try_new(fragment, &roots, vec![], &FixtureControl).unwrap(),
        );
        uses.insert(id, roots);
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
        );
        admissions.insert(id, package_admission());
    }
    extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::from([
            (ordinals[0], drafts[0].clone()),
            (ordinals[1], drafts[1].clone()),
        ]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap()
}

#[test]
fn router_has_independent_summary_and_occurrence_preserving_provider_branches() {
    for mode in [Colocation::None, Colocation::Singleton, Colocation::ByKey] {
        let mut packages = packages(mode, false);
        let program = compile(&Arc::new(packages.remove(&ROUTER).unwrap()), 4, None).unwrap();
        let graph = program.graph();
        let root = &graph.nodes()[graph.root().index()];
        let ProgramNodeKind::Project { input, exprs, .. } = root.kind() else {
            panic!("provider occurrences have an actual subordinate Project")
        };
        let input_slots = graph.nodes()[input.index()].output_layout().slots();
        let projection_slots = root.output_layout().slots();
        assert_eq!(projection_slots.len(), input_slots.len() + 4);
        assert!(
            projection_slots
                .iter()
                .all(|slot| !input_slots.contains(slot)),
            "every subordinate prefix and provider alias has a fresh identity"
        );
        for (ordinal, expected) in input_slots.iter().enumerate() {
            assert!(
                matches!(graph.expressions().node(exprs[ordinal]).unwrap().kind(),
                StaticExprKind::SlotId(actual) if actual == expected)
            );
        }
        let Some(StaticSinkProgram::ChangeStreamRouter {
            branches,
            routes,
            summary_branch,
            summary_layout,
            effect_slot,
            arena,
        }) = graph.sink()
        else {
            panic!("router")
        };
        assert_eq!(branches.len(), 3);
        assert_eq!(routes.len(), 2);
        assert_eq!(*summary_branch, 2);
        assert_eq!(summary_layout.schema().fields().len(), 3);
        assert!(
            summary_layout
                .slots()
                .iter()
                .all(|slot| !root.output_layout().slots().contains(slot))
        );
        assert!(
            summary_layout
                .schema()
                .fields()
                .iter()
                .all(|field| field.data_type() == &DataType::Int64 && !field.is_nullable())
        );
        for (index, route) in routes.iter().enumerate() {
            let branch = &branches[route.branch as usize];
            let columns = branch.output_columns();
            assert_eq!(columns.len(), 2);
            assert_ne!(columns[0], columns[1]);
            assert!(!columns.contains(effect_slot));
            assert_eq!(
                branch.contract().role,
                ProgramOutputRole::ProviderRoute {
                    target: WriteTargetOrdinal::try_new(index as u32).unwrap()
                }
            );
            assert_eq!(
                branch.contract().delivery,
                ExchangeDeliveryRequirement::MustDrain
            );
            let ProgramWriteLayoutPreference::ClusterBy(keys) = &route.layout else {
                panic!("layout remains an explicit hint")
            };
            assert_eq!(keys.as_ref(), &columns[..1]);
            let mut read_slots = Vec::new();
            for slot in columns {
                let ordinal = root
                    .output_layout()
                    .slots()
                    .iter()
                    .position(|candidate| candidate == slot)
                    .unwrap();
                let StaticExprKind::SlotId(source) =
                    graph.expressions().node(exprs[ordinal]).unwrap().kind()
                else {
                    panic!("source read")
                };
                read_slots.push(*source);
            }
            assert_eq!(
                read_slots,
                vec![graph.nodes()[input.index()].output_layout().slots()[1]; 2]
            );
            match mode {
                Colocation::None => {
                    assert_eq!(branch.partition_type(), DataStreamPartitionType::Random);
                    assert!(branch.partition_exprs().is_empty());
                }
                Colocation::Singleton => {
                    assert_eq!(
                        branch.partition_type(),
                        DataStreamPartitionType::Unpartitioned
                    );
                    assert!(branch.partition_exprs().is_empty());
                }
                Colocation::ByKey => {
                    assert_eq!(
                        branch.contract().routing,
                        StaticStreamRouting::HierarchicalHashV1
                    );
                    assert_eq!(branch.partition_exprs().len(), 1);
                    assert!(
                        matches!(arena.node(branch.partition_exprs()[0]).unwrap().kind(), StaticExprKind::SlotId(slot) if *slot == root.output_layout().slots()[1])
                    );
                }
            }
        }
        assert_eq!(
            graph
                .requirements()
                .entries()
                .iter()
                .filter(|entry| matches!(
                    entry,
                    BindingRequirement::ExchangeOutput {
                        delivery: ExchangeDeliveryRequirement::MustDrain,
                        ..
                    }
                ))
                .count(),
            3
        );
        for fragment in WRITERS {
            let writer = compile(
                &Arc::new(packages.remove(&fragment).unwrap()),
                if matches!(mode, Colocation::Singleton) {
                    1
                } else {
                    4
                },
                None,
            )
            .unwrap();
            let (node, facts) = writer.exchange_inputs().iter().next().unwrap();
            assert!(matches!(
                facts.role,
                ProgramOutputRole::ProviderRoute { .. }
            ));
            assert_eq!(facts.delivery, ExchangeDeliveryRequirement::MustDrain);
            let receiver = writer.graph().nodes()[node.index()].output_layout();
            assert_eq!(receiver.schema().field(0).name(), "c1");
            assert_eq!(receiver.schema().field(1).name(), "c2");
            assert_ne!(receiver.slots()[0], receiver.slots()[1]);
        }
    }
}

#[test]
fn logical_finish_preserves_both_input_orders_and_all_writer_targets() {
    for summary_first in [false, true] {
        let mut packages = packages(Colocation::None, summary_first);
        let program = compile(&Arc::new(packages.remove(&FINISH).unwrap()), 1, Some(1)).unwrap();
        let graph = program.graph();
        let ProgramNodeKind::TableFinish {
            count_mode,
            input_roles,
            inputs,
            expected_targets,
            logical_summary_layout,
            writer_multiplex_layout,
            ..
        } = graph.nodes()[graph.root().index()].kind()
        else {
            panic!("finish")
        };
        assert_eq!(
            *count_mode,
            novarocks_local_program::FinishCountMode::LogicalChanges
        );
        assert_eq!(expected_targets.len(), 2);
        assert_eq!(inputs.len(), 2);
        let summary_index = input_roles.logical_summary.unwrap() as usize;
        let writer_index = input_roles.writer_results as usize;
        assert_eq!(summary_index, if summary_first { 0 } else { 1 });
        assert_ne!(summary_index, writer_index);
        let summary = &graph.nodes()[inputs[summary_index].index()];
        assert!(matches!(
            summary.kind(),
            ProgramNodeKind::ExchangeSource { .. }
        ));
        let facts = &program.exchange_inputs()[&inputs[summary_index]];
        assert_eq!(facts.role, ProgramOutputRole::LogicalSummary);
        assert_eq!(facts.delivery, ExchangeDeliveryRequirement::MustDrain);
        assert_eq!(
            logical_summary_layout.as_ref().unwrap().slots(),
            summary.output_layout().slots()
        );
        let multiplex = &graph.nodes()[inputs[writer_index].index()];
        assert!(matches!(multiplex.kind(), ProgramNodeKind::UnionAll { .. }));
        assert_eq!(names(multiplex.output_layout()), WRITER_RELATION);
        assert_eq!(names(writer_multiplex_layout), WRITER_RELATION);
        assert_eq!(
            writer_multiplex_layout.slots(),
            multiplex.output_layout().slots()
        );
        assert_eq!(
            program
                .exchange_inputs()
                .values()
                .filter(|fact| fact.role == ProgramOutputRole::WriterResults)
                .count(),
            2
        );
    }
}

#[test]
fn incomplete_router_cuts_and_role_or_hierarchy_drift_are_rejected_before_compilation() {
    let complete = packages(Colocation::ByKey, false).remove(&ROUTER).unwrap();
    compile(&Arc::new(complete.clone()), 4, None)
        .expect("the complete graph compiles before fault injection");
    for fault in 0..4 {
        let mut input = complete.clone().into_input();
        let cuts = &mut input.cuts.outbound;
        match fault {
            0 => {
                let summary = cuts.iter_mut().find(|cut| cut.edge == SUMMARY).unwrap();
                summary.delivery = DeliveryRequirement::DemandDriven;
            }
            1 => {
                let route = cuts.iter_mut().find(|cut| cut.edge == ROUTES[0]).unwrap();
                let Distribution::Hash { scheme, .. } = &mut route.partitioning.source else {
                    panic!("hash")
                };
                scheme.definition.algorithm = PartitionHashAlgorithm::NativeExchangeV1;
            }
            2 => {
                let summary = cuts.iter_mut().find(|cut| cut.edge == SUMMARY).unwrap();
                summary.payload = ExchangePayloadContract::Relational;
            }
            3 => {
                input.cuts.outbound = cuts
                    .iter()
                    .filter(|cut| cut.edge != ROUTES[1])
                    .cloned()
                    .collect();
            }
            _ => unreachable!(),
        }
        assert!(
            FragmentPackage::try_new(input, package_admission(), &FixtureControl).is_err(),
            "fault {fault} must not obtain a valid package"
        );
    }
}

#[test]
fn router_and_logical_finish_keep_every_original_control_refusal_prefix() {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    struct Trace {
        refuse_at: Option<usize>,
        cause: CompileControlError,
        refused: AtomicBool,
        events: Mutex<Vec<(CompilePhase, u32)>>,
    }
    impl PureCompileControl for Trace {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(
                !self.refused.load(Ordering::SeqCst),
                "no callback after the original refusal"
            );
            let mut events = self.events.lock().unwrap();
            let index = events.len();
            events.push((phase, units));
            if self.refuse_at == Some(index) {
                self.refused.store(true, Ordering::SeqCst);
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
    }
    let packages = packages(Colocation::ByKey, true);
    let functions = functions();
    for fragment in [ROUTER, FINISH] {
        let source = Arc::new(packages[&fragment].clone());
        let providers = providers();
        let invoke = |control: &dyn PureCompileControl| {
            let checked =
                validate_fragment_providers(source.clone(), &providers, &FixtureControl).unwrap();
            compile_fragment(
                checked,
                &functions,
                options(
                    if fragment == ROUTER { 4 } else { 1 },
                    (fragment == FINISH).then_some(1),
                ),
                control,
            )
        };
        let baseline = Trace {
            refuse_at: None,
            cause: CompileControlError::Cancelled,
            refused: AtomicBool::new(false),
            events: Mutex::new(vec![]),
        };
        invoke(&baseline).expect("the independently frozen graph compiles before control faults");
        let expected = baseline.events.into_inner().unwrap();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for index in 0..expected.len() {
                let trace = Trace {
                    refuse_at: Some(index),
                    cause: cause.clone(),
                    refused: AtomicBool::new(false),
                    events: Mutex::new(vec![]),
                };
                assert!(
                    matches!(invoke(&trace), Err(FragmentCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*trace.events.lock().unwrap(), expected[..=index]);
            }
        }
    }
}
