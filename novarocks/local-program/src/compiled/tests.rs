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

//! Final-owner correspondence tests, not complete compiler/native acceptance.
use super::*;
use crate::*;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_connector_contract::*;
use novarocks_functions::FunctionValueType;
use novarocks_type_contract::CompilePhase;
use novarocks_types::SlotId;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        trace.push(units);
        match self.stop.clone() {
            Some((at, cause)) if at == index => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Clone, Debug)]
pub(super) struct Fixture {
    pub(super) graph: LocalProgramGraph,
    pub(super) units: Vec<Arc<novarocks_expression::CompiledExprSet>>,
    pub(super) calls: Vec<(ProgramCallSite, novarocks_functions::PureCallSpecialization)>,
    pub(super) channels: Vec<(ProgramChannelSite, FunctionValueType)>,
}
impl std::ops::Deref for Fixture {
    type Target = LocalProgramGraph;
    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}
pub(super) fn finish(
    fixture: Fixture,
    operators: Vec<LocalOperatorProvenance>,
    allowed: &BTreeSet<DiagnosticSourceNodeId>,
    facts: CompiledProgramFacts,
    control: &dyn PureCompileControl,
) -> Result<LocalProgram, LocalProgramCompileError> {
    LocalProgram::try_new(
        fixture.graph,
        fixture.units,
        fixture.calls,
        fixture.channels,
        operators,
        allowed,
        facts,
        control,
    )
}
fn checked(writer: bool, sink: bool, legacy: bool) -> Fixture {
    checked_with(writer, sink, legacy, false)
}
/// The same graph whose one value channel is nullable when `nullable` holds.
fn checked_with(writer: bool, sink: bool, legacy: bool, nullable: bool) -> Fixture {
    let control = Control::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Int64,
        nullable,
    )]));
    let value_type = FunctionValueType::new(DataType::Int64, nullable);
    let layout = StaticLayout::try_new_typed_for_compile(
        schema.clone(),
        Arc::from([SlotId::new(1)]),
        vec![(value_type.typed_binding().clone(), None)],
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &control,
    )
    .unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
        layout.clone(),
    )
    .unwrap();
    let node = |index, kind| {
        if legacy {
            ProgramNode::new(index as i32, kind, layout.clone())
        } else {
            ProgramNode::new_local(
                ProgramNodeId::new(index),
                vec![DiagnosticSourceNodeId::new(u32::MAX)],
                kind,
                layout.clone(),
            )
        }
    };
    let mut nodes = vec![node(0, ProgramNodeKind::Values { values })];
    let mut units = Vec::new();
    if writer {
        units.push(crate::test_support::constant_unit(
            &[crate::test_support::integer_typed(7, value_type.clone())],
            &[(0, novarocks_type_contract::EvaluationDemand::Value)],
        ));
        nodes.push(node(
            1,
            ProgramNodeKind::TableWriter {
                input: ProgramNodeId::new(0),
                target: WriteTargetOrdinal::try_new(0).unwrap(),
                expected_layout: layout.clone(),
                projection: StaticWriterProjection {
                    storage_types: vec![None],
                    expressions: vec![crate::test_support::root(0, 0)],
                    layout: layout.clone(),
                },
                writer_multiplex_layout: layout.clone(),
                partial_aggregates: vec![],
                partial_preparation: None,
            },
        ));
    }
    let root = ProgramNodeId::new(nodes.len() - 1);
    let mut entries = if writer {
        vec![BindingRequirement::TableWriter {
            node: root,
            layout: layout.clone(),
        }]
    } else {
        vec![]
    };
    if sink {
        entries.push(BindingRequirement::ResultSink {
            layout: layout.clone(),
        });
    }
    let requirements = BindingRequirements::try_new(entries).unwrap();
    let graph = LocalProgramGraph::try_new_with_sink(
        nodes,
        root,
        &units,
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        requirements,
        sink.then_some(StaticSinkProgram::Result),
    )
    .unwrap();
    let mut channels = vec![(
        ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0,
        },
        FunctionValueType::new(DataType::Int64, nullable),
    )];
    if writer {
        for role in [
            ProgramChannelLayoutRole::NodeOutput,
            ProgramChannelLayoutRole::WriterProjection,
            ProgramChannelLayoutRole::WriterMultiplex,
        ] {
            channels.push((
                ProgramChannelSite::Layout {
                    node: root,
                    role,
                    ordinal: 0,
                },
                FunctionValueType::new(DataType::Int64, nullable),
            ));
        }
    }
    Fixture {
        graph,
        units,
        calls: vec![],
        channels,
    }
}
fn operators(writer: bool, source: u32) -> Vec<LocalOperatorProvenance> {
    (0..if writer { 2 } else { 1 })
        .map(|index| LocalOperatorProvenance {
            id: LocalOperatorId::new(index),
            lowered_nodes: Box::from([ProgramNodeId::new(index as usize)]),
            sources: Box::from([DiagnosticSourceNodeId::new(source)]),
            origin: if index == 0 {
                LocalOperatorOrigin::Direct
            } else {
                LocalOperatorOrigin::Split { piece: 1 }
            },
            cost_owner: LocalOperatorId::new(index),
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        })
        .collect()
}
fn allowed() -> BTreeSet<DiagnosticSourceNodeId> {
    BTreeSet::from([
        DiagnosticSourceNodeId::new(u32::MAX),
        DiagnosticSourceNodeId::new(0),
    ])
}
struct IdentityProvider;
impl ConnectorWriteRecipeCompiler for IdentityProvider {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        _: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<ConnectorError>> {
        Ok(draft.clone())
    }
}
fn recipe(field: Field) -> ConnectorWriteRecipe {
    // A pure contract fixture, not the installed Iceberg private compiler.
    let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let binding = ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        bytes::Bytes::from_static(b"fixture"),
    );
    let draft = ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        ConnectorWriteInputShape::Data {
            fields: vec![{
                // These fixtures declare Plain signed integers explicitly.
                assert!(matches!(
                    field.data_type(),
                    DataType::Int32 | DataType::Int64
                ));
                let value_type =
                    FunctionValueType::new(field.data_type().clone(), field.is_nullable());
                ConnectorWriteFieldBinding::try_new_typed(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    field,
                    value_type,
                )
                .unwrap()
            }],
        },
    )
    .unwrap();
    ConnectorWriteRecipe::try_compile_with_provider(&draft, &IdentityProvider, &Control::default())
        .unwrap()
}
#[test]
fn final_owner_has_one_complete_chain_and_sparse_actual_graph_origins() {
    let checked = checked(false, true, false);
    let graph_ptr = checked.graph.nodes().as_ptr();
    let program = finish(
        checked,
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts {
            writes: BTreeMap::new(),
            exchange_inputs: BTreeMap::new(),
            scan_inputs: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        },
        &Control::default(),
    )
    .unwrap();
    assert_eq!(program.graph().nodes().as_ptr(), graph_ptr);
    assert!(program.graph().nodes()[0].legacy_native_node_id().is_none());
    assert_eq!(
        program.graph().nodes()[0].local_id(),
        Some(ProgramNodeId::new(0))
    );
    assert_eq!(
        program
            .provenance()
            .get(LocalOperatorId::new(0))
            .unwrap()
            .sources
            .as_ref(),
        &[DiagnosticSourceNodeId::new(u32::MAX)]
    );
    assert!(program.write_recipes().is_empty());
}
#[test]
fn final_owner_rejects_foreign_origins_legacy_identity_and_missing_sink() {
    for (legacy, sink, source, expected) in [
        (
            false,
            true,
            0,
            LocalProgramCompileError::Origins(CompiledOriginsError::SourceMismatch),
        ),
        (
            true,
            true,
            u32::MAX,
            LocalProgramCompileError::Origins(CompiledOriginsError::InvalidLocalIdentity),
        ),
        (
            false,
            false,
            u32::MAX,
            LocalProgramCompileError::MissingSink,
        ),
    ] {
        assert_eq!(
            finish(
                checked(false, sink, legacy),
                operators(false, source),
                &allowed(),
                CompiledProgramFacts {
                    writes: BTreeMap::new(),
                    exchange_inputs: BTreeMap::new(),
                    scan_inputs: BTreeMap::new(),
                    aggregates: BTreeMap::new(),
                },
                &Control::default()
            )
            .unwrap_err(),
            expected
        );
    }
}
#[test]
fn actual_writer_recipe_coverage_and_exact_projection_fields_are_mandatory() {
    let id = ProgramNodeId::new(1);
    assert_eq!(
        finish(
            checked(true, true, false),
            operators(true, u32::MAX),
            &allowed(),
            CompiledProgramFacts {
                writes: BTreeMap::new(),
                exchange_inputs: BTreeMap::new(),
                scan_inputs: BTreeMap::new(),
                aggregates: BTreeMap::new(),
            },
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::Provider(ProviderLinkError::MissingWriterRecipe(id))
    );
    let exact = recipe(Field::new("v", DataType::Int64, false));
    let program = finish(
        checked(true, true, false),
        operators(true, u32::MAX),
        &allowed(),
        CompiledProgramFacts {
            writes: BTreeMap::from([(id, exact.clone())]),
            exchange_inputs: BTreeMap::new(),
            scan_inputs: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        },
        &Control::default(),
    )
    .unwrap();
    assert_eq!(program.write_recipes()[&id], exact);
    assert_eq!(
        finish(
            checked(false, true, false),
            operators(false, u32::MAX),
            &allowed(),
            CompiledProgramFacts {
                writes: BTreeMap::from([(ProgramNodeId::new(0), exact)]),
                exchange_inputs: BTreeMap::new(),
                scan_inputs: BTreeMap::new(),
                aggregates: BTreeMap::new(),
            },
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::Provider(ProviderLinkError::UnexpectedWriterRecipe(
            ProgramNodeId::new(0)
        ))
    );
    let renamed = recipe(Field::new("foreign", DataType::Int64, false));
    assert_eq!(
        finish(
            checked(true, true, false),
            operators(true, u32::MAX),
            &allowed(),
            CompiledProgramFacts {
                writes: BTreeMap::from([(id, renamed)]),
                exchange_inputs: BTreeMap::new(),
                scan_inputs: BTreeMap::new(),
                aggregates: BTreeMap::new(),
            },
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::Provider(ProviderLinkError::FieldMismatch {
            node: id,
            ordinal: 0
        })
    );
}
#[test]
fn writer_projection_admits_either_nullability_direction_but_nothing_else() {
    let id = ProgramNodeId::new(1);
    let facts = |field| CompiledProgramFacts {
        writes: BTreeMap::from([(id, recipe(field))]),
        exchange_inputs: BTreeMap::new(),
        scan_inputs: BTreeMap::new(),
        aggregates: BTreeMap::new(),
    };
    // A nullable value feeding a NOT NULL target field is the writer's row
    // obligation, and a non-null value feeding a nullable one needs none.
    for (nullable_value, nullable_target) in [(true, false), (false, true), (true, true)] {
        let program = finish(
            checked_with(true, true, false, nullable_value),
            operators(true, u32::MAX),
            &allowed(),
            facts(Field::new("v", DataType::Int64, nullable_target)),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            program.write_recipes()[&id]
                .draft()
                .input()
                .fields_iter()
                .next()
                .unwrap()
                .field()
                .is_nullable(),
            nullable_target
        );
    }
    // Only top-level nullability is relaxed: a name or carrier still differs.
    for foreign in [
        Field::new("foreign", DataType::Int64, false),
        Field::new("v", DataType::Int32, false),
    ] {
        let error = finish(
            checked_with(true, true, false, true),
            operators(true, u32::MAX),
            &allowed(),
            facts(foreign),
            &Control::default(),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                LocalProgramCompileError::Provider(
                    ProviderLinkError::FieldMismatch { .. } | ProviderLinkError::TypeMismatch(_)
                )
            ),
            "{error:?}"
        );
    }
}
#[test]
fn every_final_author_callback_propagates_original_control_without_rechecking() {
    let baseline = Control::default();
    finish(
        checked(true, true, false),
        operators(true, u32::MAX),
        &allowed(),
        CompiledProgramFacts {
            writes: BTreeMap::from([(
                ProgramNodeId::new(1),
                recipe(Field::new("v", DataType::Int64, false)),
            )]),
            exchange_inputs: BTreeMap::new(),
            scan_inputs: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        },
        &baseline,
    )
    .unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.iter().any(|units| *units > 0));
    for index in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                stop: Some((index, cause.clone())),
                ..Control::default()
            };
            let error = finish(
                checked(true, true, false),
                operators(true, u32::MAX),
                &allowed(),
                CompiledProgramFacts {
                    writes: BTreeMap::from([(
                        ProgramNodeId::new(1),
                        recipe(Field::new("v", DataType::Int64, false)),
                    )]),
                    exchange_inputs: BTreeMap::new(),
                    scan_inputs: BTreeMap::new(),
                    aggregates: BTreeMap::new(),
                },
                &control,
            )
            .unwrap_err();
            assert_eq!(error, LocalProgramCompileError::Control(cause.clone()));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
            assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
    }
}
#[test]
fn exchange_input_addresses_cover_exactly_the_actual_exchange_sources() {
    let address = CompiledExchangeInput {
        wire_layout: exchange_test_layout(
            &[FunctionValueType::new(DataType::Int64, false)],
            &[1],
            "wire",
        ),
        routing_layout: None,
        role: crate::ProgramOutputRole::Relational,
        delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
        routing: crate::CompiledExchangeRouting::Unpartitioned,
        receiver_node: 3,
        edge: 5,
        source_fragment: 1,
        hash_partition_slots: Box::default(),
    };
    // A non-exchange node and a node outside the graph both refuse an address.
    for node in [ProgramNodeId::new(0), ProgramNodeId::new(9)] {
        assert_eq!(
            finish(
                checked(false, true, false),
                operators(false, u32::MAX),
                &allowed(),
                CompiledProgramFacts {
                    writes: BTreeMap::new(),
                    exchange_inputs: BTreeMap::from([(node, address.clone())]),
                    scan_inputs: BTreeMap::new(),
                    aggregates: BTreeMap::new(),
                },
                &Control::default()
            )
            .unwrap_err(),
            LocalProgramCompileError::ExchangeInputMismatch(node)
        );
    }
    let program = finish(
        checked(false, true, false),
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts {
            writes: BTreeMap::new(),
            exchange_inputs: BTreeMap::new(),
            scan_inputs: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        },
        &Control::default(),
    )
    .unwrap();
    assert!(program.exchange_inputs().is_empty());
}
#[test]
fn scan_input_address_on_a_non_scan_node_is_refused() {
    // The Values root is neither a scan nor a declared scan requirement.
    assert_eq!(
        finish(
            checked(false, true, false),
            operators(false, u32::MAX),
            &allowed(),
            CompiledProgramFacts {
                writes: BTreeMap::new(),
                exchange_inputs: BTreeMap::new(),
                scan_inputs: BTreeMap::from([(
                    ProgramNodeId::new(0),
                    CompiledScanInput { scan_node: 4 }
                )]),
                aggregates: BTreeMap::new(),
            },
            &Control::default()
        )
        .unwrap_err(),
        LocalProgramCompileError::ScanInputMismatch(ProgramNodeId::new(0))
    );
}
#[test]
fn aggregate_facts_cover_exactly_the_actual_aggregate_nodes() {
    // The Values root is not an Aggregate, and a node outside the graph is
    // not one either: neither may carry a grouping guarantee.
    for node in [ProgramNodeId::new(0), ProgramNodeId::new(9)] {
        assert_eq!(
            finish(
                checked(false, true, false),
                operators(false, u32::MAX),
                &allowed(),
                CompiledProgramFacts {
                    aggregates: BTreeMap::from([(
                        node,
                        CompiledAggregate {
                            grouping: CompiledAggregateGrouping::Complete,
                        },
                    )]),
                    ..CompiledProgramFacts::default()
                },
                &Control::default(),
            )
            .unwrap_err(),
            LocalProgramCompileError::AggregateMismatch(node)
        );
    }
    let program = finish(
        checked(false, true, false),
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .unwrap();
    assert!(program.aggregates().is_empty());
}

fn exchange_test_layout(types: &[FunctionValueType], slots: &[u32], name: &str) -> StaticLayout {
    let fields = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            Field::new(
                format!("{name}_{ordinal}"),
                ty.data_type().clone(),
                ty.nullable(),
            )
        })
        .collect::<Vec<_>>();
    StaticLayout::try_new_typed_for_compile(
        Arc::new(Schema::new(fields)),
        slots
            .iter()
            .copied()
            .map(SlotId::new)
            .collect::<Vec<_>>()
            .into(),
        types
            .iter()
            .map(|ty| (ty.typed_binding().clone(), None))
            .collect(),
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control::default(),
    )
    .unwrap()
}

#[test]
fn exchange_wire_preserves_ordinal_binding_and_allows_only_root_nullability_widening() {
    let wire_types = [
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int32, false),
    ];
    let local_types = [
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int32, true),
    ];
    let wire = exchange_test_layout(&wire_types, &[2, 1], "source");
    let local = exchange_test_layout(&local_types, &[2, 1], "destination");
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    validate_exchange_wire_layout(ProgramNodeId::new(7), &wire, &local, &mut work).unwrap();
    work.finish().unwrap();

    let reversed_types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int64, true),
    ];
    for (source, destination) in [
        (local.clone(), wire.clone()),
        (
            wire.clone(),
            exchange_test_layout(&local_types[..1], &[2], "missing"),
        ),
        (
            wire.clone(),
            exchange_test_layout(&local_types, &[1, 2], "wrong_slots"),
        ),
        (
            wire,
            exchange_test_layout(&reversed_types, &[2, 1], "wrong_order"),
        ),
    ] {
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        assert_eq!(
            validate_exchange_wire_layout(ProgramNodeId::new(7), &source, &destination, &mut work),
            Err(LocalProgramCompileError::ExchangeInputMismatch(
                ProgramNodeId::new(7)
            ))
        );
    }
}

#[test]
fn exchange_wire_rejects_nested_nullability_nominal_and_width_changes() {
    let nested = |child_nullable| {
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, child_nullable))),
            false,
        )
    };
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    for (source, destination) in [
        (nested(false), nested(true)),
        (json, FunctionValueType::new(DataType::LargeBinary, true)),
        (
            FunctionValueType::new(DataType::Int32, false),
            FunctionValueType::new(DataType::Int64, true),
        ),
    ] {
        let wire = exchange_test_layout(&[source], &[5], "wire");
        let local = exchange_test_layout(&[destination], &[5], "local");
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        assert_eq!(
            validate_exchange_wire_layout(ProgramNodeId::new(7), &wire, &local, &mut work),
            Err(LocalProgramCompileError::ExchangeInputMismatch(
                ProgramNodeId::new(7)
            ))
        );
    }
}

fn stream_fixture(units: Vec<Arc<novarocks_expression::CompiledExprSet>>, split: bool) -> Fixture {
    let source = checked(false, true, false);
    let layout = source.graph.nodes()[0].output_layout().clone();
    let branch = StaticStreamBranch::try_new(
        7,
        DataStreamPartitionType::HashPartitioned,
        vec![crate::test_support::root(0, 0)],
        vec![SlotId::new(1)],
        None,
        StaticStreamContract {
            role: ProgramOutputRole::Relational,
            delivery: ExchangeDeliveryRequirement::DemandDriven,
            routing: if split {
                StaticStreamRouting::Ordinary
            } else {
                StaticStreamRouting::ConditionalKeyV1 {
                    applies: crate::test_support::root(0, 1),
                }
            },
        },
    )
    .unwrap();
    let sink = if split {
        StaticSinkProgram::try_split(
            vec![branch],
            vec![crate::test_support::root(0, 0)],
            &units,
            false,
        )
        .unwrap()
    } else {
        StaticSinkProgram::try_data_stream(branch, &units).unwrap()
    };
    let graph = LocalProgramGraph::try_new_with_sink(
        source.graph.nodes().to_vec(),
        source.graph.root(),
        &units,
        source.graph.profile(),
        BindingRequirements::try_new(vec![BindingRequirement::ExchangeOutput {
            role: ProgramOutputRole::Relational,
            delivery: ExchangeDeliveryRequirement::DemandDriven,
            branch: 0,
            layout,
        }])
        .unwrap(),
        Some(sink),
    )
    .unwrap();
    Fixture {
        graph,
        units,
        calls: vec![],
        channels: source.channels,
    }
}

#[test]
fn final_owner_checks_sink_demand_against_its_actual_owned_unit_table() {
    use novarocks_type_contract::EvaluationDemand::{TruthOnly, Value};
    let value = crate::test_support::boolean(true);
    let actual = crate::test_support::constant_unit(
        std::slice::from_ref(&value),
        &[(0, Value), (0, TruthOnly)],
    );
    let source = stream_fixture(vec![actual.clone()], false);
    let program = finish(
        source,
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        program.unit(ProgramUnitId::new(0)).unwrap(),
        &actual
    ));
    let mut foreign = stream_fixture(vec![actual], false);
    foreign.units = vec![crate::test_support::constant_unit(
        &[value],
        &[(0, TruthOnly), (0, Value)],
    )];
    let error = finish(
        foreign,
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .unwrap_err();
    assert_eq!(
        error,
        LocalProgramCompileError::Sink(StaticSinkError::InvalidExpression)
    );
}

#[test]
fn final_owner_keeps_nonempty_split_predicates_outside_compiled_capability() {
    let source = stream_fixture(
        vec![crate::test_support::constant_unit(
            &[crate::test_support::boolean(true)],
            &[(0, novarocks_type_contract::EvaluationDemand::Value)],
        )],
        true,
    );
    assert_eq!(
        finish(
            source,
            operators(false, u32::MAX),
            &allowed(),
            CompiledProgramFacts::default(),
            &Control::default(),
        )
        .unwrap_err(),
        LocalProgramCompileError::InvalidExpression,
    );
}

#[test]
fn final_owner_definition_budget_counts_actual_unit_table_positions() {
    let half = novarocks_expression::MAX_STATIC_EXPRESSIONS / 2;
    let unit = crate::test_support::large_static_unit(half);
    assert_eq!(unit.definitions().nodes().len(), half);
    let mut exact = checked(false, true, false);
    exact.units = vec![unit.clone(), unit.clone()];
    let accepted = finish(
        exact,
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        accepted
            .units()
            .iter()
            .map(|unit| unit.definitions().nodes().len())
            .sum::<usize>(),
        novarocks_expression::MAX_STATIC_EXPRESSIONS
    );
    assert!(Arc::ptr_eq(&accepted.units()[0], &accepted.units()[1]));
    let mut over = checked(false, true, false);
    over.units = vec![
        unit.clone(),
        unit,
        crate::test_support::constant_unit(
            &[crate::test_support::integer(9)],
            &[(0, novarocks_type_contract::EvaluationDemand::Value)],
        ),
    ];
    let error = match finish(
        over,
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    ) {
        Ok(_) => panic!("expected the original fragment resource limit"),
        Err(error) => error,
    };
    assert_eq!(error, LocalProgramCompileError::TooManyDefinitions);
    use novarocks_failure_contract::{FailureClass, FailureProjection, ResourceDimension};
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
    let facts = error.failure_fact();
    assert_eq!(
        facts.resource().unwrap().dimension,
        ResourceDimension::Items
    );
    assert_eq!(
        facts.resource().unwrap().limit,
        Some(novarocks_expression::MAX_STATIC_EXPRESSIONS as u64)
    );
}

#[test]
fn final_owner_constant_backing_keeps_shared_and_independent_sources() {
    fn constant(
        unit: &novarocks_expression::CompiledExprSet,
    ) -> &novarocks_functions::ConstantValue {
        let [definition] = unit.definitions().nodes() else {
            panic!("expected one constant definition");
        };
        let novarocks_expression::StaticExprKind::Constant(value) = definition.kind() else {
            panic!("expected a constant");
        };
        value
    }

    let text = "x".repeat(9 * 1024 * 1024);
    let shared = crate::test_support::utf8(&text);
    let independent = crate::test_support::utf8(&text);
    let shared_source = Arc::downgrade(shared.pool().array());
    let independent_source = Arc::downgrade(independent.pool().array());
    assert!(!shared_source.ptr_eq(&independent_source));
    let mut fixture = checked(false, true, false);
    fixture.units = [&shared, &shared, &independent]
        .into_iter()
        .map(|value| {
            crate::test_support::constant_unit(
                std::slice::from_ref(value),
                &[(0, novarocks_type_contract::EvaluationDemand::Value)],
            )
        })
        .collect();
    let program = finish(
        fixture,
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .unwrap();

    assert_eq!(program.units().len(), 3);
    let keep = program.units()[2].clone();
    let from_shared = constant(&program.units()[0]);
    let from_alias = constant(&program.units()[1]);
    let from_independent = constant(&keep);
    assert_eq!(
        from_shared.pool().backing_identity(),
        shared.pool().backing_identity()
    );
    assert_eq!(
        from_alias.pool().backing_identity(),
        shared.pool().backing_identity()
    );
    assert_eq!(
        from_independent.pool().backing_identity(),
        independent.pool().backing_identity()
    );
    for value in [from_shared, from_independent] {
        let arrow = value
            .pool()
            .array()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(arrow.value(value.ordinal() as usize), text);
    }

    drop(shared);
    drop(independent);
    drop(program);
    assert!(shared_source.upgrade().is_none());
    assert!(independent_source.upgrade().is_some());
    drop(keep);
    assert!(independent_source.upgrade().is_none());
}

#[test]
fn final_owner_reference_budget_sums_individually_legal_units_at_near_exact_and_over() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_type_contract::{EvaluationDemand, MAX_CONTROL_USE_REFERENCES};
    let root_count = MAX_CONTROL_USE_REFERENCES / 2;
    let roots = vec![(0, EvaluationDemand::Value); root_count];
    let unit = crate::test_support::constant_unit(&[crate::test_support::integer(7)], &roots);
    assert_eq!(unit.control_flow().use_reference_count(), root_count);
    let one = crate::test_support::constant_unit(
        &[crate::test_support::integer(9)],
        &[(0, EvaluationDemand::Value)],
    );
    for extra in [false, true] {
        let mut fixture = checked(false, true, false);
        fixture.units = vec![unit.clone(), unit.clone()];
        if extra {
            fixture.units.push(one.clone());
        }
        let result = finish(
            fixture,
            operators(false, u32::MAX),
            &allowed(),
            CompiledProgramFacts::default(),
            &Control::default(),
        );
        if extra {
            let error = match result {
                Ok(_) => panic!("fragment references exceeded the original shared bound"),
                Err(error) => error,
            };
            assert_eq!(
                error,
                LocalProgramCompileError::Calls(ProgramResolvedCallsError::TooManyItems)
            );
            assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        } else {
            let program = result.unwrap();
            assert_eq!(
                program
                    .units()
                    .iter()
                    .map(|unit| unit.control_flow().use_reference_count())
                    .sum::<usize>(),
                MAX_CONTROL_USE_REFERENCES
            );
            assert!(Arc::ptr_eq(&program.units()[0], &program.units()[1]));
        }
    }
    let near = crate::test_support::constant_unit(
        &[crate::test_support::integer(7)],
        &roots[..root_count - 1],
    );
    let mut fixture = checked(false, true, false);
    fixture.units = vec![unit, near];
    let program = finish(
        fixture,
        operators(false, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        program
            .units()
            .iter()
            .map(|unit| unit.control_flow().use_reference_count())
            .sum::<usize>(),
        MAX_CONTROL_USE_REFERENCES - 1
    );
}

#[test]
fn final_owner_rechecks_actual_unit_input_scope_against_its_real_project_input() {
    use novarocks_constant_contract::{ConstantPoolId, ConstantPools, ConstantReference};
    use novarocks_expression::{
        EvaluationEnvironment, ScopeLayout, ScopeLayouts, compile_expr_set_pure,
    };
    use novarocks_expression_contract::{
        BoundExprSetBuilder, ExprKind, ExprPort, RootScope, UnitLimits, ValueId,
    };
    use novarocks_type_contract::{
        EvaluationDemand, MAX_CONTROL_DEPTH, SemanticParameters, ValueTypeStorageLifetime,
    };
    let source = checked(false, true, false);
    let narrow = source.graph.nodes()[0].output_layout().clone();
    let ty = FunctionValueType::new(DataType::Int64, false);
    let current = crate::test_support::read_unit(
        &narrow,
        std::slice::from_ref(&ty),
        &[(ValueId::new(30), ty.clone())],
        &[(0, EvaluationDemand::Value)],
    );
    let make = || {
        let graph = LocalProgramGraph::try_new_with_sink(
            vec![
                source.graph.nodes()[0].clone(),
                ProgramNode::new_local(
                    ProgramNodeId::new(1),
                    vec![DiagnosticSourceNodeId::new(u32::MAX)],
                    ProgramNodeKind::Project {
                        input: ProgramNodeId::new(0),
                        is_subordinate: false,
                        validate_final_result_input: false,
                        exprs: vec![crate::test_support::root(0, 0)],
                        expr_slot_ids: vec![SlotId::new(1)],
                        expr_slot_schemas: None,
                        output_indices: None,
                    },
                    narrow.clone(),
                ),
            ],
            ProgramNodeId::new(1),
            std::slice::from_ref(&current),
            CompileProfile::new(
                NonZeroUsize::new(1).unwrap(),
                None,
                narrow.identity().unwrap(),
                KernelAbiVersion::CURRENT,
            ),
            BindingRequirements::try_new(vec![BindingRequirement::ResultSink {
                layout: narrow.clone(),
            }])
            .unwrap(),
            Some(StaticSinkProgram::Result),
        )
        .unwrap();
        Fixture {
            graph,
            units: vec![current.clone()],
            calls: vec![],
            channels: (0..2)
                .map(|node| {
                    (
                        ProgramChannelSite::Layout {
                            node: ProgramNodeId::new(node),
                            role: ProgramChannelLayoutRole::NodeOutput,
                            ordinal: 0,
                        },
                        ty.clone(),
                    )
                })
                .collect(),
        }
    };
    let accepted = finish(
        make(),
        operators(true, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        accepted.units()[0]
            .input(accepted.units()[0].roots()[0].use_id)
            .unwrap()
            .ordinal,
        0
    );
    assert!(Arc::ptr_eq(
        accepted.units()[0].scopes()[0].schema(),
        narrow.schema()
    ));
    let wide = StaticLayout::try_new(
        Arc::new(Schema::new(
            (0..3)
                .map(|ordinal| Field::new(format!("wide{ordinal}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        )),
        Arc::from([SlotId::new(10), SlotId::new(20), SlotId::new(30)]),
    )
    .unwrap();
    let foreign = crate::test_support::read_unit(
        &wide,
        &[ty.clone(), ty.clone(), ty.clone()],
        &[
            (ValueId::new(10), ty.clone()),
            (ValueId::new(20), ty.clone()),
            (ValueId::new(30), ty.clone()),
        ],
        &[(2, EvaluationDemand::Value)],
    );
    assert_eq!(foreign.input(foreign.roots()[0].use_id).unwrap().ordinal, 2);
    assert_eq!(foreign.scopes()[0].schema().fields().len(), 3);
    assert!(Arc::ptr_eq(foreign.scopes()[0].schema(), wide.schema()));
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let value = crate::test_support::integer_typed(7, ty.clone());
    let pool = ConstantPoolId::new(0);
    let mut constants = ConstantPools::empty();
    constants.insert(pool, value.pool().clone()).unwrap();
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: MAX_CONTROL_DEPTH,
    });
    let definition = draft
        .add(
            None,
            value.value_type().clone(),
            ExprKind::Constant(ConstantReference {
                pool,
                ordinal: value.ordinal(),
            }),
        )
        .unwrap();
    let root = draft
        .root(definition, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let bound = draft
        .seal(
            vec![ExprPort {
                values: Box::default(),
            }],
            &mut work,
        )
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    let empty_scope = ScopeLayout::try_new(
        Arc::new(Schema::empty()),
        Arc::from([]),
        Arc::from([]),
        ValueTypeStorageLifetime::pure(),
        &mut work,
    )
    .unwrap();
    let declared_empty = compile_expr_set_pure(
        &bound,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &SemanticParameters::try_new([]).unwrap(),
        },
        ScopeLayouts {
            ports: &[empty_scope],
            all_ports: None,
        },
        crate::test_support::policy(),
        &crate::test_support::actual_rng_catalogue(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(declared_empty.scopes().len(), 1);
    assert!(declared_empty.scopes()[0].schema().fields().is_empty());
    assert_eq!(declared_empty.root(root).unwrap().scope, RootScope::Port(0));

    for (replacement, description) in [
        (foreign, "a width-three port with a read at ordinal two"),
        (declared_empty, "a declared zero-column port"),
    ] {
        let mut actual = make();
        let graph_error = match LocalProgramGraph::try_new_with_sink(
            actual.graph.nodes().to_vec(),
            actual.graph.root(),
            std::slice::from_ref(&replacement),
            actual.graph.profile(),
            actual.graph.requirements().clone(),
            actual.graph.sink().cloned(),
        ) {
            Ok(_) => panic!("graph accepted {description} for an actual one-column input"),
            Err(error) => error,
        };
        assert_eq!(graph_error, LocalProgramError::InvalidExpression);
        actual.units = vec![replacement];
        let error = match finish(
            actual,
            operators(true, u32::MAX),
            &allowed(),
            CompiledProgramFacts::default(),
            &Control::default(),
        ) {
            Ok(_) => panic!("final owner accepted {description} for an actual one-column input"),
            Err(error) => error,
        };
        assert_eq!(error, LocalProgramCompileError::InvalidExpression);
    }
}

#[test]
fn final_owner_checks_unmapped_runtime_fields_with_a_canonical_value_subset() {
    use novarocks_expression_contract::ValueId;
    use novarocks_type_contract::EvaluationDemand;
    let source = checked(false, true, false);
    let output = source.graph.nodes()[0].output_layout().clone();
    let ty = FunctionValueType::new(DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("physical_first", DataType::Int64, false),
        Field::new("physical_republication", DataType::Int64, false),
    ]));
    let input = StaticLayout::try_new_typed_for_compile(
        schema.clone(),
        Arc::from([SlotId::new(10), SlotId::new(20)]),
        vec![
            (ty.typed_binding().clone(), None),
            (ty.typed_binding().clone(), None),
        ],
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control::default(),
    )
    .unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .unwrap(),
        input.clone(),
    )
    .unwrap();
    // A canonical ValueId map may choose its first actual publication only.
    // The complete schema still retains the second physical runtime column.
    // Top-level field labels do not identify expression inputs.
    let scope_layout = |second_type| {
        StaticLayout::try_new(
            Arc::new(Schema::new(vec![
                Field::new("scope_alias", DataType::Int64, false),
                Field::new("scope_other_alias", second_type, false),
            ])),
            Arc::from([SlotId::new(30), SlotId::new(40)]),
        )
        .unwrap()
    };
    let valid_scope = scope_layout(DataType::Int64);
    let current = crate::test_support::read_unit(
        &valid_scope,
        &[ty.clone(), ty.clone()],
        &[(ValueId::new(10), ty.clone())],
        &[(0, EvaluationDemand::Value)],
    );
    assert_eq!(current.scopes()[0].schema().fields().len(), 2);
    assert_eq!(current.scopes()[0].columns().len(), 1);
    assert_eq!(current.scopes()[0].columns()[0].ordinal, 0);
    let nodes = vec![
        ProgramNode::new_local(
            ProgramNodeId::new(0),
            vec![DiagnosticSourceNodeId::new(u32::MAX)],
            ProgramNodeKind::Values { values },
            input,
        ),
        ProgramNode::new_local(
            ProgramNodeId::new(1),
            vec![DiagnosticSourceNodeId::new(u32::MAX)],
            ProgramNodeKind::Project {
                input: ProgramNodeId::new(0),
                is_subordinate: false,
                validate_final_result_input: false,
                exprs: vec![crate::test_support::root(0, 0)],
                expr_slot_ids: vec![SlotId::new(1)],
                expr_slot_schemas: None,
                output_indices: None,
            },
            output.clone(),
        ),
    ];
    let graph = LocalProgramGraph::try_new_with_sink(
        nodes,
        ProgramNodeId::new(1),
        std::slice::from_ref(&current),
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            output.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(vec![BindingRequirement::ResultSink { layout: output }])
            .unwrap(),
        Some(StaticSinkProgram::Result),
    )
    .unwrap();
    let make = || Fixture {
        graph: graph.clone(),
        units: vec![current.clone()],
        calls: vec![],
        channels: [(0, 0), (0, 1), (1, 0)]
            .into_iter()
            .map(|(node, ordinal)| {
                (
                    ProgramChannelSite::Layout {
                        node: ProgramNodeId::new(node),
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal,
                    },
                    ty.clone(),
                )
            })
            .collect(),
    };
    finish(
        make(),
        operators(true, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .expect("canonical subset and top-level field aliases are lawful");
    let foreign_scope = scope_layout(DataType::Boolean);
    let foreign = crate::test_support::read_unit(
        &foreign_scope,
        &[ty.clone(), FunctionValueType::new(DataType::Boolean, false)],
        &[(ValueId::new(10), ty.clone())],
        &[(0, EvaluationDemand::Value)],
    );
    assert_eq!(foreign.scopes()[0].columns().len(), 1);
    assert_eq!(foreign.scopes()[0].columns()[0].ordinal, 0);
    assert_eq!(
        foreign.scopes()[0].schema().field(1).data_type(),
        &DataType::Boolean
    );
    // Check the final owned-table swap first: the Graph was legally authored
    // with the actual complete input schema, not with the foreign schema.
    let mut actual = make();
    actual.units = vec![foreign.clone()];
    let error = match finish(
        actual,
        operators(true, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    ) {
        Ok(_) => panic!("unmapped runtime field differs despite equal width and mapped type"),
        Err(error) => error,
    };
    assert_eq!(error, LocalProgramCompileError::InvalidExpression);
    let graph_error = match LocalProgramGraph::try_new_with_sink(
        graph.nodes().to_vec(),
        graph.root(),
        std::slice::from_ref(&foreign),
        graph.profile(),
        graph.requirements().clone(),
        graph.sink().cloned(),
    ) {
        Ok(_) => panic!("Graph must also check the complete actual runtime schema"),
        Err(error) => error,
    };
    assert_eq!(graph_error, LocalProgramError::InvalidExpression);

    // Retain the carrier-mismatch case above. This second actual input has an
    // unmapped LargeBinary column, so JSON differs only in its complete nominal facts.
    let binary = FunctionValueType::new(DataType::LargeBinary, false);
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    assert_eq!(binary.data_type(), json.data_type());
    assert_eq!(binary.nullable(), json.nullable());
    assert_ne!(binary, json);
    let nominal_schema = Arc::new(Schema::new(vec![
        Field::new("physical_first", DataType::Int64, false),
        Field::new("physical_unmapped", DataType::LargeBinary, false),
    ]));
    let nominal_input = StaticLayout::try_new_typed_for_compile(
        nominal_schema.clone(),
        Arc::from([SlotId::new(10), SlotId::new(20)]),
        vec![
            (ty.typed_binding().clone(), None),
            (binary.typed_binding().clone(), None),
        ],
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control::default(),
    )
    .unwrap();
    let nominal_values = StaticValues::try_new(
        RecordBatch::try_new(
            nominal_schema,
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(arrow_array::LargeBinaryArray::from(vec![
                    b"\x01\x20".as_slice(),
                ])),
            ],
        )
        .unwrap(),
        nominal_input.clone(),
    )
    .unwrap();
    let nominal_scope = scope_layout(DataType::LargeBinary);
    let plain_unit = crate::test_support::read_unit(
        &nominal_scope,
        &[ty.clone(), binary.clone()],
        &[(ValueId::new(10), ty.clone())],
        &[(0, EvaluationDemand::Value)],
    );
    let nominal_graph = LocalProgramGraph::try_new_with_sink(
        vec![
            ProgramNode::new_local(
                ProgramNodeId::new(0),
                vec![DiagnosticSourceNodeId::new(u32::MAX)],
                ProgramNodeKind::Values {
                    values: nominal_values,
                },
                nominal_input,
            ),
            graph.nodes()[1].clone(),
        ],
        graph.root(),
        std::slice::from_ref(&plain_unit),
        graph.profile(),
        graph.requirements().clone(),
        graph.sink().cloned(),
    )
    .unwrap();
    let nominal_fixture = || Fixture {
        graph: nominal_graph.clone(),
        units: vec![plain_unit.clone()],
        calls: vec![],
        channels: [
            (0, 0, ty.clone()),
            (0, 1, binary.clone()),
            (1, 0, ty.clone()),
        ]
        .into_iter()
        .map(|(node, ordinal, ty)| {
            (
                ProgramChannelSite::Layout {
                    node: ProgramNodeId::new(node),
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal,
                },
                ty,
            )
        })
        .collect(),
    };
    finish(
        nominal_fixture(),
        operators(true, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    )
    .expect("the actual UTF8 ordinal is valid with its original complete type");
    let foreign_nominal = crate::test_support::read_unit(
        &nominal_scope,
        &[ty.clone(), json.clone()],
        &[(ValueId::new(10), ty.clone())],
        &[(0, EvaluationDemand::Value)],
    );
    let scope = &foreign_nominal.scopes()[0];
    assert_eq!(scope.schema(), plain_unit.scopes()[0].schema());
    assert_eq!(scope.columns().len(), 1);
    assert_eq!(scope.columns()[0].ordinal, 0);
    assert_eq!(scope.columns()[0].ty, ty);
    assert_eq!(scope.ordinal_type(1), Some(&json));
    assert_eq!(plain_unit.scopes()[0].ordinal_type(1), Some(&binary));
    let mut actual = nominal_fixture();
    actual.units = vec![foreign_nominal.clone()];
    let error = match finish(
        actual,
        operators(true, u32::MAX),
        &allowed(),
        CompiledProgramFacts::default(),
        &Control::default(),
    ) {
        Ok(_) => panic!("unmapped ordinal must retain its original complete nominal type"),
        Err(error) => error,
    };
    assert_eq!(error, LocalProgramCompileError::InvalidExpression);
    let graph_error = match LocalProgramGraph::try_new_with_sink(
        nominal_graph.nodes().to_vec(),
        nominal_graph.root(),
        std::slice::from_ref(&foreign_nominal),
        nominal_graph.profile(),
        nominal_graph.requirements().clone(),
        nominal_graph.sink().cloned(),
    ) {
        Ok(_) => panic!("Graph must reject an unmapped ordinal's different nominal type"),
        Err(error) => error,
    };
    assert_eq!(graph_error, LocalProgramError::InvalidExpression);
}

mod read;
