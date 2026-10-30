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

//! Writer-family fragments (`Values -> TableWriter -> Stream` and
//! `ExchangeSource -> TableFinish -> Result`) built through the real physical
//! builders, package extraction and pure provider validation, then compiled
//! into the final LocalProgram owner. The provider compiler is a pure
//! contract fixture that keeps the draft; it is not the installed Iceberg port.

use super::*;
use crate::writer::admit_writer_family;
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use novarocks_connector_contract::*;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    BindingRequirement, KernelAbiVersion, LocalProgram, ProgramChannelLayoutRole,
    ProgramChannelSite, ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeKind,
    StaticExprKind, StaticSinkProgram,
};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, control_argument_semantics,
};
use std::{num::NonZeroUsize, time::Duration};

const WRITER_FRAGMENT: FragmentId = FragmentId::new(1);
const FINISH_FRAGMENT: FragmentId = FragmentId::new(2);
const EDGE: EdgeId = EdgeId::new(7);

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn ty(data_type: DataType, nullable: bool) -> ValueType {
    ValueType::new(data_type, nullable)
}

fn binding() -> ConnectorWriteBinding {
    let instance = ConnectorInstanceId::parse("lake").unwrap();
    ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("alpha").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    )
}

/// Provider source fields keep their attributes on the write recipe. Its
/// separately authored execution bindings determine the static projection.
fn target_fields() -> Vec<Field> {
    vec![
        Field::new("c1", DataType::Int64, false).with_metadata(std::collections::HashMap::<
            String,
            String,
        >::from([(
            "provider.field-id".to_string(),
            "1".to_string(),
        )])),
        Field::new("c2", DataType::Utf8, true),
    ]
}

fn draft() -> ConnectorWriteRecipeDraft {
    draft_with_types([
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Utf8, true),
    ])
}
fn draft_with_types(declared: [FunctionValueType; 2]) -> ConnectorWriteRecipeDraft {
    let binding = binding();
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        Bytes::from_static(b"writer-handle"),
    );
    ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        ConnectorWriteInputShape::Data {
            fields: declared
                .into_iter()
                .enumerate()
                .map(|(ordinal, execution)| {
                    let name = if ordinal == 0 { "c1" } else { "c2" };
                    let mut field =
                        Field::new(name, execution.data_type().clone(), execution.nullable());
                    if ordinal == 0 {
                        field =
                            field.with_metadata(std::collections::HashMap::<String, String>::from(
                                [("provider.field-id".to_owned(), "1".to_owned())],
                            ));
                    }
                    ConnectorWriteFieldBinding::try_new_typed(
                        ConnectorWriteFieldToken::from_bytes([ordinal as u8 + 1; 32]),
                        field,
                        execution,
                    )
                    .unwrap()
                })
                .collect(),
        },
    )
    .unwrap()
}

fn property(distribution: Distribution) -> PhysicalProperties {
    PhysicalProperties {
        distribution,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn dop_domain() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 8,
        requires_power_of_two: false,
    }
}

/// The writer multiplex relation (`root == false`) or the Root result
/// relation (`root == true`), exactly as the SPI defines them.
fn relation_fields(
    builder: &mut FragmentBuilder,
    owner: NodeId,
    root: bool,
) -> Box<[WriterRelationField]> {
    use WriterDerivedKind as K;
    use WriterRelationFieldRole as R;
    let mut specs = vec![
        ("kind", DataType::Int8, false, R::Kind, K::RelationKind),
        (
            "write_target_ordinal",
            DataType::Int32,
            root,
            R::TargetOrdinal,
            K::WriteTargetOrdinal,
        ),
        (
            "row_count",
            DataType::Int64,
            true,
            R::RowCount,
            K::AffectedRows,
        ),
        (
            "commit_fragment",
            DataType::Binary,
            true,
            R::CommitFragment,
            K::CommitFragment,
        ),
    ];
    if root {
        specs.extend([
            (
                "input_fields",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                true,
                R::Auxiliary,
                K::RelationAuxiliary,
            ),
            (
                "blob_type",
                DataType::Utf8,
                true,
                R::Auxiliary,
                K::RelationAuxiliary,
            ),
            (
                "body",
                DataType::Binary,
                true,
                R::Auxiliary,
                K::RelationAuxiliary,
            ),
            (
                "properties",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, false),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
                R::Auxiliary,
                K::RelationAuxiliary,
            ),
        ]);
    }
    specs
        .into_iter()
        .map(|(name, carrier, nullable, role, kind)| {
            let ty = ty(carrier, nullable);
            let value = builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: owner,
                        kind,
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: name.into(),
                ty,
                role,
            }
        })
        .collect()
}

struct Fixture {
    writer: Arc<FragmentPackage>,
    finish: Arc<FragmentPackage>,
    /// Physical TableWriter / TableFinish node IDs.
    writer_node: NodeId,
    finish_node: NodeId,
}

/// `INSERT INTO t VALUES (1, 'a')` without statistics: the writer fragment
/// writes one Values row of a non-null BIGINT and a non-null STRING into a
/// NOT NULL `c1` and a nullable `c2`; the finish fragment publishes the Root
/// result relation.
fn fixture() -> Fixture {
    fixture_with_types(
        [
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Utf8, false),
        ],
        draft(),
    )
    .unwrap()
}
fn fixture_with_types(
    source_types: [FunctionValueType; 2],
    draft: ConnectorWriteRecipeDraft,
) -> Result<Fixture, Box<dyn std::error::Error>> {
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let mut builder = FragmentBuilder::new(WRITER_FRAGMENT);
    let source = builder.reserve_node_id().unwrap();
    let cells = source_types.into_iter().enumerate().map(|(ordinal, ty)| {
        let literal = if ty.nullable() {
            LiteralValue::Null
        } else if ordinal == 0 {
            LiteralValue::Int64(1)
        } else {
            LiteralValue::Utf8("a".into())
        };
        (ty, literal)
    });
    let mut row = Vec::new();
    let mut values = Vec::new();
    for (ordinal, (value_type, literal)) in cells.enumerate() {
        row.push(
            builder
                .add_expression(source, value_type.clone(), ExprKind::Literal(literal))
                .unwrap(),
        );
        values.push(
            builder
                .add_value(
                    value_type,
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
        );
    }
    builder
        .insert_node_unchecked(PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: source,
                columns: values.clone().into_boxed_slice(),
            },
            kind: NodeKind::Values {
                rows: Box::from([row.into_boxed_slice()]),
            },
        })
        .unwrap();
    let writer = builder.reserve_node_id().unwrap();
    let fields = relation_fields(&mut builder, writer, false);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: writer,
            inputs: Box::from([source]),
            required_inputs: Box::from([property(Distribution::Singleton)]),
            output_properties: property(Distribution::Unconstrained),
            output: OutputPort {
                node: writer,
                columns: fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableWriter {
                target: WriterTarget {
                    handle: draft.payload().clone(),
                    write_target_ordinal: ordinal,
                    input: values.clone().into_boxed_slice(),
                    required_distribution: Distribution::Singleton,
                    target_fields: draft
                        .input()
                        .fields_iter()
                        .zip(&values)
                        .map(|(binding, value)| WriterTargetField {
                            provider_name: binding.field().name().clone().into(),
                            token: binding.token(),
                            input: *value,
                            ty: binding
                                .execution_type()
                                .expect("provider complete target declaration")
                                .clone(),
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
        })
        .unwrap();
    let producer = builder.finish_definition(
        writer,
        FragmentSink::Stream { edge: EDGE },
        dop_domain(),
        novarocks_physical_plan::StagePlacementPolicy::Singleton,
    )?;

    let mut builder = FragmentBuilder::new(FINISH_FRAGMENT);
    let exchange = builder.reserve_node_id().unwrap();
    let imported = fields
        .iter()
        .map(|field| {
            let value = builder
                .add_value(
                    field.ty.clone(),
                    ValueOrigin::ExchangeImport {
                        edge: EDGE,
                        source_value: field.value,
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: field.name.clone(),
                ty: field.ty.clone(),
                role: field.role,
            }
        })
        .collect::<Box<[_]>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: exchange,
                columns: imported.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::ExchangeSource {
                edge: EDGE,
                imports: fields
                    .iter()
                    .zip(&imported)
                    .map(|(a, b)| (a.value, b.value))
                    .collect(),
            },
        })
        .unwrap();
    let finish = builder.reserve_node_id().unwrap();
    let outputs = relation_fields(&mut builder, finish, true);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: finish,
            inputs: Box::from([exchange]),
            required_inputs: Box::from([property(Distribution::Singleton)]),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: finish,
                columns: outputs.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableFinish(WriterFinishSpec {
                count_mode: novarocks_physical_plan::FinishCountMode::WriterReported,
                inputs: novarocks_physical_plan::FinishInputs {
                    writer_results: 0,
                    logical_summary: None,
                },
                logical_summary: None,
                expected_target_ordinals: Box::from([ordinal]),
                input_schema: WriterRelationSchema {
                    revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: imported.clone(),
                },
                output_schema: WriterRelationSchema {
                    revision: ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: outputs.clone(),
                },
                final_aggregates: Box::default(),
                grouped_unpivot: None,
            }),
        })
        .unwrap();
    let consumer = builder
        .finish_definition(
            finish,
            FragmentSink::Result,
            dop_domain(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([1; 16]).unwrap());
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(Edge {
        role: novarocks_physical_plan::OutputRole::WriterResults,
        delivery: novarocks_physical_plan::DeliveryRequirement::MustDrain,
        id: EDGE,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: WRITER_FRAGMENT,
            projection: fields.iter().map(|field| field.value).collect(),
        },
        destination: EdgeDestination {
            fragment: FINISH_FRAGMENT,
            node: exchange,
            receive_mapping: fields
                .iter()
                .zip(&imported)
                .map(|(a, b)| (a.value, b.value))
                .collect(),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Singleton,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Singleton,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: FINISH_FRAGMENT,
        output: OutputPort {
            node: finish,
            columns: outputs.iter().map(|field| field.value).collect(),
        },
        fields: outputs
            .iter()
            .map(|field| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: field.name.clone(),
                alias: None,
                value: field.value,
                ty: field.ty.clone(),
            })
            .collect(),
    })
    .unwrap();
    let plan = plan.finish()?;

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
    let mut packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::from([(ordinal, draft)]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )?;
    Ok(Fixture {
        writer: Arc::new(packages.remove(&WRITER_FRAGMENT).unwrap()),
        finish: Arc::new(packages.remove(&FINISH_FRAGMENT).unwrap()),
        writer_node: writer,
        finish_node: finish,
    })
}

/// One complete eager use per literal or materialized-value root site in one domain.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let mut uses = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(index, (&site, root))| {
            let id = ExpressionUseId::new(index as u32);
            assert!(matches!(
                fragment.expressions().get(root.expr).unwrap().kind,
                ExprKind::Literal(_) | ExprKind::Value(_)
            ));
            // A literal has no argument, so its control shape is trivially eager.
            let _ = control_argument_semantics(ControlShape::Eager, 0, 0, root.demand);
            uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: id,
                    domain: EvaluationDomainId::new(0),
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, id)
        })
        .collect();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        uses,
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

/// Keeps the draft unchanged; the public fields are borrowed as they are.
struct Port;
impl ConnectorWriteRecipeCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        _: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<ConnectorError>> {
        Ok(draft.clone())
    }
}

fn providers() -> PureProviderProgramCatalog<ConnectorError> {
    let provider = ConnectorProviderId::parse("alpha").unwrap();
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            false,
            true,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            None,
            Some(Arc::new(Port)
                as Arc<
                    dyn ConnectorWriteRecipeCompiler<Error = ConnectorError>,
                >),
        )],
        &FixtureControl,
    )
    .unwrap()
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

fn options(pipeline_dop: usize, root_sink_dop: Option<usize>) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(pipeline_dop).unwrap(),
        root_sink_dop: root_sink_dop.map(|dop| NonZeroUsize::new(dop).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: Duration::from_millis(1_000),
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
    package: &Arc<FragmentPackage>,
    pipeline_dop: usize,
    root_sink_dop: Option<usize>,
) -> Result<LocalProgram, FragmentCompileError> {
    let validated =
        validate_fragment_providers(Arc::clone(package), &providers(), &FixtureControl).unwrap();
    compile_fragment(
        validated,
        &functions(),
        options(pipeline_dop, root_sink_dop),
        &FixtureControl,
    )
}

fn names(layout: &novarocks_local_program::StaticLayout) -> Vec<String> {
    layout
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

const WRITER_RELATION: [&str; 4] = [
    "kind",
    "write_target_ordinal",
    "row_count",
    "commit_fragment",
];

#[test]
fn writer_fragment_compiles_a_positional_relation_over_synthetic_slot_reads() {
    let fixture = fixture();
    for dop in [1, 4] {
        let program = compile(&fixture.writer, dop, None).unwrap();
        let graph = program.graph();
        let root = graph.root();
        let ProgramNodeKind::TableWriter {
            input,
            target,
            expected_layout,
            projection,
            writer_multiplex_layout,
            partial_aggregates,
            partial_preparation,
        } = graph.nodes()[root.index()].kind()
        else {
            panic!("the writer fragment roots at its TableWriter");
        };
        assert!(partial_aggregates.is_empty());
        assert!(partial_preparation.is_none());
        assert_eq!(target.get(), 0);
        assert!(matches!(
            graph.nodes()[input.index()].kind(),
            ProgramNodeKind::Values { .. }
        ));
        // The projection is the provider's own fields, with the value's
        // nullability: `c1` is fed a non-null value and stays NOT NULL, `c2`
        // is nullable in the provider but fed a non-null value.
        let fields = projection.layout.schema().fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(
            fields[0].as_ref(),
            &Field::new("c1", DataType::Int64, false)
        );
        assert!(projection.layout.is_typed());
        assert!(fields[0].metadata().is_empty());
        assert_eq!(
            program.write_recipes()[&root]
                .draft()
                .input()
                .fields_iter()
                .next()
                .unwrap()
                .field()
                .metadata()
                .get("provider.field-id")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            fields[1].as_ref(),
            &target_fields()[1].clone().with_nullable(false)
        );
        assert_eq!(expected_layout.schema(), projection.layout.schema());
        assert_eq!(expected_layout.slots(), projection.layout.slots());
        // Each projected field is a synthetic slot read of its input
        // occurrence, never a slot the projection itself produces.
        let input_slots = graph.nodes()[input.index()].output_layout().slots();
        for (ordinal, expr) in projection.expressions.iter().enumerate() {
            match projection.arena.node(*expr).unwrap().kind() {
                StaticExprKind::SlotId(slot) => assert_eq!(*slot, input_slots[ordinal]),
                other => panic!("writer projection {ordinal} is {other:?}"),
            }
            assert!(!projection.layout.slots().contains(&input_slots[ordinal]));
        }
        // The multiplex relation is named by its frozen fields with fresh slots.
        assert_eq!(names(writer_multiplex_layout), WRITER_RELATION);
        assert_eq!(
            graph.nodes()[root.index()].output_layout().slots(),
            writer_multiplex_layout.slots()
        );
        // The recipe moved into the writer, which is bound as a TableWriter.
        assert_eq!(program.write_recipes().len(), 1);
        assert_eq!(
            program.write_recipes()[&root].draft().input().field_count(),
            2
        );
        assert!(
            graph
                .requirements()
                .entries()
                .iter()
                .any(|requirement| matches!(
                    requirement,
                    BindingRequirement::TableWriter { node, .. } if *node == root
                ))
        );
        assert!(matches!(
            graph.sink(),
            Some(StaticSinkProgram::DataStream { .. })
        ));
        // Every projected read is bound to the writer input's occurrence.
        let channels = program.checked().channels();
        for ordinal in 0..2u32 {
            let site = ProgramExpressionRootSite::WriterProjection {
                node: root,
                expression: ordinal,
            };
            assert_eq!(
                site.arena(),
                novarocks_local_program::ProgramExpressionArena::WriterProjection(root)
            );
            assert!(
                channels
                    .channel_type(ProgramChannelSite::Layout {
                        node: root,
                        role: ProgramChannelLayoutRole::WriterProjection,
                        ordinal,
                    })
                    .is_some()
            );
        }
        let bound = program
            .checked()
            .slots()
            .iter()
            .filter(|(occurrence, _)| {
                occurrence.arena
                    == novarocks_local_program::ProgramExpressionArena::WriterProjection(root)
            })
            .map(|(_, source)| *source)
            .collect::<Vec<_>>();
        assert_eq!(
            bound,
            (0..2u32)
                .map(
                    |ordinal| ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                        node: *input,
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal,
                    })
                )
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn finish_fragment_reads_the_writer_relation_and_publishes_the_root_relation() {
    let fixture = fixture();
    let program = compile(&fixture.finish, 2, Some(1)).unwrap();
    let graph = program.graph();
    let root = graph.root();
    let ProgramNodeKind::TableFinish {
        count_mode: novarocks_local_program::FinishCountMode::WriterReported,
        input_roles:
            novarocks_local_program::FinishInputs {
                writer_results: 0,
                logical_summary: None,
            },
        logical_summary_layout: None,
        inputs,
        expected_targets,
        writer_multiplex_layout,
        root_result_layout,
        final_aggregates,
    } = graph.nodes()[root.index()].kind()
    else {
        panic!("the finish fragment roots at its TableFinish");
    };
    assert_eq!(expected_targets.len(), 1);
    assert!(final_aggregates.calls.is_empty() && final_aggregates.unpivot.is_none());
    let [input] = inputs.as_slice() else {
        panic!("one writer-result receiver");
    };
    let receiver = &graph.nodes()[input.index()];
    assert!(matches!(
        receiver.kind(),
        ProgramNodeKind::ExchangeSource { .. }
    ));
    // The receiver is the writer relation by name; the finish reads it as is.
    assert_eq!(names(receiver.output_layout()), WRITER_RELATION);
    assert_eq!(
        writer_multiplex_layout.slots(),
        receiver.output_layout().slots()
    );
    assert_eq!(
        writer_multiplex_layout.schema(),
        receiver.output_layout().schema()
    );
    // The Root relation keeps its exact nested child names.
    assert_eq!(
        names(root_result_layout),
        [
            "kind",
            "write_target_ordinal",
            "row_count",
            "commit_fragment",
            "input_fields",
            "blob_type",
            "body",
            "properties"
        ]
    );
    let properties = root_result_layout.schema().field(7);
    let DataType::Map(entries, false) = properties.data_type() else {
        panic!("properties is a map");
    };
    assert_eq!(entries.name(), "entries");
    let DataType::List(item) = root_result_layout.schema().field(4).data_type() else {
        panic!("input_fields is a list");
    };
    assert_eq!(item.name(), "item");
    assert!(program.write_recipes().is_empty());
    assert!(
        graph
            .requirements()
            .entries()
            .iter()
            .any(|requirement| matches!(
                requirement,
                BindingRequirement::TableFinish { node, .. } if *node == root
            ))
    );
    assert!(matches!(graph.sink(), Some(StaticSinkProgram::Result)));
    assert_eq!(
        program.exchange_inputs()[input].receiver_node,
        fixture.finish.fragment().nodes()[&fixture.finish_node].inputs[0].get()
    );
}

fn unsupported(error: FragmentCompileError) -> &'static str {
    match error {
        FragmentCompileError::Unsupported { feature, .. } => feature,
        other => panic!("expected an explicit refusal, got {other:?}"),
    }
}

/// A partitioned writer input is co-located per instance only, so without a
/// compiled local shuffle it runs on one driver.
#[test]
fn a_partitioned_writer_input_is_refused_at_more_than_one_driver() {
    let fixture = fixture();
    let mut control =
        CompileCheckpoints::try_new(&FixtureControl, CompilePhase::LowerProgram).unwrap();
    let recipe =
        validate_fragment_providers(Arc::clone(&fixture.writer), &providers(), &FixtureControl)
            .unwrap();
    let recipe = recipe.writes().get(&fixture.writer_node).cloned();
    let mut writer = fixture.writer.fragment().nodes()[&fixture.writer_node].clone();
    let NodeKind::TableWriter { target } = &mut writer.kind else {
        unreachable!()
    };
    target.required_distribution = Distribution::Hash {
        keys: Box::from([target.input[0]]),
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
    };
    admit_writer_family(
        &fixture.writer,
        &writer,
        recipe.as_ref(),
        NonZeroUsize::new(1).unwrap(),
        &mut control,
    )
    .unwrap();
    let error = admit_writer_family(
        &fixture.writer,
        &writer,
        recipe.as_ref(),
        NonZeroUsize::new(2).unwrap(),
        &mut control,
    )
    .unwrap_err();
    assert_eq!(
        unsupported(error),
        "partitioned writer input at pipeline DOP > 1"
    );
    control.finish().unwrap();
}

/// Neither family runs below its fragment root: one writer per fragment, and
/// a finish never shares a fragment with a writer.
#[test]
fn a_writer_family_node_below_its_fragment_root_is_refused() {
    let fixture = fixture();
    let mut control =
        CompileCheckpoints::try_new(&FixtureControl, CompilePhase::LowerProgram).unwrap();
    let recipe =
        validate_fragment_providers(Arc::clone(&fixture.writer), &providers(), &FixtureControl)
            .unwrap();
    let recipe = recipe.writes().get(&fixture.writer_node).cloned();
    let mut writer = fixture.writer.fragment().nodes()[&fixture.writer_node].clone();
    writer.id = NodeId::new(fixture.writer_node.get() + 100);
    let error = admit_writer_family(
        &fixture.writer,
        &writer,
        recipe.as_ref(),
        NonZeroUsize::new(1).unwrap(),
        &mut control,
    )
    .unwrap_err();
    assert!(unsupported(error).contains("one writer per fragment"));
    control.finish().unwrap();
}

// ---------------------------------------------------------------------------
// Writer statistics: the Iceberg Theta sketch collected on write over `c1`
// (BIGINT) and `c2` (STRING), exactly as the frontend plans and freezes it.

use novarocks_connector_iceberg_functions::{
    ICEBERG_THETA_AGGREGATE_NAME, ICEBERG_THETA_IMPLEMENTATION_IDENTITY,
    ICEBERG_THETA_STATE_FORMAT_IDENTITY, IcebergFunctionBundle,
};
use novarocks_functions::{
    AggregateBindingSelection, AggregateKernelPhase, AggregatePreparationOptions,
    AggregateStateFormatIdentity, CallArgumentUses, CallEffectInput, FunctionBindingRequest,
    FunctionBindingSelection, FunctionBundleContributor, FunctionResultType, PureCallPreparation,
    PurePreparationSource, ScopedExpressionEffects,
};
use novarocks_local_program::{
    ProgramCallSite, ProgramNodeExpressionRole, ProgramStateTemplate,
    UnpivotConstant as LocalConstant,
};
use novarocks_type_contract::{
    CallProofScope, DecimalOverflowPolicy, EvaluationDemand, ExpressionEffects, SemanticParameters,
};

const THETA_OVERLOADS: [&str; 17] = [
    "boolean",
    "tinyint",
    "smallint",
    "int",
    "long",
    "float",
    "double",
    "decimal",
    "date",
    "time-micros",
    "timestamp-micros",
    "timestamp-nanos",
    "string",
    "large-string",
    "binary",
    "large-binary",
    "fixed",
];
const BLOB_TYPE: &str = "apache-datasketches-theta-v1";

/// The installed Iceberg bundle sealed with its own pure owner, as the
/// process seals it.
fn theta_catalog() -> PureEngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    IcebergFunctionBundle.contribute(&mut builder).unwrap();
    builder
        .seal_pure(THETA_OVERLOADS.map(|suffix| InstalledPureKernel {
            function: FunctionId::try_new("parametric.aggregate/$iceberg_theta_stat/v1").unwrap(),
            kind: FunctionKind::Aggregate,
            implementation: PureImplementationDeclaration {
                overload:
                    FunctionOverloadId::try_new(format!("iceberg/theta-stat/{suffix}/v1")).unwrap(),
                implementation:
                    PureImplementationId::try_new(ICEBERG_THETA_IMPLEMENTATION_IDENTITY).unwrap(),
                abi: PureKernelAbi::AggregateV1,
            },
            aggregate_state_format: Some(
                AggregateStateFormatIdentity::try_new(ICEBERG_THETA_STATE_FORMAT_IDENTITY).unwrap(),
            ),
        }))
        .unwrap()
}

/// The Theta binding the frontend resolves for one input type, at `phase`.
fn theta_binding(
    catalog: &PureEngineFunctionCatalog,
    input: &ValueType,
    phase: AggregatePhase,
) -> AggregateBinding {
    let arguments = [StaticFunctionArgument::Value {
        value_type: input.clone(),
        constant: None,
    }];
    let bound = catalog
        .metadata()
        .resolve_bound_trusted(
            ICEBERG_THETA_AGGREGATE_NAME,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            &FixtureControl,
        )
        .unwrap();
    let aggregate = bound.selected.aggregate.clone().unwrap();
    let FunctionResultType::Scalar(result_type) = bound.selected.result_type.clone() else {
        panic!("Theta returns one scalar")
    };
    AggregateBinding {
        state_interpretation: None,
        state_argument_contract: aggregate.state_argument_contract,
        function: BoundFunction {
            legacy_metadata: None,
            function_id: bound.function_id,
            overload: bound.selected.overload.clone(),
            kind: FunctionKind::Aggregate,
            argument_types: bound.selected.argument_types.clone(),
            result_type,
        },
        phase,
        logical_argument_count: 1,
        intermediate_type: aggregate.intermediate_type,
        state_format: aggregate.state_format,
    }
}

fn selection(binding: &AggregateBinding) -> Arc<FunctionBindingSelection> {
    Arc::new(FunctionBindingSelection {
        overload: binding.function.overload.clone(),
        argument_types: binding.function.argument_types.clone(),
        result_type: FunctionResultType::Scalar(binding.function.result_type.clone()),
        aggregate: Some(AggregateBindingSelection {
            state_argument_contract: binding.state_argument_contract,
            intermediate_type: binding.intermediate_type.clone(),
            state_format: binding.state_format.clone(),
        }),
    })
}

/// The original logical request of one writer call: its one value argument.
fn theta_request(binding: &AggregateBinding) -> PhysicalCallRequest {
    let [novarocks_type_contract::FunctionArgumentType::Value(input)] =
        binding.function.argument_types.as_ref()
    else {
        panic!("Theta has one value argument")
    };
    PhysicalCallRequest {
        arguments: Box::from([StaticFunctionArgument::Value {
            value_type: input.clone(),
            constant: None,
        }]),
        logical_argument_count: 1,
        expected_result_type: None,
        constant_policy: options(1, None).constants,
    }
}

fn binary(nullable: bool) -> ValueType {
    ty(DataType::Binary, nullable)
}

fn list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
}

fn map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

const LIST_POOL: ConstantPoolId = ConstantPoolId::new(1);
const MAP_POOL: ConstantPoolId = ConstantPoolId::new(2);

/// One checked pool of each collection constant: input-field ID lists `[1]`
/// and `[2]`, and one empty property map.
fn pools() -> [(ConstantPoolId, ConstantPool); 2] {
    use arrow_array::{Array, ListArray, MapArray, StringArray, StructArray};
    let DataType::List(item) = list_type() else {
        unreachable!()
    };
    let raw = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(1)]),
        Some(vec![Some(2)]),
    ]);
    let lists = ListArray::try_new(item, raw.offsets().clone(), raw.values().clone(), None)
        .unwrap()
        .to_data();
    let list_type = ty(list_type(), false);
    let list = ConstantPool::try_new(
        Arc::new(list_type.try_to_field("input_fields").unwrap()),
        list_type,
        lists,
        options(1, None).constants,
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let DataType::Map(entries, _) = map_type() else {
        unreachable!()
    };
    let DataType::Struct(fields) = entries.data_type().clone() else {
        unreachable!()
    };
    let children = StructArray::try_new(
        fields,
        vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
        None,
    )
    .unwrap();
    let empty =
        ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([Some(Vec::<
            Option<i32>,
        >::new(
        ))]);
    let maps = MapArray::try_new(entries, empty.offsets().clone(), children, None, false)
        .unwrap()
        .to_data();
    let map_type = ty(map_type(), false);
    let map = ConstantPool::try_new(
        Arc::new(map_type.try_to_field("properties").unwrap()),
        map_type,
        maps,
        options(1, None).constants,
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    [(LIST_POOL, list), (MAP_POOL, map)]
}

struct StatisticsFixture {
    writer: Arc<FragmentPackage>,
    finish: Arc<FragmentPackage>,
    writer_node: NodeId,
    finish_node: NodeId,
    catalog: PureEngineFunctionCatalog,
}

/// `INSERT INTO t VALUES (1, 'a')` into a target collecting a Theta sketch of
/// each column: the writer fragment's two partial calls write two auxiliary
/// channels, and the finish fragment's two final calls feed one grouped
/// Unpivot whose two mappings expand target 0's sketches into Root artifact
/// rows with the input-field list, the blob-type literal and a property map.
fn statistics_fixture() -> StatisticsFixture {
    let catalog = theta_catalog();
    let draft = draft();
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let mut builder = FragmentBuilder::new(WRITER_FRAGMENT);
    let source = builder.reserve_node_id().unwrap();
    let cells = [
        (ty(DataType::Int64, false), LiteralValue::Int64(1)),
        (ty(DataType::Utf8, false), LiteralValue::Utf8("a".into())),
    ];
    let mut row = Vec::new();
    let mut values = Vec::new();
    for (ordinal, (value_type, literal)) in cells.into_iter().enumerate() {
        row.push(
            builder
                .add_expression(source, value_type.clone(), ExprKind::Literal(literal))
                .unwrap(),
        );
        values.push(
            builder
                .add_value(
                    value_type,
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
        );
    }
    builder
        .insert_node_unchecked(PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: source,
                columns: values.clone().into_boxed_slice(),
            },
            kind: NodeKind::Values {
                rows: Box::from([row.into_boxed_slice()]),
            },
        })
        .unwrap();
    let writer = builder.reserve_node_id().unwrap();
    let mut fields = relation_fields(&mut builder, writer, false).into_vec();
    for channel in 0..2 {
        let value = builder
            .add_value(
                binary(true),
                ValueOrigin::WriterDerived {
                    writer_node: writer,
                    kind: WriterDerivedKind::RelationAuxiliary,
                },
            )
            .unwrap();
        fields.push(WriterRelationField {
            value,
            name: format!("auxiliary_channel_{channel}").into(),
            ty: binary(true),
            role: WriterRelationFieldRole::Auxiliary,
        });
    }
    let inputs = [ty(DataType::Int64, false), ty(DataType::Utf8, false)];
    let partial = AggregatePhase::Partial {
        sequence: AggregateSequenceId::new(1),
    };
    let partial_aggregates = (0..2)
        .map(|channel| WriterAggregateCall {
            input: values[channel],
            binding: theta_binding(&catalog, &inputs[channel], partial),
            output: fields[4 + channel].value,
        })
        .collect::<Box<[_]>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: writer,
            inputs: Box::from([source]),
            required_inputs: Box::from([property(Distribution::Singleton)]),
            output_properties: property(Distribution::Unconstrained),
            output: OutputPort {
                node: writer,
                columns: fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableWriter {
                target: WriterTarget {
                    handle: draft.payload().clone(),
                    write_target_ordinal: ordinal,
                    input: values.clone().into_boxed_slice(),
                    required_distribution: Distribution::Singleton,
                    target_fields: draft
                        .input()
                        .fields_iter()
                        .zip(&values)
                        .map(|(binding, value)| WriterTargetField {
                            provider_name: binding.field().name().clone().into(),
                            token: binding.token(),
                            input: *value,
                            ty: binding
                                .execution_type()
                                .expect("provider complete target declaration")
                                .clone(),
                            hidden: false,
                        })
                        .collect(),
                    output_schema: WriterRelationSchema {
                        revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: fields.clone().into_boxed_slice(),
                    },
                    partial_aggregates: partial_aggregates.clone(),
                },
            },
        })
        .unwrap();
    let producer = with_requests(
        builder
            .finish_definition(
                writer,
                FragmentSink::Stream { edge: EDGE },
                dop_domain(),
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
        partial_aggregates.iter().enumerate().map(|(call, item)| {
            (
                PhysicalCallSite::WriterPartial {
                    node: writer,
                    call: call as u32,
                },
                &item.binding,
            )
        }),
    );

    let mut builder = FragmentBuilder::new(FINISH_FRAGMENT);
    let exchange = builder.reserve_node_id().unwrap();
    let imported = fields
        .iter()
        .map(|field| {
            let value = builder
                .add_value(
                    field.ty.clone(),
                    ValueOrigin::ExchangeImport {
                        edge: EDGE,
                        source_value: field.value,
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: field.name.clone(),
                ty: field.ty.clone(),
                role: field.role,
            }
        })
        .collect::<Box<[_]>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: exchange,
                columns: imported.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::ExchangeSource {
                edge: EDGE,
                imports: fields
                    .iter()
                    .zip(&imported)
                    .map(|(a, b)| (a.value, b.value))
                    .collect(),
            },
        })
        .unwrap();
    let finish = builder.reserve_node_id().unwrap();
    let outputs = relation_fields(&mut builder, finish, true);
    let final_phase = AggregatePhase::Final {
        sequence: AggregateSequenceId::new(1),
    };
    let final_aggregates = (0..2)
        .map(|channel| {
            let binding = theta_binding(&catalog, &inputs[channel], final_phase);
            let output = builder
                .add_value(
                    binding.function.result_type.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: finish,
                        kind: WriterDerivedKind::RelationAuxiliary,
                    },
                )
                .unwrap();
            WriterAggregateCall {
                input: imported[4 + channel].value,
                binding,
                output,
            }
        })
        .collect::<Box<[_]>>();
    let grouping_output = builder
        .add_value(
            ty(DataType::Int32, false),
            ValueOrigin::WriterDerived {
                writer_node: finish,
                kind: WriterDerivedKind::GroupingKey,
            },
        )
        .unwrap();
    let mappings = (0..2u32)
        .map(|channel| {
            let blob = builder
                .add_expression(
                    finish,
                    ty(DataType::Utf8, false),
                    ExprKind::Literal(LiteralValue::Utf8(BLOB_TYPE.into())),
                )
                .unwrap();
            WriterGroupedUnpivotMapping {
                write_target_ordinal: ordinal,
                input: final_aggregates[channel as usize].output,
                constants: Box::from([
                    UnpivotConstant::Int32List(ConstantReference {
                        pool: LIST_POOL,
                        ordinal: channel,
                    }),
                    UnpivotConstant::Scalar(blob),
                    UnpivotConstant::Utf8Map(ConstantReference {
                        pool: MAP_POOL,
                        ordinal: 0,
                    }),
                ]),
            }
        })
        .collect::<Box<[_]>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: finish,
            inputs: Box::from([exchange]),
            required_inputs: Box::from([property(Distribution::Singleton)]),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: finish,
                columns: outputs.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableFinish(WriterFinishSpec {
                count_mode: novarocks_physical_plan::FinishCountMode::WriterReported,
                inputs: novarocks_physical_plan::FinishInputs {
                    writer_results: 0,
                    logical_summary: None,
                },
                logical_summary: None,
                expected_target_ordinals: Box::from([ordinal]),
                input_schema: WriterRelationSchema {
                    revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: imported.clone(),
                },
                output_schema: WriterRelationSchema {
                    revision: ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: outputs.clone(),
                },
                final_aggregates: final_aggregates.clone(),
                grouped_unpivot: Some(WriterGroupedUnpivotSpec {
                    statistics_target_ordinals: Box::from([ordinal]),
                    grouping_input: imported[1].value,
                    grouping_output,
                    passthrough_output: outputs[1].value,
                    value_output: outputs[6].value,
                    literal_outputs: Box::from([
                        outputs[4].value,
                        outputs[5].value,
                        outputs[7].value,
                    ]),
                    mappings,
                    max_output_rows: 1024,
                    max_output_bytes: 1 << 20,
                }),
            }),
        })
        .unwrap();
    let consumer = with_requests(
        builder
            .finish_definition(
                finish,
                FragmentSink::Result,
                dop_domain(),
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
        final_aggregates.iter().enumerate().map(|(call, item)| {
            (
                PhysicalCallSite::WriterFinal {
                    node: finish,
                    call: call as u32,
                },
                &item.binding,
            )
        }),
    );

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([2; 16]).unwrap());
    for (id, pool) in pools() {
        plan.insert_constant_pool(id, pool).unwrap();
    }
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(Edge {
        role: novarocks_physical_plan::OutputRole::WriterResults,
        delivery: novarocks_physical_plan::DeliveryRequirement::MustDrain,
        id: EDGE,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: WRITER_FRAGMENT,
            projection: fields.iter().map(|field| field.value).collect(),
        },
        destination: EdgeDestination {
            fragment: FINISH_FRAGMENT,
            node: exchange,
            receive_mapping: fields
                .iter()
                .zip(&imported)
                .map(|(a, b)| (a.value, b.value))
                .collect(),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Singleton,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Singleton,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: FINISH_FRAGMENT,
        output: OutputPort {
            node: finish,
            columns: outputs.iter().map(|field| field.value).collect(),
        },
        fields: outputs
            .iter()
            .map(|field| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: field.name.clone(),
                alias: None,
                value: field.value,
                ty: field.ty.clone(),
            })
            .collect(),
    })
    .unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the statistics plan validates: {error:?}"));

    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let (root_uses, frozen) = freeze_writer_calls(fragment, &catalog);
        calls.insert(id, frozen);
        uses.insert(id, root_uses);
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
        );
        admissions.insert(id, package_admission());
    }
    let mut packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::from([(ordinal, draft)]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap_or_else(|error| panic!("the statistics packages extract: {error:?}"));
    StatisticsFixture {
        writer: Arc::new(packages.remove(&WRITER_FRAGMENT).unwrap()),
        finish: Arc::new(packages.remove(&FINISH_FRAGMENT).unwrap()),
        writer_node: writer,
        finish_node: finish,
        catalog,
    }
}

/// Attach the original request of every writer call to its fragment.
fn with_requests<'a>(
    fragment: Fragment,
    sites: impl Iterator<Item = (PhysicalCallSite, &'a AggregateBinding)>,
) -> Fragment {
    let mut entries = fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(definition, request)| (*definition, request.clone()))
        .collect::<Vec<_>>();
    for (site, binding) in sites {
        entries.push((
            PhysicalCallDefinition::Relational(site),
            theta_request(binding),
        ));
    }
    fragment
        .with_call_requests_observed(entries, &FixtureControl)
        .unwrap()
}

/// Root uses and frozen writer calls of one fragment, numbered as the
/// frontend numbers them: every expression root is a leaf use in domain 0;
/// each writer call then takes its relational domain and use, followed by
/// the domain and use the frontend mints for the call's materialized input.
/// That input occurrence is a frontend loan only: it enters no flow use.
fn freeze_writer_calls(
    fragment: &Fragment,
    catalog: &PureEngineFunctionCatalog,
) -> (PhysicalRootUses, FrozenFragmentCalls) {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = ExpressionUseId::new(u32::try_from(ordinal).unwrap());
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain: EvaluationDomainId::new(0),
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, id));
    }
    let mut next_use = u32::try_from(uses.len()).unwrap();
    let mut domains = vec![ExpressionEvaluationDomain {
        id: EvaluationDomainId::new(0),
        parent: None,
        guard: None,
    }];
    let mut sites = Vec::new();
    for node in fragment.nodes().values() {
        let (calls, partial): (&[WriterAggregateCall], bool) = match &node.kind {
            NodeKind::TableWriter { target } => (&target.partial_aggregates, true),
            NodeKind::TableFinish(spec) => (&spec.final_aggregates, false),
            _ => continue,
        };
        for (call, item) in calls.iter().enumerate() {
            let site = if partial {
                PhysicalCallSite::WriterPartial {
                    node: node.id,
                    call: call as u32,
                }
            } else {
                PhysicalCallSite::WriterFinal {
                    node: node.id,
                    call: call as u32,
                }
            };
            let mut context = || {
                let context = ExpressionEffectContext {
                    use_id: ExpressionUseId::new(next_use),
                    domain: EvaluationDomainId::new(domains.len() as u32),
                    demand: EvaluationDemand::Value,
                };
                domains.push(ExpressionEvaluationDomain {
                    id: context.domain,
                    parent: None,
                    guard: None,
                });
                next_use += 1;
                context
            };
            let relational = context();
            let input = context();
            sites.push((site, item, relational, input));
        }
    }
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let root_uses = PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let frozen = sites
        .into_iter()
        .map(|(site, item, context, input)| {
            let binding = &item.binding;
            let selected = selection(binding);
            let request = theta_request(binding);
            let [StaticFunctionArgument::Value { value_type, .. }] = request.arguments.as_ref()
            else {
                unreachable!()
            };
            let arguments = [StaticFunctionArgument::Value {
                value_type: value_type.clone(),
                constant: None,
            }];
            let state_type = fragment.values()[&item.input].ty.clone();
            let argument_uses = [Some(input.use_id)];
            let partial = matches!(site, PhysicalCallSite::WriterPartial { .. });
            let phase = if partial {
                AggregateKernelPhase::Partial
            } else {
                AggregateKernelPhase::Final
            };
            let token = catalog
                .prepare_fresh(
                    CallEffectInput {
                        context,
                        argument_uses: if partial {
                            CallArgumentUses::SelectedChannels(&argument_uses)
                        } else {
                            CallArgumentUses::AggregateMerge {
                                phase,
                                state_context: input,
                                state_input_type: &state_type,
                            }
                        },
                        function_id: &binding.function.function_id,
                        kind: FunctionKind::Aggregate,
                        selected: selected.as_ref(),
                        request: FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: 1,
                            expected_result_type: None,
                        },
                        environment: &[],
                        parameters: &parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        proof_scope: CallProofScope::Domain(context.domain),
                    },
                    Arc::clone(&selected),
                    PureCallPreparation::Aggregate {
                        arguments: ScopedExpressionEffects::primitive(
                            context,
                            ExpressionEffects::PURE_VALUE,
                        ),
                        options: AggregatePreparationOptions {
                            state_interpretation: None,
                            phase,
                            distinct: false,
                            order_keys: Arc::from([]),
                            state_input_type: (!partial).then(|| state_type.clone()),
                        },
                    },
                    &FixtureControl,
                )
                .unwrap_or_else(|error| panic!("the frontend prepares {site:?}: {error}"));
            FrozenPhysicalCall {
                materialized_writer_input: Some(input),
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site,
                context,
                effects: token.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            }
        })
        .collect();
    let calls = FrozenFragmentCalls::try_new(fragment, &root_uses, frozen, &FixtureControl)
        .unwrap_or_else(|error| panic!("frozen writer calls validate: {error}"));
    (root_uses, calls)
}

fn compile_with(
    package: &Arc<FragmentPackage>,
    catalog: &PureEngineFunctionCatalog,
    pipeline_dop: usize,
    root_sink_dop: Option<usize>,
) -> Result<LocalProgram, FragmentCompileError> {
    let validated =
        validate_fragment_providers(Arc::clone(package), &providers(), &FixtureControl).unwrap();
    compile_fragment(
        validated,
        catalog,
        options(pipeline_dop, root_sink_dop),
        &FixtureControl,
    )
}

/// Each partial call reads the projected field of its target input value and
/// writes its auxiliary multiplex channel; each is prepared at its writer
/// site in its Partial phase, under the frozen relational context.
#[test]
fn writer_partial_calls_read_the_projection_and_write_their_auxiliary_channels() {
    let fixture = statistics_fixture();
    for dop in [1, 4] {
        let program = compile_with(&fixture.writer, &fixture.catalog, dop, None)
            .unwrap_or_else(|error| panic!("the statistics writer compiles: {error}"));
        let graph = program.graph();
        let root = graph.root();
        let ProgramNodeKind::TableWriter {
            projection,
            writer_multiplex_layout,
            partial_aggregates,
            ..
        } = graph.nodes()[root.index()].kind()
        else {
            panic!("the writer fragment roots at its TableWriter");
        };
        assert_eq!(
            names(writer_multiplex_layout),
            [
                "kind",
                "write_target_ordinal",
                "row_count",
                "commit_fragment",
                "auxiliary_channel_0",
                "auxiliary_channel_1"
            ]
        );
        assert_eq!(partial_aggregates.len(), 2);
        for (channel, call) in partial_aggregates.iter().enumerate() {
            assert_eq!(call.input_slot_id, projection.layout.slots()[channel]);
            assert_eq!(
                call.intermediate_slot_id,
                writer_multiplex_layout.slots()[4 + channel]
            );
            assert_eq!(
                call.resolved.state_format.as_str(),
                ICEBERG_THETA_STATE_FORMAT_IDENTITY
            );
            assert_eq!(call.resolved.intermediate_type, DataType::Binary);
            let site = ProgramCallSite::WriterPartial {
                node: root,
                call: channel as u32,
            };
            let Some(ProgramStateTemplate::Aggregate { kernel, .. }) = program.state_template(site)
            else {
                panic!("partial call {channel} has its prepared aggregate");
            };
            assert_eq!(kernel.contract().phase(), AggregateKernelPhase::Partial);
        }
    }
}

/// The actual source fixture authors its call and Value root domains once.
/// Compilation consumes that exact receipt instead of rebuilding a context.
#[test]
fn a_writer_call_input_is_its_own_materialized_occurrence_and_the_frozen_preparation_accepts_it() {
    let fixture = statistics_fixture();
    for (package, phase) in [
        (&fixture.writer, AggregateKernelPhase::Partial),
        (&fixture.finish, AggregateKernelPhase::Final),
    ] {
        let root_sink = (phase == AggregateKernelPhase::Final).then_some(1);
        let program = compile_with(package, &fixture.catalog, 1, root_sink)
            .unwrap_or_else(|error| panic!("the statistics fragment compiles: {error}"));
        let frozen = package
            .calls()
            .entries()
            .values()
            .map(|call| call.context)
            .collect::<Vec<_>>();
        assert_eq!(frozen.len(), 2);
        let resolved = program.checked().channels().expressions().resolved_calls();
        let mut prepared = 0;
        for (site, call) in resolved.calls() {
            if !matches!(
                site,
                ProgramCallSite::WriterPartial { .. } | ProgramCallSite::WriterFinal { .. }
            ) {
                continue;
            }
            prepared += 1;
            let context = call.call_contract().context();
            assert!(
                frozen.contains(&context),
                "{site:?} keeps its frozen context"
            );
            assert_eq!(
                call.specialization().source(),
                PurePreparationSource::Frozen
            );
            let original = package
                .calls()
                .entries()
                .values()
                .find(|source| source.context == context)
                .unwrap();
            let input = original
                .materialized_writer_input
                .expect("actual writer Value receipt");
            assert!(
                package
                    .expression_uses()
                    .flow()
                    .domains()
                    .contains_key(&input.domain)
            );
            assert_ne!(input.use_id, context.use_id);
            assert_ne!(input.domain, context.domain);
            assert_eq!(input.demand, EvaluationDemand::Value);
            let flow = &resolved.snapshot().flows()
                [&novarocks_local_program::ProgramExpressionArena::Main];
            assert!(!flow.uses().contains_key(&context.use_id));
        }
        assert_eq!(prepared, 2);
    }
}

/// Each final call merges its auxiliary receiver channel into a fresh
/// internal final channel; the grouped Unpivot groups by the receiver's
/// target ordinal, passes it through to the Root's, and expands each target
/// mapping's final value with its list, blob-type root and map constants.
#[test]
fn finish_final_calls_and_the_grouped_unpivot_lower_onto_their_relations() {
    let fixture = statistics_fixture();
    let program = compile_with(&fixture.finish, &fixture.catalog, 1, Some(1))
        .unwrap_or_else(|error| panic!("the statistics finish compiles: {error}"));
    let graph = program.graph();
    let root = graph.root();
    let ProgramNodeKind::TableFinish {
        inputs,
        writer_multiplex_layout,
        root_result_layout,
        final_aggregates,
        ..
    } = graph.nodes()[root.index()].kind()
    else {
        panic!("the finish fragment roots at its TableFinish");
    };
    let receiver = graph.nodes()[inputs[0].index()].output_layout();
    assert_eq!(writer_multiplex_layout.slots(), receiver.slots());
    let unpivot = final_aggregates
        .unpivot
        .as_ref()
        .expect("a grouped Unpivot");
    let mut internal = Vec::new();
    for (channel, call) in final_aggregates.calls.iter().enumerate() {
        assert_eq!(
            call.intermediate_input_slot_id,
            receiver.slots()[4 + channel]
        );
        internal.push(call.final_output_slot_id);
        let site = ProgramCallSite::WriterFinal {
            node: root,
            call: channel as u32,
        };
        let Some(ProgramStateTemplate::Aggregate { kernel, .. }) = program.state_template(site)
        else {
            panic!("final call {channel} has its prepared aggregate");
        };
        assert_eq!(kernel.contract().phase(), AggregateKernelPhase::Final);
        // The final output is an internal typed channel of the call's result.
        let output = program
            .checked()
            .channels()
            .channel_type(ProgramChannelSite::WriterFinalOutput {
                node: root,
                call: channel as u32,
            })
            .expect("a typed final output channel");
        assert_eq!(output, &binary(false));
    }
    internal.push(unpivot.grouping_output_slot_id);
    // Every internal channel is fresh: no relation slot carries it.
    for slot in &internal {
        assert!(!receiver.slots().contains(slot));
        assert!(!root_result_layout.slots().contains(slot));
    }
    internal.sort();
    internal.dedup();
    assert_eq!(internal.len(), 3);
    let root_slots = root_result_layout.slots();
    assert_eq!(unpivot.grouping_input_slot_id, receiver.slots()[1]);
    assert_eq!(unpivot.passthrough_output_slot_id, root_slots[1]);
    assert_eq!(unpivot.value_output_slot_id, root_slots[6]);
    assert_eq!(
        unpivot.literal_output_slot_ids,
        [root_slots[4], root_slots[5], root_slots[7]]
    );
    assert_eq!(
        (unpivot.max_output_rows, unpivot.max_output_bytes),
        (1024, 1 << 20)
    );
    assert_eq!(unpivot.mappings.len(), 2);
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    for (mapping, lowered) in unpivot.mappings.iter().enumerate() {
        assert_eq!(lowered.grouping_key, 0);
        assert_eq!(
            lowered.input_value_slot_id,
            final_aggregates.calls[mapping].final_output_slot_id
        );
        let [
            LocalConstant::Int32List(fields),
            LocalConstant::Scalar { expr_id, nullable },
            LocalConstant::Utf8Map(properties),
        ] = lowered.constants.as_slice()
        else {
            panic!("mapping {mapping} keeps its list, scalar and map constants");
        };
        assert_eq!(fields, &vec![mapping as i32 + 1]);
        assert!(properties.is_empty());
        assert!(!nullable);
        // The blob type is the Main-arena literal its FinishUnpivotConstant
        // root evaluates.
        let site = ProgramExpressionRootSite::Node {
            node: root,
            role: ProgramNodeExpressionRole::FinishUnpivotConstant {
                mapping: mapping as u32,
                constant: 1,
            },
        };
        assert!(snapshot.bindings().contains_key(&site));
        assert_eq!(snapshot.roots().sites()[&site].definition, *expr_id);
    }
}

/// Writer statistics shapes the compiled path does not own are refused by
/// name before any channel exists: a non-Partial writer call, a partial call
/// over a value outside its target input, more than one logical argument, a
/// non-Final finish call, and a grouped Unpivot mapping for a target that is
/// not one of the finish's statistics targets.
#[test]
fn unsupported_writer_statistics_shapes_are_refused_by_name() {
    let fixture = statistics_fixture();
    let mut control =
        CompileCheckpoints::try_new(&FixtureControl, CompilePhase::LowerProgram).unwrap();
    let dop = NonZeroUsize::new(1).unwrap();
    let recipe =
        validate_fragment_providers(Arc::clone(&fixture.writer), &providers(), &FixtureControl)
            .unwrap();
    let recipe = recipe.writes().get(&fixture.writer_node).cloned();
    let writer = fixture.writer.fragment().nodes()[&fixture.writer_node].clone();
    admit_writer_family(&fixture.writer, &writer, recipe.as_ref(), dop, &mut control).unwrap();
    let refuse = |edit: &dyn Fn(&mut WriterTarget), control: &mut CompileCheckpoints<'_>| {
        let mut writer = writer.clone();
        let NodeKind::TableWriter { target } = &mut writer.kind else {
            unreachable!()
        };
        edit(target);
        unsupported(
            admit_writer_family(&fixture.writer, &writer, recipe.as_ref(), dop, control)
                .unwrap_err(),
        )
    };
    assert_eq!(
        refuse(
            &|target| target.partial_aggregates[0].binding.phase = AggregatePhase::Single,
            &mut control
        ),
        "writer partial aggregate outside its Partial phase"
    );
    assert_eq!(
        refuse(
            &|target| target.partial_aggregates[1].input = target.output_schema.fields[0].value,
            &mut control
        ),
        "writer partial aggregate over a value outside its target input"
    );
    assert_eq!(
        refuse(
            &|target| target.partial_aggregates[0].binding.logical_argument_count = 2,
            &mut control
        ),
        "writer aggregate over other than one logical value argument"
    );

    let finish = fixture.finish.fragment().nodes()[&fixture.finish_node].clone();
    admit_writer_family(&fixture.finish, &finish, None, dop, &mut control).unwrap();
    let refuse = |edit: &dyn Fn(&mut WriterFinishSpec), control: &mut CompileCheckpoints<'_>| {
        let mut finish = finish.clone();
        let NodeKind::TableFinish(spec) = &mut finish.kind else {
            unreachable!()
        };
        edit(spec);
        unsupported(admit_writer_family(&fixture.finish, &finish, None, dop, control).unwrap_err())
    };
    assert_eq!(
        refuse(
            &|spec| {
                spec.final_aggregates[0].binding.phase = AggregatePhase::Partial {
                    sequence: AggregateSequenceId::new(1),
                }
            },
            &mut control
        ),
        "writer final aggregate outside its Final phase"
    );
    assert_eq!(
        refuse(
            &|spec| spec.final_aggregates[0].input = spec.input_schema.fields[2].value,
            &mut control
        ),
        "writer final aggregate over a value outside its auxiliary writer channels"
    );
    assert_eq!(
        refuse(
            &|spec| {
                spec.grouped_unpivot.as_mut().unwrap().mappings[1].write_target_ordinal =
                    WriteTargetOrdinal::try_new(5).unwrap()
            },
            &mut control
        ),
        "writer grouped Unpivot mapping for a target outside the finish's statistics targets"
    );
    control.finish().unwrap();
}

#[test]
fn actual_writer_compilation_accepts_root_null_obligation_and_refuses_nested_binding_drift() {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    fn nested(json: bool, child_nullable: bool) -> FunctionValueType {
        let storage = ValueTypeStorageLifetime::pure();
        let child = ValueTypeRef::try_new(
            if json {
                ValueKind::Nominal(ValueLogicalType::Json)
            } else {
                ValueKind::String
            },
            child_nullable,
            storage.clone(),
        )
        .unwrap();
        let value = ValueTypeRef::try_new(
            ValueKind::Struct(Box::from([ValueField::new(
                "payload",
                child,
                storage.clone(),
            )])),
            true,
            storage.clone(),
        )
        .unwrap();
        let layout = ValueLayout::try_new(
            &DataType::Struct(vec![Field::new("payload", DataType::Utf8, child_nullable)].into()),
            storage,
        )
        .unwrap();
        FunctionValueType::try_from_value(value, layout).unwrap()
    }
    let source = [
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Utf8, false),
    ];
    let fixture = fixture_with_types(source, draft()).unwrap();
    let program = compile(&fixture.writer, 1, None).unwrap();
    let root = program.graph().root();
    let ProgramNodeKind::TableWriter { projection, .. } =
        program.graph().nodes()[root.index()].kind()
    else {
        panic!("actual writer")
    };
    assert!(projection.layout.value_binding_at(0).unwrap().nullable());
    assert!(
        !program.write_recipes()[&root]
            .draft()
            .input()
            .fields_iter()
            .next()
            .unwrap()
            .execution_type()
            .unwrap()
            .nullable()
    );
    for (actual, expected) in [
        (nested(false, true), nested(true, true)),
        (nested(true, false), nested(true, true)),
    ] {
        let declared = draft_with_types([FunctionValueType::new(DataType::Int64, false), expected]);
        let actual_source = [FunctionValueType::new(DataType::Int64, false), actual];
        match fixture_with_types(actual_source, declared) {
            Err(cause) => assert!(
                cause.to_string().contains("type") || cause.to_string().contains("binding"),
                "{cause}"
            ),
            Ok(fixture) => assert!(
                compile(&fixture.writer, 1, None).is_err(),
                "writer must refuse complete nested domain/NULL drift"
            ),
        }
    }
}

#[path = "writer_lowering_tests/change_stream.rs"]
mod change_stream;
