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

//! `INSERT INTO t VALUES (1, 'a'), (2, 'b')` without statistics, run end to
//! end in process through compiled operators only: the writer fragment's
//! compiled Values feed the compiled TableWriter, whose writer relation the
//! compiled stream sink encodes into real exchange frames; a loopback
//! transmitter delivers them to the finish fragment's positional receiver,
//! and the compiled TableFinish publishes the Root relation to its Result
//! sink. The provider is a recording write execution, not an installed one.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, BinaryArray, Int8Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_connector_contract::{
    ConnectorCodecCategory, ConnectorCodecRevision, ConnectorEncodedPayload,
    ConnectorEnvelopeHeader, ConnectorError, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
    ConnectorWriteInputShape, ConnectorWriteRecipeCompiler, ConnectorWriteRecipeDraft,
    PureProviderCompileError, PureProviderManifestEntry, PureProviderProgramCatalog,
    PureProviderProgramDefinition,
};
use novarocks_functions::ConstantPolicy;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram, ProgramNodeId, ProgramNodeKind};
use novarocks_physical_plan::{
    Distribution, Edge, EdgeDestination, EdgeId, EdgeKind, EdgePartitioning, EdgeSource, ExprKind,
    Fragment, FragmentBuilder, FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, LiteralValue, NodeId, NodeKind, OutputPort,
    PhysicalExpressionRoots, PhysicalNode, PhysicalPlan, PhysicalProperties, PhysicalRootUses,
    PipelineDopDomain, PlanBuilder, PlanLimits, PlanVersionId, PropertyProofProjectionLimits,
    ROOT_WRITE_RESULT_SCHEMA_REVISION, ResultField, ResultPort, RowMultiplicity, ValueOrigin,
    WRITER_MULTIPLEX_SCHEMA_REVISION, WriteTargetOrdinal, WriterDerivedKind, WriterFinishSpec,
    WriterRelationField, WriterRelationFieldRole, WriterRelationSchema, WriterTarget,
    WriterTargetField, extract_fragment_packages,
};
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution, root_write_result_schema, writer_output_schema,
};
use novarocks_spi::connector::{CatalogHandle, ConnectorErrorKind};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, PureCompileControl,
};
use novarocks_types::UniqueId;

use crate::exec::chunk::Chunk;
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::node::table_finish::TableFinishRuntimeBinding;
use crate::exec::node::table_write_relation::ConnectorCommitFragmentCarrierValidator;
use crate::exec::node::table_writer::{
    TableWriterPhysicalContextTemplate, TableWriterRuntimeBinding,
};
use crate::exec::operators::table_writer::tests::{
    TestFragmentEncoder, adapter, catalog_handle, commit_fragment, request_context, writer_handle,
};
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution_with_profiler;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::fragment::CompiledWriterBindings;
use crate::runtime::fragment::exchange::{
    CompiledExchangeReceivers, materialize_compiled_exchange_receivers,
};
use crate::runtime::fragment::instance::{
    ExchangeInputAssignment, ExchangeInputAssignments, FragmentSinkAssignment,
};
use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
use crate::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, ExchangeReceiverFrame, ExchangeReceiverKey,
    ExchangeReceiverPort, ExchangeTransmitRejection, FragmentIoError, FragmentIoErrorKind,
    FragmentIoOperation, NoopFragmentEventSink,
};
use crate::runtime::fragment::sink::materialize_compiled_sink;
use crate::runtime::runtime_state::RuntimeState;

const WRITER_FINST: UniqueId = UniqueId::new(0x81, 0x01);
const FINISH_FINST: UniqueId = UniqueId::new(0x82, 0x02);
const EDGE: EdgeId = EdgeId::new(9);

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
}

fn property(distribution: Distribution) -> PhysicalProperties {
    PhysicalProperties {
        distribution,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 4,
        requires_power_of_two: false,
    }
}

/// The provider's own input fields: a NOT NULL `c1` and a nullable `c2`.
fn target_fields() -> Vec<Field> {
    vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Utf8, true),
    ]
}

/// The recipe of the test provider's writer handle: its exact binding, a
/// handle payload under that binding, and the provider's input fields.
fn draft() -> ConnectorWriteRecipeDraft {
    let binding = adapter().binding().clone();
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        bytes::Bytes::from_static(b"writer-handle"),
    );
    ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        ConnectorWriteInputShape::Data {
            fields: target_fields()
                .into_iter()
                .zip([ty(DataType::Int64, false), ty(DataType::Utf8, true)])
                .enumerate()
                .map(|(ordinal, (field, execution))| {
                    // These are the fixed provider's independent value
                    // declarations; the original source Field carries no
                    // semantic authority for the complete typed projection.
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

/// The writer multiplex relation (`root == false`) or the Root result
/// relation (`root == true`): the SPI's own schema, field by field.
fn relation_fields(
    builder: &mut FragmentBuilder,
    owner: NodeId,
    root: bool,
) -> Box<[WriterRelationField]> {
    let schema = if root {
        root_write_result_schema()
    } else {
        writer_output_schema()
    };
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let (role, kind) = match index {
                0 => (
                    WriterRelationFieldRole::Kind,
                    WriterDerivedKind::RelationKind,
                ),
                1 => (
                    WriterRelationFieldRole::TargetOrdinal,
                    WriterDerivedKind::WriteTargetOrdinal,
                ),
                2 => (
                    WriterRelationFieldRole::RowCount,
                    WriterDerivedKind::AffectedRows,
                ),
                3 => (
                    WriterRelationFieldRole::CommitFragment,
                    WriterDerivedKind::CommitFragment,
                ),
                _ => (
                    WriterRelationFieldRole::Auxiliary,
                    WriterDerivedKind::RelationAuxiliary,
                ),
            };
            let ty = ty(field.data_type().clone(), field.is_nullable());
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
                name: field.name().as_str().into(),
                ty,
                role,
            }
        })
        .collect()
}

/// The two-fragment plan of `INSERT INTO t VALUES <rows>` without statistics.
/// A `None` cell is a NULL literal of a nullable BIGINT.
fn plan(rows: &[(Option<i64>, &str)]) -> (PhysicalPlan, NodeId) {
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let draft = draft();
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let source = builder.reserve_node_id().unwrap();
    let nullable = rows.iter().any(|(value, _)| value.is_none());
    let types = [ty(DataType::Int64, nullable), ty(DataType::Utf8, false)];
    let values = types
        .iter()
        .enumerate()
        .map(|(ordinal, value_type)| {
            builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let cells = rows
        .iter()
        .map(|(int, text)| {
            let int = builder
                .add_expression(
                    source,
                    types[0].clone(),
                    ExprKind::Literal(match int {
                        Some(value) => LiteralValue::Int64(*value),
                        None => LiteralValue::Null,
                    }),
                )
                .unwrap();
            let text = builder
                .add_expression(
                    source,
                    types[1].clone(),
                    ExprKind::Literal(LiteralValue::Utf8((*text).into())),
                )
                .unwrap();
            Box::from([int, text])
        })
        .collect::<Box<[_]>>();
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
            kind: NodeKind::Values { rows: cells },
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
                            ty: ty(
                                binding.field().data_type().clone(),
                                binding.field().is_nullable(),
                            ),
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
    let producer = builder
        .finish_definition(
            writer,
            FragmentSink::Stream { edge: EDGE },
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut builder = FragmentBuilder::new(FragmentId::new(2));
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
    let mapping = fields
        .iter()
        .zip(&imported)
        .map(|(a, b)| (a.value, b.value))
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
                imports: mapping.clone(),
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
                    fields: imported,
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
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([8; 16]).unwrap());
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(Edge {
        role: novarocks_physical_plan::OutputRole::WriterResults,
        delivery: novarocks_physical_plan::DeliveryRequirement::MustDrain,
        id: EDGE,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: FragmentId::new(1),
            projection: fields.iter().map(|field| field.value).collect(),
        },
        destination: EdgeDestination {
            fragment: FragmentId::new(2),
            node: exchange,
            receive_mapping: mapping,
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
        fragment: FragmentId::new(2),
        output: OutputPort {
            node: finish,
            columns: outputs.iter().map(|field| field.value).collect(),
        },
        fields: outputs
            .iter()
            .map(|field| ResultField {
                domain: crate::test_result_domain::result_value_domain(&field.ty),
                name: field.name.clone(),
                alias: None,
                value: field.value,
                ty: field.ty.clone(),
            })
            .collect(),
    })
    .unwrap();
    (plan.finish().unwrap(), exchange)
}

/// Literal cells are the only roots; each gets one eager use.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = ExpressionUseId::new(u32::try_from(ordinal).unwrap());
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain,
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
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

// Explicit small-fixture admission; these are test inputs, not defaults.
fn admission() -> FragmentPackageAdmission {
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

fn packages(plan: &PhysicalPlan) -> BTreeMap<FragmentId, FragmentPackage> {
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
        admissions.insert(id, admission());
    }
    extract_fragment_packages(
        plan,
        &BTreeMap::new(),
        &BTreeMap::from([(WriteTargetOrdinal::try_new(0).unwrap(), draft())]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap()
}

/// Keeps the draft: a pure contract fixture, not the installed Iceberg port.
struct KeepDraft;
impl ConnectorWriteRecipeCompiler for KeepDraft {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        _: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<ConnectorError>> {
        Ok(draft.clone())
    }
}

fn compile(package: FragmentPackage, pipeline_dop: usize, root_sink: bool) -> Arc<LocalProgram> {
    let provider = adapter().binding().descriptor().provider_id.clone();
    let providers = PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            false,
            true,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            None,
            Some(Arc::new(KeepDraft)
                as Arc<
                    dyn ConnectorWriteRecipeCompiler<Error = ConnectorError>,
                >),
        )],
        &FixtureControl,
    )
    .unwrap();
    let validated =
        validate_fragment_providers(Arc::new(package), &providers, &FixtureControl).unwrap();
    Arc::new(
        compile_fragment(
            validated,
            &crate::exec::expr::compiled_program::tests::rng_subset(),
            LocalCompileOptions {
                pipeline_dop: NonZeroUsize::new(pipeline_dop).unwrap(),
                root_sink_dop: root_sink.then(|| NonZeroUsize::new(1).unwrap()),
                kernel_abi: KernelAbiVersion::CURRENT,
                // Explicit fixture admission; not production defaults.
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
                exchange_wait: Duration::from_secs(120),
            },
            &FixtureControl,
        )
        .unwrap_or_else(|error| panic!("fixture fragment compiles: {error}")),
    )
}

/// Records every page the provider writer accepts and stages one artifact
/// per writer.
#[derive(Default)]
struct Recorded {
    opened: usize,
    batches: Vec<RecordBatch>,
}

struct RecordingExecution {
    catalog_handle: CatalogHandle,
    recorded: Arc<Mutex<Recorded>>,
}

#[async_trait::async_trait]
impl ConnectorWriteExecution for RecordingExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    async fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        assert_eq!(request.physical.writer_ordinal(), 0);
        assert_eq!(
            request.expected_schema.as_ref(),
            &Schema::new(target_fields()),
            "the writer opens over the provider's exact input schema"
        );
        self.recorded.lock().unwrap().opened += 1;
        Ok(Box::new(RecordingWriter {
            recorded: Arc::clone(&self.recorded),
        }))
    }
}

struct RecordingWriter {
    recorded: Arc<Mutex<Recorded>>,
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for RecordingWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.recorded.lock().unwrap().batches.push(batch);
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        Ok(vec![commit_fragment(b"staged".to_vec())])
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Accepts every canonical carrier; the real one belongs to the backend.
struct AcceptCarriers;
impl ConnectorCommitFragmentCarrierValidator for AcceptCarriers {
    fn validate(&self, _: WriteTargetOrdinal, encoded: &[u8]) -> Result<(), ConnectorError> {
        if encoded.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "empty carrier",
            ));
        }
        Ok(())
    }
}

/// Delivers every encoded frame to the receiver port, as the native data
/// plane does after its wire hop.
struct LoopbackTransmitter {
    port: Arc<dyn ExchangeReceiverPort>,
}

impl ExchangeFrameTransmitter for LoopbackTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
        self.port
            .push(
                ExchangeReceiverKey {
                    fragment_instance_id: frame.destination_fragment_instance_id,
                    node_id: frame.destination_node_id,
                },
                ExchangeReceiverFrame {
                    stream: frame.stream.unwrap_or_else(|| {
                        crate::runtime::fragment::io::exchange::fixture_stream(
                            frame.sender_fragment_instance_id,
                            frame.destination_fragment_instance_id,
                            frame.destination_node_id,
                            frame.sender_ordinal,
                        )
                    }),
                    source_fragment_instance_id: frame.sender_fragment_instance_id,
                    sender_ordinal: frame.sender_ordinal,
                    sender_count: frame.sender_count,
                    sender_id: frame.sender_id,
                    backend_number: frame.backend_number,
                    sequence: frame.sequence,
                    eos: frame.eos,
                    payload: frame.payload,
                },
            )
            .map_err(|error| {
                ExchangeTransmitRejection::Failed(FragmentIoError::new(
                    FragmentIoOperation::ExchangeTransmit,
                    FragmentIoErrorKind::RemoteRejected,
                    error.to_string(),
                ))
            })
    }
}

fn runtime_state() -> Arc<RuntimeState> {
    Arc::new(RuntimeState::for_local_execution(
        None,
        None,
        None,
        None,
        None,
        Some(crate::runtime::mem_tracker::MemTracker::new_root(
            "compiled writer tests",
        )),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ))
}

fn root_of(program: &LocalProgram) -> ProgramNodeId {
    program.graph().root()
}

fn writer_bindings(
    program: &LocalProgram,
    recorded: &Arc<Mutex<Recorded>>,
) -> CompiledWriterBindings {
    let mut bindings = CompiledWriterBindings::default();
    bindings
        .bind_writer(
            root_of(program),
            TableWriterRuntimeBinding::try_new(
                writer_handle(),
                Arc::new(RecordingExecution {
                    catalog_handle: catalog_handle(),
                    recorded: Arc::clone(recorded),
                }),
                TableWriterPhysicalContextTemplate::new([1; 16], 1, [2; 16], 0),
                request_context(),
                Arc::new(TestFragmentEncoder),
            )
            .unwrap(),
        )
        .unwrap();
    bindings
}

fn finish_bindings(program: &LocalProgram) -> CompiledWriterBindings {
    let mut bindings = CompiledWriterBindings::default();
    bindings
        .bind_finish(
            root_of(program),
            TableFinishRuntimeBinding::new(Arc::new(AcceptCarriers)),
        )
        .unwrap();
    bindings
}

fn run(
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    exchange: ExchangeBindings,
    writers: CompiledWriterBindings,
    instance: UniqueId,
    pipeline_dop: i32,
) -> Result<(), String> {
    prepare_compiled_program_pipeline_execution_with_profiler(
        Arc::clone(program),
        Duration::from_millis(10),
        sink,
        exchange,
        crate::exec::pipeline::binding::ScanBindings::default(),
        writers,
        Some((instance.high(), instance.low())),
        None,
        pipeline_dop,
        runtime_state(),
        Arc::new(NoopFragmentEventSink),
    )
    .map_err(|error| error.to_string())?
    .start()
    .join()
    .map(|_| ())
    .map_err(|error| error.to_string())
}

struct Insert {
    writer: Arc<LocalProgram>,
    finish: Arc<LocalProgram>,
    receiver: NodeId,
}

fn insert(rows: &[(Option<i64>, &str)], writer_dop: usize) -> Insert {
    let (plan, receiver) = plan(rows);
    let mut packages = packages(&plan);
    Insert {
        writer: compile(
            packages.remove(&FragmentId::new(1)).unwrap(),
            writer_dop,
            false,
        ),
        finish: compile(packages.remove(&FragmentId::new(2)).unwrap(), 1, true),
        receiver,
    }
}

/// Run the writer fragment into the finish fragment's receiver and return
/// the writer's outcome; the finish is run only when the writer succeeded.
fn run_insert(
    insert: &Insert,
    writer_dop: i32,
    recorded: &Arc<Mutex<Recorded>>,
) -> Result<Vec<Chunk>, String> {
    let port = in_process_test_exchange_receiver_port();
    let CompiledExchangeReceivers {
        registrations,
        bindings,
    } = materialize_compiled_exchange_receivers(
        &insert.finish,
        FINISH_FINST,
        &ExchangeInputAssignments::new(BTreeMap::from([(
            FragmentNodeId::new(i32::try_from(insert.receiver.get()).unwrap()),
            ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
        )])),
        Arc::clone(&port),
        Some(&crate::runtime::fragment::exchange::test_receiver_creation_factory()),
    )
    .expect("compiled receivers");
    for registration in registrations {
        port.register(registration).expect("register receiver");
    }
    let writer_sink = materialize_compiled_sink(
        &insert.writer,
        &FragmentSinkAssignment::StreamDestinations {
            destinations: vec![
                FragmentDestination::new(
                    FINISH_FINST,
                    RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
                    WRITER_FINST,
                    0,
                    1,
                )
                .expect("destination"),
            ],
            sender_id: None,
        },
        WRITER_FINST,
        Arc::new(LoopbackTransmitter {
            port: Arc::clone(&port),
        }),
        None,
        None,
    )
    .expect("compiled stream sink");
    run(
        &insert.writer,
        writer_sink,
        ExchangeBindings::default(),
        writer_bindings(&insert.writer, recorded),
        WRITER_FINST,
        writer_dop,
    )?;
    let output = ResultSinkHandle::new();
    let finish_sink = materialize_compiled_sink(
        &insert.finish,
        &FragmentSinkAssignment::None,
        FINISH_FINST,
        crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
        Some(Box::new(ResultSinkFactory::new(output.clone()))),
        None,
    )
    .expect("compiled result sink");
    run(
        &insert.finish,
        finish_sink,
        bindings,
        finish_bindings(&insert.finish),
        FINISH_FINST,
        1,
    )?;
    Ok(output.take_chunks())
}

#[test]
fn compiled_insert_values_writes_pages_and_publishes_the_root_relation() {
    let rows = [(Some(1), "a"), (Some(2), "b")];
    let insert = insert(&rows, 1);
    let writer = root_of(&insert.writer);
    let ProgramNodeKind::TableWriter { projection, .. } =
        insert.writer.graph().nodes()[writer.index()].kind()
    else {
        panic!("the compiled root is a TableWriter");
    };
    // The provider keeps its independently signed nullable target declaration.
    let provider_fields = target_fields();
    let provider_types = [ty(DataType::Int64, false), ty(DataType::Utf8, true)];
    let recipe = insert.writer.write_recipes().get(&writer).unwrap();
    assert_eq!(recipe.draft().input().field_count(), provider_fields.len());
    for (ordinal, binding) in recipe.draft().input().fields_iter().enumerate() {
        assert_eq!(binding.field(), &provider_fields[ordinal]);
        assert_eq!(binding.execution_type(), Some(&provider_types[ordinal]));
    }
    // These literal source values are required, independently of target NULL law.
    let source_types = [ty(DataType::Int64, false), ty(DataType::Utf8, false)];
    let execution_schema = Schema::new(vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Utf8, false),
    ]);
    assert_eq!(projection.layout.schema().as_ref(), &execution_schema);
    for (ordinal, source) in source_types.iter().enumerate() {
        assert_eq!(
            projection.layout.value_binding_at(ordinal),
            Some(source.typed_binding())
        );
    }
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let chunks = run_insert(&insert, 1, &recorded).expect("the compiled insert runs");

    // Every page retains that exact complete execution projection.
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.opened, 1, "one driver opens one writer");
    let mut written = Vec::new();
    for batch in &recorded.batches {
        assert_eq!(batch.schema().as_ref(), &execution_schema);
        let c1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c2 = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            written.push((c1.value(row), c2.value(row).to_string()));
        }
    }
    assert_eq!(
        written,
        [(1, "a".to_string()), (2, "b".to_string())],
        "both literal rows are written once, in order"
    );

    // The Root relation is the exact SPI schema, nested child names
    // included, so the frontend's exact-schema decoder reads it: one summary
    // of two rows and one prepared fragment of the writer's staged artifact.
    let mut summary = None;
    let mut fragments = Vec::new();
    for chunk in &chunks {
        assert_eq!(chunk.schema().as_ref(), root_write_result_schema().as_ref());
        let kinds = chunk
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        let counts = chunk
            .record_batch()
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payloads = chunk
            .record_batch()
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for row in 0..chunk.len() {
            match kinds.value(row) {
                1 => summary = Some(counts.value(row)),
                2 => fragments.push(payloads.value(row).to_vec()),
                other => panic!("unexpected Root row kind {other}"),
            }
        }
    }
    assert_eq!(summary, Some(2));
    assert_eq!(fragments, [b"staged".to_vec()]);
}

/// A nullable value feeding a NOT NULL provider field is the writer's row
/// obligation: a NULL row refuses the page before the provider writer
/// accepts anything.
#[test]
fn a_null_into_a_not_null_provider_field_is_refused_before_any_page_is_written() {
    let insert = insert(&[(Some(1), "a"), (None, "b")], 1);
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let error = run_insert(&insert, 1, &recorded).expect_err("the NULL row is refused");
    assert!(
        error.contains("Column 'c1'")
            && error.contains("is declared as non-nullable but contains null values"),
        "{error}"
    );
    assert!(
        recorded.lock().unwrap().batches.is_empty(),
        "no page reaches the provider writer"
    );
}

/// A Task that binds no writer capability, or binds one for a node that is
/// not its writer, is refused before any driver is prepared.
#[test]
fn compiled_writer_bindings_cover_exactly_the_programs_writer_family() {
    let insert = insert(&[(Some(1), "a")], 1);
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    assert!(
        CompiledWriterBindings::default()
            .validate(&insert.writer)
            .unwrap_err()
            .to_string()
            .contains("table writers")
    );
    assert!(
        writer_bindings(&insert.writer, &recorded)
            .validate(&insert.finish)
            .unwrap_err()
            .to_string()
            .contains("table writers")
    );
    assert!(
        CompiledWriterBindings::default()
            .validate(&insert.finish)
            .unwrap_err()
            .to_string()
            .contains("table finishes")
    );
    writer_bindings(&insert.writer, &recorded)
        .validate(&insert.writer)
        .unwrap();
    finish_bindings(&insert.finish)
        .validate(&insert.finish)
        .unwrap();
}

/// Collect-on-write statistics on the compiled path, checked against the
/// independent DataSketches wire oracle and SPI relation metadata: the
/// Iceberg Theta sketch of `c1` (BIGINT NOT NULL) and `c2` (nullable STRING),
/// frozen and prepared exactly as the frontend does.
pub(crate) fn compiled_statistics_finish_fixture()
-> (Arc<LocalProgram>, TableFinishRuntimeBinding, RuntimeState) {
    statistics::finish_owner_fixture()
}

pub(crate) fn compiled_statistics_writer_fixture()
-> (Arc<LocalProgram>, TableWriterRuntimeBinding, RuntimeState) {
    statistics::writer_owner_fixture()
}

mod statistics {
    use crate::exec::pipeline::{
        input::RawInput,
        step::{AdmissionNext, InputResult, NextStep},
        test_driver::TestDriver,
    };

    use super::*;

    pub(super) fn writer_owner_fixture()
    -> (Arc<LocalProgram>, TableWriterRuntimeBinding, RuntimeState) {
        let program = compiled(0, &[], 1).writer;
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let binding = binding(&recorded);
        let state = statistics_state(theta_function_set());
        (program, binding, state)
    }

    pub(super) fn finish_owner_fixture()
    -> (Arc<LocalProgram>, TableFinishRuntimeBinding, RuntimeState) {
        let program = compiled(0, &[], 1).finish;
        let binding = TableFinishRuntimeBinding::new(Arc::new(AcceptCarriers));
        let state = statistics_state(theta_function_set());
        (program, binding, state)
    }

    use std::time::Instant;

    use arrow::array::{ArrayRef, Int32Array, ListArray, MapArray, StructArray};
    use novarocks_connector_iceberg_functions::{
        ICEBERG_THETA_AGGREGATE_NAME, ICEBERG_THETA_IMPLEMENTATION_IDENTITY,
        ICEBERG_THETA_STATE_FORMAT_IDENTITY, IcebergFunctionBundle, estimate_compact_theta,
    };
    use novarocks_functions::{
        AggregateBindingSelection, AggregateKernelPhase, AggregatePreparationOptions,
        AggregateStateFormatIdentity, CallArgumentUses, CallEffectInput,
        EngineFunctionCatalogBuilder, FunctionBindingRequest, FunctionBindingSelection,
        FunctionBundleContributor, FunctionId, FunctionKind, FunctionOverloadId,
        FunctionResultType, InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
        PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
        ScopedExpressionEffects,
    };
    use novarocks_physical_plan::{
        AggregateBinding, AggregatePhase, AggregateSequenceId, BoundFunction, ConstantPool,
        ConstantPoolId, ConstantReference, FrozenPhysicalCall, PhysicalCallDefinition,
        PhysicalCallRequest, PhysicalCallSite, StaticFunctionArgument, UnpivotConstant,
        WriterAggregateCall, WriterGroupedUnpivotMapping, WriterGroupedUnpivotSpec,
    };
    use novarocks_spi::connector::write_stack::{
        ROOT_WRITE_RESULT_BLOB_TYPE_INDEX, ROOT_WRITE_RESULT_BODY_INDEX,
        ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX, ROOT_WRITE_RESULT_PROPERTIES_INDEX,
        WriterAuxiliaryChannel, WriterMultiplexSchema, WriterRowKind,
    };
    use novarocks_type_contract::{
        CallProofScope, DecimalOverflowPolicy, EvaluationDemand, ExpressionEffects,
        SemanticParameters,
    };

    use crate::exec::chunk::ChunkSchema;
    use crate::exec::node::table_write_relation::WriterMultiplexRelationSchema;
    use crate::exec::operators::compiled_writer::{
        compiled_table_finish_factory, compiled_table_writer_factory,
    };
    use crate::exec::operators::compiled_writer_statistics::CompiledFinishStatistics;
    use crate::exec::operators::{TableFinishOperatorFactory, TableWriterOperatorFactory};
    use crate::exec::pipeline::operator::Operator;
    use crate::runtime::function_set::{ExecutionFunctionSetBuilder, SealedExecutionFunctionSet};
    use crate::runtime::{ExecutionRuntime, ExecutionRuntimeConfig};

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
    const LIST_POOL: ConstantPoolId = ConstantPoolId::new(1);
    const MAP_POOL: ConstantPoolId = ConstantPoolId::new(2);
    // Independent SPI relation schema; slot numbering grants no call authority.
    const EXPECTED_CHANNELS: [u32; 2] = [10_000, 10_001];

    /// The compiled catalogue: the installed Iceberg bundle sealed with its
    /// own pure owner.
    fn theta_catalog() -> PureEngineFunctionCatalog {
        let mut builder = EngineFunctionCatalogBuilder::new();
        IcebergFunctionBundle.contribute(&mut builder).unwrap();
        builder
            .seal_pure(THETA_OVERLOADS.map(|suffix| {
                InstalledPureKernel {
                    function: FunctionId::try_new("parametric.aggregate/$iceberg_theta_stat/v1")
                        .unwrap(),
                    kind: FunctionKind::Aggregate,
                    implementation: PureImplementationDeclaration {
                        overload: FunctionOverloadId::try_new(format!(
                            "iceberg/theta-stat/{suffix}/v1"
                        ))
                        .unwrap(),
                        implementation: PureImplementationId::try_new(
                            ICEBERG_THETA_IMPLEMENTATION_IDENTITY,
                        )
                        .unwrap(),
                        abi: PureKernelAbi::AggregateV1,
                    },
                    aggregate_state_format: Some(
                        AggregateStateFormatIdentity::try_new(ICEBERG_THETA_STATE_FORMAT_IDENTITY)
                            .unwrap(),
                    ),
                }
            }))
            .unwrap()
    }

    /// P14's metadata-only function-set owner seals the original definitions.
    /// No erased aggregate implementation or legacy adapter is registered.
    fn theta_function_set() -> Arc<SealedExecutionFunctionSet> {
        let mut builder = ExecutionFunctionSetBuilder::new();
        novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
            .unwrap();
        IcebergFunctionBundle
            .contribute(builder.catalog_builder_mut())
            .unwrap();
        Arc::new(builder.seal().unwrap())
    }

    fn statistics_state(function_set: Arc<SealedExecutionFunctionSet>) -> RuntimeState {
        let runtime = Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 16 * 1024 * 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                function_set,
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .unwrap(),
        );
        RuntimeState::for_local_execution(None, None, None, None, None, None, Some(runtime))
    }

    /// The provider field types the frontend resolves each sketch over.
    fn inputs() -> [FunctionValueType; 2] {
        [ty(DataType::Int64, false), ty(DataType::Utf8, true)]
    }

    fn theta_binding(
        catalog: &PureEngineFunctionCatalog,
        input: &FunctionValueType,
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
            constant_policy: constants(),
        }
    }

    fn constants() -> ConstantPolicy {
        ConstantPolicy {
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
        }
    }

    /// The input-field ID lists `[1]` and `[2]` and one empty property map,
    /// typed exactly as the Root relation's `input_fields` and `properties`.
    fn pools() -> [(ConstantPoolId, ConstantPool); 2] {
        let root = root_write_result_schema();
        let list_type = ty(
            root.field(ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX)
                .data_type()
                .clone(),
            false,
        );
        let DataType::List(item) = &list_type.data_type() else {
            unreachable!()
        };
        let raw = ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([
            Some(vec![Some(1)]),
            Some(vec![Some(2)]),
        ]);
        let lists = ListArray::try_new(
            Arc::clone(item),
            raw.offsets().clone(),
            raw.values().clone(),
            None,
        )
        .unwrap()
        .to_data();
        let list = ConstantPool::try_new(
            Arc::new(list_type.try_to_field("input_fields").unwrap()),
            list_type,
            lists,
            constants(),
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
        let map_type = ty(
            root.field(ROOT_WRITE_RESULT_PROPERTIES_INDEX)
                .data_type()
                .clone(),
            false,
        );
        let DataType::Map(entries, sorted) = &map_type.data_type() else {
            unreachable!()
        };
        let DataType::Struct(fields) = entries.data_type() else {
            unreachable!()
        };
        let children = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
                Arc::new(StringArray::from(Vec::<&str>::new())),
            ],
            None,
        )
        .unwrap();
        let empty =
            ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([Some(Vec::<
                Option<i32>,
            >::new(
            ))]);
        let maps = MapArray::try_new(
            Arc::clone(entries),
            empty.offsets().clone(),
            children,
            None,
            *sorted,
        )
        .unwrap()
        .to_data();
        let map = ConstantPool::try_new(
            Arc::new(map_type.try_to_field("properties").unwrap()),
            map_type,
            maps,
            constants(),
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
        [(LIST_POOL, list), (MAP_POOL, map)]
    }

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

    /// The two-fragment plan of `INSERT INTO t VALUES <rows>` into write
    /// target `target`, collecting a Theta sketch of each column.
    fn plan(target: u32, rows: &[(i64, Option<&str>)]) -> (PhysicalPlan, NodeId) {
        let catalog = theta_catalog();
        let ordinal = WriteTargetOrdinal::try_new(target).unwrap();
        let mut builder = FragmentBuilder::new(FragmentId::new(1));
        let source = builder.reserve_node_id().unwrap();
        // The source declaration comes from the same explicit provider-column
        // vocabulary used by the original Theta request, independently of
        // whether this particular fixture batch happens to contain NULL.
        let types = inputs();
        let values = types
            .iter()
            .enumerate()
            .map(|(ordinal, value_type)| {
                builder
                    .add_value(
                        value_type.clone(),
                        ValueOrigin::NodeOutput {
                            node: source,
                            output_ordinal: ordinal as u32,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let cells = rows
            .iter()
            .map(|(int, text)| {
                let int = builder
                    .add_expression(
                        source,
                        types[0].clone(),
                        ExprKind::Literal(LiteralValue::Int64(*int)),
                    )
                    .unwrap();
                let text = builder
                    .add_expression(
                        source,
                        types[1].clone(),
                        ExprKind::Literal(match text {
                            Some(text) => LiteralValue::Utf8((*text).into()),
                            None => LiteralValue::Null,
                        }),
                    )
                    .unwrap();
                Box::from([int, text])
            })
            .collect::<Box<[_]>>();
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
                kind: NodeKind::Values { rows: cells },
            })
            .unwrap();
        let writer = builder.reserve_node_id().unwrap();
        let mut fields = relation_fields(&mut builder, writer, false).into_vec();
        for channel in 0..2 {
            let value = builder
                .add_value(
                    ty(DataType::Binary, true),
                    ValueOrigin::WriterDerived {
                        writer_node: writer,
                        kind: WriterDerivedKind::RelationAuxiliary,
                    },
                )
                .unwrap();
            fields.push(WriterRelationField {
                value,
                name: format!("auxiliary_channel_{channel}").into(),
                ty: ty(DataType::Binary, true),
                role: WriterRelationFieldRole::Auxiliary,
            });
        }
        let partial = AggregatePhase::Partial {
            sequence: AggregateSequenceId::new(1),
        };
        let partial_aggregates = (0..2)
            .map(|channel| WriterAggregateCall {
                input: values[channel],
                binding: theta_binding(&catalog, &inputs()[channel], partial),
                output: fields[4 + channel].value,
            })
            .collect::<Box<[_]>>();
        let draft = draft();
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
                                ty: ty(
                                    binding.field().data_type().clone(),
                                    binding.field().is_nullable(),
                                ),
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
                    dop(),
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

        let mut builder = FragmentBuilder::new(FragmentId::new(2));
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
        let mapping = fields
            .iter()
            .zip(&imported)
            .map(|(a, b)| (a.value, b.value))
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
                    imports: mapping.clone(),
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
                let binding = theta_binding(&catalog, &inputs()[channel], final_phase);
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
                        value_output: outputs[ROOT_WRITE_RESULT_BODY_INDEX].value,
                        literal_outputs: Box::from([
                            outputs[ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX].value,
                            outputs[ROOT_WRITE_RESULT_BLOB_TYPE_INDEX].value,
                            outputs[ROOT_WRITE_RESULT_PROPERTIES_INDEX].value,
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
                    dop(),
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

        let mut plan = PlanBuilder::new(PlanVersionId::try_new([8; 16]).unwrap());
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
                fragment: FragmentId::new(1),
                projection: fields.iter().map(|field| field.value).collect(),
            },
            destination: EdgeDestination {
                fragment: FragmentId::new(2),
                node: exchange,
                receive_mapping: mapping,
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
            fragment: FragmentId::new(2),
            output: OutputPort {
                node: finish,
                columns: outputs.iter().map(|field| field.value).collect(),
            },
            fields: outputs
                .iter()
                .map(|field| ResultField {
                    domain: crate::test_result_domain::result_value_domain(&field.ty),
                    name: field.name.clone(),
                    alias: None,
                    value: field.value,
                    ty: field.ty.clone(),
                })
                .collect(),
        })
        .unwrap();
        (
            plan.finish_observed(&FixtureControl)
                .unwrap_or_else(|error| panic!("the statistics plan validates: {error:?}")),
            exchange,
        )
    }

    /// Root uses and frozen writer calls, numbered as the frontend numbers
    /// them; each call's materialized input occurrence is a frontend loan
    /// that enters no flow use.
    fn freeze(
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
        let root_uses =
            PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let frozen = sites
            .into_iter()
            .map(|(site, item, context, input)| {
                let binding = &item.binding;
                let selected = Arc::new(FunctionBindingSelection {
                    overload: binding.function.overload.clone(),
                    argument_types: binding.function.argument_types.clone(),
                    result_type: FunctionResultType::Scalar(binding.function.result_type.clone()),
                    aggregate: Some(AggregateBindingSelection {
                        state_argument_contract: binding.state_argument_contract,
                        intermediate_type: binding.intermediate_type.clone(),
                        state_format: binding.state_format.clone(),
                    }),
                });
                let request = theta_request(binding);
                let arguments = request
                    .arguments
                    .iter()
                    .map(|argument| match argument {
                        StaticFunctionArgument::Value { value_type, .. } => {
                            StaticFunctionArgument::Value {
                                value_type: value_type.clone(),
                                constant: None,
                            }
                        }
                        StaticFunctionArgument::Lambda { .. } => unreachable!(),
                    })
                    .collect::<Vec<_>>();
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

    struct Compiled {
        writer: Arc<LocalProgram>,
        finish: Arc<LocalProgram>,
        receiver: NodeId,
    }

    fn compiled(target: u32, rows: &[(i64, Option<&str>)], writer_dop: usize) -> Compiled {
        compiled_with_finish_package(target, rows, writer_dop, 1, |package| package)
    }

    fn compiled_with_finish_package(
        target: u32,
        rows: &[(i64, Option<&str>)],
        writer_dop: usize,
        finish_dop: usize,
        finish_package: impl FnOnce(FragmentPackage) -> FragmentPackage,
    ) -> Compiled {
        let catalog = theta_catalog();
        let ordinal = WriteTargetOrdinal::try_new(target).unwrap();
        let (plan, receiver) = plan(target, rows);
        let mut uses = BTreeMap::new();
        let mut calls = BTreeMap::new();
        let mut pruning = BTreeMap::new();
        let mut admissions = BTreeMap::new();
        for (&id, fragment) in plan.fragments() {
            let (root_uses, frozen) = freeze(fragment, &catalog);
            calls.insert(id, frozen);
            uses.insert(id, root_uses);
            pruning.insert(
                id,
                FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
            );
            admissions.insert(id, admission());
        }
        let mut packages = extract_fragment_packages(
            &plan,
            &BTreeMap::new(),
            &BTreeMap::from([(ordinal, draft())]),
            &uses,
            &calls,
            &pruning,
            &admissions,
            &FixtureControl,
        )
        .unwrap();
        let finish = packages.remove(&FragmentId::new(2)).unwrap();
        packages.insert(FragmentId::new(2), finish_package(finish));
        let provider = adapter().binding().descriptor().provider_id.clone();
        let providers = PureProviderProgramCatalog::try_new(
            &[PureProviderManifestEntry::new(
                provider.clone(),
                false,
                true,
            )],
            vec![PureProviderProgramDefinition::new(
                provider,
                None,
                Some(Arc::new(KeepDraft)
                    as Arc<
                        dyn ConnectorWriteRecipeCompiler<Error = ConnectorError>,
                    >),
            )],
            &FixtureControl,
        )
        .unwrap();
        let mut compile = |fragment: FragmentId, dop: usize, root_sink: bool| {
            let validated = validate_fragment_providers(
                Arc::new(packages.remove(&fragment).unwrap()),
                &providers,
                &FixtureControl,
            )
            .unwrap();
            Arc::new(
                compile_fragment(
                    validated,
                    &catalog,
                    LocalCompileOptions {
                        pipeline_dop: NonZeroUsize::new(dop).unwrap(),
                        root_sink_dop: root_sink.then(|| NonZeroUsize::new(1).unwrap()),
                        kernel_abi: KernelAbiVersion::CURRENT,
                        constants: constants(),
                        exchange_wait: Duration::from_secs(120),
                    },
                    &FixtureControl,
                )
                .unwrap_or_else(|error| panic!("the statistics fragment compiles: {error}")),
            )
        };
        Compiled {
            writer: compile(FragmentId::new(1), writer_dop, false),
            finish: compile(FragmentId::new(2), finish_dop, true),
            receiver,
        }
    }

    fn binding(recorded: &Arc<Mutex<Recorded>>) -> TableWriterRuntimeBinding {
        TableWriterRuntimeBinding::try_new(
            writer_handle(),
            Arc::new(RecordingExecution {
                catalog_handle: catalog_handle(),
                recorded: Arc::clone(recorded),
            }),
            TableWriterPhysicalContextTemplate::new([1; 16], 1, [2; 16], 0),
            request_context(),
            Arc::new(TestFragmentEncoder),
        )
        .unwrap()
    }

    /// One input page per driver, over the compiled writer's actual input
    /// port: the Values layout both writers read by slot.
    fn page(program: &LocalProgram, rows: &[(i64, Option<&str>)]) -> Chunk {
        let root = program.graph().root();
        let ProgramNodeKind::TableWriter { input, .. } =
            program.graph().nodes()[root.index()].kind()
        else {
            panic!("a writer program")
        };
        let layout = program.graph().nodes()[input.index()].output_layout();
        let batch = RecordBatch::try_new(
            Arc::clone(layout.schema()),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(int, _)| *int).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, text)| *text).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        Chunk::try_new_with_chunk_schema(
            batch,
            crate::exec::chunk::ChunkSchema::from_compiled_layout(layout).unwrap(),
        )
        .unwrap()
    }

    fn expected_multiplex() -> WriterMultiplexRelationSchema {
        WriterMultiplexRelationSchema::try_new(
            WriterMultiplexSchema::try_new(
                EXPECTED_CHANNELS
                    .iter()
                    .enumerate()
                    .map(|(channel, slot)| {
                        WriterAuxiliaryChannel::try_new(
                            *slot,
                            format!("auxiliary_channel_{channel}"),
                            ty(DataType::Binary, true),
                        )
                        .unwrap()
                    })
                    .collect(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn poll_typed(
        operator: &mut Box<dyn Operator>,
        state: &RuntimeState,
    ) -> crate::runtime::fragment::ExecutionResult<Vec<Chunk>> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut calls = TestDriver::new(operator.as_mut(), state);
        let mut output = Vec::new();
        loop {
            if let Some(error) = state.error() {
                return Err(error);
            }
            assert!(
                Instant::now() <= deadline,
                "timed out waiting for operator output"
            );
            let step = calls.drive()?;
            if let Some(chunk) = step.output {
                output.push(chunk);
            }
            match step.next {
                NextStep::Finished => return Ok(output),
                NextStep::Wait(_) => std::thread::sleep(Duration::from_millis(2)),
                NextStep::Runnable | NextStep::Yield => {}
                NextStep::NeedInput => panic!("fixture signaled EOS before finalization"),
            }
        }
    }

    fn poll(operator: &mut Box<dyn Operator>, state: &RuntimeState) -> Result<Vec<Chunk>, String> {
        poll_typed(operator, state).map_err(|error| error.to_string())
    }

    /// One actual writer admission and accepted continuation, preserving raw
    /// custody across asynchronous queue capacity and the original deadline.
    fn deliver_writer_input(
        operator: &mut dyn Operator,
        state: &RuntimeState,
        chunk: Chunk,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut calls = TestDriver::new(operator, state);
        let mut input = RawInput::for_test(chunk);
        let mut step = loop {
            assert!(Instant::now() < deadline, "the writer accepts its page");
            match calls.try_push(input) {
                InputResult::Accepted(result) => break result?,
                InputResult::Rejected { failure, .. } => return Err(failure),
                InputResult::Deferred {
                    input: returned,
                    next,
                } => {
                    input = returned;
                    match next {
                        AdmissionNext::Wait(_) => std::thread::sleep(Duration::from_millis(2)),
                        AdmissionNext::Yield => {}
                        AdmissionNext::ResumeExisting => {
                            assert!(calls.drive()?.output.is_none(), "no prefix before EOS");
                        }
                    }
                }
            }
        };
        loop {
            assert!(
                step.output.is_none(),
                "writer publishes only after input EOS"
            );
            if matches!(step.next, NextStep::NeedInput) {
                return Ok(());
            }
            assert!(
                !matches!(step.next, NextStep::Finished),
                "accepted writer input remains open"
            );
            assert!(Instant::now() < deadline, "the accepted writer page drains");
            if matches!(step.next, NextStep::Wait(_)) {
                std::thread::sleep(Duration::from_millis(2));
            }
            step = calls.drive()?;
        }
    }

    /// Direct-operator fixtures use the same original runtime tracker/control
    /// factory as pipeline preparation. There is no pure or simulated creator.
    fn install_statistics_construction(
        operator: &mut dyn Operator,
        state: &RuntimeState,
    ) -> Arc<crate::runtime::mem_tracker::MemTracker> {
        use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root(
            "compiled-statistics-direct-operator",
        );
        operator.set_mem_tracker(Arc::clone(&tracker));
        if operator.creates_typed_storage() {
            let factory = try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                Arc::clone(&tracker),
                state.error_state(),
            )
            .unwrap();
            operator.set_construction_factory(factory);
        }
        tracker
    }

    fn prepared_final_operator(
        statistics: &dyn crate::exec::operators::table_finish::FinishStatisticsFactory,
        state: &RuntimeState,
    ) -> (
        Box<dyn Operator>,
        Arc<crate::runtime::mem_tracker::MemTracker>,
    ) {
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("compiled-final-construction");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker.clone(), state.error_state(),
        ).unwrap();
        let mut operator = statistics.final_aggregate(state, factory.as_ref()).unwrap();
        operator.set_mem_tracker(tracker.clone());
        operator.set_construction_factory(factory);
        (operator, tracker)
    }

    /// Run one writer per driver over that driver's pages; each driver's
    /// complete writer relation, in emission order.
    fn run_writer(
        factory: &TableWriterOperatorFactory,
        pages: &[Vec<Chunk>],
        state: &RuntimeState,
    ) -> Vec<Vec<Chunk>> {
        let dop = pages.len() as i32;
        pages
            .iter()
            .enumerate()
            .map(|(driver, pages)| {
                let mut operator = factory.create(dop, driver as i32);
                install_statistics_construction(operator.as_mut(), state);
                operator.prepare().unwrap();
                operator.bind_runtime_state(state).unwrap();
                operator.activate(state).unwrap();
                for page in pages {
                    deliver_writer_input(operator.as_mut(), state, page.clone()).unwrap();
                }
                operator.finish_for_test(state).unwrap();
                poll(&mut operator, state).unwrap()
            })
            .collect()
    }

    /// Run one finish over every writer relation chunk, each re-slotted to
    /// the finish's own receiver port as the exchange delivers it.
    fn run_finish_typed(
        factory: &TableFinishOperatorFactory,
        relation: &crate::exec::chunk::ChunkSchemaRef,
        chunks: &[Chunk],
        state: &RuntimeState,
    ) -> crate::runtime::fragment::ExecutionResult<Vec<Chunk>> {
        let mut operator = factory.create(1, 0);
        install_statistics_construction(operator.as_mut(), state);
        operator.prepare()?;
        operator.bind_runtime_state(state)?;
        operator.activate(state)?;
        for chunk in chunks {
            let delivered = Chunk::try_new_with_chunk_schema(
                chunk.record_batch().clone(),
                Arc::clone(relation),
            )
            .unwrap();
            crate::exec::operators::table_finish::complete_table_finish_input(
                operator.as_mut(),
                state,
                delivered,
            )?;
        }
        operator.finish_for_test(state)?;
        poll_typed(&mut operator, state)
    }

    fn run_finish(
        factory: &TableFinishOperatorFactory,
        relation: &crate::exec::chunk::ChunkSchemaRef,
        chunks: &[Chunk],
        state: &RuntimeState,
    ) -> Result<Vec<Chunk>, String> {
        run_finish_typed(factory, relation, chunks, state).map_err(|error| error.to_string())
    }

    fn compiled_relation(program: &LocalProgram) -> crate::exec::chunk::ChunkSchemaRef {
        let root = program.graph().root();
        let ProgramNodeKind::TableFinish {
            writer_multiplex_layout,
            ..
        } = program.graph().nodes()[root.index()].kind()
        else {
            panic!("a finish program")
        };
        ChunkSchema::from_compiled_layout(writer_multiplex_layout).unwrap()
    }

    // Retain the old comparison fixture's explicit correspondence check on
    // the real projection and genuine installed partial handles, without
    // constructing an independent legacy occurrence or executable adapter.
    fn assert_partial_correspondence(compiled: &Compiled) {
        let root = compiled.writer.graph().root();
        let ProgramNodeKind::TableWriter {
            projection,
            partial_aggregates,
            ..
        } = compiled.writer.graph().nodes()[root.index()].kind()
        else {
            panic!("actual compiled writer");
        };
        for (channel, _) in partial_aggregates.iter().enumerate() {
            let site = novarocks_local_program::ProgramCallSite::WriterPartial {
                node: root,
                call: u32::try_from(channel).unwrap(),
            };
            let Some(novarocks_local_program::ProgramStateTemplate::Aggregate { kernel, .. }) =
                compiled.writer.state_template(site)
            else {
                panic!("actual frozen partial handle");
            };
            let actual = FunctionValueType::try_from_typed_binding(
                projection.layout.value_binding_at(channel).unwrap().clone(),
            )
            .unwrap();
            let selected = kernel.contract().logical_argument_types().next().unwrap();
            assert!(
                actual
                    .fits_value_type_observed(selected, || Ok::<
                        (),
                        novarocks_type_contract::ValueTypeError,
                    >(()))
                    .unwrap(),
                "real compiled projection must fit its exact selected origin: actual={actual:?}, selected={selected:?}"
            );
        }
    }

    // Independent Apache DataSketches wire oracle. The declared fixture has
    // BIGINT canonical bytes in little-endian order and raw UTF-8 strings.
    // This calls neither the legacy adapter nor the installed NovaRocks kernel.
    fn expected_theta(rows: &[(i64, Option<&str>)]) -> [Vec<u8>; 2] {
        use datasketches::hash::value::raw_bytes;
        use datasketches::theta::ThetaSketchBuilder;
        let mut numbers = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
        let mut strings = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
        for (number, text) in rows {
            numbers.update(raw_bytes::from_slice(&number.to_le_bytes()));
            if let Some(text) = text {
                strings.update(raw_bytes::from_slice(text.as_bytes()));
            }
        }
        let compact = |sketch: datasketches::theta::ThetaSketch| {
            let compact = sketch.compact(true);
            if compact.is_empty() {
                // Ordered compact-v3 empty preamble, no hash payload.
                vec![1, 3, 3, 0, 0, 0x1e, 0, 0]
            } else {
                compact.serialize()
            }
        };
        [compact(numbers), compact(strings)]
    }

    fn expected_writer_batches(target: u32, rows: &[(i64, Option<&str>)]) -> Vec<RecordBatch> {
        let sketches = expected_theta(rows);
        let relation = expected_multiplex();
        let schema = relation.chunk_schema().arrow_schema_ref();
        let batch =
            |kind: WriterRowKind, count: Option<i64>, fragment: Option<&[u8]>, partial: bool| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int8Array::from(vec![kind.to_wire()])) as ArrayRef,
                        Arc::new(Int32Array::from(vec![i32::try_from(target).unwrap()])),
                        Arc::new(Int64Array::from(vec![count])),
                        Arc::new(BinaryArray::from(vec![fragment])),
                        Arc::new(BinaryArray::from(vec![
                            partial.then_some(sketches[0].as_slice()),
                        ])),
                        Arc::new(BinaryArray::from(vec![
                            partial.then_some(sketches[1].as_slice()),
                        ])),
                    ],
                )
                .unwrap()
            };
        // Two small compact bodies fit one packed partial row. It precedes
        // the separate row-count and the provider's one fixed staged fragment.
        vec![
            batch(WriterRowKind::AggregatePartial, None, None, true),
            batch(
                WriterRowKind::RowCount,
                Some(i64::try_from(rows.len()).unwrap()),
                None,
                false,
            ),
            batch(WriterRowKind::CommitFragment, None, Some(b"staged"), false),
        ]
    }

    // Fixed SPI root rows: kind, target, count, fragment, one input field id,
    // artifact body. These are expected data, not output read back as an oracle.
    type ExpectedRootRow = (
        i8,
        Option<i32>,
        Option<i64>,
        Option<Vec<u8>>,
        Option<i32>,
        Option<Vec<u8>>,
    );
    fn expected_root_batch(rows: &[ExpectedRootRow]) -> RecordBatch {
        let schema = root_write_result_schema();
        let DataType::List(item) = schema.field(4).data_type() else {
            unreachable!()
        };
        let lists = ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>(
            rows.iter().map(|row| row.4.map(|id| vec![Some(id)])),
        );
        let lists = ListArray::new(
            item.clone(),
            lists.offsets().clone(),
            lists.values().clone(),
            lists.nulls().cloned(),
        );
        let DataType::Map(entries, sorted) = schema.field(7).data_type() else {
            unreachable!()
        };
        let DataType::Struct(fields) = entries.data_type() else {
            unreachable!()
        };
        let children = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
                Arc::new(StringArray::from(Vec::<&str>::new())),
            ],
            None,
        );
        let maps = MapArray::try_new(
            entries.clone(),
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![
                0_i32;
                rows.len() + 1
            ])),
            children,
            Some(arrow::buffer::NullBuffer::from(
                rows.iter().map(|row| row.4.is_some()).collect::<Vec<_>>(),
            )),
            *sorted,
        )
        .unwrap();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int8Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int32Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(BinaryArray::from(
                    rows.iter().map(|row| row.3.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(lists),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|row| row.4.map(|_| BLOB_TYPE))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(BinaryArray::from(
                    rows.iter().map(|row| row.5.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(maps),
            ],
        )
        .unwrap()
    }
    fn expected_root_batches(
        target: u32,
        drivers: usize,
        rows: &[(i64, Option<&str>)],
    ) -> Vec<RecordBatch> {
        use novarocks_spi::connector::write_stack::RootRowKind;
        let target = i32::try_from(target).unwrap();
        let sketches = expected_theta(rows);
        vec![
            expected_root_batch(&[(
                RootRowKind::Summary.to_wire(),
                None,
                Some(i64::try_from(rows.len()).unwrap()),
                None,
                None,
                None,
            )]),
            expected_root_batch(
                &(0..drivers)
                    .map(|_| {
                        (
                            RootRowKind::PreparedFragment.to_wire(),
                            Some(target),
                            None,
                            Some(b"staged".to_vec()),
                            None,
                            None,
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
            expected_root_batch(&[
                (
                    RootRowKind::ArtifactDraft.to_wire(),
                    Some(target),
                    None,
                    None,
                    Some(1),
                    Some(sketches[0].clone()),
                ),
                (
                    RootRowKind::ArtifactDraft.to_wire(),
                    Some(target),
                    None,
                    None,
                    Some(2),
                    Some(sketches[1].clone()),
                ),
            ]),
        ]
    }

    fn batches(chunks: &[Chunk]) -> Vec<RecordBatch> {
        chunks
            .iter()
            .map(|chunk| chunk.record_batch().clone())
            .collect()
    }

    /// Every column of `actual` equals `expected` and so do field names and
    /// carriers; the two relations differ only in their slot numbering.
    fn assert_same_rows(actual: &[Chunk], expected: &[RecordBatch]) {
        let actual = batches(actual);
        assert_eq!(actual.len(), expected.len(), "same batch count");
        for (actual, expected) in actual.iter().zip(expected) {
            assert_eq!(actual.schema().fields(), expected.schema().fields());
            assert_eq!(actual.columns(), expected.columns());
        }
    }

    struct StatisticsRun {
        compiled_writer: Vec<Vec<Chunk>>,
        expected_writer: Vec<Vec<RecordBatch>>,
        compiled_root: crate::runtime::fragment::ExecutionResult<Vec<Chunk>>,
        expected_root: Vec<RecordBatch>,
    }

    /// Run actual compiled writer/finish; expectations use only fixture rows,
    /// independent SDK sketches and SPI metadata. `keep` filters delivered rows.
    fn statistics_run(
        target: u32,
        driver_rows: &[Vec<(i64, Option<&str>)>],
        keep: impl Fn(i8) -> bool,
    ) -> StatisticsRun {
        let all = driver_rows.iter().flatten().copied().collect::<Vec<_>>();
        let compiled = compiled(target, &all, driver_rows.len());
        assert_partial_correspondence(&compiled);
        let function_set = theta_function_set();
        let state = statistics_state(Arc::clone(&function_set));
        let pages = driver_rows
            .iter()
            .map(|rows| {
                if rows.is_empty() {
                    Vec::new()
                } else {
                    vec![page(&compiled.writer, rows)]
                }
            })
            .collect::<Vec<_>>();
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let compiled_factory = compiled_table_writer_factory(
            &compiled.writer,
            compiled.writer.graph().root(),
            0,
            &binding(&recorded),
            &state.error_state(),
        )
        .unwrap();
        let compiled_writer = run_writer(&compiled_factory, &pages, &state);
        let expected_writer = driver_rows
            .iter()
            .map(|rows| expected_writer_batches(target, rows))
            .collect();
        let expected_root = expected_root_batches(target, driver_rows.len(), &all);
        let kept = |chunks: &[Vec<Chunk>]| {
            chunks
                .iter()
                .flatten()
                .map(|chunk| {
                    let kinds = chunk
                        .record_batch()
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .unwrap();
                    let mask = arrow::array::BooleanArray::from(
                        (0..chunk.len())
                            .map(|row| keep(kinds.value(row)))
                            .collect::<Vec<_>>(),
                    );
                    let batch =
                        arrow::compute::filter_record_batch(chunk.record_batch(), &mask).unwrap();
                    Chunk::try_new_with_chunk_schema(batch, chunk.chunk_schema_ref()).unwrap()
                })
                .filter(|chunk| !chunk.is_empty())
                .collect::<Vec<_>>()
        };
        let finish_binding = TableFinishRuntimeBinding::new(Arc::new(AcceptCarriers));
        let compiled_finish = compiled_table_finish_factory(
            &compiled.finish,
            compiled.finish.graph().root(),
            0,
            &finish_binding,
            &state.error_state(),
        )
        .unwrap();
        let compiled_root = run_finish_typed(
            &compiled_finish,
            &compiled_relation(&compiled.finish),
            &kept(&compiled_writer),
            &state,
        );
        StatisticsRun {
            compiled_writer,
            expected_writer,
            compiled_root,
            expected_root,
        }
    }

    /// The Theta body of each Root artifact row, in row order.
    fn bodies(root: &[Chunk]) -> Vec<(i32, Vec<i32>, Vec<u8>)> {
        let mut bodies = Vec::new();
        for chunk in root {
            let kinds = chunk
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            let targets = chunk
                .record_batch()
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let fields = chunk
                .record_batch()
                .column(4)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let blob = chunk
                .record_batch()
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let body = chunk
                .record_batch()
                .column(6)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for row in 0..chunk.len() {
                if kinds.value(row)
                    != novarocks_spi::connector::write_stack::RootRowKind::ArtifactDraft.to_wire()
                {
                    continue;
                }
                assert_eq!(blob.value(row), BLOB_TYPE);
                let ids = fields.value(row);
                let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
                bodies.push((
                    targets.value(row),
                    ids.values().to_vec(),
                    body.value(row).to_vec(),
                ));
            }
        }
        bodies
    }

    /// Compare every multiplex/Root field and compact Theta byte to an
    /// independent SDK wire oracle over repeated values and a NULL string.
    #[test]
    fn compiled_statistics_match_the_plan_tree_writer_and_finish_byte_for_byte() {
        let rows = vec![
            (1, Some("a")),
            (2, Some("b")),
            (2, None),
            (3, Some("a")),
            (5, Some("c")),
        ];
        let run = statistics_run(0, &[rows], |_| true);
        assert_eq!(run.compiled_writer.len(), 1);
        for (compiled, expected) in run.compiled_writer.iter().zip(&run.expected_writer) {
            assert_same_rows(compiled, expected);
            let partials = compiled
                .iter()
                .map(|chunk| {
                    let kinds = chunk
                        .record_batch()
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .unwrap();
                    (0..chunk.len())
                        .filter(|row| {
                            kinds.value(*row) == WriterRowKind::AggregatePartial.to_wire()
                        })
                        .count()
                })
                .sum::<usize>();
            assert!(partials >= 1, "the driver packs its sketches");
        }
        let compiled_root = run.compiled_root.expect("the compiled finish publishes");
        assert_same_rows(&compiled_root, &run.expected_root);
        let bodies = bodies(&compiled_root);
        assert_eq!(bodies.len(), 2, "one artifact per mapping");
        assert_eq!((bodies[0].0, bodies[0].1.as_slice()), (0, [1].as_slice()));
        assert_eq!((bodies[1].0, bodies[1].1.as_slice()), (0, [2].as_slice()));
        // Four distinct BIGINTs and three distinct non-NULL strings.
        assert_eq!(estimate_compact_theta(&bodies[0].2).unwrap(), 4.0);
        assert_eq!(estimate_compact_theta(&bodies[1].2).unwrap(), 3.0);
    }

    /// A writer that receives no row still emits each call's initial sketch,
    /// so the finish covers every channel and publishes empty sketches.
    #[test]
    fn an_empty_writer_emits_its_initial_sketches() {
        let run = statistics_run(0, &[Vec::new()], |_| true);
        for (compiled, expected) in run.compiled_writer.iter().zip(&run.expected_writer) {
            assert_same_rows(compiled, expected);
        }
        let compiled_root = run.compiled_root.expect("the compiled finish publishes");
        assert_same_rows(&compiled_root, &run.expected_root);
        let bodies = bodies(&compiled_root);
        assert_eq!(bodies.len(), 2);
        for (_, _, body) in bodies {
            assert_eq!(estimate_compact_theta(&body).unwrap(), 0.0);
        }
    }

    /// A target whose aggregate partials never arrive fails coverage at
    /// finalize, with the fixed required-coverage diagnostic.
    #[test]
    fn a_missing_partial_fails_coverage_like_the_plan_tree_finish() {
        let run = statistics_run(0, &[vec![(1, Some("a"))]], |kind| {
            kind != WriterRowKind::AggregatePartial.to_wire()
        });
        let compiled = run.compiled_root.expect_err("coverage refuses");
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
        assert_eq!(
            compiled.detail(),
            "table finish target 0 has no non-null aggregate partial for channel 0"
        );
        assert_eq!(
            compiled.failure_fact(),
            FailureFact::new(
                FailureClass::Invariant,
                crate::runtime::fragment::ExecutionReason::FinishCoverage.reason(),
            )
        );
    }

    /// Three drivers per writer and two write targets: every driver packs
    /// its own independently checked sketches, and the compiled final aggregate
    /// merges strictly by target in either arrival order into the SDK sketch
    /// over that target's original inputs, emitting targets in ascending order.
    #[test]
    fn three_drivers_and_two_targets_merge_strictly_by_target() {
        let drivers = |offset: i64| {
            vec![
                vec![(offset + 1, Some("a")), (offset + 2, Some("b"))],
                vec![(offset + 2, None), (offset + 3, Some("a"))],
                vec![(offset + 4, Some("c"))],
            ]
        };
        let runs = [
            statistics_run(0, &drivers(0), |_| true),
            statistics_run(1, &drivers(100), |_| true),
        ];
        let mut expected = BTreeMap::new();
        for (target, run) in runs.iter().enumerate() {
            assert_eq!(run.compiled_writer.len(), 3);
            for (compiled, expected) in run.compiled_writer.iter().zip(&run.expected_writer) {
                assert_same_rows(compiled, expected);
            }
            let root = run
                .compiled_root
                .as_ref()
                .expect("the compiled finish publishes");
            assert_same_rows(root, &run.expected_root);
            // Interleaved merge is compared to an independent whole-input SDK
            // sketch, never to a preceding compiled finish's actual body.
            expected.insert(
                target as i32,
                expected_theta(&drivers(target as i64 * 100).concat()).to_vec(),
            );
        }
        // Both targets' aggregate partial rows, target 1's first, through
        // the compiled final aggregate of one finish program.
        let partials = |target: u32| {
            let all = drivers(i64::from(target) * 100).concat();
            let compiled = compiled(target, &all, 3);
            let state = statistics_state(theta_function_set());
            let recorded = Arc::new(Mutex::new(Recorded::default()));
            let factory = compiled_table_writer_factory(
                &compiled.writer,
                compiled.writer.graph().root(),
                0,
                &binding(&recorded),
                &state.error_state(),
            )
            .unwrap();
            let pages = drivers(i64::from(target) * 100)
                .iter()
                .map(|rows| vec![page(&compiled.writer, rows)])
                .collect::<Vec<_>>();
            run_writer(&factory, &pages, &state)
                .into_iter()
                .flatten()
                .filter_map(|chunk| {
                    let kinds = chunk
                        .record_batch()
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .unwrap();
                    let mask = arrow::array::BooleanArray::from(
                        (0..chunk.len())
                            .map(|row| {
                                kinds.value(row) == WriterRowKind::AggregatePartial.to_wire()
                            })
                            .collect::<Vec<_>>(),
                    );
                    let batch =
                        arrow::compute::filter_record_batch(chunk.record_batch(), &mask).unwrap();
                    (batch.num_rows() > 0).then_some(batch)
                })
                .collect::<Vec<_>>()
        };
        let mut arriving = partials(1);
        let target_zero = partials(0);
        // Interleave: one target-1 row, then the target-0 rows, then the rest.
        let rest = arriving.split_off(1);
        arriving.extend(target_zero);
        arriving.extend(rest);
        let finish = compiled(0, &drivers(0).concat(), 3).finish;
        let state = statistics_state(theta_function_set());
        let (_, statistics) =
            CompiledFinishStatistics::try_new(&finish, finish.graph().root(), state.error_state())
                .unwrap()
                .expect("the finish carries statistics");
        let (mut aggregate, _tracker) = prepared_final_operator(statistics.as_ref(), &state);
        aggregate.prepare().unwrap();
        aggregate.bind_runtime_state(&state).unwrap();
        let relation = compiled_relation(&finish);
        for batch in arriving {
            TestDriver::new(aggregate.as_mut(), &state)
                .input_without_output(
                    Chunk::try_new_with_chunk_schema(batch, Arc::clone(&relation)).unwrap(),
                )
                .unwrap();
        }
        aggregate.finish_for_test(&state).unwrap();
        let output = poll(&mut aggregate, &state).unwrap();
        let [output] = output.as_slice() else {
            panic!("one final batch")
        };
        let targets = output
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(targets.values().to_vec(), [0, 1], "targets ascend");
        for (row, target) in targets.values().iter().enumerate() {
            for (call, body) in expected[target].iter().enumerate() {
                let finals = output
                    .record_batch()
                    .column(1 + call)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();
                assert_eq!(
                    finals.value(row),
                    body.as_slice(),
                    "target {target} call {call}"
                );
            }
        }
    }

    #[cfg(novarocks_dev_controls)]
    struct Reject(crate::exec::node::table_write_relation::TableWriteAggregateBoundary);

    #[cfg(novarocks_dev_controls)]
    impl crate::exec::node::table_write_relation::TableWriteAggregateGuard for Reject {
        fn check(
            &self,
            boundary: crate::exec::node::table_write_relation::TableWriteAggregateBoundary,
        ) -> Result<(), ConnectorError> {
            if boundary == self.0 {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("injected {boundary:?}"),
                ));
            }
            Ok(())
        }
    }

    /// The Task's aggregate guard installed on a compiled writer and finish
    /// is consulted by the compiled statistics at each aggregate boundary,
    /// while retaining the exact installed boundary failure.
    #[cfg(novarocks_dev_controls)]
    #[test]
    fn the_task_aggregate_guard_rejects_compiled_statistics_work() {
        use crate::exec::node::table_write_relation::TableWriteAggregateBoundary as Boundary;

        let rows = [(1, Some("a"))];
        let compiled = compiled(0, &rows, 1);
        let state = statistics_state(theta_function_set());
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let factory = compiled_table_writer_factory(
            &compiled.writer,
            compiled.writer.graph().root(),
            0,
            &binding(&recorded).with_aggregate_guard(Arc::new(Reject(Boundary::PartialUpdate))),
            &state.error_state(),
        )
        .unwrap();
        let mut writer = factory.create(1, 0);
        install_statistics_construction(writer.as_mut(), &state);
        writer.prepare().unwrap();
        writer.bind_runtime_state(&state).unwrap();
        writer.activate(&state).unwrap();
        let error = deliver_writer_input(writer.as_mut(), &state, page(&compiled.writer, &rows))
            .expect_err("the guard rejects the partial update");
        assert!(error.to_string().contains("PartialUpdate"), "{error}");
        assert!(
            recorded.lock().unwrap().batches.is_empty(),
            "no page reaches the provider writer"
        );

        // Retire the failed Task's real writer before starting an independent
        // Task scope for the final-merge fault.
        writer.on_driver_failure();
        writer.begin_exit().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let mut pending = false;
            writer.visit_exits(&mut |exit| match exit.report_to(&state.error_state()) {
                crate::exec::pipeline::operator::ExitProgress::Pending(_)
                | crate::exec::pipeline::operator::ExitProgress::Unresolved(_) => pending = true,
                crate::exec::pipeline::operator::ExitProgress::Exited => {}
            });
            if !pending {
                break;
            }
            assert!(Instant::now() < deadline, "failed writer actually exits");
            std::thread::sleep(Duration::from_millis(2));
        }
        if let Err(failure) = writer.close() {
            state.error_state().report_cleanup(&failure);
        }
        assert!(state.error().unwrap().same_occurrence(&error));
        drop(writer);
        drop(factory);

        // A clean writer relation, then a finish whose guard rejects the merge.
        let state = statistics_state(theta_function_set());
        let factory = compiled_table_writer_factory(
            &compiled.writer,
            compiled.writer.graph().root(),
            0,
            &binding(&recorded),
            &state.error_state(),
        )
        .unwrap();
        let relation =
            run_writer(&factory, &[vec![page(&compiled.writer, &rows)]], &state).concat();
        let finish = compiled_table_finish_factory(
            &compiled.finish,
            compiled.finish.graph().root(),
            0,
            &TableFinishRuntimeBinding::new(Arc::new(AcceptCarriers))
                .with_aggregate_guard(Arc::new(Reject(Boundary::FinalMerge))),
            &state.error_state(),
        )
        .unwrap();
        let error = run_finish(
            &finish,
            &compiled_relation(&compiled.finish),
            &relation,
            &state,
        )
        .expect_err("the guard rejects the final merge");
        assert!(error.contains("FinalMerge"), "{error}");
    }

    /// The statistics insert runs end to end through compiled pipelines:
    /// the writer's auxiliary sketch channels cross the real exchange encoding
    /// and the finish's Root relation carries one artifact per mapping beside
    /// the summary and the prepared fragment.
    #[test]
    fn a_compiled_statistics_insert_runs_end_to_end_through_pipelines() {
        let rows = [(1, Some("a")), (2, None), (2, Some("b"))];
        let compiled = compiled(0, &rows, 1);
        let insert = Insert {
            writer: Arc::clone(&compiled.writer),
            finish: Arc::clone(&compiled.finish),
            receiver: compiled.receiver,
        };
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let chunks = run_insert(&insert, 1, &recorded).expect("the compiled insert runs");
        assert_eq!(
            recorded
                .lock()
                .unwrap()
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );
        let mut kinds = Vec::new();
        for chunk in &chunks {
            assert_eq!(chunk.schema().as_ref(), root_write_result_schema().as_ref());
            let column = chunk
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            kinds.extend(column.values().iter().copied());
        }
        kinds.sort_unstable();
        assert_eq!(kinds, [1, 2, 3, 3], "summary, fragment and two artifacts");
        let bodies = bodies(&chunks);
        assert_eq!(estimate_compact_theta(&bodies[0].2).unwrap(), 2.0);
        assert_eq!(estimate_compact_theta(&bodies[1].2).unwrap(), 2.0);
    }
    #[test]
    fn compiled_partial_schema_creator_survives_operator_and_output_until_last_slot() {
        use crate::exec::operators::compiled_writer_statistics::CompiledWriterPartialFactory;
        let compiled = compiled(0, &[], 1);
        let state = statistics_state(theta_function_set());
        let partial = CompiledWriterPartialFactory::try_new(
            &compiled.writer,
            compiled.writer.graph().root(),
            state.error_state(),
        )
        .unwrap()
        .expect("writer partial declarations");
        let mut operator = partial.create(1, 0);
        let tracker = install_statistics_construction(operator.as_mut(), &state);
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        operator.finish_for_test(&state).unwrap();
        let mut chunks = poll(&mut operator, &state).unwrap();
        assert_eq!(chunks.len(), 1, "empty partial emits actual initial states");
        let output = chunks.pop().unwrap();
        let slot = output.chunk_schema().slots()[0].clone();
        drop(operator);
        drop(partial);
        drop(compiled);
        assert!(
            tracker.current() > 0,
            "returned real partial retains its creator"
        );
        drop(output);
        assert!(
            tracker.current() > 0,
            "independent typed slot retains schema creator"
        );
        drop(slot);
        assert_eq!(tracker.current(), 0, "last actual creator exits");
    }

    #[test]
    fn compiled_final_schema_creator_survives_sources_until_last_slot() {
        let rows = [(1, Some("a"))];
        let compiled = compiled(0, &rows, 1);
        let state = statistics_state(theta_function_set());
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let writer = compiled_table_writer_factory(
            &compiled.writer,
            compiled.writer.graph().root(),
            0,
            &binding(&recorded),
            &state.error_state(),
        )
        .unwrap();
        let source = run_writer(&writer, &[vec![page(&compiled.writer, &rows)]], &state);
        let (_, statistics) = CompiledFinishStatistics::try_new(
            &compiled.finish,
            compiled.finish.graph().root(),
            state.error_state(),
        )
        .unwrap()
        .expect("finish final declarations");
        let (mut operator, tracker) = prepared_final_operator(statistics.as_ref(), &state);
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        let relation = compiled_relation(&compiled.finish);
        for chunk in source.iter().flatten() {
            // Explicit fixture delivery uses the finish's complete prepared
            // occurrence declaration, as the real exchange binds its slots.
            let input = Chunk::try_new_with_chunk_schema(
                chunk.record_batch().clone(),
                Arc::clone(&relation),
            )
            .unwrap();
            TestDriver::new(operator.as_mut(), &state)
                .input_without_output(input)
                .unwrap();
        }
        operator.finish_for_test(&state).unwrap();
        let mut outputs = poll(&mut operator, &state).unwrap();
        assert_eq!(outputs.len(), 1);
        let output = outputs.pop().unwrap();
        assert_eq!(output.len(), 1);
        let slot = output.chunk_schema().slots()[1].clone();
        drop(operator);
        drop(statistics);
        drop(writer);
        drop(source);
        drop(relation);
        drop(compiled);
        assert!(
            tracker.current() > 0,
            "final output retains real creator responsibility"
        );
        drop(output);
        assert!(
            tracker.current() > 0,
            "independent final schema slot retains its creator"
        );
        drop(slot);
        assert_eq!(
            tracker.current(),
            0,
            "last final schema and buffer owner exits"
        );
    }

    #[test]
    fn compiled_final_prepare_preserves_first_real_tracker_limit_refusal() {
        use crate::exec::pipeline::operator::ConstructionFailure;
        use crate::runtime::fragment::ExecutionFailureCause;
        let compiled = compiled(0, &[], 1);
        let state = statistics_state(theta_function_set());
        let (_, statistics) = CompiledFinishStatistics::try_new(
            &compiled.finish,
            compiled.finish.graph().root(),
            state.error_state(),
        )
        .unwrap()
        .expect("finish final declarations");
        let (mut operator, tracker) = prepared_final_operator(statistics.as_ref(), &state);
        let original = tracker.current();
        assert!(original > 0, "real installed factory backing is observed");
        tracker.install_limit_once(original).unwrap();
        let failure = operator
            .prepare()
            .expect_err("next real creator allocation is refused");
        assert!(
            matches!(
                failure.cause(),
                ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
            ),
            "original tracker cause is retained: {failure:?}"
        );
        assert_eq!(
            tracker.current(),
            original,
            "failed scope releases its partial requests"
        );
        drop(operator);
        assert_eq!(tracker.current(), 0, "no refused schema escapes");
    }

    // Reuse the actual checked Physical writer/finish fixture, immutable pools,
    // canonical call requests, LocalCompiler and original writer/finish bodies.
    include!("compiled_writer_root_positive_statistics_tests.rs");
    include!("compiled_writer_logical_finish_graph_tests.rs");
}
