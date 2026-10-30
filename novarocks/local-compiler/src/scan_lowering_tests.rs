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

//! Provider-read scans built through the real physical builders, package
//! extraction and pure provider validation, then compiled into the final
//! LocalProgram owner. The provider compiler is a pure contract fixture that
//! canonicalizes private bytes; it is not an installed Iceberg or Paimon port.

use super::*;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_connector_contract::*;
use novarocks_functions::{
    ComparisonOperator, ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind,
    FunctionOverloadId, InstalledPureKernel, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    BindingRequirement, CompiledScanInput, KernelAbiVersion, LocalProgram,
    ProgramChannelLayoutRole, ProgramChannelSite, ProgramComparisonSite, ProgramExpressionArena,
    ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, ProgramUseRef, ScanSourceKind, StaticExprKind, StaticSinkProgram,
};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    ControlShape, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, control_argument_semantics,
};
use std::{
    collections::{BTreeSet, HashMap},
    num::{NonZeroU64, NonZeroUsize},
    sync::Mutex,
};

const PRODUCER: FragmentId = FragmentId::new(1);
const CONSUMER: FragmentId = FragmentId::new(2);
const EDGE: EdgeId = EdgeId::new(5);
const SCAN: NodeId = NodeId::new(10);
const FILTER: NodeId = NodeId::new(11);
const RECEIVER: NodeId = NodeId::new(20);
const BUILD: FragmentId = FragmentId::new(3);
const BUILD_EDGE: EdgeId = EdgeId::new(6);
const BUILD_RECEIVER: NodeId = NodeId::new(12);
const JOIN: NodeId = NodeId::new(13);
const VALUES: NodeId = NodeId::new(30);
const SCAN2: NodeId = NodeId::new(14);
const UNION: NodeId = NodeId::new(15);

pub(super) struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

/// Records every compile callback and refuses exactly one, after which no
/// further callback may arrive.
pub(super) struct Trace {
    pub(super) events: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl Trace {
    pub(super) fn new(stop: Option<(usize, CompileControlError)>) -> Self {
        Self {
            events: Mutex::default(),
            stop,
        }
    }
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after the original refusal");
        }
        events.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Guarantee {
    None,
    PruningOnly,
    Exact,
}

#[derive(Clone, Copy, PartialEq)]
enum Sink {
    /// The scan fragment streams to a Gather receiver that owns the result.
    Gather,
    /// The scan fragment itself publishes the result with these labels.
    Result([&'static str; 2]),
}

#[derive(Clone, Copy)]
struct Spec {
    residuals: usize,
    guarantee: Guarantee,
    metadata: bool,
    derived: bool,
    reordered: bool,
    filter: bool,
    /// The scan probes a broadcast hash join whose build is `VALUES (7)`.
    join: bool,
    /// The scan and a second plain scan of the same relation feed a UnionAll.
    union: bool,
    kind: ConnectorReadRelationKind,
    sink: Sink,
}
impl Spec {
    /// `SELECT v0, v1 FROM t WHERE v0 > 7` with a pruning-only provider answer.
    fn slice() -> Self {
        Self {
            residuals: 1,
            guarantee: Guarantee::PruningOnly,
            metadata: false,
            derived: false,
            reordered: false,
            filter: false,
            join: false,
            union: false,
            kind: ConnectorReadRelationKind::Table,
            sink: Sink::Gather,
        }
    }
}

pub(super) fn int64() -> ValueType {
    ValueType::new(DataType::Int64, false)
}
fn boolean() -> ValueType {
    ValueType::new(DataType::Boolean, false)
}
pub(super) fn binding() -> ConnectorReadBinding {
    let instance = ConnectorInstanceId::parse("lake").unwrap();
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("alpha").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    )
}
pub(super) fn payload(
    binding: &ConnectorReadBinding,
    category: ConnectorCodecCategory,
    value: &'static [u8],
) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            category,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        Bytes::from_static(value),
    )
}
/// Provider execution projection; source IDs and attributes have their own facts.
pub(super) fn public_schema() -> Schema {
    Schema::new(vec![
        Field::new("v0", DataType::Int64, false),
        Field::new("v1", DataType::Int64, false),
    ])
}
pub(super) fn typed_public_schema(
    source: ConnectorReadStaticFacts<ScanColumnId>,
    kind: Option<ConnectorReadMetadataKind>,
    schema: Schema,
) -> ConnectorReadPublicFacts {
    let types = schema
        .fields()
        .iter()
        .map(|field| FunctionValueType::new(field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    typed_public_schema_with_types(source, kind, schema, types)
}

pub(super) fn typed_public_schema_with_types(
    source: ConnectorReadStaticFacts<ScanColumnId>,
    kind: Option<ConnectorReadMetadataKind>,
    schema: Schema,
    types: Box<[FunctionValueType]>,
) -> ConnectorReadPublicFacts {
    use novarocks_connector_contract::{
        ConnectorReadExecutionSchema, ConnectorSourceFieldFacts, ConnectorSourceSchemaFacts,
        ConnectorSourceSchemaShape,
    };
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let storage = ValueTypeStorageLifetime::pure();
    let width = schema.fields().len();
    let schema = Arc::new(schema);
    let execution =
        ConnectorReadExecutionSchema::try_new(schema.clone(), types, storage.clone()).unwrap();
    let shape = ConnectorSourceSchemaShape::try_new(schema, storage.clone()).unwrap();
    let fields = (0..width as u32)
        .map(|ordinal| {
            ConnectorSourceFieldFacts::try_new(
                Box::from([ordinal]),
                Some(ordinal as i32 + 1),
                None,
                HashMap::new(),
                &[],
                storage.clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    // This fixture is the actual map creator: one reserved table, one insertion,
    // no removals or mutable escape. Its original geometry covers opaque source
    // iteration work only; it is no FE allocation quota or charged-bytes floor.
    let table =
        novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<String, String>(1)
            .unwrap();
    let mut attributes = HashMap::with_capacity(1);
    let key = "provider.schema";
    let value = "generation-1";
    attributes.insert(key.to_owned(), value.to_owned());
    let source_bytes = table.layout.unwrap().size() + key.len() + value.len();
    let facts = ConnectorSourceSchemaFacts::try_new_observed_with_source_invoice(
        shape,
        attributes,
        fields,
        storage,
        source_bytes,
        |_| Ok::<_, novarocks_connector_contract::ConnectorError>(()),
        |_| Ok(()),
    )
    .unwrap();
    ConnectorReadPublicFacts::try_new_typed_with_control(
        source,
        kind,
        &execution,
        &facts,
        &FixtureControl,
    )
    .unwrap()
}

struct Fixture {
    package: Arc<FragmentPackage>,
}

fn fixture(spec: Spec) -> Fixture {
    let binding = binding();
    let relation_payload = ConnectorReadRelationPayload::new(
        spec.kind,
        payload(&binding, ConnectorCodecCategory::ReadTable, b"table"),
        payload(&binding, ConnectorCodecCategory::ReadView, b"view"),
    );
    let columns = [b"c0" as &'static [u8], b"c1"].map(|bytes| ProviderColumnReference {
        column_payload: payload(&binding, ConnectorCodecCategory::ReadColumn, bytes),
    });
    let mut builder = FragmentBuilder::new(PRODUCER);
    let provider = columns
        .iter()
        .map(|column| {
            builder
                .add_value(
                    int64(),
                    ValueOrigin::ProviderField {
                        scan_node: SCAN,
                        field: column.clone(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut residuals = Vec::new();
    for ordinal in 0..spec.residuals {
        let column = builder
            .add_expression(SCAN, int64(), ExprKind::Value(provider[ordinal % 2]))
            .unwrap();
        let literal = builder
            .add_expression(
                SCAN,
                int64(),
                ExprKind::Literal(LiteralValue::Int64(7 + ordinal as i64)),
            )
            .unwrap();
        residuals.push(
            builder
                .add_expression(
                    SCAN,
                    boolean(),
                    ExprKind::Binary {
                        left: column,
                        op: BinaryOperator::Gt,
                        right: literal,
                        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                        allow_throw_exception: None,
                    },
                )
                .unwrap(),
        );
    }
    let predicate_guarantees: Box<[PredicateGuarantee]> = match spec.guarantee {
        Guarantee::None => Box::default(),
        Guarantee::PruningOnly | Guarantee::Exact => Box::from([PredicateGuarantee {
            predicate: residuals[0],
            kind: if spec.guarantee == Guarantee::Exact {
                PredicateGuaranteeKind::Exact
            } else {
                PredicateGuaranteeKind::PruningOnly
            },
        }]),
    };
    let mut output = if spec.reordered {
        vec![provider[1], provider[0]]
    } else {
        provider.clone()
    };
    let mut derived_values = Vec::new();
    if spec.derived {
        let expr = builder
            .add_expression(SCAN, int64(), ExprKind::Literal(LiteralValue::Int64(9)))
            .unwrap();
        let value = builder
            .add_value(int64(), ValueOrigin::Expr { node: SCAN, expr })
            .unwrap();
        derived_values.push(value);
        output.push(value);
    }
    let properties = PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let read = ProviderReadReference {
        binding: binding.clone(),
        input_version: ExactInputVersion::try_new([9]).unwrap(),
        relation: relation_payload.clone(),
    };
    let schema = columns
        .iter()
        .map(|column| RelationField {
            column: column.clone(),
            ty: int64(),
        })
        .collect::<Box<[_]>>();
    let relation = if spec.metadata {
        Relation::Metadata(MetadataRelation {
            kind: MetadataRelationKind::try_new("lake.entries").unwrap(),
            read,

            selection_digest: [7; 32],
            schema,
            predicate_guarantees,
            provided_properties: properties.clone(),
            coverage_evidence: Box::from([4]),
        })
    } else {
        Relation::Data(DataRelation {
            read,

            selection_digest: [7; 32],
            schema,
            predicate_guarantees,
            provided_properties: properties.clone(),
        })
    };
    builder
        .add_scan(
            SCAN,
            NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget: ScanReadBudget {
                    max_batch_rows: 100,
                    max_batch_bytes: 4096,
                },
                provider_outputs: columns
                    .iter()
                    .cloned()
                    .zip(provider.iter().copied())
                    .collect(),
                residuals: residuals.into_boxed_slice(),
                derived_values: derived_values.into_boxed_slice(),
            },
            output.clone().into_boxed_slice(),
        )
        .unwrap();
    let mut root = SCAN;
    if spec.filter {
        // A row-local filter over the unconstrained scan, reading `v1`.
        let column = builder
            .add_expression(FILTER, int64(), ExprKind::Value(provider[1]))
            .unwrap();
        let literal = builder
            .add_expression(FILTER, int64(), ExprKind::Literal(LiteralValue::Int64(3)))
            .unwrap();
        let predicate = builder
            .add_expression(
                FILTER,
                boolean(),
                ExprKind::Binary {
                    left: column,
                    op: BinaryOperator::Gt,
                    right: literal,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
            )
            .unwrap();
        builder
            .add_filter(FILTER, SCAN, Box::from([predicate]))
            .unwrap();
        root = FILTER;
    }
    let dop = PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    };
    // `... JOIN broadcast(VALUES (7)) ON v0 = c0`: every instance receives
    // the whole build, and the join keeps the scan's output.
    let mut build_fragment = None;
    if spec.join {
        let mut build = FragmentBuilder::new(BUILD);
        let column = build
            .add_value(
                int64(),
                ValueOrigin::NodeOutput {
                    node: VALUES,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let cell = build
            .add_expression(VALUES, int64(), ExprKind::Literal(LiteralValue::Int64(7)))
            .unwrap();
        build
            .add_values(VALUES, Box::from([Box::from([cell])]), Box::from([column]))
            .unwrap();
        let fragment = build
            .finish_definition(
                VALUES,
                FragmentSink::Stream { edge: BUILD_EDGE },
                dop,
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let imported = builder
            .add_value(
                int64(),
                ValueOrigin::ExchangeImport {
                    edge: BUILD_EDGE,
                    source_value: column,
                },
            )
            .unwrap();
        builder
            .add_exchange_source(
                BUILD_RECEIVER,
                BUILD_EDGE,
                Box::from([(column, imported)]),
                Box::from([imported]),
                Distribution::Broadcast,
                RowMultiplicity::Replicated,
            )
            .unwrap();
        let key = JoinKey {
            left: builder
                .add_expression(JOIN, int64(), ExprKind::Value(provider[0]))
                .unwrap(),
            right: builder
                .add_expression(JOIN, int64(), ExprKind::Value(imported))
                .unwrap(),
            null_safe: false,
        };
        let probe = builder.node_output_properties(root).unwrap().clone();
        let replicated = builder
            .node_output_properties(BUILD_RECEIVER)
            .unwrap()
            .clone();
        builder
            .add_join(
                JOIN,
                [root, BUILD_RECEIVER],
                Box::from([probe, replicated]),
                output.clone().into_boxed_slice(),
                Distribution::Unconstrained,
                NodeKind::HashJoin {
                    kind: JoinKind::Inner,
                    build_side: JoinSide::Right,
                    distribution: JoinDistribution::BroadcastBuild,
                    null_extended: Box::default(),
                    condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                        keys: Box::from([key]),
                        residual: None,
                    },
                },
            )
            .unwrap();
        build_fragment = Some((fragment, column, imported));
        root = JOIN;
    }
    // `SELECT v0, v1 FROM t UNION ALL SELECT v0, v1 FROM t`: two runtime-split
    // reads of one relation, concatenated on whichever driver holds them.
    let mut second_scan = false;
    if spec.union {
        assert!(!spec.join && !spec.filter && !spec.derived && !spec.reordered);
        let second = columns
            .iter()
            .map(|column| {
                builder
                    .add_value(
                        int64(),
                        ValueOrigin::ProviderField {
                            scan_node: SCAN2,
                            field: column.clone(),
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        builder
            .add_scan(
                SCAN2,
                NodeKind::Scan {
                    occurrence: ProviderReadOccurrenceId::new(1),
                    relation: Box::new(Relation::Data(DataRelation {
                        read: ProviderReadReference {
                            binding: binding.clone(),
                            input_version: ExactInputVersion::try_new([9]).unwrap(),
                            relation: relation_payload.clone(),
                        },

                        selection_digest: [7; 32],
                        schema: columns
                            .iter()
                            .map(|column| RelationField {
                                column: column.clone(),
                                ty: int64(),
                            })
                            .collect(),
                        predicate_guarantees: Box::default(),
                        provided_properties: properties.clone(),
                    })),
                    read_budget: ScanReadBudget {
                        max_batch_rows: 100,
                        max_batch_bytes: 4096,
                    },
                    provider_outputs: columns
                        .iter()
                        .cloned()
                        .zip(second.iter().copied())
                        .collect(),
                    residuals: Box::default(),
                    derived_values: Box::default(),
                },
                second.clone().into_boxed_slice(),
            )
            .unwrap();
        let concatenated = (0..2u32)
            .map(|ordinal| {
                builder
                    .add_value(
                        int64(),
                        ValueOrigin::NodeOutput {
                            node: UNION,
                            output_ordinal: ordinal,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        builder
            .add_row_consuming(
                UNION,
                Box::from([root, SCAN2]),
                RequiredInputs::AsProduced,
                Distribution::Unconstrained,
                concatenated.clone().into_boxed_slice(),
                NodeKind::SetOp {
                    kind: SetOperationKind::UnionAll,
                    input_mappings: Box::from([
                        output.clone().into_boxed_slice(),
                        second.into_boxed_slice(),
                    ]),
                },
            )
            .unwrap();
        output = concatenated;
        root = UNION;
        second_scan = true;
    }
    let producer_sink = match spec.sink {
        Sink::Gather => FragmentSink::Stream { edge: EDGE },
        Sink::Result(_) => FragmentSink::Result,
    };
    let producer = builder
        .finish_definition(
            root,
            producer_sink,
            dop,
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(producer).unwrap();
    if let Some((fragment, column, imported)) = build_fragment {
        plan.add_fragment(fragment).unwrap();
        plan.add_edge(Edge {
            role: novarocks_physical_plan::OutputRole::Relational,
            delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
            id: BUILD_EDGE,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                port: novarocks_physical_plan::FragmentOutputPort::Root,
                fragment: BUILD,
                projection: Box::from([column]),
            },
            destination: EdgeDestination {
                fragment: PRODUCER,
                node: BUILD_RECEIVER,
                receive_mapping: Box::from([(column, imported)]),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Broadcast,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Broadcast,
                destination_multiplicity: RowMultiplicity::Replicated,
            },
        })
        .unwrap();
    }
    match spec.sink {
        Sink::Gather => {
            let mut consumer = FragmentBuilder::new(CONSUMER);
            let imports = output
                .iter()
                .map(|&source| {
                    let imported = consumer
                        .add_value(
                            int64(),
                            ValueOrigin::ExchangeImport {
                                edge: EDGE,
                                source_value: source,
                            },
                        )
                        .unwrap();
                    (source, imported)
                })
                .collect::<Vec<_>>();
            let received = imports
                .iter()
                .map(|(_, imported)| *imported)
                .collect::<Box<[_]>>();
            consumer
                .add_exchange_source(
                    RECEIVER,
                    EDGE,
                    imports.clone().into_boxed_slice(),
                    received.clone(),
                    Distribution::Singleton,
                    RowMultiplicity::SingleCopy,
                )
                .unwrap();
            plan.add_fragment(
                consumer
                    .finish_definition(
                        RECEIVER,
                        FragmentSink::Result,
                        dop,
                        novarocks_physical_plan::StagePlacementPolicy::Singleton,
                    )
                    .unwrap(),
            )
            .unwrap();
            plan.add_edge(Edge {
                role: novarocks_physical_plan::OutputRole::Relational,
                delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
                id: EDGE,
                kind: EdgeKind::Stream,
                source: EdgeSource {
                    port: novarocks_physical_plan::FragmentOutputPort::Root,
                    fragment: PRODUCER,
                    projection: output.clone().into_boxed_slice(),
                },
                destination: EdgeDestination {
                    fragment: CONSUMER,
                    node: RECEIVER,
                    receive_mapping: imports.into_boxed_slice(),
                },
                // Gather: the edge places every row on one destination.
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
                fragment: CONSUMER,
                output: OutputPort {
                    node: RECEIVER,
                    columns: received.clone(),
                },
                fields: received
                    .iter()
                    .enumerate()
                    .map(|(ordinal, value)| ResultField {
                        domain: novarocks_physical_plan::ResultValueDomain::Plain,
                        name: format!("c{ordinal}").into(),
                        alias: None,
                        value: *value,
                        ty: int64(),
                    })
                    .collect(),
            })
            .unwrap();
        }
        Sink::Result(labels) => {
            assert!(!spec.derived, "the labeled fixture has two columns");
            plan.set_result_port(ResultPort {
                scalar_schema: None,
                fragment: PRODUCER,
                output: OutputPort {
                    node: root,
                    columns: output.clone().into_boxed_slice(),
                },
                fields: output
                    .iter()
                    .zip(labels)
                    .map(|(value, label)| ResultField {
                        domain: novarocks_physical_plan::ResultValueDomain::Plain,
                        name: label.into(),
                        alias: None,
                        value: *value,
                        ty: int64(),
                    })
                    .collect(),
            })
            .unwrap();
        }
    }
    let plan = plan.finish().unwrap();

    let draft = ConnectorReadRelationRecipeDraft::try_new(
        binding,
        relation_payload,
        columns
            .iter()
            .map(|column| column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let scan = FrozenConnectorScan::try_new(
        draft,
        ["v0", "v1"]
            .into_iter()
            .map(|name| StaticScanAssignment::new(Arc::from(name), ConnectorValueType::BigInt))
            .collect(),
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        vec![],
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(4096).unwrap(),
    )
    .unwrap();
    let source = ConnectorReadStaticFacts::try_new(
        ConnectorReadInputVersion::try_new([9]).unwrap(),
        [7; 32],
        ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, vec![]).unwrap(),
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        if spec.metadata { vec![4] } else { vec![] },
    )
    .unwrap();
    let public = typed_public_schema(
        source,
        spec.metadata
            .then(|| ConnectorReadMetadataKind::try_new("lake.entries").unwrap()),
        public_schema(),
    );
    let read = FrozenConnectorRead::try_new(scan, public).unwrap();
    let mut scans = BTreeMap::from([(ProviderReadOccurrenceId::new(0), read.clone())]);
    if second_scan {
        scans.insert(ProviderReadOccurrenceId::new(1), read);
    }

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
        &scans,
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap();
    Fixture {
        package: Arc::new(packages.remove(&PRODUCER).unwrap()),
    }
}

/// One complete eager use tree per physical root site in one root domain.
pub(super) fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    struct Author<'a> {
        fragment: &'a Fragment,
        next: u32,
        uses: Vec<ExpressionInvocation<ExprId>>,
    }
    impl Author<'_> {
        fn visit(&mut self, expr: ExprId, demand: EvaluationDemand) -> ExpressionUseId {
            let id = ExpressionUseId::new(self.next);
            self.next += 1;
            let args = match &self.fragment.expressions().get(expr).unwrap().kind {
                ExprKind::Binary { left, right, .. } => vec![*left, *right],
                ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
                _ => panic!("the scan fixture has only comparisons, values and literals"),
            };
            let arguments = args
                .iter()
                .enumerate()
                .map(|(ordinal, &child)| {
                    let (child_demand, guard) = control_argument_semantics(
                        ControlShape::Eager,
                        args.len(),
                        ordinal,
                        demand,
                    )
                    .unwrap();
                    assert!(guard.is_none());
                    self.visit(child, child_demand)
                })
                .collect::<Box<[_]>>();
            self.uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: id,
                    domain: EvaluationDomainId::new(0),
                    demand,
                },
                definition: expr,
                control: ControlShape::Eager,
                arguments,
            });
            id
        }
    }
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let mut author = Author {
        fragment,
        next: 0,
        uses: vec![],
    };
    let bindings = roots
        .sites()
        .iter()
        .map(|(&site, root)| (site, author.visit(root.expr, root.demand)))
        .collect();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        author.uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap()
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
pub(super) fn package_admission() -> FragmentPackageAdmission {
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

/// Canonicalizes private bytes only; the public facts are borrowed unchanged.
struct Port;
impl ConnectorReadProgramCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        input: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let original = input.scan().recipe();
        work.step()?;
        work.finish()?;
        let canonical = |p: &ConnectorEncodedPayload| {
            ConnectorEncodedPayload::new(p.header().clone(), Bytes::from_static(b"validated"))
        };
        ConnectorReadRelationRecipeDraft::try_new(
            original.binding().clone(),
            ConnectorReadRelationPayload::new(
                original.relation().kind(),
                canonical(original.relation().table()),
                canonical(original.relation().view()),
            ),
            original.columns().iter().map(canonical).collect(),
        )
        .map_err(|error| {
            PureProviderCompileError::Provider(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                error.to_string(),
            ))
        })
    }
}
pub(super) fn providers() -> PureProviderProgramCatalog<ConnectorError> {
    let provider = ConnectorProviderId::parse("alpha").unwrap();
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            true,
            false,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            Some(Arc::new(Port)
                as Arc<
                    dyn ConnectorReadProgramCompiler<Error = ConnectorError>,
                >),
            None,
        )],
        &FixtureControl,
    )
    .unwrap()
}

// A real RAND-only sealed subset; the fixtures call no function.
pub(super) fn functions() -> PureEngineFunctionCatalog {
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

pub(super) fn options(root_sink_dop: Option<usize>) -> LocalCompileOptions {
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

fn compile_with(
    fixture: &Fixture,
    sink: Sink,
    control: &dyn PureCompileControl,
) -> Result<LocalProgram, FragmentCompileError> {
    let validated =
        validate_fragment_providers(Arc::clone(&fixture.package), &providers(), &FixtureControl)
            .unwrap();
    let root_sink_dop = match sink {
        Sink::Gather => None,
        Sink::Result(_) => Some(1),
    };
    compile_fragment(validated, &functions(), options(root_sink_dop), control)
}

fn compile(spec: Spec) -> Result<LocalProgram, FragmentCompileError> {
    compile_with(&fixture(spec), spec.sink, &FixtureControl)
}

fn refused(spec: Spec, expected: &str) {
    match compile(spec) {
        Err(FragmentCompileError::Unsupported { node, feature }) => {
            assert_eq!(feature, expected);
            assert!(node.is_some(), "a scan refusal names its physical node");
        }
        Err(other) => panic!("expected refusal {expected:?}, got {other:?}"),
        Ok(_) => panic!("expected refusal {expected:?}, got a program"),
    }
}

fn scan_address() -> BTreeMap<ProgramNodeId, CompiledScanInput> {
    BTreeMap::from([(
        ProgramNodeId::new(0),
        CompiledScanInput {
            scan_node: SCAN.get(),
        },
    )])
}

#[test]
fn residual_scan_compiles_to_the_provider_layout_seal_address_and_residual_root() {
    let fixture = fixture(Spec::slice());
    let program = compile_with(&fixture, Sink::Gather, &FixtureControl).unwrap();
    let graph = program.graph();
    assert_eq!(graph.nodes().len(), 1);
    let node = &graph.nodes()[0];
    assert!(node.legacy_native_node_id().is_none());
    let ProgramNodeKind::Scan {
        source,
        runtime_filters,
        residuals,
        limit,
    } = node.kind()
    else {
        panic!("Scan");
    };
    assert!(runtime_filters.is_empty());
    assert!(limit.is_none());
    // The complete-input provider seal, with canonical private bytes and the
    // borrowed public facts of the checked package.
    let recipe = source.compiled().expect("compiled provider seal");
    let original = &fixture.package.scans()[&SCAN];
    assert_eq!(recipe.frozen().public_facts(), original.public_facts());
    assert_eq!(
        recipe.frozen().scan().assignments(),
        original.scan().assignments()
    );
    assert_eq!(
        recipe
            .frozen()
            .scan()
            .recipe()
            .relation()
            .table()
            .payload()
            .as_ref(),
        b"validated"
    );
    // Execution projection stays metadata-free; source facts remain on its
    // original provider recipe owner, independently of layout semantics.
    let source_facts = recipe
        .frozen()
        .public_facts()
        .source_schema_facts()
        .unwrap();
    assert_eq!(
        source_facts
            .schema_attributes()
            .get("provider.schema")
            .map(String::as_str),
        Some("generation-1")
    );
    assert_eq!(source_facts.field_fact(&[0]).unwrap().field_id(), Some(1));
    let layout = node.output_layout();
    assert_eq!(layout.schema().as_ref(), &public_schema());
    assert_eq!(layout.slots().len(), 2);
    // The physical scan node is the explicit runtime split address.
    assert_eq!(program.scan_inputs(), &scan_address());
    assert!(program.exchange_inputs().is_empty());
    let requirements = graph.requirements().entries();
    assert_eq!(requirements.len(), 2);
    assert!(requirements.iter().any(|requirement| matches!(
        requirement,
        BindingRequirement::Scan {
            node,
            kind: ScanSourceKind::TypedConnector { relation },
            layout: required,
        } if *node == ProgramNodeId::new(0)
            && relation == source.relation_header()
            && required.slots() == layout.slots()
    )));
    assert!(
        requirements
            .iter()
            .any(|requirement| matches!(requirement, BindingRequirement::ExchangeOutput { .. }))
    );
    assert!(matches!(
        graph.sink(),
        Some(StaticSinkProgram::DataStream { .. })
    ));
    // The one residual is the scan's own TruthOnly root over its output.
    assert_eq!(residuals.len(), 1);
    let predicate = residuals[0];
    let checked = program.checked();
    let snapshot = checked.channels().expressions().resolved_calls().snapshot();
    let site = ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(0),
        role: ProgramNodeExpressionRole::ScanResidual { predicate: 0 },
    };
    let use_id = snapshot.bindings()[&site];
    let root = &snapshot.flows()[&ProgramExpressionArena::Main].uses()[&use_id];
    assert_eq!(root.definition, predicate);
    assert_eq!(root.context.demand, EvaluationDemand::TruthOnly);
    let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
    let StaticExprKind::Gt(column, _) = arena.node(predicate).unwrap().kind() else {
        panic!("the ordered comparison survives lowering");
    };
    assert!(matches!(
        arena.node(*column).unwrap().kind(),
        StaticExprKind::SlotId(slot) if *slot == layout.slots()[0]
    ));
    // Its column reads the scan's own output occurrence, as root binding names.
    assert_eq!(
        checked.slots()[&ProgramUseRef {
            arena: ProgramExpressionArena::Main,
            use_id: root.arguments[0],
        }],
        ProgramLexicalSource::Input(ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0,
        })
    );
    let comparison = program
        .comparison_recipe(ProgramComparisonSite::Binary(ProgramUseRef {
            arena: ProgramExpressionArena::Main,
            use_id,
        }))
        .expect("mandatory comparison recipe");
    assert_eq!(comparison.operator(), ComparisonOperator::Gt);
}

#[test]
fn plain_scan_has_no_conjunct_and_no_root() {
    let program = compile(Spec {
        residuals: 0,
        guarantee: Guarantee::None,
        ..Spec::slice()
    })
    .unwrap();
    let ProgramNodeKind::Scan { residuals, .. } = program.graph().nodes()[0].kind() else {
        panic!("Scan");
    };
    assert!(residuals.is_empty());
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    assert!(snapshot.bindings().is_empty());
    assert_eq!(program.scan_inputs(), &scan_address());
}

#[test]
fn transparent_filter_over_the_unconstrained_scan_reads_the_scan_port() {
    let program = compile(Spec {
        filter: true,
        ..Spec::slice()
    })
    .unwrap();
    let graph = program.graph();
    assert_eq!(graph.nodes().len(), 2);
    assert!(matches!(
        graph.nodes()[1].kind(),
        ProgramNodeKind::Filter { input, .. } if *input == ProgramNodeId::new(0)
    ));
    assert_eq!(program.scan_inputs(), &scan_address());
    let checked = program.checked();
    let snapshot = checked.channels().expressions().resolved_calls().snapshot();
    let filter = snapshot.bindings()[&ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(1),
        role: ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
    }];
    let root = &snapshot.flows()[&ProgramExpressionArena::Main].uses()[&filter];
    // The Filter's `v1` reads its input, the scan's output ordinal 1.
    assert_eq!(
        checked.slots()[&ProgramUseRef {
            arena: ProgramExpressionArena::Main,
            use_id: root.arguments[0],
        }],
        ProgramLexicalSource::Input(ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 1,
        })
    );
}

#[test]
fn broadcast_join_probing_the_unconstrained_scan_inherits_its_placement() {
    let program = compile(Spec {
        join: true,
        ..Spec::slice()
    })
    .unwrap();
    let graph = program.graph();
    let (scan, _) = graph
        .nodes()
        .iter()
        .enumerate()
        .find(|(_, node)| matches!(node.kind(), ProgramNodeKind::Scan { .. }))
        .expect("the probe scan");
    assert!(
        graph
            .nodes()
            .iter()
            .any(|node| matches!(node.kind(), ProgramNodeKind::Join { .. })),
        "the join compiles over the unconstrained scan"
    );
    assert_eq!(
        program.scan_inputs(),
        &BTreeMap::from([(
            ProgramNodeId::new(scan),
            CompiledScanInput {
                scan_node: SCAN.get(),
            },
        )])
    );
}

#[test]
fn union_all_of_two_runtime_split_scans_inherits_their_placement() {
    let program = compile(Spec {
        union: true,
        ..Spec::slice()
    })
    .unwrap();
    let graph = program.graph();
    let scans = graph
        .nodes()
        .iter()
        .filter(|node| matches!(node.kind(), ProgramNodeKind::Scan { .. }))
        .count();
    assert_eq!(scans, 2, "both reads compile as scans");
    assert!(
        graph
            .nodes()
            .iter()
            .any(|node| matches!(node.kind(), ProgramNodeKind::UnionAll { .. })),
        "the union compiles over the unconstrained scans"
    );
    assert_eq!(
        program
            .scan_inputs()
            .values()
            .map(|input| input.scan_node)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([SCAN.get(), SCAN2.get()])
    );
}

#[test]
fn primitive_exact_provider_guarantee_preserves_its_validated_recipe() {
    let program = compile(Spec {
        guarantee: Guarantee::Exact,
        ..Spec::slice()
    })
    .unwrap();
    let ProgramNodeKind::Scan {
        source, residuals, ..
    } = program.graph().nodes()[0].kind()
    else {
        panic!("Scan");
    };
    assert!(source.compiled().is_some());
    assert_eq!(residuals.len(), 1);
    assert_eq!(program.scan_inputs().len(), 1);
}

#[test]
fn metadata_and_table_execute_reads_compile_as_work_unit_scans() {
    for spec in [
        Spec {
            metadata: true,
            kind: ConnectorReadRelationKind::SystemTable,
            ..Spec::slice()
        },
        Spec {
            kind: ConnectorReadRelationKind::TableExecute,
            ..Spec::slice()
        },
    ] {
        let program = compile(spec).expect("supported work-unit scan compiles");
        assert_eq!(program.scan_inputs().len(), 1);
    }
}

#[test]
fn metadata_relation_with_a_non_system_kind_is_invalid() {
    let fixture = fixture(Spec {
        metadata: true,
        kind: ConnectorReadRelationKind::SystemTable,
        ..Spec::slice()
    });
    let mut node = fixture.package.fragment().nodes()[&SCAN].clone();
    let NodeKind::Scan { relation, .. } = &mut node.kind else {
        panic!("scan");
    };
    let Relation::Metadata(metadata) = relation.as_mut() else {
        panic!("metadata");
    };
    metadata.read.relation = ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::Table,
        payload(&binding(), ConnectorCodecCategory::ReadTable, b"table"),
        payload(&binding(), ConnectorCodecCategory::ReadView, b"view"),
    );
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap();
    assert!(matches!(
        crate::scan::admit_scan(&node, None, &mut work),
        Err(FragmentCompileError::Invalid(
            "metadata relation is not a system table"
        ))
    ));
}

#[test]
fn unsupported_scan_shapes_are_refused_explicitly() {
    refused(
        Spec {
            derived: true,
            ..Spec::slice()
        },
        "derived or VARIANT scan value",
    );
    refused(
        Spec {
            reordered: true,
            ..Spec::slice()
        },
        "scan output differs from its ordered provider outputs",
    );
    for kind in [
        ConnectorReadRelationKind::TableFunction,
        ConnectorReadRelationKind::MergeTable,
    ] {
        refused(
            Spec {
                kind,
                ..Spec::slice()
            },
            "provider relation other than a table",
        );
    }
}

#[test]
fn change_window_uses_the_frozen_provider_work_unit_and_exact_output_types() {
    let source = Spec {
        kind: ConnectorReadRelationKind::ChangeWindow,
        ..Spec::slice()
    };
    let program = compile(source).expect("a frozen ChangeWindow compiles through the scan owner");
    assert_eq!(program.scan_inputs().len(), 1);
    let graph = program.graph();
    let scan = graph
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), ProgramNodeKind::Scan { .. }))
        .expect("a provider scan");
    assert_eq!(scan.output_layout().schema().as_ref(), &public_schema());
}

#[test]
fn scan_rooted_result_publishes_provider_names_only_when_labels_match() {
    let labels = Sink::Result(["v0", "v1"]);
    let program = compile(Spec {
        sink: labels,
        ..Spec::slice()
    })
    .unwrap();
    assert!(matches!(
        program.graph().sink(),
        Some(StaticSinkProgram::Result)
    ));
    assert!(program.graph().requirements().entries().iter().any(
        |requirement| matches!(requirement, BindingRequirement::ResultSink { layout }
            if layout.schema().as_ref() == &public_schema())
    ));
    // The layout cannot be relabeled, so a different SQL label is refused,
    // also through a transparent Filter that reuses the scan's schema.
    for filter in [false, true] {
        refused(
            Spec {
                sink: Sink::Result(["v0", "renamed"]),
                filter,
                ..Spec::slice()
            },
            "result labels differ from the provider scan layout",
        );
    }
}

#[test]
fn every_scan_compile_callback_keeps_the_original_control_cause() {
    let fixture = fixture(Spec::slice());
    let baseline = Trace::new(None);
    compile_with(&fixture, Sink::Gather, &baseline).unwrap();
    let expected = baseline.events.lock().unwrap().clone();
    assert!(expected.iter().any(|(_, units)| *units > 0));
    for at in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Trace::new(Some((at, cause.clone())));
            assert!(matches!(
                compile_with(&fixture, Sink::Gather, &control),
                Err(FragmentCompileError::Control(actual)) if actual == cause
            ));
            assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
        }
    }
}

#[test]
fn provider_read_recipes_must_cover_exactly_the_scan_nodes() {
    let fixture = fixture(Spec::slice());
    let validated = |edit: fn(&mut BTreeMap<NodeId, ConnectorReadProgramRecipe>)| {
        let mut validated = validate_fragment_providers(
            Arc::clone(&fixture.package),
            &providers(),
            &FixtureControl,
        )
        .unwrap();
        edit(&mut validated.reads);
        compile_fragment(validated, &functions(), options(None), &FixtureControl)
    };
    // A scan without its validated recipe is never lowered from the package.
    assert!(matches!(
        validated(|reads| reads.clear()),
        Err(FragmentCompileError::Invalid(
            "scan has no provider read recipe"
        ))
    ));
    // A recipe naming no scan node is not silently dropped.
    assert!(matches!(
        validated(|reads| {
            let recipe = reads[&SCAN].clone();
            reads.insert(NodeId::new(99), recipe);
        }),
        Err(FragmentCompileError::Invalid(
            "provider read recipe names no admitted scan node"
        ))
    ));
}
