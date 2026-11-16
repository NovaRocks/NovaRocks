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

use super::*;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_connector_contract::*;
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    EvaluationDomainId, ExpressionControlFlow, ExpressionEvaluationDomain, SemanticParameters,
    ValueLogicalType,
};
use std::{
    num::NonZeroU64,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(CompileControlError, Refuse)>,
}
#[derive(Clone, Copy)]
enum Refuse {
    Entry,
    Call(usize),
    Private,
    Quantum,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::ProviderValidation);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((error, refusal)) = self.refusal.clone() {
            let refuse = match refusal {
                Refuse::Entry => trace.len() == 1,
                Refuse::Call(index) => trace.len() == index,
                Refuse::Private => units == 17,
                Refuse::Quantum => units == 256,
            };
            if refuse {
                return Err(error);
            }
        }
        Ok(())
    }
}
struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn id(name: &str) -> ConnectorProviderId {
    ConnectorProviderId::parse(name).unwrap()
}
fn binding(name: &str) -> ConnectorReadBinding {
    let instance = ConnectorInstanceId::parse("lake").unwrap();
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: id(name),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    )
}
fn payload(
    binding: &ConnectorReadBinding,
    kind: ConnectorCodecCategory,
    value: &'static [u8],
) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            kind,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        Bytes::from_static(value),
    )
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn integer() -> ValueType {
    ValueType::new(DataType::Int64, false)
}

fn add_scan(
    builder: &mut FragmentBuilder,
    node: NodeId,
    occurrence: u32,
    provider: &str,
    width: usize,
) -> (Vec<ValueId>, FrozenConnectorRead) {
    let binding = binding(provider);
    let relation_payload = ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::Table,
        payload(&binding, ConnectorCodecCategory::ReadTable, b"table"),
        payload(&binding, ConnectorCodecCategory::ReadView, b"view"),
    );
    let column = ProviderColumnReference {
        column_payload: payload(&binding, ConnectorCodecCategory::ReadColumn, b"column"),
    };
    let output = (0..width)
        .map(|_| {
            builder
                .add_value(
                    integer(),
                    ValueOrigin::ProviderField {
                        scan_node: node,
                        field: column.clone(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .add_scan(
            node,
            NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(occurrence),
                relation: Box::new(Relation::Data(DataRelation {
                    read: ProviderReadReference {
                        binding: binding.clone(),
                        input_version: ExactInputVersion::try_new([9]).unwrap(),
                        relation: relation_payload.clone(),
                    },

                    selection_digest: [7; 32],
                    schema: (0..width)
                        .map(|_| RelationField {
                            column: column.clone(),
                            ty: integer(),
                        })
                        .collect(),
                    predicate_guarantees: Box::default(),
                    provided_properties: properties(),
                })),
                read_budget: ScanReadBudget {
                    max_batch_rows: 100,
                    max_batch_bytes: 4096,
                },
                provider_outputs: output
                    .iter()
                    .map(|value| (column.clone(), *value))
                    .collect(),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
            output.clone().into_boxed_slice(),
        )
        .unwrap();
    let draft = ConnectorReadRelationRecipeDraft::try_new(
        binding,
        relation_payload,
        (0..width).map(|_| column.column_payload.clone()).collect(),
    )
    .unwrap();
    let scan = FrozenConnectorScan::try_new(
        draft,
        (0..width)
            .map(|i| {
                StaticScanAssignment::new(Arc::from(format!("v{i}")), ConnectorValueType::BigInt)
            })
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
        vec![],
    )
    .unwrap();
    let public = ConnectorReadPublicFacts::try_new(
        source,
        None,
        Schema::new(
            (0..width)
                .map(|i| Field::new(format!("v{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ),
        vec![ValueLogicalType::Physical; width],
    )
    .unwrap();
    (output, FrozenConnectorRead::try_new(scan, public).unwrap())
}

// Each writer publishes its actual complete multiplex relation through one
// exact outbound cut. The package constructor independently validates it.
fn add_writer(
    builder: &mut FragmentBuilder,
    input_node: NodeId,
    input: ValueId,
    provider: &str,
) -> (NodeId, ConnectorWriteRecipeDraft, FragmentCuts) {
    let node = NodeId::new(50);
    let binding = binding(provider);
    let handle = payload(&binding, ConnectorCodecCategory::WriteHandle, b"writer");
    let token = ConnectorWriteFieldToken::from_bytes([4; 32]);
    let specs = [
        (
            "kind",
            DataType::Int8,
            false,
            WriterRelationFieldRole::Kind,
            WriterDerivedKind::RelationKind,
        ),
        (
            "write_target_ordinal",
            DataType::Int32,
            false,
            WriterRelationFieldRole::TargetOrdinal,
            WriterDerivedKind::WriteTargetOrdinal,
        ),
        (
            "row_count",
            DataType::Int64,
            true,
            WriterRelationFieldRole::RowCount,
            WriterDerivedKind::AffectedRows,
        ),
        (
            "commit_fragment",
            DataType::Binary,
            true,
            WriterRelationFieldRole::CommitFragment,
            WriterDerivedKind::CommitFragment,
        ),
    ];
    let fields = specs
        .into_iter()
        .map(|(name, data_type, nullable, role, kind)| {
            let ty = ValueType::new(data_type, nullable);
            let value = builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: node,
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
        .collect::<Vec<_>>();
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let projected_target_fields_0: Box<[novarocks_physical_plan::WriterTargetField]> =
        Box::from([WriterTargetField {
            storage_type: None,
            projected: builder
                .add_value(
                    (integer()).clone(),
                    novarocks_physical_plan::ValueOrigin::WriterDerived {
                        writer_node: node,
                        kind: novarocks_physical_plan::WriterDerivedKind::ProjectedInput,
                    },
                )
                .unwrap(),
            provider_name: "v".into(),
            token,
            input,
            ty: integer(),
            hidden: false,
        }]);
    builder
        .add_row_consuming(
            node,
            Box::from([input_node]),
            RequiredInputs::AsProduced,
            Distribution::Unconstrained,
            fields.iter().map(|f| f.value).collect(),
            NodeKind::TableWriter {
                target: WriterTarget {
                    handle: handle.clone(),
                    write_target_ordinal: ordinal,
                    input: Box::from([input]),
                    required_distribution: Distribution::Unconstrained,
                    target_fields: projected_target_fields_0,
                    output_schema: WriterRelationSchema {
                        revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: fields.clone().into_boxed_slice(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
        )
        .unwrap();
    let projected = fields
        .iter()
        .map(|f| CutValue {
            value: f.value,
            ty: f.ty.clone(),
        })
        .collect::<Vec<_>>();
    let imports = projected
        .iter()
        .enumerate()
        .map(|(i, source)| CutImport {
            source: source.clone(),
            destination: ValueId::new(1000 + i as u32),
        })
        .collect::<Vec<_>>();
    let cuts = FragmentCuts {
        inbound: Box::default(),
        runtime_filters: Box::default(),
        runtime_filter_bindings: Box::default(),
        outbound: Box::from([OutboundFragmentCut {
            edge: EdgeId::new(8),
            kind: EdgeKind::Stream,
            destination_fragment: FragmentId::new(100),
            destination_node: NodeId::new(100),
            projection: projected.into_boxed_slice(),
            destination_imports: imports.clone().into_boxed_slice(),
            partitioning: EdgePartitioning {
                source: Distribution::Unconstrained,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Unconstrained,
                destination_multiplicity: RowMultiplicity::SingleCopy,
                routing_only: Box::default(),
            },
            source_port: novarocks_physical_plan::FragmentOutputPort::Root,
            delivery: novarocks_physical_plan::DeliveryRequirement::MustDrain,
            payload: novarocks_physical_plan::ExchangePayloadContract::WriterResults(
                WriterResultCut {
                    write_target_ordinal: ordinal,
                    schema_revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: fields
                        .iter()
                        .zip(imports)
                        .map(|(f, i)| WriterResultCutField {
                            source: f.value,
                            destination: i.destination,
                            name: f.name.clone(),
                            ty: f.ty.clone(),
                            role: f.role,
                        })
                        .collect(),
                },
            ),
        }]),
    };
    let draft = ConnectorWriteRecipeDraft::try_new(
        ConnectorWriteBinding::new(
            binding.descriptor().clone(),
            binding.catalog_handle().clone(),
        ),
        handle,
        ConnectorWriteInputShape::Data {
            fields: vec![ConnectorWriteFieldBinding::new(
                token,
                Field::new("v", DataType::Int64, false),
            )],
        },
    )
    .unwrap();
    (node, draft, cuts)
}

fn checked_package(
    read_provider: Option<&str>,
    read_count: usize,
    width: usize,
    write_provider: Option<&str>,
    sparse_max: bool,
) -> Arc<FragmentPackage> {
    let mut builder = FragmentBuilder::new(FragmentId::new(90));
    let mut scans = BTreeMap::new();
    let mut writes = BTreeMap::new();
    let mut inputs = Vec::new();
    let mut columns = Vec::new();
    if let Some(provider) = read_provider {
        for i in 0..read_count {
            let node = if sparse_max {
                assert_eq!(read_count, 1);
                assert!(write_provider.is_none());
                NodeId::new(u32::MAX)
            } else {
                NodeId::new(10 + i as u32)
            };
            let (values, frozen) = add_scan(&mut builder, node, 200 + i as u32, provider, width);
            scans.insert(node, frozen);
            inputs.push(node);
            columns.push(values);
        }
    }
    let (mut root, values) = if inputs.is_empty() {
        let node = NodeId::new(1);
        builder
            .add_values(node, Box::default(), Box::default())
            .unwrap();
        (node, Vec::new())
    } else if inputs.len() == 1 {
        (inputs[0], columns.remove(0))
    } else {
        let node = NodeId::new(40);
        let output = (0..width)
            .map(|i| {
                builder
                    .add_value(
                        integer(),
                        ValueOrigin::NodeOutput {
                            node,
                            output_ordinal: i as u32,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        builder
            .add_row_consuming(
                node,
                inputs.into_boxed_slice(),
                RequiredInputs::AsProduced,
                Distribution::Unconstrained,
                output.clone().into_boxed_slice(),
                NodeKind::SetOp {
                    kind: SetOperationKind::UnionAll,
                    input_mappings: columns.into_iter().map(Vec::into_boxed_slice).collect(),
                },
            )
            .unwrap();
        (node, output)
    };
    let (sink, cuts) = if let Some(provider) = write_provider {
        assert_eq!(values.len(), 1);
        let (writer, draft, cuts) = add_writer(&mut builder, root, values[0], provider);
        root = writer;
        writes.insert(writer, draft);
        (
            FragmentSink::Stream {
                edge: EdgeId::new(8),
            },
            cuts,
        )
    } else {
        (FragmentSink::Noop, FragmentCuts::default())
    };
    let fragment = builder
        .finish_definition(
            root,
            sink,
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        vec![],
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let expression_uses =
        PhysicalRootUses::try_new(&fragment, flow, vec![], &FixtureControl).unwrap();
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &FixtureControl).unwrap();
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &FixtureControl).unwrap();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([7; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants: novarocks_physical_plan::ConstantPools::empty(),
                fragment,
                expression_uses,
                calls,
                pruning,
                cuts,
                result: None,
                parameters: SemanticParameters::default(),
                scans,
                writes,
                annotations: Box::default(),
                layouts: Default::default(),
            },
            package_admission(),
            &FixtureControl,
        )
        .unwrap(),
    )
}

#[derive(Clone, Copy, Default)]
enum Behavior {
    #[default]
    Normal,
    Reject,
    RejectWrite,
    ObservePrivate,
    ObserveWritePrivate,
}
struct Port {
    provider: ConnectorProviderId,
    behavior: Behavior,
    reads: AtomicUsize,
    writes: AtomicUsize,
    seen: Mutex<Vec<usize>>,
}
impl Port {
    fn new(provider: &str, behavior: Behavior) -> Arc<Self> {
        Arc::new(Self {
            provider: id(provider),
            behavior,
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            seen: Mutex::default(),
        })
    }
    fn observe(
        &self,
        control: &dyn PureCompileControl,
        write: bool,
    ) -> Result<(), PureProviderCompileError<ConnectorError>> {
        self.seen
            .lock()
            .unwrap()
            .push(control as *const dyn PureCompileControl as *const () as usize);
        if matches!(self.behavior, Behavior::ObservePrivate)
            || (write && matches!(self.behavior, Behavior::ObserveWritePrivate))
        {
            control.checkpoint(CompilePhase::ProviderValidation, 17)?;
        }
        if matches!(self.behavior, Behavior::Reject)
            || (write && matches!(self.behavior, Behavior::RejectWrite))
        {
            return Err(PureProviderCompileError::Provider(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "private table rejected",
            )));
        }
        Ok(())
    }
}
impl ConnectorReadProgramCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        input: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.observe(control, false)?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let original = input.scan().recipe();
        assert_eq!(original.binding().descriptor().provider_id, self.provider);
        assert_eq!(original.relation().kind(), ConnectorReadRelationKind::Table);
        assert_eq!(original.relation().table().payload().as_ref(), b"table");
        assert_eq!(original.relation().view().payload().as_ref(), b"view");
        assert_eq!(
            input.public_facts().source().input_version().as_bytes(),
            [9]
        );
        assert_eq!(input.public_facts().source().selection_digest(), [7; 32]);
        for (i, field) in input.public_facts().schema().fields().iter().enumerate() {
            assert_eq!(field.name(), &format!("v{i}"));
            assert_eq!(field.data_type(), &DataType::Int64);
            assert!(!field.is_nullable());
            assert_eq!(
                input.public_facts().logical_types()[i],
                ValueLogicalType::Physical
            );
            assert_eq!(original.columns()[i].payload().as_ref(), b"column");
            work.step()?;
        }
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
        .map_err(|e| {
            PureProviderCompileError::Provider(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                e.to_string(),
            ))
        })
    }
}
impl ConnectorWriteRecipeCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        input: &ConnectorWriteRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<ConnectorError>> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.observe(control, true)?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        assert_eq!(input.binding().descriptor().provider_id, self.provider);
        assert_eq!(input.payload().payload().as_ref(), b"writer");
        let ConnectorWriteInputShape::Data { fields } = input.input() else {
            panic!("fixture admits only exact data writer")
        };
        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields[0].token(),
            ConnectorWriteFieldToken::from_bytes([4; 32])
        );
        assert_eq!(fields[0].field(), &Field::new("v", DataType::Int64, false));
        work.step()?;
        work.finish()?;
        ConnectorWriteRecipeDraft::try_new(
            input.binding().clone(),
            ConnectorEncodedPayload::new(
                input.payload().header().clone(),
                Bytes::from_static(b"validated-writer"),
            ),
            input.input().clone(),
        )
        .map_err(PureProviderCompileError::Provider)
    }
}
fn catalogue(
    port: &Arc<Port>,
    read: bool,
    write: bool,
) -> PureProviderProgramCatalog<ConnectorError> {
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            port.provider.clone(),
            read,
            write,
        )],
        vec![PureProviderProgramDefinition::new(
            port.provider.clone(),
            read.then(|| {
                Arc::clone(port) as Arc<dyn ConnectorReadProgramCompiler<Error = ConnectorError>>
            }),
            write.then(|| {
                Arc::clone(port) as Arc<dyn ConnectorWriteRecipeCompiler<Error = ConnectorError>>
            }),
        )],
        &FixtureControl,
    )
    .unwrap()
}

#[test]
fn sparse_max_scan_retains_the_same_package_and_complete_public_scan_facts() {
    let package = checked_package(Some("alpha"), 1, 1, None, true);
    let port = Port::new("alpha", Behavior::Normal);
    let control = Control::default();
    let result = validate_fragment_providers(
        Arc::clone(&package),
        &catalogue(&port, true, false),
        &control,
    )
    .unwrap();
    assert!(Arc::ptr_eq(result.package(), &package));
    assert_eq!(
        result.reads().keys().copied().collect::<Vec<_>>(),
        vec![NodeId::new(u32::MAX)]
    );
    assert!(result.writes().is_empty());
    let original = &package.scans()[&NodeId::new(u32::MAX)];
    let compiled = result.reads()[&NodeId::new(u32::MAX)].frozen();
    assert_eq!(compiled.public_facts(), original.public_facts());
    assert_eq!(compiled.scan().assignments(), original.scan().assignments());
    assert_eq!(
        compiled.scan().unenforced_predicate(),
        original.scan().unenforced_predicate()
    );
    assert_eq!(
        compiled.scan().enforced_predicate(),
        original.scan().enforced_predicate()
    );
    assert_eq!(
        compiled
            .scan()
            .recipe()
            .relation()
            .table()
            .payload()
            .as_ref(),
        b"validated"
    );
    assert_eq!(port.reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        port.seen.lock().unwrap().as_slice(),
        &[&control as *const Control as usize]
    );
}

#[test]
fn repeated_source_occurrences_and_writer_each_compile_once_with_exact_node_keys() {
    let package = checked_package(Some("alpha"), 2, 1, Some("alpha"), false);
    let occurrences = [NodeId::new(10), NodeId::new(11)].map(|node| {
        let NodeKind::Scan { occurrence, .. } = &package.fragment().nodes()[&node].kind else {
            panic!("the fixture must contain an actual scan occurrence");
        };
        *occurrence
    });
    assert_ne!(occurrences[0], occurrences[1]);
    assert_eq!(
        package.scans()[&NodeId::new(10)].scan().recipe(),
        package.scans()[&NodeId::new(11)].scan().recipe()
    );
    let port = Port::new("alpha", Behavior::Normal);
    let control = Control::default();
    let result = validate_fragment_providers(
        Arc::clone(&package),
        &catalogue(&port, true, true),
        &control,
    )
    .unwrap();
    assert!(Arc::ptr_eq(result.package(), &package));
    assert_eq!(result.reads().len(), 2);
    assert_eq!(
        result.writes().keys().copied().collect::<Vec<_>>(),
        vec![NodeId::new(50)]
    );
    for (&node, original) in package.scans() {
        assert_eq!(
            result.reads()[&node].frozen().public_facts(),
            original.public_facts()
        );
    }
    let compiled = result.writes()[&NodeId::new(50)].draft();
    let original = &package.writes()[&NodeId::new(50)];
    assert_eq!(compiled.binding(), original.binding());
    assert_eq!(compiled.input(), original.input());
    assert_eq!(compiled.payload().header(), original.payload().header());
    assert_eq!(compiled.payload().payload().as_ref(), b"validated-writer");
    assert_eq!(port.reads.load(Ordering::Relaxed), 2);
    assert_eq!(port.writes.load(Ordering::Relaxed), 1);
    assert_eq!(
        port.seen.lock().unwrap().as_slice(),
        &[&control as *const Control as usize; 3]
    );
}

#[test]
fn empty_provider_package_returns_no_recipes_or_private_callbacks() {
    let package = checked_package(None, 0, 0, None, false);
    let port = Port::new("alpha", Behavior::Normal);
    let result = validate_fragment_providers(
        Arc::clone(&package),
        &catalogue(&port, true, true),
        &Control::default(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(result.package(), &package));
    assert!(result.reads().is_empty() && result.writes().is_empty());
    assert_eq!(port.reads.load(Ordering::Relaxed), 0);
    assert_eq!(port.writes.load(Ordering::Relaxed), 0);
}

#[test]
fn unknown_provider_wrong_facet_and_private_refusal_keep_the_exact_read_node() {
    let package = checked_package(Some("alpha"), 1, 1, None, true);
    let cases = [
        (Port::new("other", Behavior::Normal), true, false, 0),
        (Port::new("alpha", Behavior::Normal), false, true, 1),
        (Port::new("alpha", Behavior::Reject), true, false, 2),
    ];
    for (port, read, write, case) in cases {
        let error = match validate_fragment_providers(
            Arc::clone(&package),
            &catalogue(&port, read, write),
            &Control::default(),
        ) {
            Ok(_) => panic!("invalid provider input returned a fragment"),
            Err(error) => error,
        };
        let ProviderPreparationError::Read { node, error } = error else {
            panic!("missing exact read error owner")
        };
        assert_eq!(node, NodeId::new(u32::MAX));
        match (case, error) {
            (
                0,
                PureProviderProgramError::Catalog(PureProviderCatalogError::MissingProvider(
                    actual,
                )),
            ) => assert_eq!(actual, id("alpha")),
            (
                1,
                PureProviderProgramError::Catalog(PureProviderCatalogError::ReadUnavailable(
                    actual,
                )),
            ) => assert_eq!(actual, id("alpha")),
            (2, PureProviderProgramError::Provider(actual)) => {
                assert_eq!(actual.message(), "private table rejected")
            }
            _ => panic!("provider failure lost its category"),
        }
        assert_eq!(port.reads.load(Ordering::Relaxed), usize::from(case == 2));
        assert_eq!(port.writes.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn writer_failure_after_valid_reads_never_publishes_a_partial_fragment() {
    let package = checked_package(Some("alpha"), 1, 1, Some("missing"), false);
    let port = Port::new("alpha", Behavior::Normal);
    let error = match validate_fragment_providers(
        package,
        &catalogue(&port, true, true),
        &Control::default(),
    ) {
        Ok(_) => panic!("partial read recipes published despite writer failure"),
        Err(error) => error,
    };
    assert!(
        matches!(error,ProviderPreparationError::Write { node,error:PureProviderProgramError::Catalog(PureProviderCatalogError::MissingProvider(provider)) }
        if node==NodeId::new(50) && provider==id("missing"))
    );
    assert_eq!(port.reads.load(Ordering::Relaxed), 1);
    assert_eq!(port.writes.load(Ordering::Relaxed), 0);
}

#[test]
fn writer_wrong_facet_and_private_refusal_preserve_the_node_after_successful_reads() {
    let package = checked_package(Some("alpha"), 1, 1, Some("alpha"), false);
    for reject in [false, true] {
        let port = Port::new(
            "alpha",
            if reject {
                Behavior::RejectWrite
            } else {
                Behavior::Normal
            },
        );
        let catalog = catalogue(&port, true, reject);
        let error = match validate_fragment_providers(
            Arc::clone(&package),
            &catalog,
            &Control::default(),
        ) {
            Ok(_) => panic!("a writer failure published the partial read stage"),
            Err(error) => error,
        };
        let ProviderPreparationError::Write { node, error } = error else {
            panic!("the actual writer failure lost its node owner");
        };
        assert_eq!(node, NodeId::new(50));
        if reject {
            assert!(matches!(error, PureProviderProgramError::Provider(error)
                if error.kind() == ConnectorErrorKind::InvalidRequest && error.message() == "private table rejected"));
        } else {
            assert!(
                matches!(error, PureProviderProgramError::Catalog(PureProviderCatalogError::WriteUnavailable(provider))
                if provider == id("alpha"))
            );
        }
        assert_eq!(port.reads.load(Ordering::Relaxed), 1);
        assert_eq!(port.writes.load(Ordering::Relaxed), usize::from(reject));
    }
}

#[test]
fn entry_private_quantum_and_final_publication_keep_all_three_control_categories() {
    let package = checked_package(Some("alpha"), 1, 1, Some("alpha"), false);
    let port = Port::new("alpha", Behavior::Normal);
    let catalog = catalogue(&port, true, true);
    let baseline = Control::default();
    validate_fragment_providers(Arc::clone(&package), &catalog, &baseline).unwrap();
    let successful_trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(
        successful_trace.last(),
        Some(&1),
        "the stage must flush its completed writer before publication"
    );
    for cause in causes() {
        let p = Port::new("alpha", Behavior::Normal);
        let c = catalogue(&p, true, true);
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((cause.clone(), Refuse::Entry)),
        };
        assert!(
            matches!(validate_fragment_providers(Arc::clone(&package),&c,&control),Err(ProviderPreparationError::Control(error)) if error==cause)
        );
        assert_eq!(p.reads.load(Ordering::Relaxed), 0);
        assert_eq!(p.writes.load(Ordering::Relaxed), 0);
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((cause.clone(), Refuse::Call(successful_trace.len()))),
        };
        assert!(
            matches!(validate_fragment_providers(Arc::clone(&package),&c,&control),Err(ProviderPreparationError::Control(error)) if error==cause)
        );
        assert_eq!(p.reads.load(Ordering::Relaxed), 1);
        assert_eq!(p.writes.load(Ordering::Relaxed), 1);
        assert_eq!(control.trace.lock().unwrap().len(), successful_trace.len());
        let wide = checked_package(Some("alpha"), 1, 320, None, false);
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((cause.clone(), Refuse::Quantum)),
        };
        assert!(
            matches!(validate_fragment_providers(wide,&c,&control),Err(ProviderPreparationError::Control(error)) if error==cause)
        );
        assert_eq!(control.trace.lock().unwrap().last(), Some(&256));
    }
}

#[test]
fn private_primary_control_is_not_rechecked_or_replaced_and_writer_is_not_called() {
    let package = checked_package(Some("alpha"), 1, 1, Some("alpha"), false);
    for cause in causes() {
        let port = Port::new("alpha", Behavior::ObservePrivate);
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((cause.clone(), Refuse::Private)),
        };
        assert!(
            matches!(validate_fragment_providers(Arc::clone(&package),&catalogue(&port,true,true),&control),Err(ProviderPreparationError::Control(error)) if error==cause)
        );
        assert_eq!(control.trace.lock().unwrap().last(), Some(&17));
        assert_eq!(port.reads.load(Ordering::Relaxed), 1);
        assert_eq!(port.writes.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn writer_private_primary_control_keeps_the_cause_after_compiled_read_work() {
    let package = checked_package(Some("alpha"), 1, 1, Some("alpha"), false);
    for cause in causes() {
        let port = Port::new("alpha", Behavior::ObserveWritePrivate);
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((cause.clone(), Refuse::Private)),
        };
        assert!(
            matches!(validate_fragment_providers(Arc::clone(&package), &catalogue(&port,true,true), &control),
            Err(ProviderPreparationError::Control(error)) if error == cause)
        );
        assert_eq!(control.trace.lock().unwrap().last(), Some(&17));
        assert_eq!(port.reads.load(Ordering::Relaxed), 1);
        assert_eq!(port.writes.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn ordinary_node_error_keeps_the_exact_read_cause_and_actual_control_prefix() {
    let package = checked_package(Some("missing"), 1, 1, None, true);
    let port = Port::new("alpha", Behavior::Normal);
    let catalog = catalogue(&port, true, true);
    let check_original = |result| {
        assert!(matches!(result, Err(ProviderPreparationError::Read { node,
            error: PureProviderProgramError::Catalog(PureProviderCatalogError::MissingProvider(provider))
        }) if node == NodeId::new(u32::MAX) && provider == id("missing")));
    };
    let baseline = Control::default();
    check_original(validate_fragment_providers(
        Arc::clone(&package),
        &catalog,
        &baseline,
    ));
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace, [0, 0, 0]);
    for cause in causes() {
        for at in 1..=trace.len() {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((cause.clone(), Refuse::Call(at))),
            };
            assert!(
                matches!(validate_fragment_providers(Arc::clone(&package), &catalog, &control), Err(ProviderPreparationError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((cause, Refuse::Call(trace.len() + 1))),
        };
        check_original(validate_fragment_providers(
            Arc::clone(&package),
            &catalog,
            &control,
        ));
        assert_eq!(*control.trace.lock().unwrap(), trace);
    }
    assert_eq!(port.reads.load(Ordering::Relaxed), 0);
    assert_eq!(port.writes.load(Ordering::Relaxed), 0);
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
fn package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
    novarocks_physical_plan::FragmentPackageAdmission {
        plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}
