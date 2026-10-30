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

//! Frozen writer recipes authored from the sealing write session, checked by
//! the original extraction law and carried byte-identically by the v2
//! package codec.
//!
//! The plan is the single-writer dataflow `INSERT INTO t VALUES (1, 'a'),
//! (2, 'b')` lowers to without statistics, built through the real physical
//! builders: `Values -> TableWriter -> Stream` and `ExchangeSource ->
//! TableFinish -> Result`. The session is a scripted sealing session over a
//! minimal provider; it is not an installed Iceberg session.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use novarocks_physical_plan as p;
use novarocks_spi::connector::write_stack::{
    ConnectorWriteTargetPlan, ProviderWriteRuntime, WriteRuntimeAdapter,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
    ConnectorWriteInputShape,
};
use novarocks_sql::compiler::SqlCompileControl;
use novarocks_type_contract::{
    CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, FunctionValueType,
};

use super::*;

/// A provider that owns nothing but a marker recipe.
struct MarkerProvider {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
}

impl ProviderWriteRuntime for MarkerProvider {
    type CommitHandle = ();
    type WriterHandle = ();
    type CommitFragment = ();

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

fn adapter() -> WriteRuntimeAdapter<MarkerProvider> {
    let instance = ConnectorInstanceId::parse("lake").expect("instance");
    WriteRuntimeAdapter::new(Arc::new(MarkerProvider {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("alpha").expect("provider"),
            instance_id: instance.clone(),
        },
        catalog_handle: CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    }))
}

/// The provider's own input fields: a NOT NULL `c1` and a nullable `c2`.
fn input_shape() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::Data {
        fields: vec![
            ConnectorWriteFieldBinding::try_new_typed(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                Field::new("c1", DataType::Int64, false),
                ty(DataType::Int64, false),
            )
            .expect("provider c1 complete declaration"),
            ConnectorWriteFieldBinding::try_new_typed(
                ConnectorWriteFieldToken::from_bytes([2; 32]),
                Field::new("c2", DataType::Utf8, true),
                ty(DataType::Utf8, true),
            )
            .expect("provider c2 complete declaration"),
        ],
    }
}

/// A scripted sealing session: the sealed target plans and the encoder the
/// plan's handle payloads come from.
pub(crate) struct SealedSession {
    targets: Vec<ConnectorWriteTargetPlan>,
    catalog: CatalogHandle,
}

impl SealedSession {
    fn sealing(ordinals: &[u32]) -> Self {
        let adapter = adapter();
        Self {
            targets: ordinals
                .iter()
                .map(|ordinal| {
                    ConnectorWriteTargetPlan::new(
                        WriteTargetOrdinal::try_new(*ordinal).expect("ordinal"),
                        adapter.wrap_writer_handle(()),
                        input_shape(),
                    )
                })
                .collect(),
            catalog: adapter.binding().catalog_handle().clone(),
        }
    }
}

impl WriteRecipeSession for SealedSession {
    fn write_targets(&self) -> &[ConnectorWriteTargetPlan] {
        &self.targets
    }

    fn writer_handle_payload(
        &self,
        handle: &ConnectorWriterHandle,
    ) -> Result<ConnectorEncodedPayload, String> {
        Ok(ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                handle.binding().descriptor().provider_id.clone(),
                self.catalog.clone(),
                ConnectorCodecCategory::WriteHandle,
                ConnectorCodecRevision::try_new(1).expect("revision"),
            ),
            bytes::Bytes::from_static(b"sealed-writer-handle"),
        ))
    }
}

fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
}

fn property(distribution: p::Distribution) -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn dop() -> p::PipelineDopDomain {
    p::PipelineDopDomain {
        min: 1,
        max: 8,
        requires_power_of_two: false,
    }
}

/// The writer multiplex relation (`root == false`) or the Root result
/// relation (`root == true`), exactly as the SPI defines them.
fn relation_fields(
    builder: &mut p::FragmentBuilder,
    owner: p::NodeId,
    root: bool,
) -> Box<[p::WriterRelationField]> {
    use novarocks_spi::connector::write_stack::{root_write_result_schema, writer_output_schema};
    use p::WriterDerivedKind as K;
    use p::WriterRelationFieldRole as R;
    // The relation is the SPI's own schema, field by field; only the planner
    // roles are stated here.
    let schema = if root {
        root_write_result_schema()
    } else {
        writer_output_schema()
    };
    let specs = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let (role, kind) = match index {
                0 => (R::Kind, K::RelationKind),
                1 => (R::TargetOrdinal, K::WriteTargetOrdinal),
                2 => (R::RowCount, K::AffectedRows),
                3 => (R::CommitFragment, K::CommitFragment),
                _ => (R::Auxiliary, K::RelationAuxiliary),
            };
            (
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
                role,
                kind,
            )
        })
        .collect::<Vec<_>>();
    specs
        .into_iter()
        .map(|(name, carrier, nullable, role, kind)| {
            let ty = ty(carrier, nullable);
            let value = builder
                .add_value(
                    ty.clone(),
                    p::ValueOrigin::WriterDerived {
                        writer_node: owner,
                        kind,
                    },
                )
                .expect("relation value");
            p::WriterRelationField {
                value,
                name: name.into(),
                ty,
                role,
            }
        })
        .collect()
}

/// One checked constant pool of a whole Values column.
fn pool(
    field: &str,
    value_type: FunctionValueType,
    data: arrow::array::ArrayData,
) -> p::ConstantPool {
    p::ConstantPool::try_new(
        Arc::new(Field::new(field, value_type.data_type().clone(), false)),
        value_type,
        data,
        crate::application::test_constant_policy(),
        CompilePhase::Validate,
        &SqlCompileControl::unbounded(),
    )
    .expect("constant pool")
}

/// The completed plan `INSERT INTO t VALUES (1, 'a'), (2, 'b')` lowers to
/// without statistics, with the session that sealed its one target.
pub(crate) fn insert_values_plan() -> (p::PhysicalPlan, SealedSession) {
    insert_values_plan_writing(0, &[0])
}

fn insert_values_plan_writing(written: u32, sealed: &[u32]) -> (p::PhysicalPlan, SealedSession) {
    let session = SealedSession::sealing(sealed);
    let handle = session
        .writer_handle_payload(&adapter().wrap_writer_handle(()))
        .expect("handle payload");
    let ordinal = WriteTargetOrdinal::try_new(written).expect("ordinal");
    let ints = p::ConstantPoolId::new(1);
    let strings = p::ConstantPoolId::new(2);
    let writer_fragment = p::FragmentId::new(1);
    let finish_fragment = p::FragmentId::new(2);
    let edge = p::EdgeId::new(7);

    let mut builder = p::FragmentBuilder::new(writer_fragment);
    let source = builder.reserve_node_id().expect("source id");
    let columns = [
        (ints, ty(DataType::Int64, false)),
        (strings, ty(DataType::Utf8, false)),
    ];
    let mut values = Vec::new();
    for (ordinal, (_, value_type)) in columns.iter().enumerate() {
        values.push(
            builder
                .add_value(
                    value_type.clone(),
                    p::ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .expect("values output"),
        );
    }
    let rows = (0..2u32)
        .map(|row| {
            columns
                .iter()
                .map(|(pool, value_type)| {
                    builder
                        .add_expression(
                            source,
                            value_type.clone(),
                            p::ExprKind::Constant(p::ConstantReference {
                                pool: *pool,
                                ordinal: row,
                            }),
                        )
                        .expect("constant cell")
                })
                .collect::<Box<[_]>>()
        })
        .collect::<Box<[_]>>();
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(p::Distribution::Singleton),
            output: p::OutputPort {
                node: source,
                columns: values.clone().into_boxed_slice(),
            },
            kind: p::NodeKind::Values { rows },
        })
        .expect("values");
    let writer = builder.reserve_node_id().expect("writer id");
    let fields = relation_fields(&mut builder, writer, false);
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: writer,
            inputs: Box::from([source]),
            required_inputs: Box::from([property(p::Distribution::Singleton)]),
            output_properties: property(p::Distribution::Unconstrained),
            output: p::OutputPort {
                node: writer,
                columns: fields.iter().map(|field| field.value).collect(),
            },
            kind: p::NodeKind::TableWriter {
                target: p::WriterTarget {
                    handle,
                    write_target_ordinal: ordinal,
                    input: values.clone().into_boxed_slice(),
                    required_distribution: p::Distribution::Singleton,
                    target_fields: input_shape()
                        .fields_iter()
                        .zip(&values)
                        .map(|(binding, value)| p::WriterTargetField {
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
                    output_schema: p::WriterRelationSchema {
                        revision: p::WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: fields.clone(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
        })
        .expect("writer");
    let producer = builder
        .finish_definition(
            writer,
            p::FragmentSink::Stream { edge },
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .expect("writer fragment");

    let mut builder = p::FragmentBuilder::new(finish_fragment);
    let exchange = builder.reserve_node_id().expect("exchange id");
    let imported = fields
        .iter()
        .map(|field| {
            let value = builder
                .add_value(
                    field.ty.clone(),
                    p::ValueOrigin::ExchangeImport {
                        edge,
                        source_value: field.value,
                    },
                )
                .expect("import");
            p::WriterRelationField {
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
        .insert_node_unchecked(p::PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(p::Distribution::Singleton),
            output: p::OutputPort {
                node: exchange,
                columns: imported.iter().map(|field| field.value).collect(),
            },
            kind: p::NodeKind::ExchangeSource {
                edge,
                imports: mapping.clone(),
            },
        })
        .expect("exchange");
    let finish = builder.reserve_node_id().expect("finish id");
    let outputs = relation_fields(&mut builder, finish, true);
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: finish,
            inputs: Box::from([exchange]),
            required_inputs: Box::from([property(p::Distribution::Singleton)]),
            output_properties: property(p::Distribution::Singleton),
            output: p::OutputPort {
                node: finish,
                columns: outputs.iter().map(|field| field.value).collect(),
            },
            kind: p::NodeKind::TableFinish(p::WriterFinishSpec {
                count_mode: novarocks_physical_plan::FinishCountMode::WriterReported,
                inputs: novarocks_physical_plan::FinishInputs {
                    writer_results: 0,
                    logical_summary: None,
                },
                logical_summary: None,
                expected_target_ordinals: Box::from([ordinal]),
                input_schema: p::WriterRelationSchema {
                    revision: p::WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: imported,
                },
                output_schema: p::WriterRelationSchema {
                    revision: p::ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: outputs.clone(),
                },
                final_aggregates: Box::default(),
                grouped_unpivot: None,
            }),
        })
        .expect("finish");
    let consumer = builder
        .finish_definition(
            finish,
            p::FragmentSink::Result,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .expect("finish fragment");

    let mut plan = p::PlanBuilder::new(p::PlanVersionId::try_new([4; 16]).expect("version"));
    plan.insert_constant_pool(
        ints,
        pool(
            "c1",
            ty(DataType::Int64, false),
            Int64Array::from(vec![1, 2]).to_data(),
        ),
    )
    .expect("int pool");
    plan.insert_constant_pool(
        strings,
        pool(
            "c2",
            ty(DataType::Utf8, false),
            StringArray::from(vec!["a", "b"]).to_data(),
        ),
    )
    .expect("string pool");
    plan.add_fragment(producer).expect("add writer fragment");
    plan.add_fragment(consumer).expect("add finish fragment");
    plan.add_edge(p::Edge {
        role: p::OutputRole::WriterResults,
        delivery: p::DeliveryRequirement::MustDrain,
        id: edge,
        kind: p::EdgeKind::Stream,
        source: p::EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: writer_fragment,
            projection: fields.iter().map(|field| field.value).collect(),
        },
        destination: p::EdgeDestination {
            fragment: finish_fragment,
            node: exchange,
            receive_mapping: mapping,
        },
        partitioning: p::EdgePartitioning {
            source: p::Distribution::Singleton,
            source_multiplicity: p::RowMultiplicity::SingleCopy,
            destination: p::Distribution::Singleton,
            destination_multiplicity: p::RowMultiplicity::SingleCopy,
        },
    })
    .expect("edge");
    plan.set_result_port(p::ResultPort {
        scalar_schema: None,
        fragment: finish_fragment,
        output: p::OutputPort {
            node: finish,
            columns: outputs.iter().map(|field| field.value).collect(),
        },
        fields: outputs
            .iter()
            .map(|field| p::ResultField {
                domain: p::ResultValueDomain::Plain,
                name: field.name.clone(),
                alias: None,
                value: field.value,
                ty: field.ty.clone(),
            })
            .collect(),
    })
    .expect("result port");
    (
        plan.finish_observed(&SqlCompileControl::unbounded())
            .expect("validated writer plan"),
        session,
    )
}

/// One complete eager use per constant root site in one root domain.
fn root_uses(fragment: &p::Fragment) -> p::PhysicalRootUses {
    let control = SqlCompileControl::unbounded();
    let roots = p::PhysicalExpressionRoots::try_new(fragment, &control).expect("roots");
    let mut uses = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(index, (&site, root))| {
            let id = ExpressionUseId::new(index as u32);
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
    let flow = ExpressionControlFlow::<p::ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &control,
    )
    .expect("flow");
    p::PhysicalRootUses::try_new(fragment, flow, bindings, &control).expect("root uses")
}

/// Package admission within the test encode limits; not a production sizing.
fn admission() -> p::FragmentPackageAdmission {
    p::FragmentPackageAdmission {
        plan_limits: p::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: p::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

fn extract(
    plan: &p::PhysicalPlan,
    writes: &BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>,
) -> Result<BTreeMap<p::FragmentId, p::FragmentPackage>, p::FragmentPackageExtractionError> {
    let control = SqlCompileControl::unbounded();
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let root_uses = root_uses(fragment);
        calls.insert(
            id,
            p::FrozenFragmentCalls::try_new(fragment, &root_uses, vec![], &control).expect("calls"),
        );
        uses.insert(id, root_uses);
        pruning.insert(
            id,
            p::FrozenFragmentPruning::try_new(id, vec![], &control).expect("pruning"),
        );
        admissions.insert(id, admission());
    }
    p::extract_fragment_packages(
        plan,
        &BTreeMap::new(),
        writes,
        &uses,
        &calls,
        &pruning,
        &admissions,
        &control,
    )
}

/// Real checked writer/finish packages frozen by the production artifact author.
/// This component fixture installs no provider runtime or application workload.
pub(crate) fn frozen_write_fragments() -> (
    p::PhysicalPlan,
    crate::query_execution::native_fragment::NativeFragmentAttachment,
) {
    let (plan, session) = insert_values_plan();
    let control = SqlCompileControl::unbounded();
    let writes = author_frozen_writes(&plan, &session, &control).expect("sealed write recipes");
    let packages = extract(&plan, &writes).expect("checked writer/finish packages");
    let fragments = packages
        .into_iter()
        .map(|(id, package)| {
            let artifact =
                crate::native::fragment_encoder::frozen::FragmentArtifact::freeze_package(
                    &package,
                    plan.edges(),
                    &crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
                        novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
                        novarocks_plan_codec::physical_package_v2::test_support::encode_limits(),
                    ),
                    &control,
                )
                .expect("production frozen writer facts");
            (id.get(), artifact)
        })
        .collect();
    let root = crate::query_execution::physical_encoding::completed_plan_topology(&plan)
        .unwrap()
        .anchor;
    let attachment =
        crate::query_execution::native_fragment::NativeFragmentAttachment::for_completed_plan(
            fragments, root,
        )
        .unwrap();
    (plan, attachment)
}

fn writer_target(plan: &p::PhysicalPlan) -> (p::NodeId, &p::WriterTarget) {
    plan.fragments()
        .values()
        .flat_map(|fragment| fragment.nodes().values())
        .find_map(|node| match &node.kind {
            p::NodeKind::TableWriter { target } => Some((node.id, target)),
            _ => None,
        })
        .expect("the plan writes")
}

/// The session authors exactly the written target's recipe: its binding, the
/// handle payload the plan's own writer states and the signed input shape.
/// The extraction law places it on the writer, and the v2 package codec
/// carries it byte-identically.
#[test]
fn the_sealing_session_authors_the_written_recipe_the_package_carries() {
    use novarocks_plan_codec::physical_package_v2::test_support::{decode_limits, encode_limits};
    use novarocks_plan_codec::physical_package_v2::{
        decode_fragment_package, encode_fragment_package,
    };
    use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
    use prost::Message;

    let (plan, session) = insert_values_plan();
    let control = SqlCompileControl::unbounded();
    let writes = author_frozen_writes(&plan, &session, &control).expect("frozen writes");
    let ordinal = WriteTargetOrdinal::try_new(0).expect("ordinal");
    assert_eq!(writes.keys().copied().collect::<Vec<_>>(), [ordinal]);
    let draft = &writes[&ordinal];
    let (writer, target) = writer_target(&plan);
    assert_eq!(draft.binding(), session.targets[0].handle().binding());
    assert_eq!(draft.payload(), &target.handle);
    assert_eq!(draft.input(), session.targets[0].input());

    let packages = extract(&plan, &writes).expect("checked packages");
    assert_eq!(packages.len(), 2);
    let model = FragmentDecodeResourceModel::try_new(&control).expect("decode model");
    for (id, package) in &packages {
        let writes_here = package
            .fragment()
            .nodes()
            .get(&writer)
            .is_some_and(|node| matches!(node.kind, p::NodeKind::TableWriter { .. }));
        let expected = if writes_here {
            BTreeMap::from([(writer, draft.clone())])
        } else {
            BTreeMap::new()
        };
        assert_eq!(package.writes(), &expected, "fragment {id:?}");
        let frozen = crate::native::fragment_encoder::frozen::FragmentArtifact::freeze_package(
            package,
            plan.edges(),
            &crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
                novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
                encode_limits(),
            ),
            &control,
        )
        .expect("freeze the same checked write package");
        let facts = frozen.facts();
        assert_eq!(facts.declares_table_writer(), writes_here);
        if writes_here {
            assert_eq!(facts.table_writers().len(), 1);
            assert_eq!(facts.table_writers()[0].node(), writer);
            assert_eq!(facts.table_writers()[0].target(), ordinal);
        } else {
            assert!(facts.table_writers().is_empty());
        }

        let bytes = encode_fragment_package(package, &encode_limits(), &control)
            .unwrap_or_else(|error| panic!("fragment {id:?} encodes: {error}"))
            .encode_to_vec();
        // The recipe travels in the writer fragment's package and the finish
        // fragment carries none; each receiver reads its package back and the
        // sender reproduces the exact bytes.
        let decoded = decode_fragment_package(&bytes, &model, &decode_limits(), &control)
            .unwrap_or_else(|error| panic!("fragment {id:?} receives: {error:?}"));
        assert_eq!(
            decoded.writes(),
            &expected,
            "fragment {id:?} decodes its recipe"
        );
        let again = encode_fragment_package(&decoded, &encode_limits(), &control)
            .expect("re-encode")
            .encode_to_vec();
        assert_eq!(again, bytes, "fragment {id:?} roundtrips byte-identically");
    }
}

/// A sealed target the plan does not write belongs to another of the
/// statement's queries: it is not authored, and the extraction law -- which
/// refuses an unused recipe -- accepts the authored set.
#[test]
fn a_sealed_target_the_plan_does_not_write_is_not_authored() {
    let (plan, session) = insert_values_plan_writing(0, &[0, 1]);
    let writes = author_frozen_writes(&plan, &session, &SqlCompileControl::unbounded())
        .expect("frozen writes");
    assert_eq!(
        writes
            .keys()
            .map(|ordinal| ordinal.get())
            .collect::<Vec<_>>(),
        [0]
    );
    extract(&plan, &writes).expect("only the written recipe is carried");
}

/// A written target the session did not seal has no recipe author; nothing
/// is defaulted.
#[test]
fn a_written_target_the_session_did_not_seal_is_refused() {
    let (plan, session) = insert_values_plan_writing(0, &[1]);
    match author_frozen_writes(&plan, &session, &SqlCompileControl::unbounded()) {
        Err(PackageFreezeError::Facts(reason)) => {
            assert!(reason.contains("did not seal"), "{reason}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// A handle payload that is not the binding's own is refused by the draft
/// law, before any package is authored.
#[test]
fn a_handle_payload_foreign_to_its_binding_is_refused_by_the_draft_law() {
    struct ForeignEncoder(SealedSession);
    impl WriteRecipeSession for ForeignEncoder {
        fn write_targets(&self) -> &[ConnectorWriteTargetPlan] {
            self.0.write_targets()
        }
        fn writer_handle_payload(
            &self,
            handle: &ConnectorWriterHandle,
        ) -> Result<ConnectorEncodedPayload, String> {
            let instance = ConnectorInstanceId::parse("other").expect("instance");
            Ok(ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    handle.binding().descriptor().provider_id.clone(),
                    CatalogHandle::new(instance, CatalogVersion::from_bytes([5; 32])),
                    ConnectorCodecCategory::WriteHandle,
                    ConnectorCodecRevision::try_new(1).expect("revision"),
                ),
                bytes::Bytes::from_static(b"foreign"),
            ))
        }
    }
    let (plan, session) = insert_values_plan();
    match author_frozen_writes(
        &plan,
        &ForeignEncoder(session),
        &SqlCompileControl::unbounded(),
    ) {
        Err(PackageFreezeError::Write(reason)) => {
            assert!(reason.contains("writer recipe"), "{reason}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// A real sealed RAND-only subset; the finish binds no function, but the
/// compiler refuses an empty pure catalog. This is not the Server catalogue.
fn sealed_rand_subset() -> novarocks_functions::PureEngineFunctionCatalog {
    use novarocks_functions::{
        EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
        InstalledPureKernel, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    };
    let actual = novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
        .expect("builtin catalogue");
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .expect("rand definition")
                .clone(),
        )
        .expect("register rand");
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
        .expect("sealed rand subset")
}

/// The FE commit path is carrier-independent: the Root relation a compiled
/// TableFinish publishes -- the frozen relation names and types under the
/// program's own slots, nested child names included -- passes the
/// frontend's exact-schema decoder unchanged, and a relation whose list
/// child is named otherwise does not.
#[test]
fn a_compiled_root_relation_passes_the_frontend_exact_schema_decoder() {
    use arrow::array::{ArrayRef, BinaryArray, Int8Array, Int32Array, new_null_array};
    use arrow::record_batch::RecordBatch;
    use novarocks_execution::exec::chunk::{Chunk, ChunkSchema};
    use novarocks_local_program::{KernelAbiVersion, ProgramNodeKind};

    use crate::query_execution::write_result::{RootWriteDecodeContract, RootWriteResultDecoder};

    let (plan, session) = insert_values_plan();
    let control = SqlCompileControl::unbounded();
    let writes = author_frozen_writes(&plan, &session, &control).expect("frozen writes");
    let mut packages = extract(&plan, &writes).expect("checked packages");
    let finish = packages
        .remove(&p::FragmentId::new(2))
        .expect("finish fragment package");
    let providers =
        novarocks_connector_contract::PureProviderProgramCatalog::<std::io::Error>::try_new(
            &[],
            vec![],
            &control,
        )
        .expect("empty provider catalog");
    let validated = novarocks_local_compiler::validate_fragment_providers(
        Arc::new(finish),
        &providers,
        &control,
    )
    .expect("the finish carries no provider recipe");
    let program = novarocks_local_compiler::compile_fragment(
        validated,
        &sealed_rand_subset(),
        novarocks_local_compiler::LocalCompileOptions {
            pipeline_dop: std::num::NonZeroUsize::new(1).expect("dop"),
            root_sink_dop: std::num::NonZeroUsize::new(1),
            kernel_abi: KernelAbiVersion::CURRENT,
            constants: crate::application::test_constant_policy(),
            exchange_wait: std::time::Duration::from_secs(60),
        },
        &control,
    )
    .expect("the finish fragment compiles");
    let ProgramNodeKind::TableFinish {
        root_result_layout, ..
    } = program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("the finish fragment roots at its TableFinish");
    };

    let root_chunk = |schema: novarocks_execution::exec::chunk::ChunkSchemaRef| {
        let arrow = schema.arrow_schema_ref();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![None, Some(0)])),
            Arc::new(Int64Array::from(vec![Some(2), None])),
            Arc::new(BinaryArray::from(vec![None, Some(b"staged".as_ref())])),
        ];
        for field in arrow.fields().iter().skip(4) {
            columns.push(new_null_array(field.data_type(), 2));
        }
        let batch = RecordBatch::try_new(arrow, columns).expect("root batch");
        Chunk::try_new_with_chunk_schema(batch, schema).expect("root chunk")
    };
    let ordinal = WriteTargetOrdinal::try_new(0).expect("ordinal");
    let mut decoder = RootWriteResultDecoder::for_test_local(
        RootWriteDecodeContract::try_new(&[ordinal], session.write_targets())
            .expect("decode contract"),
    );
    decoder
        .apply_chunk(&root_chunk(
            ChunkSchema::from_compiled_layout(root_result_layout).expect("compiled root schema"),
        ))
        .expect("the compiled Root relation decodes");
    decoder.observe_root_eof().expect("eof");
    let set = decoder.finish().expect("a complete prepared write set");
    assert_eq!(set.row_count(), 2);
    assert_eq!(set.fragments(), &[(ordinal, b"staged".to_vec())]);

    // The decoder is exact: a list child named `element` is another relation.
    let mut drifted = root_result_layout
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    drifted[4] = Field::new(
        "input_fields",
        DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
        true,
    );
    let drifted = ChunkSchema::try_ref_from_schema_and_slot_ids(
        &arrow::datatypes::Schema::new(drifted),
        root_result_layout.slots(),
    )
    .expect("drifted schema");
    let mut decoder = RootWriteResultDecoder::for_test_local(
        RootWriteDecodeContract::try_new(&[ordinal], session.write_targets())
            .expect("decode contract"),
    );
    assert!(decoder.apply_chunk(&root_chunk(drifted)).is_err());
}
