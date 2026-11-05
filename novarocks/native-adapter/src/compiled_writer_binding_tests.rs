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

//! The compiled writer binder against the installed test write execution.
//!
//! The test write execution belongs to provider `test` over catalog
//! `write_catalog` at generation `[9; 32]`; its codec decodes a non-empty
//! UTF-8 handle payload under exactly that header. The pure write compiler
//! keeps the draft, because the test provider has no private facts.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use bytes::Bytes;
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorWriteBinding, ConnectorWriteFieldBinding,
    ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteRecipeCompiler,
    ConnectorWriteRecipeDraft, PureProviderCompileError,
};
use novarocks_spi::connector::ConnectorStopOwner;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use novarocks_types::{QueryExecutionId, UniqueId};

use super::*;
use crate::connector_write_test_support::{
    RecordingWriteExecution, TEST_WRITE_CATALOG, test_write_adapter, test_write_scan_runtime,
};

struct Unbounded;
impl PureCompileControl for Unbounded {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

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

/// A write binding of provider `test` over `catalog` at generation `[9; 32]`.
fn binding(catalog: &str) -> ConnectorWriteBinding {
    let instance = ConnectorInstanceId::try_from_canonical(catalog).unwrap();
    ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("test").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([9; 32])),
    )
}

/// A provider-validated recipe whose handle payload is `handle` under its
/// binding's own header.
fn recipe(binding: ConnectorWriteBinding, handle: &'static [u8]) -> ConnectorWriteRecipe {
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        Bytes::from_static(handle),
    );
    let draft = ConnectorWriteRecipeDraft::try_new(
        binding,
        payload,
        ConnectorWriteInputShape::Data {
            fields: vec![
                ConnectorWriteFieldBinding::try_new_typed(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("c1", DataType::Int64, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                )
                .unwrap(),
            ],
        },
    )
    .unwrap();
    ConnectorWriteRecipe::try_compile_with_provider(&draft, &KeepDraft, &Unbounded).unwrap()
}

fn execution_id() -> QueryExecutionId {
    QueryExecutionId::new(
        novarocks_types::QueryId::new(5, 6),
        novarocks_types::AttemptId::new(3).expect("attempt"),
    )
    .expect("execution id")
}

const FINST: UniqueId = UniqueId::new(7, 8);

fn bind(
    runtime: &novarocks_worker::TypedScanRuntime,
    recipe: &ConnectorWriteRecipe,
) -> Result<TableWriterRuntimeBinding, CompiledWriterBindingError> {
    let options = QueryOptions::default();
    bind_compiled_writer(
        ProgramNodeId::new(1),
        recipe,
        &CompiledWriteTaskFacts {
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            #[cfg(novarocks_dev_controls)]
            observation_controls: None,
            backend_process_id: novarocks_types::BackendProcessId::new_v7(),
            runtime,
            task_key: FINST,
            query_options: &options,
            stop: ConnectorStopOwner::new().view(),
        },
    )
}

/// A recipe binds the installed write execution of exactly its binding: the
/// handle is decoded from the recipe's own payload and names that binding,
/// and the writer context is the Task's attempt with writer ordinal 0.
#[test]
fn a_compiled_writer_binds_the_installed_execution_of_its_binding() {
    let runtime = test_write_scan_runtime(
        execution_id(),
        FINST,
        Arc::new(RecordingWriteExecution::new()),
    );
    let writer = bind(&runtime, &recipe(binding(TEST_WRITE_CATALOG), b"target-0"))
        .expect("the recipe binds");
    assert_eq!(writer.handle().binding(), test_write_adapter().binding());
    let physical = writer.physical_template().for_driver(2);
    assert_eq!(physical.writer_ordinal(), 0);
    assert_eq!(physical.driver_id(), 2);
    assert_eq!(physical.execution_attempt_id(), 3);
}

/// A recipe for a catalog this query installed no write execution for has no
/// write capability on this backend; nothing is defaulted.
#[test]
fn a_recipe_for_an_uninstalled_catalog_is_refused() {
    let runtime = test_write_scan_runtime(
        execution_id(),
        FINST,
        Arc::new(RecordingWriteExecution::new()),
    );
    let error = match bind(&runtime, &recipe(binding("other_catalog"), b"target-0")) {
        Ok(_) => panic!("an uninstalled catalog must be refused"),
        Err(error) => error,
    };
    assert_eq!(error.node(), Some(1));
    use novarocks_worker::CatalogExecutionAccessError;
    let actual = crate::backend_test_support::find_source::<CatalogExecutionAccessError>(&error)
        .expect("original query-leased catalog lookup");
    assert_eq!(
        actual,
        &CatalogExecutionAccessError::LeaseUnavailable {
            catalog: binding("other_catalog").catalog_handle().clone()
        }
    );
    use novarocks_execution_contract::failure::FailureProjection;
    assert_eq!(error.failure_fact(), actual.failure_fact());
}

/// A handle payload the installed decoder refuses is refused, not repaired.
#[test]
fn a_handle_the_installed_decoder_refuses_is_refused() {
    let runtime = test_write_scan_runtime(
        execution_id(),
        FINST,
        Arc::new(RecordingWriteExecution::new()),
    );
    let error = match bind(&runtime, &recipe(binding(TEST_WRITE_CATALOG), b"")) {
        Ok(_) => panic!("an undecodable handle must be refused"),
        Err(error) => error,
    };
    use novarocks_spi::connector::{
        ConnectorCodecError, ConnectorCodecErrorKind, ConnectorFieldPath,
    };
    let actual = crate::backend_test_support::find_source::<ConnectorCodecError>(&error)
        .expect("the installed decoder's actual source");
    assert_eq!(actual.kind(), &ConnectorCodecErrorKind::InvalidValue);
    assert_eq!(
        actual.path(),
        &ConnectorFieldPath::root("writer_handle").field("provider_payload")
    );
    use novarocks_execution_contract::failure::FailureProjection;
    assert_eq!(error.failure_fact(), actual.failure_fact());
}

/// A finish binds the Task's canonical carrier validator, which refuses a
/// carrier that is not a canonical commit fragment.
#[test]
fn a_compiled_finish_binds_the_canonical_carrier_validator() {
    use novarocks_execution::exec::node::table_write_relation::ConnectorCommitFragmentCarrierValidator;

    let runtime = test_write_scan_runtime(
        execution_id(),
        FINST,
        Arc::new(RecordingWriteExecution::new()),
    );
    let options = QueryOptions::default();
    bind_compiled_finish(
        ProgramNodeId::new(2),
        &CompiledWriteTaskFacts {
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            #[cfg(novarocks_dev_controls)]
            observation_controls: None,
            backend_process_id: novarocks_types::BackendProcessId::new_v7(),
            runtime: &runtime,
            task_key: FINST,
            query_options: &options,
            stop: ConnectorStopOwner::new().view(),
        },
    )
    .expect("the finish binds");
    let validator = RootCommitFragmentCarrierValidator::new(execution_id(), 2);
    assert!(
        validator
            .validate(
                novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).unwrap(),
                b"not a canonical carrier",
            )
            .is_err()
    );
}

/// The pure provider catalog that seals the test provider's writer recipes.
pub(crate) fn writer_providers()
-> novarocks_connector_contract::PureProviderProgramCatalog<ConnectorError> {
    use novarocks_connector_contract::{
        PureProviderManifestEntry, PureProviderProgramCatalog, PureProviderProgramDefinition,
    };
    let provider = ConnectorProviderId::parse("test").unwrap();
    PureProviderProgramCatalog::try_new(
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
        &Unbounded,
    )
    .unwrap()
}

/// The v2 bytes of the writer fragment of `INSERT INTO t VALUES (42)`
/// without statistics over the test write catalog, streaming its writer
/// relation to a finish fragment's receiver. Returns the bytes and that
/// receiver node.
pub(crate) fn writer_producer_package() -> (Vec<u8>, u32) {
    use novarocks_physical_plan::*;
    use novarocks_plan_codec::physical_package_v2::encode_fragment_package;
    use novarocks_plan_codec::physical_package_v2::test_support::encode_limits;
    use novarocks_spi::connector::write_stack::{root_write_result_schema, writer_output_schema};
    use novarocks_type_contract::{
        ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
        ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    };
    use prost::Message;
    use std::collections::BTreeMap;

    const EDGE: EdgeId = EdgeId::new(3);
    let int64 = || ValueType::new(DataType::Int64, false);
    let property = |distribution| PhysicalProperties {
        distribution,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let dop = PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    };
    let draft = recipe(binding(TEST_WRITE_CATALOG), b"target-0")
        .draft()
        .clone();
    let relation = |builder: &mut FragmentBuilder, owner: NodeId, root: bool| {
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
                let ty = ValueType::new(field.data_type().clone(), field.is_nullable());
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
            .collect::<Box<[_]>>()
    };

    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let pool = ConstantPoolId::new(42);
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let source = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            int64(),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let cell = builder
        .add_expression(
            source,
            int64(),
            ExprKind::Constant(ConstantReference { pool, ordinal: 0 }),
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: property(Distribution::Singleton),
            output: OutputPort {
                node: source,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([cell])]),
            },
        })
        .unwrap();
    let writer = builder.reserve_node_id().unwrap();
    let fields = relation(&mut builder, writer, false);
    let input = draft.input().fields_iter().next().unwrap();
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
                    input: Box::from([value]),
                    required_distribution: Distribution::Singleton,
                    target_fields: Box::from([WriterTargetField {
                        provider_name: input.field().name().clone().into(),
                        token: input.token(),
                        input: value,
                        ty: int64(),
                        hidden: false,
                    }]),
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
            dop,
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
    let outputs = relation(&mut builder, finish, true);
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
            dop,
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([6; 16]).unwrap());
    // Explicit fixture admission; these values are not production defaults.
    let constants = novarocks_functions::ConstantPolicy {
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
    };
    plan.insert_constant_pool(
        pool,
        ConstantPool::try_new(
            Arc::new(int64().try_to_field("c1").unwrap()),
            int64(),
            arrow::array::Array::to_data(&arrow::array::Int64Array::from(vec![42_i64])),
            constants,
            CompilePhase::Validate,
            &Unbounded,
        )
        .unwrap(),
    )
    .unwrap();
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
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: field.name.clone(),
                alias: None,
                value: field.value,
                ty: field.ty.clone(),
            })
            .collect(),
    })
    .unwrap();
    let plan = plan.finish_observed(&Unbounded).unwrap();

    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let roots = PhysicalExpressionRoots::try_new(fragment, &Unbounded).unwrap();
        let mut invocations = Vec::new();
        let bindings = roots
            .sites()
            .iter()
            .enumerate()
            .map(|(index, (&site, root))| {
                let use_id = ExpressionUseId::new(index as u32);
                invocations.push(ExpressionInvocation {
                    context: ExpressionEffectContext {
                        use_id,
                        domain: EvaluationDomainId::new(0),
                        demand: root.demand,
                    },
                    definition: root.expr,
                    control: ControlShape::Eager,
                    arguments: Box::default(),
                });
                (site, use_id)
            })
            .collect::<Vec<_>>();
        let domains = if invocations.is_empty() {
            vec![]
        } else {
            vec![ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(0),
                parent: None,
                guard: None,
            }]
        };
        let flow = ExpressionControlFlow::try_new(
            domains,
            invocations,
            fragment.expressions(),
            CompilePhase::Validate,
            &Unbounded,
        )
        .unwrap();
        let root_uses = PhysicalRootUses::try_new(fragment, flow, bindings, &Unbounded).unwrap();
        calls.insert(
            id,
            FrozenFragmentCalls::try_new(fragment, &root_uses, vec![], &Unbounded).unwrap(),
        );
        uses.insert(id, root_uses);
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &Unbounded).unwrap(),
        );
        admissions.insert(
            id,
            FragmentPackageAdmission {
                plan_limits: PlanLimits::FROZEN,
                source_retained_bytes: 64 << 20,
                property_projection_limits: PropertyProofProjectionLimits {
                    max_request_bytes: 16 << 20,
                    max_coexisting_bytes: 256 << 20,
                    max_projection_work: 16 << 20,
                },
            },
        );
    }
    let mut packages = extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::from([(ordinal, draft)]),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &Unbounded,
    )
    .unwrap();
    let producer = packages.remove(&FragmentId::new(1)).unwrap();
    let bytes = encode_fragment_package(&producer, &encode_limits(), &Unbounded)
        .unwrap()
        .encode_to_vec();
    (bytes, exchange.get())
}
