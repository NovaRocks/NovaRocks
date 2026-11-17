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

use arrow::{array::Int64Array, datatypes::DataType};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorWriteBinding, ConnectorWriteFieldToken,
    ConnectorWriteLayoutRecipeCompiler, ConnectorWriteLayoutRecipeDraft, PureProviderCompileError,
    PureProviderManifestEntry, PureProviderProgramCatalog, PureProviderProgramDefinition,
};
use novarocks_execution::exec::pipeline::executor::run_compiled_write_layout_component_for_test;
use novarocks_local_compiler::{compile_fragment, validate_fragment_providers};
use novarocks_local_program::{
    DataStreamPartitionType, ProgramNodeKind, StaticExprKind, StaticSinkProgram,
};
use novarocks_physical_plan::*;
use novarocks_test_compile_to_execute::compiler_results::compile_options;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationRegionId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationRegion, ExpressionInvocation, ExpressionUseId,
    PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}

struct LayoutCompiler;
impl ConnectorWriteLayoutRecipeCompiler for LayoutCompiler {
    type Error = ConnectorError;

    fn compile_private(
        &self,
        draft: &ConnectorWriteLayoutRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteLayoutRecipeDraft, PureProviderCompileError<Self::Error>> {
        control.checkpoint(CompilePhase::ProviderValidation, 0)?;
        assert_eq!(draft.payload().payload().as_ref(), b"layout-fixture");
        assert_eq!(
            draft.inputs(),
            &[ConnectorWriteFieldToken::from_bytes([3; 32])]
        );
        Ok(draft.clone())
    }
}

fn sender_package() -> FragmentPackage {
    let source_id = FragmentId::new(11);
    let receiver_id = FragmentId::new(12);
    let edge = EdgeId::new(13);
    let mut builder = FragmentBuilder::new(source_id);
    let values = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let columns = [0, 1].map(|output_ordinal| {
        builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: values,
                    output_ordinal,
                },
            )
            .unwrap()
    });
    let mut constants = ConstantPools::empty();
    let rows: Box<[_]> = [[900, 13], [800, -42], [700, 13]]
        .into_iter()
        .map(|row| {
            Box::new(row.map(|value| {
                let source = novarocks_functions::ConstantValue::from_i64(
                    Arc::new(ty.try_to_field("layout-fixture").unwrap()),
                    ty.clone(),
                    value,
                    compile_options().constants,
                    CompilePhase::Validate,
                    &Control,
                )
                .unwrap();
                let pool = ConstantPoolId::new(u32::try_from(constants.entries().len()).unwrap());
                let reference = ConstantReference {
                    pool,
                    ordinal: source.ordinal(),
                };
                constants.insert(pool, source.pool().clone()).unwrap();
                builder
                    .add_expression(values, ty.clone(), ExprKind::Constant(reference))
                    .unwrap()
            })) as Box<[_]>
        })
        .collect();
    builder.add_values(values, rows, Box::new(columns)).unwrap();

    let root = builder.reserve_node_id().unwrap();
    let key = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: root,
                output_ordinal: 2,
            },
        )
        .unwrap();
    builder
        .add_row_widening(
            root,
            values,
            Box::new([columns[0], columns[1], key]),
            NodeKind::WriteLayoutKey {
                key_inputs: Box::new([columns[1]]),
                output_key: key,
            },
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Stream { edge },
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let control = &Control;
    let roots = PhysicalExpressionRoots::try_new(&fragment, control).unwrap();
    let domain = EvaluationRegionId::new(0);
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
                    region: domain,
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, use_id)
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationRegion {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        control,
    )
    .unwrap();
    let expression_uses = PhysicalRootUses::try_new(&fragment, flow, bindings, control).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], control).unwrap();
    let pruning = FrozenFragmentPruning::try_new(source_id, vec![], control).unwrap();

    let projected = columns.map(|value| CutValue {
        value,
        ty: ty.clone(),
    });
    let imported_key = ValueId::new(23);
    let imports = [
        CutImport {
            source: projected[0].clone(),
            destination: ValueId::new(21),
        },
        CutImport {
            source: projected[1].clone(),
            destination: ValueId::new(22),
        },
    ];
    let scheme = HashPartitionScheme {
        space: PartitionSpaceId::try_new([1; 32]).unwrap(),
        count: PartitionCountParameter {
            id: PartitionCountParameterId::try_new([2; 32]).unwrap(),
            admissible: PartitionCountDomain {
                min: 1,
                max: 3,
                requires_power_of_two: false,
            },
        },
        definition: HashDefinition {
            algorithm: PartitionHashAlgorithm::NativeHierarchicalV1,
        },
    };
    let cuts = FragmentCuts {
        outbound: Box::new([OutboundFragmentCut {
            edge,
            kind: EdgeKind::Stream,
            destination_fragment: receiver_id,
            destination_node: NodeId::new(0),
            projection: Box::new(projected),
            destination_imports: Box::new(imports),
            partitioning: EdgePartitioning {
                source: Distribution::Hash {
                    keys: Box::new([key]),
                    scheme: scheme.clone(),
                },
                destination: Distribution::Hash {
                    keys: Box::new([imported_key]),
                    scheme,
                },
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination_multiplicity: RowMultiplicity::SingleCopy,
                routing_only: Box::new([(key, imported_key)]),
            },
            source_port: FragmentOutputPort::Root,
            payload: ExchangePayloadContract::Relational,
            delivery: DeliveryRequirement::DemandDriven,
        }]),
        ..FragmentCuts::default()
    };
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let instance = ConnectorInstanceId::parse("layout_fixture").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([4; 32]));
    let binding = ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let header = ConnectorEnvelopeHeader::new(
        provider,
        catalog,
        ConnectorCodecCategory::WriteLayout,
        ConnectorCodecRevision::try_new(1).unwrap(),
    );
    let layout = ConnectorWriteLayoutRecipeDraft::try_new(
        binding,
        ConnectorEncodedPayload::new(header, b"layout-fixture".to_vec().into()),
        vec![ConnectorWriteFieldToken::from_bytes([3; 32])],
    )
    .unwrap();
    FragmentPackage::try_new(
        FragmentPackageInput {
            constants,
            version: PlanVersionId::try_new([11; 16]).unwrap(),
            required: RequiredContracts::default(),
            fragment,
            expression_uses,
            calls,
            pruning,
            cuts,
            result: None,
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            layouts: BTreeMap::from([(root, layout)]),
            annotations: Box::default(),
        },
        FragmentPackageAdmission {
            plan_limits: PlanLimits::FROZEN,
            source_retained_bytes: 1 << 20,
            property_projection_limits: PropertyProofProjectionLimits {
                max_request_bytes: 1 << 20,
                max_coexisting_bytes: 2 << 20,
                max_projection_work: 1 << 20,
            },
        },
        control,
    )
    .unwrap()
}

#[test]
fn compiled_write_layout_sends_exact_keys_beside_the_logical_write_inputs() {
    let package = Arc::new(sender_package());
    let cut = &package.cuts().outbound[0];
    assert_eq!(cut.projection.len(), 2);
    assert_eq!(cut.destination_imports.len(), 2);
    let [(source, destination)] = cut.partitioning.routing_only.as_ref() else {
        panic!("exactly one hidden layout key");
    };
    assert!(cut.projection.iter().all(|value| value.value != *source));
    assert!(
        cut.destination_imports
            .iter()
            .all(|value| value.destination != *destination)
    );

    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let providers = PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(provider.clone(), false, false).with_write_layout()],
        vec![
            PureProviderProgramDefinition::new(provider, None, None)
                .with_write_layout(Arc::new(LayoutCompiler)),
        ],
        &Control,
    )
    .unwrap();
    let validated = validate_fragment_providers(package, &providers, &Control).unwrap();
    let functions = novarocks_sql_testkit::installed_builtin_owner_catalogue();
    let program =
        Arc::new(compile_fragment(validated, &functions, compile_options(), &Control).unwrap());
    let root = &program.graph().nodes()[program.graph().root().index()];
    let ProgramNodeKind::WriteLayoutKey { output_key, .. } = root.kind() else {
        panic!("expected a compiled layout root");
    };
    let Some(StaticSinkProgram::DataStream { branch, arena }) = program.graph().sink() else {
        panic!("expected one stream branch");
    };
    let [partition] = branch.partition_exprs() else {
        panic!("expected one partition expression");
    };
    assert_eq!(
        branch.partition_type(),
        DataStreamPartitionType::HashPartitioned
    );
    assert!(matches!(
        arena.nodes()[partition.index()].kind(),
        StaticExprKind::SlotId(slot) if slot == output_key
    ));
    assert_eq!(branch.output_columns(), root.output_layout().slots());
    assert_eq!(branch.output_columns().last(), Some(output_key));

    let chunks = run_compiled_write_layout_component_for_test(program, |batch| {
        assert_eq!(batch.num_columns(), 1);
        let partition = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        Ok(Int64Array::from_iter_values(
            partition.values().iter().map(|value| 10 * value + 1),
        ))
    })
    .unwrap();
    let mut rows = Vec::new();
    for chunk in chunks {
        let batch = chunk.record_batch();
        assert_eq!(batch.num_columns(), 3);
        let columns = [0, 1, 2].map(|index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        });
        for row in 0..batch.num_rows() {
            rows.push(columns.map(|column| column.value(row)));
        }
    }
    assert_eq!(rows, [[900, 13, 131], [800, -42, -419], [700, 13, 131]]);
}
