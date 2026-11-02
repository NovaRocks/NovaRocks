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

// Real package carriers for task lifecycle tests. Every successful fixture is
// validated, encoded, decoded and compiled through the production package path.

use super::*;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{ConstantPolicy, ConstantPool};
use novarocks_physical_plan::*;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    PureCompileControl, SemanticParameters,
};
use std::collections::BTreeMap;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum BodySink {
    Result,
    Stream(DataStreamPartitionType),
}

#[derive(Clone)]
pub(crate) struct Body {
    pub(crate) frozen: proto::FrozenFragment,
    pub(crate) assignment: proto::TaskAssignment,
}

fn constants() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 65536,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 32,
        max_type_nodes: 256,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
fn pool(array: ArrayRef, nullable: bool) -> ConstantPool {
    let value = novarocks_functions::FunctionValueType::new(array.data_type().clone(), nullable);
    ConstantPool::try_new(
        Arc::new(Field::new("fixture", array.data_type().clone(), nullable)),
        value,
        array.to_data(),
        constants(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap()
}
fn distribution_for(kind: DataStreamPartitionType, value: ValueId) -> Distribution {
    match kind {
        DataStreamPartitionType::Unpartitioned => Distribution::Singleton,
        DataStreamPartitionType::Random => Distribution::RoundRobin,
        DataStreamPartitionType::BucketShuffleHashPartitioned => {
            panic!("bucket-shuffle fixture is not authored")
        }
        DataStreamPartitionType::HashPartitioned => Distribution::Hash {
            keys: Box::from([value]),
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
        },
    }
}
fn edge_partition(
    kind: DataStreamPartitionType,
    source: ValueId,
    destination: ValueId,
) -> EdgePartitioning {
    EdgePartitioning {
        source: distribution_for(kind, source),
        source_multiplicity: RowMultiplicity::SingleCopy,
        destination: distribution_for(kind, destination),
        destination_multiplicity: RowMultiplicity::SingleCopy,
    }
}

fn finish(
    builder: FragmentBuilder,
    root: NodeId,
    sink: FragmentSink,
    dop: u32,
    constants: ConstantPools,
    cuts: FragmentCuts,
) -> Body {
    let fragment = builder
        .finish_definition(
            root,
            sink,
            PipelineDopDomain {
                min: dop,
                max: dop,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut invocations = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, expression)) in roots.sites().iter().enumerate() {
        let use_id = ExpressionUseId::new(ordinal as u32);
        invocations.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id,
                domain,
                demand: expression.demand,
            },
            definition: expression.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, use_id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control).unwrap();
    let result = matches!(
        fragment.sink(),
        FragmentSink::Result | FragmentSink::RootResult(_)
    )
    .then(|| {
        let fields = fragment.nodes()[&root]
            .output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| {
                let name = match fragment.sink() {
                    FragmentSink::RootResult(contract) => match contract.output() {
                        novarocks_result_contract::FrozenRootOutput::ClientRows(schema) => {
                            schema.columns()[ordinal].name.clone()
                        }
                        novarocks_result_contract::FrozenRootOutput::InternalFacts(
                            novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
                        ) => {
                            ["input_fields", "blob_type", "body", "properties"][ordinal].to_owned()
                        }
                        _ => format!("result-{ordinal}"),
                    },
                    _ => format!("result-{ordinal}"),
                };
                ResultField {
                    domain: ResultValueDomain::Plain,
                    name: name.into(),
                    alias: None,
                    value: *value,
                    ty: fragment.values()[value].ty.clone(),
                }
            })
            .collect();
        ResultPort {
            scalar_schema: match fragment.sink() {
                FragmentSink::RootResult(contract) => match contract.output() {
                    novarocks_result_contract::FrozenRootOutput::ScalarValue(schema) => {
                        Some(schema.clone())
                    }
                    _ => None,
                },
                _ => None,
            },
            fragment: fragment.id(),
            output: fragment.nodes()[&root].output.clone(),
            fields,
        }
    });
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            constants,
            version: PlanVersionId::try_new([1; 16]).unwrap(),
            required: RequiredContracts::default(),
            pruning: FrozenFragmentPruning::try_new(fragment.id(), vec![], &Control).unwrap(),
            fragment,
            expression_uses: uses,
            calls,
            cuts,
            result,
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        FragmentPackageAdmission {
            plan_limits: PlanLimits::FROZEN,
            source_retained_bytes: 64 << 20,
            property_projection_limits: PropertyProofProjectionLimits {
                max_request_bytes: 16 << 20,
                max_coexisting_bytes: 256 << 20,
                max_projection_work: 16 << 20,
            },
        },
        &Control,
    )
    .unwrap();
    let encoded = novarocks_plan_codec::physical_package_v2::encode_fragment_package(
        &package,
        &novarocks_plan_codec::physical_package_v2::test_support::encode_limits(),
        &Control,
    )
    .unwrap();
    Body {
        frozen: proto::FrozenFragment {
            package: encoded.encode_to_vec().into(),
        },
        assignment: proto::TaskAssignment {
            instance_ordinal: 3,
            initial_scan_ranges: Vec::new(),
            sink_edge_ids: Vec::new(),
        },
    }
}

impl Body {
    pub(crate) fn values(dop: u32) -> Self {
        let mut body =
            Self::bounded_root(novarocks_result_contract::FrozenRootOutput::CountOnly, &[]);
        body.set_dop(dop);
        body
    }
    pub(crate) fn with_sink(dop: u32, sink: BodySink, edge_ids: Vec<u32>) -> Self {
        let mut builder = FragmentBuilder::new(FragmentId::new(7));
        let root = NodeId::new(10);
        let ty = novarocks_functions::FunctionValueType::new(DataType::Int64, false);
        let value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: root,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .add_values(root, Box::default(), Box::from([value]))
            .unwrap();
        let mut cuts = FragmentCuts::default();
        let sink = match sink {
            BodySink::Result => FragmentSink::Result,
            BodySink::Stream(kind) => {
                let edge = EdgeId::new(1);
                cuts.outbound = Box::from([OutboundFragmentCut {
                    edge,
                    kind: EdgeKind::Stream,
                    destination_fragment: FragmentId::new(8),
                    destination_node: NodeId::new(11),
                    projection: Box::from([CutValue {
                        value,
                        ty: ty.clone(),
                    }]),
                    destination_imports: Box::from([CutImport {
                        source: CutValue {
                            value,
                            ty: ty.clone(),
                        },
                        destination: ValueId::new(0),
                    }]),
                    partitioning: edge_partition(kind, value, ValueId::new(0)),
                    source_port: novarocks_physical_plan::FragmentOutputPort::Root,
                    delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
                    payload: novarocks_physical_plan::ExchangePayloadContract::Relational,
                }]);
                FragmentSink::Stream { edge }
            }
        };
        let mut body = finish(builder, root, sink, dop, ConstantPools::empty(), cuts);
        body.assignment.sink_edge_ids = edge_ids;
        body
    }
    pub(crate) fn bounded_root(
        output: novarocks_result_contract::FrozenRootOutput,
        rows: &[i64],
    ) -> Self {
        let mut builder = FragmentBuilder::new(FragmentId::new(7));
        let root = NodeId::new(10);
        let value = builder
            .add_value(
                novarocks_functions::FunctionValueType::new(DataType::Int64, false),
                ValueOrigin::NodeOutput {
                    node: root,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let source = pool(Arc::new(Int64Array::from(rows.to_vec())), false);
        let mut constants = ConstantPools::empty();
        let mut cells: Vec<Box<[ExprId]>> = Vec::new();
        for ordinal in 0..rows.len() {
            let expression = builder
                .add_expression(
                    root,
                    source.value_type().clone(),
                    ExprKind::Constant(ConstantReference {
                        pool: ConstantPoolId::new(0),
                        ordinal: ordinal as u32,
                    }),
                )
                .unwrap();
            cells.push(Box::from([expression]));
        }
        if !rows.is_empty() {
            constants.insert(ConstantPoolId::new(0), source).unwrap();
        }
        builder
            .add_values(root, cells.into_boxed_slice(), Box::from([value]))
            .unwrap();
        let invalid_scalar = matches!(
            output,
            novarocks_result_contract::FrozenRootOutput::InternalFacts(
                novarocks_result_contract::InternalResultDomain::ScalarValueV1
            )
        );
        let valid_output = if invalid_scalar {
            novarocks_result_contract::FrozenRootOutput::CountOnly
        } else {
            output
        };
        let contract = novarocks_result_contract::RootOutputContract::new(
            novarocks_result_contract::RootProfileId::V1,
            valid_output,
        );
        let mut body = finish(
            builder,
            root,
            FragmentSink::RootResult(Box::new(contract)),
            1,
            constants,
            FragmentCuts::default(),
        );
        if invalid_scalar {
            body.mutate_package(|package| {
                let sink = package.fragment.as_mut().unwrap().sink.as_mut().unwrap();
                let wire::fragment_sink::Kind::RootResult(contract) = sink.kind.as_mut().unwrap()
                else {
                    unreachable!()
                };
                contract.output_kind = Some(novarocks_proto_models::result::RootOutputKind {
                    kind: Some(
                        novarocks_proto_models::result::root_output_kind::Kind::InternalFacts(
                            novarocks_proto_models::result::InternalResultDomain::ScalarValueV1
                                as i32,
                        ),
                    ),
                });
            });
        }
        body
    }
    pub(crate) fn bounded_statistics() -> Self {
        use arrow::array::{
            BinaryArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
        };
        use arrow::buffer::OffsetBuffer;
        let mut builder = FragmentBuilder::new(FragmentId::new(7));
        let child = NodeId::new(9);
        let root = NodeId::new(10);
        let binary = pool(
            Arc::new(BinaryArray::from(vec![[255_u8, 0, 3].as_slice()])),
            false,
        );
        let label = pool(Arc::new(StringArray::from(vec!["theta"])), false);
        let list = pool(
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Int32, false)),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                Arc::new(Int32Array::from(vec![7])),
                None,
            )),
            false,
        );
        let entries_fields = arrow::datatypes::Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]);
        let entries = StructArray::new(
            entries_fields.clone(),
            vec![
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
            ],
            None,
        );
        let map = pool(
            Arc::new(MapArray::new(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(entries_fields),
                    false,
                )),
                OffsetBuffer::new(vec![0_i32, 0].into()),
                entries,
                None,
                false,
            )),
            false,
        );
        let input = builder
            .add_value(
                binary.value_type().clone(),
                ValueOrigin::NodeOutput {
                    node: child,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let input_expression = builder
            .add_expression(
                child,
                binary.value_type().clone(),
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(0),
                    ordinal: 0,
                }),
            )
            .unwrap();
        builder
            .add_values(
                child,
                vec![vec![input_expression].into_boxed_slice()].into_boxed_slice(),
                Box::from([input]),
            )
            .unwrap();
        let mut outputs = Vec::new();
        for (ordinal, value) in [
            list.value_type(),
            label.value_type(),
            binary.value_type(),
            map.value_type(),
        ]
        .iter()
        .enumerate()
        {
            outputs.push(
                builder
                    .add_value(
                        (*value).clone(),
                        ValueOrigin::NodeOutput {
                            node: root,
                            output_ordinal: ordinal as u32,
                        },
                    )
                    .unwrap(),
            );
        }
        let label_expression = builder
            .add_expression(
                root,
                label.value_type().clone(),
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(1),
                    ordinal: 0,
                }),
            )
            .unwrap();
        builder
            .add_row_rewriting(
                root,
                child,
                Some(&BTreeMap::new()),
                outputs.clone().into_boxed_slice(),
                NodeKind::Unpivot {
                    spec: UnpivotSpec {
                        passthrough: Box::default(),
                        value_output: outputs[2],
                        literal_outputs: Box::from([outputs[0], outputs[1], outputs[3]]),
                        mappings: Box::from([UnpivotValueMapping {
                            input,
                            constants: Box::from([
                                UnpivotConstant::Int32List(ConstantReference {
                                    pool: ConstantPoolId::new(2),
                                    ordinal: 0,
                                }),
                                UnpivotConstant::Scalar(label_expression),
                                UnpivotConstant::Utf8Map(ConstantReference {
                                    pool: ConstantPoolId::new(3),
                                    ordinal: 0,
                                }),
                            ]),
                        }]),
                        max_output_rows: 4096,
                        max_output_bytes: 32 << 20,
                    },
                },
            )
            .unwrap();
        let mut constants = ConstantPools::empty();
        for (id, source) in [binary, label, list, map].into_iter().enumerate() {
            constants
                .insert(ConstantPoolId::new(id as u32), source)
                .unwrap();
        }
        finish(
            builder,
            root,
            FragmentSink::RootResult(Box::new(
                novarocks_result_contract::RootOutputContract::new(
                    novarocks_result_contract::RootProfileId::V1,
                    novarocks_result_contract::FrozenRootOutput::InternalFacts(
                        novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
                    ),
                ),
            )),
            1,
            constants,
            FragmentCuts::default(),
        )
    }
    pub(crate) fn mutate_package(&mut self, mutate: impl FnOnce(&mut wire::FragmentPackage)) {
        let mut package = wire::FragmentPackage::decode(self.frozen.package.as_ref()).unwrap();
        mutate(&mut package);
        self.frozen.package = package.encode_to_vec().into();
    }
    pub(crate) fn set_dop(&mut self, dop: u32) {
        self.mutate_package(|package| {
            package.fragment.as_mut().unwrap().dop_domain = Some(wire::PipelineDopDomain {
                min: dop,
                max: dop,
                requires_power_of_two: false,
            })
        });
    }
    pub(crate) fn frozen_bytes(&self) -> FrozenBytes {
        FrozenBytes::freeze(self.frozen.encode_to_vec().into())
    }
    pub(crate) fn input(&self, descriptor: &TaskDescriptor) -> TaskCreationInput {
        let assignment = novarocks_task_codec::creation::decode_task_assignment(
            self.assignment.clone(),
            descriptor,
            FieldPath::root("assignment"),
        )
        .expect("a structurally legal assignment");
        TaskCreationInput::new(self.frozen_bytes(), Box::new(assignment))
    }
}
