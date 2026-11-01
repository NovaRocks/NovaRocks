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
use crate::{
    physical_aggregate_binding_v2::{
        AggregateBindingInput, encode_aggregate_bindings, materialize_aggregate_bindings,
        prepare_aggregate_binding_headers, prepare_aggregate_bindings_materialization,
    },
    physical_binding_v2::{
        ArgumentTypeIds, BindingSource, FunctionBindingInput, ResultTypeIds,
        encode_function_bindings, materialize_function_bindings, prepare_function_binding_headers,
        prepare_function_bindings_materialization,
    },
    physical_connector_payload_v2::{
        ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
    },
    physical_expression_v2::{
        ExpressionProjectionLimits, ExpressionTypeIds, decode_expression_definitions,
        encode_expression_definitions,
    },
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, ValueSource, decode_values, encode_values},
};
use arrow::{
    array::{Array, Int64Array, ListArray, MapArray, StringArray, StructArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field},
};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
    ConnectorWriteFieldToken,
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_proto_models::physical_control_v2::Empty;
use novarocks_type_contract::{
    AggregateStateArgumentContract, AggregateStateFormatId, BucketLayoutAlgorithm,
    CompileControlError, FunctionArgumentType, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionValueType, PartitionCountParameterId, PartitionHashAlgorithm, PartitionSpaceId,
    PureCompileControl, SemanticParameters, ValueLogicalType,
};
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};
const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control(Mutex<State>);
#[derive(Default)]
struct State {
    active: bool,
    trace: Vec<(CompilePhase, u32)>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        *self.0.lock().unwrap() = State {
            active: true,
            trace: vec![],
            stop,
        };
    }
    fn disarm(&self) {
        self.0.lock().unwrap().active = false;
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.0.lock().unwrap().trace.clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut s = self.0.lock().unwrap();
        if !s.active {
            return Ok(());
        }
        let at = s.trace.len();
        if let Some((stop, _)) = s.stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        s.trace.push((phase, units));
        match s.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> TableWriteNodeProjectionLimits {
    TableWriteNodeProjectionLimits {
        node: NodeProjectionLimits {
            max_input_nodes: 8,
            max_value_references: 8192,
            max_list_items: 8192,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 16 << 20,
            max_coexisting_source_and_request_bytes: 32 << 20,
            max_work: 2_000_000_000,
            properties: properties::PhysicalPropertyProjectionLimits {
                max_value_references: 8192,
                max_allocation_requests: 8192,
                max_allocation_request_bytes: 16 << 20,
                max_coexisting_source_and_request_bytes: 32 << 20,
                max_work: 2_000_000_000,
            },
        },
        schema: WriterSchemaProjectionLimits {
            max_fields: 8192,
            max_name_bytes: 1 << 20,
            max_type_references: 8192,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 16 << 20,
            max_coexisting_source_and_request_bytes: 32 << 20,
            max_work: 2_000_000_000,
        },
        binding: BindingProjectionLimits {
            max_definitions: 1024,
            max_type_references: 8192,
            max_request_bytes: 16 << 20,
            max_allocation_requests: 8192,
            max_coexisting_source_and_request_bytes: 32 << 20,
            max_work: 2_000_000_000,
        },
    }
}
fn tl() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 32,
        max_expanded_nodes: 64,
        max_string_bytes: 8192,
    }
}
fn vl() -> ValueProjectionLimits {
    ValueProjectionLimits {
        max_definitions: 8,
        max_origin_references: 32,
        max_allocation_requests: 64,
        max_allocation_request_bytes: 64 << 10,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 16 << 20,
        origins: ValueOriginProjectionLimits {
            max_allocation_requests: 16,
            max_allocation_request_bytes: 64 << 10,
            max_coexisting_source_and_request_bytes: SOURCE,
            max_work: 1 << 20,
        },
    }
}
fn pl() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 2,
        max_payload_bytes: 8192,
        max_allocation_requests: 64,
        max_allocation_request_bytes: 64 << 10,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 1 << 20,
    }
}
fn el() -> ExpressionProjectionLimits {
    ExpressionProjectionLimits {
        max_definitions: 8,
        max_type_references: 16,
        max_expression_references: 16,
        max_new_allocation_requests: 32,
        max_new_allocation_request_bytes: 64 << 10,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_cumulative_work: 256 << 20,
    }
}
fn properties() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn wp(unconstrained: bool) -> wire::PhysicalProperties {
    wire::PhysicalProperties {
        distribution: Some(wire::Distribution {
            kind: Some(if unconstrained {
                wire::distribution::Kind::Unconstrained(Empty {})
            } else {
                wire::distribution::Kind::Singleton(Empty {})
            }),
        }),
        row_multiplicity: wire::RowMultiplicity::SingleCopy as i32,
        ordering: vec![],
    }
}
fn dictionary() -> FunctionValueType {
    FunctionValueType::new(
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )
}
fn nested() -> FunctionValueType {
    FunctionValueType::new(
        DataType::Struct(
            vec![Arc::new(
                Field::new("original-child", DataType::Utf8, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "unknown".into(),
                        "unchanged".into(),
                    )]),
                ),
            )]
            .into(),
        ),
        true,
    )
}
fn nominal() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn aggregate(phase: p::AggregatePhase) -> p::AggregateBinding {
    p::AggregateBinding {
        state_interpretation: None,
        function: p::BoundFunction::from_exact_signature(
            FunctionId::try_new("test/f").unwrap(),
            FunctionOverloadId::try_new("test/o").unwrap(),
            FunctionKind::Aggregate,
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Value(dictionary()),
            ])),
            nested(),
        ),
        phase,
        logical_argument_count: 1,
        intermediate_type: dictionary(),
        state_format: AggregateStateFormatId::try_new("state/v1").unwrap(),
        state_argument_contract: AggregateStateArgumentContract::ValueRootNullabilityIndependent,
    }
}

fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 3,
        max_array_nodes: 8,
        max_logical_elements: 64,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 8,
        max_type_nodes: 32,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 16 << 20,
        max_library_validation_bytes: 16 << 20,
    }
}
fn pool(a: &dyn Array) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(
            Field::new("original_source", a.data_type().clone(), true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "source.tag".into(),
                    "unchanged".into(),
                )]),
            ),
        ),
        FunctionValueType::new(a.data_type().clone(), true),
        a.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap()
}
fn reference(pool: u32, ordinal: u32) -> p::ConstantReference {
    p::ConstantReference {
        pool: p::ConstantPoolId::new(pool),
        ordinal,
    }
}
fn payload() -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([0; 32]),
            ),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![17, 0, 255].into(),
    )
}
fn relation_schema() -> p::WriterRelationSchema {
    let roles = [
        p::WriterRelationFieldRole::Kind,
        p::WriterRelationFieldRole::TargetOrdinal,
        p::WriterRelationFieldRole::RowCount,
        p::WriterRelationFieldRole::CommitFragment,
        p::WriterRelationFieldRole::Auxiliary,
    ];
    let types = [
        FunctionValueType::new(DataType::Int64, true),
        nested(),
        dictionary(),
        nominal(),
        FunctionValueType::new(DataType::Int64, false),
    ];
    p::WriterRelationSchema {
        revision: 0,
        fields: roles
            .into_iter()
            .zip(types)
            .enumerate()
            .map(|(i, (role, ty))| p::WriterRelationField {
                value: p::ValueId::new(if i % 2 == 0 { 0 } else { u32::MAX }),
                name: if i == 0 { "".into() } else { "雪λ".into() },
                ty,
                role,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }
}
fn writer_calls() -> Box<[p::WriterAggregateCall]> {
    Box::from([
        p::WriterAggregateCall {
            input: p::ValueId::new(0),
            binding: aggregate(p::AggregatePhase::Partial {
                sequence: p::AggregateSequenceId::new(0),
            }),
            output: p::ValueId::new(u32::MAX),
        },
        p::WriterAggregateCall {
            input: p::ValueId::new(u32::MAX),
            binding: aggregate(p::AggregatePhase::Final {
                sequence: p::AggregateSequenceId::new(u32::MAX),
            }),
            output: p::ValueId::new(0),
        },
    ])
}
fn header(kind: p::NodeKind) -> p::PhysicalNode {
    p::PhysicalNode {
        id: p::NodeId::new(u32::MAX),
        inputs: Box::from([p::NodeId::new(0)]),
        required_inputs: Box::from([p::PhysicalProperties {
            distribution: p::Distribution::Unconstrained,
            ..properties()
        }]),
        output_properties: properties(),
        output: p::OutputPort {
            node: p::NodeId::new(u32::MAX),
            columns: Box::from([
                p::ValueId::new(0),
                p::ValueId::new(u32::MAX),
                p::ValueId::new(0),
            ]),
        },
        kind,
    }
}
fn writer() -> p::PhysicalNode {
    header(p::NodeKind::TableWriter {
        target: p::WriterTarget {
            handle: payload(),
            write_target_ordinal: WriteTargetOrdinal::try_new(4095).unwrap(),
            input: Box::from([
                p::ValueId::new(u32::MAX),
                p::ValueId::new(0),
                p::ValueId::new(u32::MAX),
            ]),
            required_distribution: p::Distribution::Singleton,
            target_fields: Box::from([
                p::WriterTargetField {
                    token: ConnectorWriteFieldToken::from_bytes([0; 32]),
                    provider_name: "".into(),
                    input: p::ValueId::new(0),
                    ty: FunctionValueType::new(DataType::Int64, false),
                    hidden: false,
                },
                p::WriterTargetField {
                    token: ConnectorWriteFieldToken::from_bytes([255; 32]),
                    provider_name: "雪λ".into(),
                    input: p::ValueId::new(u32::MAX),
                    ty: nested(),
                    hidden: true,
                },
            ]),
            output_schema: relation_schema(),
            partial_aggregates: writer_calls(),
        },
    })
}
fn group() -> p::WriterGroupedUnpivotSpec {
    p::WriterGroupedUnpivotSpec {
        statistics_target_ordinals: Box::from([
            WriteTargetOrdinal::try_new(4095).unwrap(),
            WriteTargetOrdinal::try_new(0).unwrap(),
            WriteTargetOrdinal::try_new(4095).unwrap(),
        ]),
        grouping_input: p::ValueId::new(0),
        grouping_output: p::ValueId::new(u32::MAX),
        passthrough_output: p::ValueId::new(7),
        value_output: p::ValueId::new(7),
        literal_outputs: Box::from([p::ValueId::new(0), p::ValueId::new(u32::MAX)]),
        mappings: Box::from([p::WriterGroupedUnpivotMapping {
            write_target_ordinal: WriteTargetOrdinal::try_new(4095).unwrap(),
            input: p::ValueId::new(0),
            constants: Box::from([
                p::UnpivotConstant::Scalar(p::ExprId::new(0)),
                p::UnpivotConstant::Int32List(reference(0, 1)),
                p::UnpivotConstant::Utf8Map(reference(7, 1)),
            ]),
        }]),
        max_output_rows: u64::MAX,
        max_output_bytes: 0,
    }
}
fn finisher(grouped: bool) -> p::PhysicalNode {
    header(p::NodeKind::TableFinish(p::WriterFinishSpec {
        count_mode: novarocks_physical_plan::FinishCountMode::WriterReported,
        inputs: novarocks_physical_plan::FinishInputs {
            writer_results: 0,
            logical_summary: None,
        },
        logical_summary: None,
        expected_target_ordinals: Box::from([
            WriteTargetOrdinal::try_new(4095).unwrap(),
            WriteTargetOrdinal::try_new(0).unwrap(),
            WriteTargetOrdinal::try_new(4095).unwrap(),
        ]),
        input_schema: relation_schema(),
        output_schema: relation_schema(),
        final_aggregates: writer_calls(),
        grouped_unpivot: grouped.then(group),
    }))
}
fn target(n: &mut p::PhysicalNode) -> &mut p::WriterTarget {
    match &mut n.kind {
        p::NodeKind::TableWriter { target } => target,
        _ => panic!(),
    }
}
fn finish_spec(n: &mut p::PhysicalNode) -> &mut p::WriterFinishSpec {
    match &mut n.kind {
        p::NodeKind::TableFinish(v) => v,
        _ => panic!(),
    }
}
fn raw_target(n: &mut wire::PhysicalNode) -> &mut wire::WriterTarget {
    match n.kind.as_mut() {
        Some(wire::physical_node::Kind::TableWriter(v)) => v,
        _ => panic!(),
    }
}
fn raw_finish(n: &mut wire::PhysicalNode) -> &mut wire::WriterFinish {
    match n.kind.as_mut() {
        Some(wire::physical_node::Kind::TableFinish(v)) => v,
        _ => panic!(),
    }
}
struct Ids {
    a: Vec<u32>,
    b: Vec<u32>,
    writer: bool,
}
impl Ids {
    fn new(n: &p::PhysicalNode) -> Self {
        let id = |t: &FunctionValueType| {
            if *t == nested() {
                2
            } else if *t == dictionary() {
                1
            } else if *t == nominal() {
                3
            } else if t.nullable() {
                0
            } else {
                4
            }
        };
        match physical(n).unwrap() {
            Body::Writer(t) => Self {
                a: t.target_fields.iter().map(|f| id(&f.ty)).collect(),
                b: t.output_schema.fields.iter().map(|f| id(&f.ty)).collect(),
                writer: true,
            },
            Body::Finish(t) => Self {
                a: t.input_schema.fields.iter().map(|f| id(&f.ty)).collect(),
                b: t.output_schema.fields.iter().map(|f| id(&f.ty)).collect(),
                writer: false,
            },
        }
    }
    fn view(&self) -> TableWriteTypeIds<'_> {
        if self.writer {
            TableWriteTypeIds::Writer {
                target_fields: &self.a,
                output_schema: &self.b,
            }
        } else {
            TableWriteTypeIds::Finish {
                input_schema: &self.a,
                output_schema: &self.b,
            }
        }
    }
}
fn expected_schema() -> wire::WriterRelationSchema {
    wire::WriterRelationSchema {
        revision: 0,
        fields: [
            wire::WriterRelationFieldRole::Kind,
            wire::WriterRelationFieldRole::TargetOrdinal,
            wire::WriterRelationFieldRole::RowCount,
            wire::WriterRelationFieldRole::CommitFragment,
            wire::WriterRelationFieldRole::Auxiliary,
        ]
        .into_iter()
        .zip([0, 2, 1, 3, 4])
        .enumerate()
        .map(|(i, (role, type_id))| wire::WriterRelationField {
            value_id: Some(if i % 2 == 0 { 0 } else { u32::MAX }),
            name: if i == 0 { "".into() } else { "雪λ".into() },
            value_type_id: Some(type_id),
            role: role as i32,
        })
        .collect(),
    }
}
fn expected_calls() -> Vec<wire::WriterAggregateCall> {
    vec![
        wire::WriterAggregateCall {
            input_value_id: Some(0),
            aggregate_binding_id: Some(0),
            output_value_id: Some(u32::MAX),
        },
        wire::WriterAggregateCall {
            input_value_id: Some(u32::MAX),
            aggregate_binding_id: Some(u32::MAX),
            output_value_id: Some(0),
        },
    ]
}
fn expected_group() -> wire::WriterGroupedUnpivot {
    wire::WriterGroupedUnpivot {
        statistics_target_ordinals: vec![4095, 0, 4095],
        grouping_input_value_id: Some(0),
        grouping_output_value_id: Some(u32::MAX),
        passthrough_output_value_id: Some(7),
        value_output_id: Some(7),
        literal_output_ids: vec![0, u32::MAX],
        mappings: vec![wire::WriterGroupedUnpivotMapping {
            write_target_ordinal: 4095,
            input_value_id: Some(0),
            constants: vec![
                wire::UnpivotConstant {
                    kind: Some(wire::unpivot_constant::Kind::ScalarExprId(0)),
                },
                wire::UnpivotConstant {
                    kind: Some(wire::unpivot_constant::Kind::Int32List(
                        wire::ConstantReference {
                            pool_id: Some(0),
                            row_ordinal: 1,
                        },
                    )),
                },
                wire::UnpivotConstant {
                    kind: Some(wire::unpivot_constant::Kind::Utf8Map(
                        wire::ConstantReference {
                            pool_id: Some(7),
                            row_ordinal: 1,
                        },
                    )),
                },
            ],
        }],
        max_output_rows: u64::MAX,
        max_output_bytes: 0,
    }
}
fn expected(writer: bool, grouped: bool) -> wire::PhysicalNode {
    wire::PhysicalNode {
        id: u32::MAX,
        input_node_ids: vec![0],
        required_inputs: vec![wp(true)],
        output_properties: Some(wp(false)),
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![0, u32::MAX, 0],
        }),
        kind: Some(if writer {
            wire::physical_node::Kind::TableWriter(wire::WriterTarget {
                handle_payload_id: Some(u32::MAX),
                write_target_ordinal: 4095,
                input_value_ids: vec![u32::MAX, 0, u32::MAX],
                required_distribution: Some(wire::Distribution {
                    kind: Some(wire::distribution::Kind::Singleton(Empty {})),
                }),
                target_fields: vec![
                    wire::WriterTargetField {
                        token: vec![0; 32],
                        provider_name: "".into(),
                        input_value_id: Some(0),
                        value_type_id: Some(4),
                        hidden: false,
                    },
                    wire::WriterTargetField {
                        token: vec![255; 32],
                        provider_name: "雪λ".into(),
                        input_value_id: Some(u32::MAX),
                        value_type_id: Some(2),
                        hidden: true,
                    },
                ],
                output_schema: Some(expected_schema()),
                partial_aggregates: expected_calls(),
            })
        } else {
            wire::physical_node::Kind::TableFinish(wire::WriterFinish {
                count_mode: wire::FinishCountMode::WriterReported as i32,
                writer_results_input: Some(0),
                logical_summary_input: None,
                logical_summary: None,
                expected_target_ordinals: vec![4095, 0, 4095],
                input_schema: Some(expected_schema()),
                output_schema: Some(expected_schema()),
                final_aggregates: expected_calls(),
                grouped_unpivot: grouped.then(expected_group),
            })
        }),
    }
}
struct Fixture {
    roots: Vec<(u32, FunctionValueType)>,
    values: Vec<p::ValueDef>,
    arena: p::ExprArena,
    parameters: SemanticParameters,
    pools: p::ConstantPools,
}
impl Fixture {
    fn new() -> Self {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let values = [0, 7, u32::MAX]
            .into_iter()
            .enumerate()
            .map(|(i, id)| p::ValueDef {
                id: p::ValueId::new(id),
                ty: ty.clone(),
                origin: p::ValueOrigin::NodeOutput {
                    node: p::NodeId::new(0),
                    output_ordinal: i as u32,
                },
            })
            .collect();
        let mut pools = p::ConstantPools::empty();
        pools
            .insert(
                p::ConstantPoolId::new(u32::MAX),
                pool(&Int64Array::from(vec![Some(999), Some(-7), None])),
            )
            .unwrap();
        let list = ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([
            Some(vec![Some(99)]),
            Some(vec![Some(-2), Some(0)]),
        ]);
        pools
            .insert(p::ConstantPoolId::new(0), pool(&list))
            .unwrap();
        let fields = vec![
            Arc::new(Field::new("original_key", DataType::Utf8, false)),
            Arc::new(Field::new("original_value", DataType::Utf8, true)),
        ]
        .into();
        let entries = StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec!["unused", "z", "a"])),
                Arc::new(StringArray::from(vec!["unused", "雪", "λ"])),
            ],
            None,
        );
        let map = MapArray::new(
            Arc::new(Field::new(
                "original_entries",
                entries.data_type().clone(),
                false,
            )),
            OffsetBuffer::new(vec![0, 1, 3].into()),
            entries,
            None,
            false,
        );
        pools.insert(p::ConstantPoolId::new(7), pool(&map)).unwrap();
        let nodes = [
            (0, p::ExprKind::Constant(reference(u32::MAX, 1))),
            (7, p::ExprKind::Value(p::ValueId::new(0))),
            (u32::MAX, p::ExprKind::Constant(reference(u32::MAX, 2))),
        ]
        .into_iter()
        .map(|(id, kind)| p::ExprNode {
            id: p::ExprId::new(id),
            owner: p::NodeId::new(u32::MAX),
            lambda_scope: None,
            ty: ty.clone(),
            kind,
        });
        let arena = p::ExprArena::try_from_definitions_observed(
            nodes,
            &p::PlanLimits::default(),
            &Control::default(),
        )
        .unwrap();
        Self {
            roots: vec![
                (0, ty),
                (1, dictionary()),
                (2, nested()),
                (3, nominal()),
                (4, FunctionValueType::new(DataType::Int64, false)),
            ],
            values,
            arena,
            parameters: SemanticParameters::try_new([]).unwrap(),
            pools,
        }
    }
    fn with<R>(
        &self,
        n: &p::PhysicalNode,
        c: &Control,
        run: impl FnOnce(
            &EncodedValues<'_, '_, '_>,
            &EncodedExpressions<'_, '_, '_>,
            &DecodedExpressions<'_, '_, '_>,
            &MaterializedAggregateBindings<'_, '_, '_>,
        ) -> R,
    ) -> R {
        c.disarm();
        let types = encode_type_table_sources(&self.roots, &[], tl(), c).unwrap();
        let read_types = decode_type_table(types.as_wire(), tl(), c).unwrap();
        let payload_sources = match physical(n).unwrap() {
            Body::Writer(t) => vec![(u32::MAX, &t.handle)],
            Body::Finish(_) => vec![],
        };
        let payloads = encode_connector_payloads(&payload_sources, 16 << 10, pl(), c).unwrap();
        let read_payloads =
            decode_connector_payloads(payloads.as_wire(), 16 << 10, pl(), c).unwrap();
        let vinputs = self
            .values
            .iter()
            .map(|source| ValueSource {
                source,
                value_type_id: 0,
            })
            .collect::<Vec<_>>();
        let values = encode_values(&vinputs, &payloads, &types, 32 << 10, vl()).unwrap();
        let read_values = decode_values(
            values.as_wire(),
            &read_payloads,
            &read_types,
            32 << 10,
            vl(),
        )
        .unwrap();
        let body = physical(n).unwrap();
        let calls = calls(&body);
        let parameter_ids = calls
            .iter()
            .map(|call| {
                call.binding
                    .function
                    .argument_types
                    .iter()
                    .map(|arg| match arg {
                        FunctionArgumentType::Value(_) => vec![],
                        FunctionArgumentType::Lambda {
                            parameter_types, ..
                        } => parameter_types
                            .iter()
                            .map(|ty| match &ty.data_type() {
                                DataType::Struct(_) => 2,
                                DataType::Int64 => 0,
                                _ => panic!("fixture has an unauthored Lambda parameter type"),
                            })
                            .collect::<Vec<_>>(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let argument_ids = calls
            .iter()
            .enumerate()
            .map(|(i, call)| {
                call.binding
                    .function
                    .argument_types
                    .iter()
                    .enumerate()
                    .map(|(j, arg)| match arg {
                        FunctionArgumentType::Value(_) => ArgumentTypeIds::Value(1),
                        FunctionArgumentType::Lambda { .. } => ArgumentTypeIds::Lambda {
                            parameters: &parameter_ids[i][j],
                            result: 3,
                        },
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let wide=calls.len()>16 || calls.iter().any(|call|call.binding.function.argument_types.len()>16 || call.binding.function.argument_types.iter().any(|arg|matches!(arg,FunctionArgumentType::Lambda{parameter_types,..} if parameter_types.len()>16)));
        // This is the complete setup's actual coexistence invoice, not a new
        // admission rule. Large signatures need their original live Box arrays.
        let previous = if wide { 128 << 10 } else { 32 << 10 };
        let finputs = calls
            .iter()
            .enumerate()
            .map(|(i, call)| FunctionBindingInput {
                id: if i + 1 == calls.len() {
                    u32::MAX
                } else {
                    i as u32
                },
                source: BindingSource::Scalar(&call.binding.function),
                arguments: &argument_ids[i],
                result: ResultTypeIds::Scalar(2),
            })
            .collect::<Vec<_>>();
        let functions =
            encode_function_bindings(&types, &finputs, previous, limits().binding, c).unwrap();
        let mut ainputs = calls
            .iter()
            .enumerate()
            .map(|(i, call)| {
                let id = if i + 1 == calls.len() {
                    u32::MAX
                } else {
                    i as u32
                };
                AggregateBindingInput {
                    id,
                    source: &call.binding,
                    function_binding_id: id,
                    intermediate_value_type_id: 1,
                }
            })
            .collect::<Vec<_>>();
        // Retain the original two-call alias fixture unchanged. Wider actual
        // namespaces use one separately borrowed definition per source call.
        if calls.len() == 2 {
            ainputs.insert(
                1,
                AggregateBindingInput {
                    id: 9,
                    source: &calls[0].binding,
                    function_binding_id: 0,
                    intermediate_value_type_id: 1,
                },
            );
        }
        let aggregates = encode_aggregate_bindings(
            &types,
            &functions,
            &ainputs,
            2 * previous,
            limits().binding,
            c,
        )
        .unwrap();
        let fh = prepare_function_binding_headers(
            functions.as_wire(),
            &read_types,
            previous,
            limits().binding,
            c,
        )
        .unwrap();
        let ah = prepare_aggregate_binding_headers(
            aggregates.as_wire(),
            &fh,
            2 * previous,
            limits().binding,
        )
        .unwrap();
        let mf = materialize_function_bindings(
            prepare_function_bindings_materialization(&fh, 2 * previous, limits().binding).unwrap(),
        )
        .unwrap();
        let ma = materialize_aggregate_bindings(
            prepare_aggregate_bindings_materialization(&ah, &mf, 4 * previous, limits().binding)
                .unwrap(),
        )
        .unwrap();
        let einputs = self
            .arena
            .iter()
            .map(|(expr, _)| ExpressionTypeIds {
                expr: *expr,
                value_type_id: 0,
                lambda_parameter_type_ids: &[],
                function_binding_id: None,
                aggregate_binding_id: None,
            })
            .collect::<Vec<_>>();
        let mut expression_limits = el();
        expression_limits.max_coexisting_source_and_request_bytes = 4 * SOURCE;
        let expressions = encode_expression_definitions(
            &self.arena,
            &einputs,
            &types,
            &functions,
            &aggregates,
            &self.parameters,
            &self.pools,
            4 * previous,
            expression_limits,
            c,
        )
        .unwrap();
        let read = decode_expression_definitions(
            expressions.as_wire(),
            &read_values,
            &fh,
            &ah,
            &self.parameters,
            &self.pools,
            8 * previous,
            expression_limits,
        )
        .unwrap();
        run(&values, &expressions, &read, &ma)
    }
}

fn layout<T>(n: usize) -> usize {
    Layout::array::<T>(n).unwrap().size()
}
fn exact(
    mut l: TableWriteNodeProjectionLimits,
    f: TableWriteNodeProjectionFacts,
) -> TableWriteNodeProjectionLimits {
    l.node.max_input_nodes = f.input_node_count;
    l.node.max_value_references = f.value_reference_count;
    l.node.max_list_items = f.list_item_count;
    l.node.max_allocation_requests = f.allocation_requests_upper_bound;
    l.node.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound;
    l.node.max_coexisting_source_and_request_bytes =
        f.coexisting_source_and_request_bytes_upper_bound;
    l.node.max_work = f.cumulative_work_upper_bound;
    l
}
fn under(l: &mut TableWriteNodeProjectionLimits, axis: usize) {
    match axis {
        0 => l.node.max_input_nodes -= 1,
        1 => l.node.max_value_references -= 1,
        2 => l.node.max_list_items -= 1,
        3 => l.node.max_allocation_requests -= 1,
        4 => l.node.max_allocation_request_bytes -= 1,
        5 => l.node.max_coexisting_source_and_request_bytes -= 1,
        6 => l.node.max_work -= 1,
        _ => unreachable!(),
    }
}

fn prefixes(c: &Control, action: impl Fn() -> Result<(), Error>) {
    c.arm(None);
    let baseline_result = action();
    let baseline = c.trace();
    assert!(!baseline.is_empty());
    assert!(baseline_result.is_ok() || matches!(baseline_result, Err(Error::InvalidShape(_))));
    for at in 0..baseline.len() {
        for cause in CAUSES {
            c.arm(Some((at, cause.clone())));
            assert!(matches!(action(),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(c.trace(), baseline[..=at]);
        }
    }
    c.disarm();
}

fn hash(algorithm: PartitionHashAlgorithm) -> p::Distribution {
    p::Distribution::Hash {
        keys: vec![
            p::ValueId::new(u32::MAX),
            p::ValueId::new(0),
            p::ValueId::new(u32::MAX),
        ]
        .into_boxed_slice(),
        scheme: p::HashPartitionScheme {
            space: PartitionSpaceId::try_new([7; 32]).unwrap(),
            count: p::PartitionCountParameter {
                id: PartitionCountParameterId::try_new([8; 32]).unwrap(),
                admissible: p::PartitionCountDomain {
                    min: 2,
                    max: 8,
                    requires_power_of_two: true,
                },
            },
            definition: p::HashDefinition { algorithm },
        },
    }
}
fn bucket(algorithm: PartitionHashAlgorithm) -> p::Distribution {
    p::Distribution::BucketShuffle {
        keys: vec![
            p::ValueId::new(0),
            p::ValueId::new(u32::MAX),
            p::ValueId::new(0),
        ]
        .into_boxed_slice(),
        scheme: p::BucketPartitionScheme {
            space: PartitionSpaceId::try_new([9; 32]).unwrap(),
            bucket_count: 3,
            hash: algorithm,
            layout: BucketLayoutAlgorithm::DenseZeroBasedV1,
            ordinal_domain: p::BucketOrdinalDomainProof {
                first_ordinal: 0,
                ordinal_count: 3,
                evidence_digest: [10; 32],
            },
        },
    }
}
fn expected_hash(algorithm: i32) -> wire::Distribution {
    wire::Distribution {
        kind: Some(wire::distribution::Kind::Hash(wire::HashDistribution {
            key_value_ids: vec![u32::MAX, 0, u32::MAX],
            scheme: Some(wire::HashPartitionScheme {
                partition_space: vec![7; 32],
                count: Some(wire::PartitionCountParameter {
                    id: vec![8; 32],
                    admissible: Some(wire::PartitionCountDomain {
                        min: 2,
                        max: 8,
                        requires_power_of_two: true,
                    }),
                }),
                algorithm,
            }),
        })),
    }
}
fn expected_bucket(algorithm: i32) -> wire::Distribution {
    wire::Distribution {
        kind: Some(wire::distribution::Kind::BucketShuffle(
            wire::BucketDistribution {
                key_value_ids: vec![0, u32::MAX, 0],
                scheme: Some(wire::BucketPartitionScheme {
                    partition_space: vec![9; 32],
                    bucket_count: 3,
                    hash: algorithm,
                    layout: 1,
                    ordinal_domain: Some(wire::BucketOrdinalDomainProof {
                        first_ordinal: 0,
                        ordinal_count: 3,
                        evidence_digest: vec![10; 32],
                    }),
                }),
            },
        )),
    }
}

// These fixtures deliberately preserve raw duplicate/empty schema declarations
// and phase variants. They are namespace/representation inputs, not a lawful
// complete provider-backed FragmentPackage.
#[test]
fn table_writer_full_header_fields_payload_and_all_distribution_forms_have_hand_wire_oracles() {
    let c = Control::default();
    let f = Fixture::new();
    let mut variants = vec![
        (
            p::Distribution::Singleton,
            wire::distribution::Kind::Singleton(Empty {}),
        ),
        (
            p::Distribution::Unconstrained,
            wire::distribution::Kind::Unconstrained(Empty {}),
        ),
        (
            p::Distribution::RoundRobin,
            wire::distribution::Kind::RoundRobin(Empty {}),
        ),
        (
            p::Distribution::Broadcast,
            wire::distribution::Kind::Broadcast(Empty {}),
        ),
    ];
    for (algorithm, tag) in [
        (PartitionHashAlgorithm::NativeExchangeV1, 1),
        (PartitionHashAlgorithm::NativeBucketCrc32V1, 2),
    ] {
        variants.push((hash(algorithm), expected_hash(tag).kind.unwrap()));
        variants.push((bucket(algorithm), expected_bucket(tag).kind.unwrap()));
    }
    for (dist, wire_dist) in variants {
        let mut n = writer();
        target(&mut n).required_distribution = dist;
        let ids = Ids::new(&n);
        let mut golden = expected(true, false);
        raw_target(&mut golden).required_distribution = Some(wire::Distribution {
            kind: Some(wire_dist),
        });
        f.with(&n, &c, |v, e, r, a| {
            c.arm(None);
            let emitted = encode_table_write_node(&n, v, e, ids.view(), SOURCE, limits())
                .unwrap()
                .0;
            assert_eq!(emitted, golden);
            c.arm(None);
            let owned = decode_table_write_node(&golden, r, a, SOURCE, limits())
                .unwrap()
                .0;
            assert_eq!(owned, n);
            let mut owned_copy = owned.clone();
            let t = target(&mut owned_copy);
            assert_eq!(t.handle.header(), target(&mut n.clone()).handle.header());
            assert_eq!(
                t.handle.payload().as_ptr(),
                r.values()
                    .payloads()
                    .payload(u32::MAX)
                    .unwrap()
                    .unwrap()
                    .payload()
                    .as_ptr()
            );
        });
    }
}
#[test]
fn table_finish_schemas_grouped_presence_constants_ordinals_and_raw_bounds_are_complete() {
    let c = Control::default();
    let f = Fixture::new();
    for present in [false, true] {
        let n = finisher(present);
        let ids = Ids::new(&n);
        f.with(&n, &c, |v, e, r, a| {
            c.arm(None);
            assert_eq!(
                encode_table_write_node(&n, v, e, ids.view(), SOURCE, limits())
                    .unwrap()
                    .0,
                expected(false, present)
            );
            c.arm(None);
            let owned = decode_table_write_node(&expected(false, present), r, a, SOURCE, limits())
                .unwrap()
                .0;
            assert_eq!(owned, n);
            if present {
                let g = match &owned.kind {
                    p::NodeKind::TableFinish(t) => t.grouped_unpivot.as_ref().unwrap(),
                    _ => panic!(),
                };
                assert_eq!(
                    g.mappings[0].constants[1],
                    p::UnpivotConstant::Int32List(reference(0, 1))
                );
                assert_eq!(
                    g.mappings[0].constants[2],
                    p::UnpivotConstant::Utf8Map(reference(7, 1))
                );
                assert!(std::ptr::eq(e.pools(), r.pools()));
            }
        });
    }
}
#[test]
fn table_write_inline_aggregate_copies_preserve_four_phases_lambda_dictionary_and_field_arcs() {
    let f = Fixture::new();
    let c = Control::default();
    for phase in [
        p::AggregatePhase::Single,
        p::AggregatePhase::Partial {
            sequence: p::AggregateSequenceId::new(0),
        },
        p::AggregatePhase::Intermediate {
            sequence: p::AggregateSequenceId::new(u32::MAX),
        },
        p::AggregatePhase::Final {
            sequence: p::AggregateSequenceId::new(0),
        },
    ] {
        let mut n = writer();
        let t = target(&mut n);
        t.partial_aggregates[0].binding.phase = phase;
        t.partial_aggregates[0].binding.function.argument_types =
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Lambda {
                    parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        Box::from([nested(), FunctionValueType::new(DataType::Int64, true)]),
                    ),
                    result_type: nominal(),
                },
            ]));
        let ids = Ids::new(&n);
        f.with(&n, &c, |v, e, r, a| {
            c.arm(None);
            let wire = encode_table_write_node(&n, v, e, ids.view(), SOURCE, limits())
                .unwrap()
                .0;
            c.arm(None);
            let mut owned = decode_table_write_node(&wire, r, a, SOURCE, limits())
                .unwrap()
                .0;
            assert_eq!(owned, n);
            let b = &target(&mut owned).partial_aggregates[0].binding;
            assert_eq!(b.phase, phase);
            assert!(b.function.legacy_metadata.is_none());
            let original = &a.definitions().iter().find(|(id, _)| *id == 0).unwrap().1;
            assert_eq!(b.intermediate_type, original.intermediate_type);
            assert_eq!(b.intermediate_type.data_type(), &DataType::Utf8);
            assert!(
                b.intermediate_type
                    .value_type()
                    .shares_storage(original.intermediate_type.value_type())
            );
            assert!(std::ptr::eq(
                b.intermediate_type.data_type(),
                original.intermediate_type.data_type()
            ));
            match (
                &original.function.result_type.data_type(),
                &b.function.result_type.data_type(),
            ) {
                (DataType::Struct(o), DataType::Struct(actual)) => {
                    assert!(Arc::ptr_eq(&o[0], &actual[0]))
                }
                _ => panic!(),
            };
        });
    }
}
#[test]
fn table_write_prepare_refuses_missing_fields_unknown_roles_types_tokens_ordinals_and_refs() {
    let f = Fixture::new();
    let c = Control::default();
    for is_writer in [true, false] {
        let n = if is_writer { writer() } else { finisher(true) };
        let ids = Ids::new(&n);
        f.with(&n, &c, |v, e, r, a| {
            for case in 0..if is_writer { 14 } else { 10 } {
                let mut wire = expected(is_writer, !is_writer);
                match case {
                    0 => wire.output = None,
                    1 => wire.output.as_mut().unwrap().node_id = None,
                    2 => wire.output_properties = None,
                    _ => {
                        if is_writer {
                            let t = raw_target(&mut wire);
                            match case {
                                3 => t.handle_payload_id = None,
                                4 => t.handle_payload_id = Some(17),
                                5 => t.required_distribution = None,
                                6 => t.required_distribution.as_mut().unwrap().kind = None,
                                7 => t.output_schema = None,
                                8 => t.output_schema.as_mut().unwrap().fields[0].role = 0,
                                9 => t.target_fields[0].token.pop().map(|_| ()).unwrap(),
                                10 => t.target_fields[0].value_type_id = Some(17),
                                11 => t.partial_aggregates[0].aggregate_binding_id = Some(17),
                                12 => t.partial_aggregates[0].input_value_id = None,
                                13 => t.write_target_ordinal = 4096,
                                _ => unreachable!(),
                            }
                        } else {
                            let t = raw_finish(&mut wire);
                            match case {
                                3 => t.input_schema = None,
                                4 => t.output_schema = None,
                                5 => t.output_schema.as_mut().unwrap().fields[0].value_id = None,
                                6 => t.final_aggregates[0].aggregate_binding_id = None,
                                7 => t.expected_target_ordinals[0] = u32::MAX,
                                8 => {
                                    t.grouped_unpivot.as_mut().unwrap().mappings[0].constants[0]
                                        .kind = None
                                }
                                9 => {
                                    t.grouped_unpivot.as_mut().unwrap().grouping_input_value_id =
                                        None
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                };
                c.arm(None);
                assert!(
                    prepare_table_write_node_decode(&wire, r, a, SOURCE, limits()).is_err(),
                    "case {case}"
                );
            }
            if is_writer {
                for case in 0..12 {
                    let mut wire = expected(true, false);
                    raw_target(&mut wire).required_distribution = Some(if case < 5 {
                        expected_hash(1)
                    } else {
                        expected_bucket(1)
                    });
                    if case < 5 {
                        let Some(wire::distribution::Kind::Hash(h)) = raw_target(&mut wire)
                            .required_distribution
                            .as_mut()
                            .unwrap()
                            .kind
                            .as_mut()
                        else {
                            panic!()
                        };
                        let s = h.scheme.as_mut().unwrap();
                        match case {
                            0 => s.algorithm = 0,
                            1 => s.algorithm = 99,
                            2 => s.partition_space = vec![7; 31],
                            3 => s.partition_space = vec![7; 33],
                            4 => s.partition_space = vec![0; 32],
                            _ => unreachable!(),
                        }
                    } else if case < 10 {
                        let Some(wire::distribution::Kind::BucketShuffle(h)) =
                            raw_target(&mut wire)
                                .required_distribution
                                .as_mut()
                                .unwrap()
                                .kind
                                .as_mut()
                        else {
                            panic!()
                        };
                        let s = h.scheme.as_mut().unwrap();
                        match case {
                            5 => s.hash = 0,
                            6 => s.layout = 99,
                            7 => s.ordinal_domain.as_mut().unwrap().evidence_digest = vec![10; 31],
                            8 => s.partition_space = vec![0; 32],
                            9 => s.partition_space = vec![9; 33],
                            _ => unreachable!(),
                        }
                    } else if case == 10 {
                        wire.output_properties.as_mut().unwrap().row_multiplicity = 0;
                    } else {
                        wire.output_properties
                            .as_mut()
                            .unwrap()
                            .ordering
                            .push(wire::OrderingKey {
                                value_id: Some(0),
                                direction: 99,
                                null_ordering: 1,
                            });
                    }
                    c.arm(None);
                    assert!(
                        matches!(
                            prepare_table_write_node_decode(&wire, r, a, SOURCE, limits()),
                            Err(Error::Properties(_))
                        ),
                        "closed case {case}"
                    );
                }
            }
            for case in 0..3 {
                let mut wire = expected(is_writer, !is_writer);
                let props = wire.output_properties.as_mut().unwrap();
                match case {
                    0 => props.row_multiplicity = 0,
                    1 => props.ordering.push(wire::OrderingKey {
                        value_id: Some(0),
                        direction: 99,
                        null_ordering: 1,
                    }),
                    2 => props.ordering.push(wire::OrderingKey {
                        value_id: None,
                        direction: 1,
                        null_ordering: 1,
                    }),
                    _ => unreachable!(),
                }
                c.arm(None);
                assert!(matches!(
                    prepare_table_write_node_decode(&wire, r, a, SOURCE, limits()),
                    Err(Error::Properties(_))
                ));
            }
            if is_writer {
                let wrong = TableWriteTypeIds::Writer {
                    target_fields: &[0, 2],
                    output_schema: &ids.b,
                };
                c.arm(None);
                assert!(matches!(
                    prepare_table_write_node_encode(&n, v, e, wrong, SOURCE, limits()),
                    Err(Error::InvalidShape(_))
                ));
            }
        });
    }
}
fn simple_writer() -> p::PhysicalNode {
    let mut n = writer();
    let t = target(&mut n);
    t.input = Box::from([p::ValueId::new(0)]);
    t.partial_aggregates = Box::default();
    t.target_fields = Box::from([p::WriterTargetField {
        token: ConnectorWriteFieldToken::from_bytes([5; 32]),
        provider_name: "p".into(),
        input: p::ValueId::new(0),
        ty: FunctionValueType::new(DataType::Int64, false),
        hidden: true,
    }]);
    t.output_schema = p::WriterRelationSchema {
        revision: 7,
        fields: Box::from([p::WriterRelationField {
            value: p::ValueId::new(u32::MAX),
            name: "s".into(),
            ty: FunctionValueType::new(DataType::Int64, true),
            role: p::WriterRelationFieldRole::Auxiliary,
        }]),
    };
    n
}
fn shared_upper() -> usize {
    use std::{
        mem::{align_of, size_of},
        sync::atomic::AtomicUsize,
    };
    let a = align_of::<*mut u8>()
        .max(align_of::<usize>())
        .max(align_of::<AtomicUsize>());
    Layout::from_size_align(
        size_of::<*mut u8>() + size_of::<usize>() + size_of::<AtomicUsize>() + 3 * (a - 1),
        a,
    )
    .unwrap()
    .pad_to_align()
    .size()
}
#[test]
fn table_write_independent_layout_and_exact_under_seven_axes_cover_both_nodes_and_directions() {
    let f = Fixture::new();
    let c = Control::default();
    let n = simple_writer();
    let ids = Ids::new(&n);
    f.with(&n, &c, |v, e, r, a| {
        c.arm(None);
        let (wire, s) = encode_table_write_node(&n, v, e, ids.view(), SOURCE, limits()).unwrap();
        c.arm(None);
        let (_, d) = decode_table_write_node(&wire, r, a, SOURCE, limits()).unwrap();
        let send = layout::<u32>(1)
            + layout::<wire::PhysicalProperties>(1)
            + layout::<u32>(3)
            + layout::<u32>(1)
            + layout::<wire::WriterTargetField>(1)
            + layout::<wire::WriterRelationField>(1)
            + 32
            + 2;
        let receive = 2
            * (layout::<p::NodeId>(1)
                + layout::<p::PhysicalProperties>(1)
                + layout::<p::ValueId>(3)
                + layout::<p::ValueId>(1)
                + layout::<p::WriterTargetField>(1)
                + layout::<p::WriterRelationField>(1))
            + 4
            + shared_upper();
        assert_eq!(s.allocation_requests_upper_bound, 9);
        assert_eq!(s.allocation_request_bytes_upper_bound, send);
        assert_eq!(d.allocation_requests_upper_bound, 17);
        assert_eq!(d.allocation_request_bytes_upper_bound, receive);
    });
    for n in [writer(), finisher(true)] {
        let ids = Ids::new(&n);
        f.with(&n, &c, |v, e, r, a| {
            c.arm(None);
            let (wire, s) =
                encode_table_write_node(&n, v, e, ids.view(), SOURCE, limits()).unwrap();
            c.arm(None);
            let (_, d) = decode_table_write_node(&wire, r, a, SOURCE, limits()).unwrap();
            let sl = exact(limits(), s);
            let dl = exact(limits(), d);
            c.arm(None);
            assert_eq!(
                encode_table_write_node(&n, v, e, ids.view(), SOURCE, sl)
                    .unwrap()
                    .1,
                s
            );
            c.arm(None);
            assert_eq!(
                decode_table_write_node(&wire, r, a, SOURCE, dl).unwrap().1,
                d
            );
            for axis in 0..7 {
                let mut l = sl;
                under(&mut l, axis);
                c.arm(None);
                assert!(
                    matches!(
                        encode_table_write_node(&n, v, e, ids.view(), SOURCE, l),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ),
                    "encode axis {axis}"
                );
                let mut l = dl;
                under(&mut l, axis);
                c.arm(None);
                assert!(
                    matches!(
                        decode_table_write_node(&wire, r, a, SOURCE, l),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ),
                    "decode axis {axis}"
                );
                if axis >= 3 {
                    c.arm(None);
                    assert!(matches!(
                        decode_table_write_node(&wire, r, a, SOURCE, l),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                    let numeric_prefix = c.trace();
                    for cause in CAUSES {
                        c.arm(Some((numeric_prefix.len(), cause)));
                        assert!(matches!(
                            decode_table_write_node(&wire, r, a, SOURCE, l),
                            Err(Error::Control(CompileControlError::ResourceExhausted))
                        ));
                        assert_eq!(c.trace(), numeric_prefix);
                    }
                }
            }
        });
    }
}
#[test]
fn table_write_original_loan_and_actual_wire_capacity_floors_refuse_foreign_sources() {
    let f = Fixture::new();
    let other = Fixture::new();
    let c = Control::default();
    let n = writer();
    let ids = Ids::new(&n);
    f.with(&n, &c, |v, e, r, a| {
        other.with(&n, &c, |_, foreign, fr, fa| {
            c.arm(None);
            assert!(matches!(
                encode_table_write_node(&n, v, foreign, ids.view(), SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            c.arm(None);
            assert!(matches!(
                decode_table_write_node(&expected(true, false), r, fa, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            c.arm(None);
            assert!(matches!(
                decode_table_write_node(&expected(true, false), fr, a, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        });
        c.arm(None);
        let clone = n.clone();
        assert!(matches!(
            encode_table_write_node(&clone, v, e, ids.view(), SOURCE, limits()),
            Err(Error::Binding(_)) | Err(Error::Payload(_))
        ));
        c.arm(None);
        assert!(matches!(
            encode_table_write_node(&n, v, e, ids.view(), 0, limits()),
            Err(Error::InvalidShape(_))
        ));
        let mut over = expected(true, false);
        raw_target(&mut over)
            .target_fields
            .reserve_exact(SOURCE / std::mem::size_of::<wire::WriterTargetField>() + 1);
        c.arm(None);
        assert!(matches!(
            prepare_table_write_node_decode(&over, r, a, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}
#[test]
fn table_write_each_actual_small_prepare_emit_and_ordinary_tail_prefix_keeps_three_causes() {
    let f = Fixture::new();
    let c = Control::default();
    for n in [simple_writer(), {
        let mut n = finisher(false);
        finish_spec(&mut n).final_aggregates = Box::default();
        n
    }] {
        let ids = Ids::new(&n);
        f.with(&n, &c, |v, e, r, a| {
            c.arm(None);
            let wire = encode_table_write_node(&n, v, e, ids.view(), SOURCE, limits())
                .unwrap()
                .0;
            prefixes(&c, || {
                encode_table_write_node(&n, v, e, ids.view(), SOURCE, limits()).map(|_| ())
            });
            prefixes(&c, || {
                decode_table_write_node(&wire, r, a, SOURCE, limits()).map(|_| ())
            });
            prefixes(&c, || {
                prepare_table_write_node_encode(&n, v, e, ids.view(), SOURCE, limits()).map(|_| ())
            });
            prefixes(&c, || {
                prepare_table_write_node_decode(&wire, r, a, SOURCE, limits()).map(|_| ())
            });
            c.disarm();
            let pe =
                prepare_table_write_node_encode(&n, v, e, ids.view(), SOURCE, limits()).unwrap();
            let expected_facts = *pe.facts();
            c.arm(None);
            assert_eq!(pe.emit().unwrap().1, expected_facts);
            c.arm(None);
            let baseline = {
                c.disarm();
                let p = prepare_table_write_node_decode(&wire, r, a, SOURCE, limits()).unwrap();
                c.arm(None);
                assert_eq!(p.emit().unwrap().0, n);
                c.trace()
            };
            for at in 0..baseline.len() {
                for cause in CAUSES {
                    c.disarm();
                    let p = prepare_table_write_node_decode(&wire, r, a, SOURCE, limits()).unwrap();
                    c.arm(Some((at, cause.clone())));
                    assert!(matches!(p.emit(),Err(Error::Control(actual)) if actual==cause));
                    assert_eq!(c.trace(), baseline[..=at]);
                }
            }
            c.disarm();
            let p =
                prepare_table_write_node_encode(&n, v, e, ids.view(), SOURCE, limits()).unwrap();
            c.arm(None);
            assert_eq!(p.emit().unwrap().0, wire);
            let baseline = c.trace();
            for at in 0..baseline.len() {
                for cause in CAUSES {
                    c.disarm();
                    let p = prepare_table_write_node_encode(&n, v, e, ids.view(), SOURCE, limits())
                        .unwrap();
                    c.arm(Some((at, cause.clone())));
                    assert!(matches!(p.emit(),Err(Error::Control(actual)) if actual==cause));
                    assert_eq!(c.trace(), baseline[..=at]);
                }
            }
            let mut bad = wire.clone();
            bad.output.as_mut().unwrap().node_id = None;
            prefixes(&c, || {
                prepare_table_write_node_decode(&bad, r, a, SOURCE, limits()).map(|_| ())
            });
        });
    }
}
#[test]
fn table_write_wide_real_fields_and_group_constants_sample_quantum_and_numeric_primary() {
    let f = Fixture::new();
    let c = Control::default();
    let mut n = writer();
    let t = target(&mut n);
    t.partial_aggregates = Box::default();
    let field = t.target_fields[0].clone();
    t.target_fields = (0..320)
        .map(|_| field.clone())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let ids = Ids::new(&n);
    f.with(&n,&c,|v,e,r,a|{c.arm(None);let (wire,_) =encode_table_write_node(&n,v,e,ids.view(),SOURCE,limits()).unwrap();let baseline=c.trace();assert!(baseline.iter().any(|(_,u)|*u==256));for at in [0,baseline.len()/2,baseline.len()-1]{for cause in CAUSES{c.arm(Some((at,cause.clone())));assert!(matches!(encode_table_write_node(&n,v,e,ids.view(),SOURCE,limits()),Err(Error::Control(actual)) if actual==cause));assert_eq!(c.trace(),baseline[..=at]);}}c.arm(None);let owned=decode_table_write_node(&wire,r,a,SOURCE,limits()).unwrap().0;assert_eq!(owned,n);let mut tiny=limits();tiny.node.max_allocation_requests=0;c.arm(None);assert!(matches!(prepare_table_write_node_encode(&n,v,e,ids.view(),SOURCE,tiny),Err(Error::Control(CompileControlError::ResourceExhausted))));let numeric_prefix=c.trace();for cause in CAUSES{c.arm(Some((numeric_prefix.len(),cause)));assert!(matches!(prepare_table_write_node_encode(&n,v,e,ids.view(),SOURCE,tiny),Err(Error::Control(CompileControlError::ResourceExhausted))));assert_eq!(c.trace(),numeric_prefix);} });
    let mut n = finisher(true);
    let g = finish_spec(&mut n).grouped_unpivot.as_mut().unwrap();
    g.mappings[0].constants = (0..320)
        .map(|_| p::UnpivotConstant::Scalar(p::ExprId::new(0)))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let ids = Ids::new(&n);
    f.with(&n, &c, |v, e, r, a| {
        c.arm(None);
        let wire = encode_table_write_node(&n, v, e, ids.view(), SOURCE, limits())
            .unwrap()
            .0;
        c.arm(None);
        let owned = decode_table_write_node(&wire, r, a, SOURCE, limits())
            .unwrap()
            .0;
        assert_eq!(owned, n);
    });
    // The new containing family, not only its shared schema/Grouped child,
    // owns all 320 actual WriterAggregateCall occurrences and addresses.
    let mut n = writer();
    target(&mut n).partial_aggregates = (0..320)
        .map(|i| p::WriterAggregateCall {
            input: p::ValueId::new(if i % 2 == 0 { 0 } else { u32::MAX }),
            binding: aggregate(p::AggregatePhase::Partial {
                sequence: p::AggregateSequenceId::new(i as u32),
            }),
            output: p::ValueId::new(if i % 2 == 0 { u32::MAX } else { 0 }),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let ids = Ids::new(&n);
    let source = 8 * SOURCE;
    f.with(&n,&c,|v,e,r,a|{
        c.arm(None);
        let (wire,sent)=encode_table_write_node(&n,v,e,ids.view(),source,limits()).unwrap();
        let send_trace=c.trace();
        let quantum=send_trace.iter().position(|(_,u)|*u==256).expect("actual containing-call/source lookup quantum");
        let mut emitted=wire.clone();
        let actual=&raw_target(&mut emitted).partial_aggregates;
        assert_eq!(actual.len(),320);
        for (i,call) in actual.iter().enumerate(){
            assert_eq!(call.aggregate_binding_id,Some(if i==319{u32::MAX}else{i as u32}));
            assert_eq!(call.input_value_id,Some(if i%2==0{0}else{u32::MAX}));
            assert_eq!(call.output_value_id,Some(if i%2==0{u32::MAX}else{0}));
        }
        for at in [0,quantum,send_trace.len()-1]{for cause in CAUSES{
            c.arm(Some((at,cause.clone())));
            assert!(matches!(encode_table_write_node(&n,v,e,ids.view(),source,limits()),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(c.trace(),send_trace[..=at]);
        }}
        c.arm(None);
        let (mut owned,received)=decode_table_write_node(&wire,r,a,source,limits()).unwrap();
        let read_trace=c.trace();
        let quantum=read_trace.iter().position(|(_,u)|*u==256).expect("actual aggregate-copy count quantum");
        let calls=&target(&mut owned).partial_aggregates;
        assert_eq!(calls.len(),320);
        for (i,call) in calls.iter().enumerate(){
            assert_eq!(call.binding.phase,p::AggregatePhase::Partial{sequence:p::AggregateSequenceId::new(i as u32)});
            assert_eq!(call.binding.logical_argument_count,1);
            assert_eq!(call.binding.intermediate_type,dictionary());
            assert_eq!(call.binding.function.result_type,nested());
            assert!(call.binding.function.legacy_metadata.is_none());
            match &call.binding.function.argument_types[0]{FunctionArgumentType::Value(t)=>assert_eq!(*t,dictionary()),_=>panic!()}
        }
        for at in [0,quantum,read_trace.len()-1]{for cause in CAUSES{
            c.arm(Some((at,cause.clone())));
            assert!(matches!(decode_table_write_node(&wire,r,a,source,limits()),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(c.trace(),read_trace[..=at]);
        }}
        // Only genuine final seven-axis facts select these envelopes. Once
        // the original author refuses, a later control callback cannot replace
        // Resource, even when signature copies have accumulated their costs.
        for axis in [3,4,6]{
            let mut sl=exact(limits(),sent);under(&mut sl,axis);
            c.arm(None);assert!(matches!(prepare_table_write_node_encode(&n,v,e,ids.view(),source,sl),Err(Error::Control(CompileControlError::ResourceExhausted))));
            let prefix=c.trace();
            for cause in CAUSES{c.arm(Some((prefix.len(),cause)));assert!(matches!(prepare_table_write_node_encode(&n,v,e,ids.view(),source,sl),Err(Error::Control(CompileControlError::ResourceExhausted))));assert_eq!(c.trace(),prefix);}
            let mut dl=exact(limits(),received);under(&mut dl,axis);
            c.arm(None);assert!(matches!(prepare_table_write_node_decode(&wire,r,a,source,dl),Err(Error::Control(CompileControlError::ResourceExhausted))));
            let prefix=c.trace();
            for cause in CAUSES{c.arm(Some((prefix.len(),cause)));assert!(matches!(prepare_table_write_node_decode(&wire,r,a,source,dl),Err(Error::Control(CompileControlError::ResourceExhausted))));assert_eq!(c.trace(),prefix);}
        }
    });
}

#[test]
fn table_write_parent_whole_writer_and_finish_exact_replay_and_control_prefixes() {
    let fixture = Fixture::new();
    let c = Control::default();
    for node in [writer(), finisher(true)] {
        let ids = Ids::new(&node);
        fixture.with(&node, &c, |values, expressions, read, aggregates| {
            for decode in [false, true] {
                c.arm(None);
                let raw = encode_table_write_node(&node, values, expressions, ids.view(), SOURCE, limits()).unwrap().0;
                let run = |l, snapshots: &mut Vec<NodeProjectionFacts>| {
                    let phase = if decode { CompilePhase::Decode } else { CompilePhase::Encode };
                    let owner = if decode { read.original_control() } else { values.original_control() };
                    let mut work = CompileCheckpoints::try_new(owner, phase)?;
                    let mut admit = |f: &NodeProjectionFacts| { snapshots.push(*f); Ok(()) };
                    let result = if decode {
                        prepare_table_write_node_decode_in(&raw, read, aggregates, SOURCE, l, &mut admit, &mut work)
                            .and_then(|p| p.emit_in(&mut admit, &mut work)).map(|(out, f)| { assert_eq!(out, node); f })
                    } else {
                        prepare_table_write_node_encode_in(&node, values, expressions, ids.view(), SOURCE, l, &mut admit, &mut work)
                            .and_then(|p| p.emit_in(&mut admit, &mut work)).map(|(out, f)| { assert_eq!(out, raw); f })
                    };
                    finish(work, result)
                };
                c.arm(None);
                let mut snapshots = vec![];
                let f = run(limits(), &mut snapshots).unwrap();
                let trace = c.trace();
                assert!(!snapshots.is_empty());
                for prefix in &snapshots {
                    assert!(prefix.allocation_requests_upper_bound <= f.allocation_requests_upper_bound);
                    assert!(prefix.allocation_request_bytes_upper_bound <= f.allocation_request_bytes_upper_bound);
                    assert!(prefix.cumulative_work_upper_bound <= f.cumulative_work_upper_bound);
                    assert_eq!(prefix.coexisting_source_and_request_bytes_upper_bound, SOURCE + prefix.allocation_request_bytes_upper_bound);
                }
                c.arm(None);
                assert_eq!(run(exact(limits(), f), &mut vec![]).unwrap(), f);
                for axis in 0..7 {
                    let mut l = exact(limits(), f); under(&mut l, axis);
                    c.arm(None);
                    assert!(matches!(run(l, &mut vec![]), Err(Error::Control(CompileControlError::ResourceExhausted))));
                }
                for at in 0..trace.len() {
                    for cause in CAUSES {
                        c.arm(Some((at, cause.clone())));
                        assert!(matches!(run(limits(), &mut vec![]), Err(Error::Control(actual)) if actual == cause));
                        assert_eq!(c.trace(), trace[..=at]);
                    }
                }
            }
        });
    }
}

#[test]
fn table_write_parent_known_containers_refuse_before_pending_callback() {
    let fixture = Fixture::new();
    let c = Control::default();
    let node = writer();
    let ids = Ids::new(&node);
    fixture.with(&node, &c, |values, expressions, read, aggregates| {
        c.arm(None);
        let raw = encode_table_write_node(&node, values, expressions, ids.view(), SOURCE, limits())
            .unwrap()
            .0;
        for decode in [false, true] {
            for cause in CAUSES {
                c.arm(Some((1, cause)));
                let owner = if decode {
                    read.original_control()
                } else {
                    values.original_control()
                };
                let mut work = CompileCheckpoints::try_new(owner, CompilePhase::Decode).unwrap();
                for _ in 0..255 {
                    work.step().unwrap();
                }
                let mut l = limits();
                l.node.max_allocation_requests = 0;
                let mut admit = |_: &NodeProjectionFacts| -> Result<(), CompileControlError> {
                    panic!("known container cap precedes parent")
                };
                let result = if decode {
                    prepare_table_write_node_decode_in(
                        &raw, read, aggregates, SOURCE, l, &mut admit, &mut work,
                    )
                    .map(|_| ())
                } else {
                    prepare_table_write_node_encode_in(
                        &node,
                        values,
                        expressions,
                        ids.view(),
                        SOURCE,
                        l,
                        &mut admit,
                        &mut work,
                    )
                    .map(|_| ())
                };
                assert!(matches!(
                    finish(work, result),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
            }
        }
    });
}

#[test]
fn finish_summary_is_three_original_references_with_strict_header_presence() {
    let c = Control::default();
    let f = Fixture::new();
    // Existing namespace IDs 0,7,MAX are enough to test the representation's
    // precise reference domain. Static non-null Int64/port laws stay with IR
    // validation; this test does not certify a complete physical graph.
    let baseline = finisher(false);
    let mut logical = baseline.clone();
    let spec = finish_spec(&mut logical);
    spec.count_mode = p::FinishCountMode::LogicalChanges;
    spec.inputs.logical_summary = Some(1);
    spec.logical_summary = Some(p::LogicalSummaryColumns {
        inserted: p::ValueId::new(0),
        replaced: p::ValueId::new(7),
        deleted: p::ValueId::new(u32::MAX),
    });
    // Each physical owner must use the original binding loans it authored.
    let base_ids = Ids::new(&baseline);
    let (base_raw, base) = f.with(&baseline, &c, |v, e, _, _| {
        c.arm(None);
        encode_table_write_node(&baseline, v, e, base_ids.view(), SOURCE, limits()).unwrap()
    });
    let ids = Ids::new(&logical);
    f.with(&logical, &c, |v, e, r, a| {
        c.arm(None);
        let (raw, facts) =
            encode_table_write_node(&logical, v, e, ids.view(), SOURCE, limits()).unwrap();
        assert_eq!(facts.value_reference_count, base.value_reference_count + 3);
        assert_eq!(facts.list_item_count, base.list_item_count + 3);
        assert_eq!(
            facts.allocation_requests_upper_bound,
            base.allocation_requests_upper_bound
        );
        assert_eq!(
            facts.allocation_request_bytes_upper_bound,
            base.allocation_request_bytes_upper_bound
        );
        let mut under = limits();
        under.node.max_value_references = base.value_reference_count;
        c.arm(None);
        assert!(encode_table_write_node(&logical, v, e, ids.view(), SOURCE, under).is_err());
        c.arm(None);
        let (_, received_base) =
            decode_table_write_node(&base_raw, r, a, SOURCE, limits()).unwrap();
        c.arm(None);
        let (owned, received) = decode_table_write_node(&raw, r, a, SOURCE, limits()).unwrap();
        assert_eq!(owned, logical);
        assert_eq!(
            received.value_reference_count,
            received_base.value_reference_count + 3
        );
        assert_eq!(received.list_item_count, received_base.list_item_count + 3);
        c.arm(None);
        assert!(decode_table_write_node(&raw, r, a, SOURCE, under).is_err());
        for fault in 0..9 {
            let mut bad = raw.clone();
            let finish = raw_finish(&mut bad);
            match fault {
                0 => finish.count_mode = 0,
                1 => finish.count_mode = i32::MAX,
                2 => finish.writer_results_input = None,
                3 => finish.logical_summary_input = None,
                4 => finish.logical_summary_input = finish.writer_results_input,
                5 => finish.logical_summary = None,
                6 => finish.logical_summary.as_mut().unwrap().inserted_value_id = None,
                7 => finish.logical_summary.as_mut().unwrap().replaced_value_id = None,
                _ => finish.logical_summary.as_mut().unwrap().deleted_value_id = None,
            }
            c.arm(None);
            assert!(
                matches!(
                    decode_table_write_node(&bad, r, a, SOURCE, limits()),
                    Err(Error::InvalidShape(_))
                ),
                "fault {fault}"
            );
        }
        let mut unknown = raw;
        raw_finish(&mut unknown)
            .logical_summary
            .as_mut()
            .unwrap()
            .inserted_value_id = Some(404);
        c.arm(None);
        assert!(decode_table_write_node(&unknown, r, a, SOURCE, limits()).is_err());
    });
}
