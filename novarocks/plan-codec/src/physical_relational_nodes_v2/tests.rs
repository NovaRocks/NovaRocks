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
    physical_aggregate_binding_v2::{encode_aggregate_bindings, prepare_aggregate_binding_headers},
    physical_binding_v2::{
        BindingProjectionLimits, encode_function_bindings, prepare_function_binding_headers,
    },
    physical_connector_payload_v2::{
        ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
    },
    physical_expression_v2::{
        ExpressionDefinitionSource, ExpressionProjectionLimits, ExpressionTypeIds,
        decode_expression_definitions, encode_expression_definitions,
    },
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, ValueSource, decode_values, encode_values},
};
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_expression_contract::{ExprId, RootOrdinal};
use novarocks_proto_models::physical_control_v2::Empty;
use novarocks_type_contract::{
    CompileControlError, FunctionValueType, PureCompileControl, SemanticParameters,
};
use std::sync::{Arc, Mutex};
const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    state: Mutex<(bool, Option<(usize, CompileControlError)>)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let (active, stop) = self.state.lock().unwrap().clone();
        if !active {
            return Ok(());
        }
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.state.lock().unwrap() = (true, stop);
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn limits() -> RelationalNodeProjectionLimits {
    RelationalNodeProjectionLimits {
        max_input_nodes: 8,
        max_value_references: 8192,
        max_list_items: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 1 << 20,
        max_coexisting_source_and_request_bytes: 4 << 20,
        max_work: 64 << 20,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 8192,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 1 << 20,
            max_coexisting_source_and_request_bytes: 4 << 20,
            max_work: 64 << 20,
        },
    }
}
fn types_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 64,
        max_expanded_nodes: 128,
        max_string_bytes: 8192,
    }
}
fn binding_limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 8,
        max_type_references: 16,
        max_request_bytes: 8192,
        max_allocation_requests: 64,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 1 << 20,
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 0,
        max_payload_bytes: 0,
        max_allocation_requests: 0,
        max_allocation_request_bytes: 0,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 1 << 20,
    }
}
fn value_limits() -> ValueProjectionLimits {
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
fn expression_limits() -> ExpressionProjectionLimits {
    ExpressionProjectionLimits {
        max_definitions: 8,
        max_type_references: 16,
        max_expression_references: 16,
        max_new_allocation_requests: 32,
        max_new_allocation_request_bytes: 64 << 10,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_cumulative_work: 128 << 20,
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
fn pool(array: &dyn Array) -> ConstantPool {
    let field = Arc::new(
        Field::new("original_source", array.data_type().clone(), true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "source.tag".into(),
                "unchanged".into(),
            )]),
        ),
    );
    ConstantPool::try_new(
        field,
        FunctionValueType::new(array.data_type().clone(), true),
        array.to_data(),
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
fn unit_from_sources(
    nodes: impl IntoIterator<Item = novarocks_expression_contract::ExprNode>,
    values: &[p::ValueDef],
    control: &dyn PureCompileControl,
) -> novarocks_expression_contract::BoundExprSet {
    use novarocks_expression_contract::{BoundExprSetBuilder, ExprPort, RootScope, UnitLimits};
    use novarocks_type_contract::{CompileCheckpoints, EvaluationDemand};
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: p::PlanLimits::FROZEN.expression_semantic_depth,
    });
    for node in nodes {
        let demand = if *node.ty.data_type() == DataType::Boolean {
            EvaluationDemand::TruthOnly
        } else {
            EvaluationDemand::Value
        };
        let id = builder.add(node.lambda_scope, node.ty, node.kind).unwrap();
        assert_eq!(id, node.id);
        builder.root(id, demand, RootScope::Port(0)).unwrap();
    }
    let ports = vec![ExprPort {
        values: values
            .iter()
            .map(|value| (value.id, value.ty.clone()))
            .collect(),
    }];
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
    let unit = builder
        .seal(ports, &mut work)
        .unwrap()
        .finish(std::collections::BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    unit
}

struct Fixture {
    roots: Vec<(u32, FunctionValueType)>,
    definitions: Vec<p::ValueDef>,
    unit: novarocks_expression_contract::BoundExprSet,
    parameters: SemanticParameters,
    pools: p::ConstantPools,
}
impl Fixture {
    fn new() -> Self {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let definitions: Vec<p::ValueDef> = [0, 7, u32::MAX]
            .into_iter()
            .map(|id| p::ValueDef {
                id: p::ValueId::new(id),
                ty: ty.clone(),
                origin: p::ValueOrigin::NodeOutput {
                    node: p::NodeId::new(0),
                    output_ordinal: id,
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
        let nodes = [
            (
                0,
                novarocks_expression_contract::ExprKind::Constant(reference(u32::MAX, 1)),
            ),
            (
                1,
                novarocks_expression_contract::ExprKind::Value(p::ValueId::new(0)),
            ),
            (
                2,
                novarocks_expression_contract::ExprKind::Constant(reference(u32::MAX, 2)),
            ),
        ]
        .into_iter()
        .map(|(id, kind)| novarocks_expression_contract::ExprNode {
            id: ExprId::new(id),
            lambda_scope: None,
            ty: ty.clone(),
            kind,
        });
        let unit = unit_from_sources(nodes, &definitions, &Control::default());
        Self {
            roots: vec![(0, ty)],
            definitions,
            unit,
            parameters: SemanticParameters::try_new([]).unwrap(),
            pools,
        }
    }
    fn with_tokens<R>(
        &self,
        window: &p::PhysicalNode,
        control: &Control,
        run: impl FnOnce(
            &EncodedValues<'_, '_, '_>,
            &EncodedExpressions<'_, '_, '_>,
            &DecodedExpressions<'_, '_, '_>,
            &crate::physical_binding_v2::MaterializedFunctionBindings<'_, '_>,
            &crate::physical_aggregate_binding_v2::MaterializedAggregateBindings<'_, '_, '_>,
        ) -> R,
    ) -> R {
        let types = encode_type_table_sources(&self.roots, &[], types_limits(), control).unwrap();
        let read_types = decode_type_table(types.as_wire(), types_limits(), control).unwrap();
        let payloads = encode_connector_payloads(&[], 64 << 10, payload_limits(), control).unwrap();
        let read_payloads =
            decode_connector_payloads(payloads.as_wire(), 64 << 10, payload_limits(), control)
                .unwrap();
        let value_inputs = self
            .definitions
            .iter()
            .map(|source| ValueSource {
                source,
                value_type_id: 0,
            })
            .collect::<Vec<_>>();
        let values =
            encode_values(&value_inputs, &payloads, &types, 256 << 10, value_limits()).unwrap();
        let read_values = decode_values(
            values.as_wire(),
            &read_payloads,
            &read_types,
            256 << 10,
            value_limits(),
        )
        .unwrap();
        let p::NodeKind::Window(spec) = &window.kind else {
            panic!("expected original window source")
        };
        let mut binding_sources = spec
            .calls
            .iter()
            .map(|call| &call.binding)
            .collect::<Vec<_>>();
        let aggregate_sources = spec
            .calls
            .iter()
            .filter_map(|call| call.aggregate_binding.as_deref())
            .collect::<Vec<_>>();
        binding_sources.extend(aggregate_sources.iter().map(|binding| &binding.function));
        let argument_ids = binding_sources
            .iter()
            .map(|binding| {
                binding
                    .argument_types
                    .iter()
                    .map(|argument| {
                        assert!(matches!(
                            argument,
                            novarocks_type_contract::FunctionArgumentType::Value(_)
                        ));
                        crate::physical_binding_v2::ArgumentTypeIds::Value(0)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let function_inputs = binding_sources
            .iter()
            .zip(&argument_ids)
            .enumerate()
            .map(
                |(id, (binding, arguments))| crate::physical_binding_v2::FunctionBindingInput {
                    id: id as u32,
                    source: crate::physical_binding_v2::BindingSource::Scalar(binding),
                    arguments,
                    result: crate::physical_binding_v2::ResultTypeIds::Scalar(0),
                },
            )
            .collect::<Vec<_>>();
        let functions = encode_function_bindings(
            &types,
            &function_inputs,
            64 << 10,
            binding_limits(),
            control,
        )
        .unwrap();
        let aggregate_inputs = aggregate_sources
            .iter()
            .enumerate()
            .map(
                |(ordinal, binding)| crate::physical_aggregate_binding_v2::AggregateBindingInput {
                    id: if ordinal == 0 {
                        u32::MAX
                    } else {
                        ordinal as u32
                    },
                    source: binding,
                    function_binding_id: (spec.calls.len() + ordinal) as u32,
                    intermediate_value_type_id: Some(0),
                },
            )
            .collect::<Vec<_>>();
        let aggregates = encode_aggregate_bindings(
            &types,
            &functions,
            &aggregate_inputs,
            128 << 10,
            binding_limits(),
            control,
        )
        .unwrap();
        let functions_read = prepare_function_binding_headers(
            functions.as_wire(),
            &read_types,
            64 << 10,
            binding_limits(),
            control,
        )
        .unwrap();
        let aggregates_read = prepare_aggregate_binding_headers(
            aggregates.as_wire(),
            &functions_read,
            128 << 10,
            binding_limits(),
        )
        .unwrap();
        let inputs = self
            .unit
            .definitions()
            .iter()
            .map(|node| ExpressionTypeIds {
                expr: node.id,
                value_type_id: 0,
                lambda_parameter_type_ids: &[],
                function_binding_id: None,
            })
            .collect::<Vec<_>>();
        let expressions = encode_expression_definitions(
            Some(ExpressionDefinitionSource::from_unit(&self.unit)),
            &inputs,
            &types,
            &functions,
            &aggregates,
            &self.parameters,
            &self.pools,
            512 << 10,
            expression_limits(),
            control,
        )
        .unwrap();
        let wire_roots = crate::test_constants::wire_roots(&self.unit);
        let read = decode_expression_definitions(
            expressions.as_wire(),
            &wire_roots,
            &read_values,
            &functions_read,
            &aggregates_read,
            &self.parameters,
            &self.pools,
            512 << 10,
            expression_limits(),
        )
        .unwrap();
        let materialized_functions = crate::physical_binding_v2::materialize_function_bindings(
            crate::physical_binding_v2::prepare_function_bindings_materialization(
                &functions_read,
                128 << 10,
                binding_limits(),
            )
            .unwrap(),
        )
        .unwrap();
        let materialized_aggregates =
            crate::physical_aggregate_binding_v2::materialize_aggregate_bindings(
                crate::physical_aggregate_binding_v2::prepare_aggregate_bindings_materialization(
                    &aggregates_read,
                    &materialized_functions,
                    256 << 10,
                    binding_limits(),
                )
                .unwrap(),
            )
            .unwrap();
        run(
            &values,
            &expressions,
            &read,
            &materialized_functions,
            &materialized_aggregates,
        )
    }
}
fn property() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn sort(expr: u32, direction: p::SortDirection, null_ordering: p::NullOrdering) -> p::SortExpr {
    p::SortExpr {
        expr: RootOrdinal::new(expr),
        direction,
        null_ordering,
    }
}
fn order() -> Box<[p::SortExpr]> {
    Box::from([
        sort(1, p::SortDirection::Descending, p::NullOrdering::First),
        sort(0, p::SortDirection::Ascending, p::NullOrdering::Last),
    ])
}
fn window_call(root: u32, output: u32) -> p::WindowCallSpec {
    use novarocks_type_contract::{
        FunctionArgumentType, FunctionId, FunctionKind, FunctionOverloadId, TypedFactSlice,
    };
    let ty = FunctionValueType::new(DataType::Int64, true);
    p::WindowCallSpec {
        binding: novarocks_expression_contract::BoundFunction::from_exact_signature(
            FunctionId::try_new("fixture/window/value/v1").unwrap(),
            FunctionOverloadId::try_new("fixture/window/value/int64/v1").unwrap(),
            FunctionKind::Window,
            TypedFactSlice::from_pure_boxed(Box::from([FunctionArgumentType::Value(ty.clone())])),
            ty,
        ),
        distinct: false,
        arguments: Box::from([RootOrdinal::new(root)]),
        order_by: Box::default(),
        frame: None,
        ignore_nulls: false,
        aggregate_binding: None,
        output: p::ValueId::new(output),
    }
}
fn wire_window_call(binding: u32, root: u32, output: u32) -> wire::WindowCall {
    wire::WindowCall {
        function_binding_id: Some(binding),
        aggregate_binding_id: None,
        distinct: false,
        argument_roots: vec![root],
        order_by: vec![],
        frame: None,
        ignore_nulls: false,
        output_value_id: Some(output),
    }
}
fn source_ref<'a>(
    at: usize,
    window: &'a p::PhysicalNode,
    ordinary: &'a p::PhysicalNode,
) -> &'a p::PhysicalNode {
    if at == 5 { window } else { ordinary }
}
fn source(at: usize) -> p::PhysicalNode {
    let kind = match at {
        0 => p::NodeKind::HashJoin {
            kind: p::JoinKind::FullOuter,
            condition: p::HashJoinCondition::Ordinary {
                keys: Box::from([
                    p::JoinKey {
                        left: RootOrdinal::new(2),
                        right: RootOrdinal::new(1),
                        null_safe: false,
                        match_relation: novarocks_type_contract::JoinKeyMatch::Grouping,
                    },
                    p::JoinKey {
                        left: RootOrdinal::new(0),
                        right: RootOrdinal::new(1),
                        null_safe: true,
                        match_relation: novarocks_type_contract::JoinKeyMatch::Grouping,
                    },
                ]),
                residual: Some(RootOrdinal::new(0)),
            },
            build_side: p::JoinSide::Right,
            distribution: p::JoinDistribution::BroadcastBuild,
            null_extended: Box::from([p::ValueId::new(u32::MAX), p::ValueId::new(7)]),
        },
        1 => p::NodeKind::NestLoopJoin {
            kind: p::JoinKind::Inner,
            distribution: p::NestLoopJoinDistribution::BroadcastRight,
            predicate: Some(RootOrdinal::new(2)),
            null_extended: Box::from([p::ValueId::new(7)]),
        },
        2 => p::NodeKind::Sort {
            order_by: order(),
            mode: p::SortMode::Global,
        },
        3 => p::NodeKind::Sort {
            order_by: order(),
            mode: p::SortMode::Analytic {
                partition_by: Box::from([sort(
                    2,
                    p::SortDirection::Descending,
                    p::NullOrdering::Last,
                )]),
            },
        },
        4 => p::NodeKind::Sort {
            order_by: order(),
            mode: p::SortMode::PartitionTopN {
                partition_by: Box::from([
                    sort(2, p::SortDirection::Descending, p::NullOrdering::Last),
                    sort(1, p::SortDirection::Ascending, p::NullOrdering::First),
                ]),
                limit: u64::MAX,
                kind: p::PartitionTopNType::DenseRank,
            },
        },
        5 => p::NodeKind::Window(p::WindowSpec {
            partition_by: Box::from([sort(
                1,
                p::SortDirection::Descending,
                p::NullOrdering::First,
            )]),
            order_by: Box::from([
                sort(2, p::SortDirection::Ascending, p::NullOrdering::Last),
                sort(1, p::SortDirection::Descending, p::NullOrdering::First),
            ]),
            calls: Box::from([
                window_call(2, 7),
                window_call(0, 0),
                window_call(1, u32::MAX),
            ]),
        }),
        6 => p::NodeKind::SetOp {
            kind: p::SetOperationKind::Except,
            input_mappings: Box::from([
                Box::from([
                    p::ValueId::new(u32::MAX),
                    p::ValueId::new(0),
                    p::ValueId::new(7),
                ]),
                Box::from([
                    p::ValueId::new(7),
                    p::ValueId::new(u32::MAX),
                    p::ValueId::new(0),
                ]),
            ]),
        },
        7 => p::NodeKind::ExchangeSource {
            edge: p::EdgeId::new(u32::MAX),
            imports: Box::from([
                (p::ValueId::new(99), p::ValueId::new(u32::MAX)),
                (p::ValueId::new(0), p::ValueId::new(7)),
                (p::ValueId::new(u32::MAX), p::ValueId::new(0)),
            ]),
        },
        _ => panic!(),
    };
    let inputs: Box<[p::NodeId]> = if at <= 1 {
        Box::from([p::NodeId::new(0), p::NodeId::new(7)])
    } else {
        Box::from([p::NodeId::new(0)])
    };
    let required_inputs =
        vec![p::passthrough_requirement(&property()); inputs.len()].into_boxed_slice();
    p::PhysicalNode {
        expressions: None,
        id: p::NodeId::new(u32::MAX),
        inputs,
        required_inputs,
        output_properties: property(),
        output: p::OutputPort {
            node: p::NodeId::new(u32::MAX),
            columns: Box::from([
                p::ValueId::new(7),
                p::ValueId::new(u32::MAX),
                p::ValueId::new(0),
            ]),
        },
        kind,
    }
}
fn ws(
    expr: u32,
    direction: wire::SortDirection,
    nulls: wire::NullOrdering,
) -> wire::SortExpression {
    wire::SortExpression {
        root_ordinal: Some(expr),
        direction: direction as i32,
        null_ordering: nulls as i32,
    }
}
fn wire_order() -> Vec<wire::SortExpression> {
    vec![
        ws(
            1,
            wire::SortDirection::Descending,
            wire::NullOrdering::First,
        ),
        ws(0, wire::SortDirection::Ascending, wire::NullOrdering::Last),
    ]
}
fn expected_property(unconstrained: bool) -> wire::PhysicalProperties {
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
fn ordinary_wire_keys_mut(node: &mut wire::HashJoinNode) -> &mut Vec<wire::JoinKey> {
    let Some(wire::hash_join_node::Condition::Ordinary(condition)) = node.condition.as_mut() else {
        panic!("test fixture must carry the explicit ordinary condition");
    };
    &mut condition.keys
}

fn expected(at: usize) -> wire::PhysicalNode {
    let kind = match at {
        0 => wire::physical_node::Kind::HashJoin(wire::HashJoinNode {
            kind: wire::JoinKind::FullOuter as i32,
            condition: Some(wire::hash_join_node::Condition::Ordinary(
                wire::OrdinaryHashJoinCondition {
                    keys: vec![
                        wire::JoinKey {
                            left_root: Some(2),
                            right_root: Some(1),
                            null_safe: false,
                            match_relation: wire::JoinKeyMatch::Grouping as i32,
                        },
                        wire::JoinKey {
                            left_root: Some(0),
                            right_root: Some(1),
                            null_safe: true,
                            match_relation: wire::JoinKeyMatch::Grouping as i32,
                        },
                    ],
                    residual_root: Some(0),
                },
            )),
            build_side: wire::JoinSide::Right as i32,
            distribution: wire::JoinDistribution::BroadcastBuild as i32,
            null_extended_value_ids: vec![u32::MAX, 7],
        }),
        1 => wire::physical_node::Kind::NestLoopJoin(wire::NestLoopJoinNode {
            kind: wire::JoinKind::Inner as i32,
            distribution: wire::NestLoopJoinDistribution::BroadcastRight as i32,
            predicate_root: Some(2),
            null_extended_value_ids: vec![7],
        }),
        2 => wire::physical_node::Kind::Sort(wire::SortNode {
            order_by: wire_order(),
            mode: Some(wire::SortMode {
                kind: Some(wire::sort_mode::Kind::Global(Empty {})),
            }),
        }),
        3 => wire::physical_node::Kind::Sort(wire::SortNode {
            order_by: wire_order(),
            mode: Some(wire::SortMode {
                kind: Some(wire::sort_mode::Kind::Analytic(wire::AnalyticSort {
                    partition_by: vec![ws(
                        2,
                        wire::SortDirection::Descending,
                        wire::NullOrdering::Last,
                    )],
                })),
            }),
        }),
        4 => wire::physical_node::Kind::Sort(wire::SortNode {
            order_by: wire_order(),
            mode: Some(wire::SortMode {
                kind: Some(wire::sort_mode::Kind::PartitionTopN(wire::PartitionTopN {
                    partition_by: vec![
                        ws(2, wire::SortDirection::Descending, wire::NullOrdering::Last),
                        ws(1, wire::SortDirection::Ascending, wire::NullOrdering::First),
                    ],
                    limit: u64::MAX,
                    kind: wire::PartitionTopNType::DenseRank as i32,
                })),
            }),
        }),
        5 => wire::physical_node::Kind::Window(wire::WindowNode {
            partition_by: vec![ws(
                1,
                wire::SortDirection::Descending,
                wire::NullOrdering::First,
            )],
            order_by: vec![
                ws(2, wire::SortDirection::Ascending, wire::NullOrdering::Last),
                ws(
                    1,
                    wire::SortDirection::Descending,
                    wire::NullOrdering::First,
                ),
            ],
            calls: vec![
                wire_window_call(0, 2, 7),
                wire_window_call(1, 0, 0),
                wire_window_call(2, 1, u32::MAX),
            ],
        }),
        6 => wire::physical_node::Kind::SetOperation(wire::SetOperationNode {
            kind: wire::SetOperationKind::Except as i32,
            input_mappings: vec![
                wire::ValueIds {
                    value_ids: vec![u32::MAX, 0, 7],
                },
                wire::ValueIds {
                    value_ids: vec![7, u32::MAX, 0],
                },
            ],
        }),
        7 => wire::physical_node::Kind::ExchangeSource(wire::ExchangeSourceNode {
            edge_id: Some(u32::MAX),
            imports: vec![
                wire::ValueMapping {
                    source_value_id: Some(99),
                    destination_value_id: Some(u32::MAX),
                },
                wire::ValueMapping {
                    source_value_id: Some(0),
                    destination_value_id: Some(7),
                },
                wire::ValueMapping {
                    source_value_id: Some(u32::MAX),
                    destination_value_id: Some(0),
                },
            ],
        }),
        _ => panic!(),
    };
    let input_node_ids = if at <= 1 { vec![0, 7] } else { vec![0] };
    let required_inputs = vec![expected_property(true); input_node_ids.len()];
    wire::PhysicalNode {
        expressions: None,
        id: u32::MAX,
        input_node_ids,
        required_inputs,
        output_properties: Some(expected_property(false)),
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![7, u32::MAX, 0],
        }),
        kind: Some(kind),
    }
}
#[test]
fn relational_six_kinds_and_all_sort_modes_match_independent_complete_wire() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            for at in 0..8 {
                c.arm(None);
                assert_eq!(
                    encode_relational_node(
                        source_ref(at, &window, &source(at)),
                        values,
                        expressions,
                        SOURCE,
                        limits()
                    )
                    .unwrap()
                    .0,
                    expected(at)
                );
                c.arm(None);
                assert_eq!(
                    decode_relational_node(
                        &expected(at),
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    )
                    .unwrap()
                    .0,
                    source(at)
                );
            }
        },
    );
}
#[test]
fn relational_prepared_emission_keeps_original_loans_and_scope() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            for at in [0, 1, 2, 5, 6, 7] {
                let fallback = source(at);
                let node = source_ref(at, &window, &fallback);
                let wire = expected(at);
                c.arm(None);
                let prepared =
                    prepare_relational_node_encode(node, values, expressions, SOURCE, limits())
                        .unwrap();
                let facts = *prepared.facts();
                let (output, emitted) = prepared.emit().unwrap();
                assert_eq!(output, wire);
                assert_eq!(facts, emitted);
                c.arm(None);
                let prepared = prepare_relational_node_decode(
                    &wire,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits(),
                )
                .unwrap();
                let facts = *prepared.facts();
                let (output, emitted) = prepared.emit().unwrap();
                assert_eq!(&output, node);
                assert_eq!(facts, emitted);
            }
            let other_types =
                encode_type_table_sources(&fixture.roots, &[], types_limits(), &c).unwrap();
            let inputs = fixture
                .definitions
                .iter()
                .map(|source| ValueSource {
                    source,
                    value_type_id: 0,
                })
                .collect::<Vec<_>>();
            let other_values = encode_values(
                &inputs,
                values.payloads(),
                &other_types,
                256 << 10,
                value_limits(),
            )
            .unwrap();
            c.arm(None);
            assert!(matches!(
                encode_relational_node(&source(0), &other_values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            let other_control = Control::default();
            let other_payloads =
                encode_connector_payloads(&[], 64 << 10, payload_limits(), &other_control).unwrap();
            let other_values = encode_values(
                &inputs,
                &other_payloads,
                values.types(),
                256 << 10,
                value_limits(),
            )
            .unwrap();
            c.arm(None);
            assert!(matches!(
                encode_relational_node(&source(7), &other_values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        },
    );
}
#[test]
fn relational_exchange_remote_source_ids_are_not_local_value_references() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            assert!(values.value(99).unwrap().is_none());
            assert!(read.values().value(99).unwrap().is_none());
            c.arm(None);
            let (wire, facts) =
                encode_relational_node(&source(7), values, expressions, SOURCE, limits()).unwrap();
            assert_eq!(facts.value_reference_count, 6);
            assert_eq!(wire, expected(7));
            c.arm(None);
            assert_eq!(
                decode_relational_node(
                    &expected(7),
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits()
                )
                .unwrap()
                .0,
                source(7)
            );
            let mut node = source(7);
            if let p::NodeKind::ExchangeSource { imports, .. } = &mut node.kind {
                imports[0].1 = p::ValueId::new(99);
            }
            c.arm(None);
            assert!(matches!(
                encode_relational_node(&node, values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            let mut node = expected(7);
            if let Some(wire::physical_node::Kind::ExchangeSource(v)) = node.kind.as_mut() {
                v.imports[0].destination_value_id = Some(99);
            }
            c.arm(None);
            assert!(matches!(
                decode_relational_node(
                    &node,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits()
                ),
                Err(Error::InvalidShape(_))
            ));
        },
    );
}
#[test]
fn relational_independent_layouts_include_every_inner_request_and_only_local_refs() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            for at in 0..8 {
                let (extra_e, extra_d, requests, items, refs) = match at {
                    0 => (
                        2 * std::mem::size_of::<wire::JoinKey>() + 2 * std::mem::size_of::<u32>(),
                        4 * std::mem::size_of::<p::JoinKey>()
                            + 4 * std::mem::size_of::<p::ValueId>(),
                        2,
                        9,
                        5,
                    ),
                    1 => (
                        std::mem::size_of::<u32>(),
                        2 * std::mem::size_of::<p::ValueId>(),
                        1,
                        6,
                        4,
                    ),
                    2 => (
                        2 * std::mem::size_of::<wire::SortExpression>(),
                        4 * std::mem::size_of::<p::SortExpr>(),
                        1,
                        6,
                        3,
                    ),
                    3 => (
                        3 * std::mem::size_of::<wire::SortExpression>(),
                        6 * std::mem::size_of::<p::SortExpr>(),
                        2,
                        7,
                        3,
                    ),
                    4 => (
                        4 * std::mem::size_of::<wire::SortExpression>(),
                        8 * std::mem::size_of::<p::SortExpr>(),
                        2,
                        8,
                        3,
                    ),
                    5 => (
                        3 * std::mem::size_of::<wire::SortExpression>()
                            + 3 * std::mem::size_of::<wire::WindowCall>()
                            + 3 * std::mem::size_of::<u32>(),
                        6 * std::mem::size_of::<p::SortExpr>()
                            + 6 * std::mem::size_of::<p::WindowCallSpec>()
                            + 6 * std::mem::size_of::<RootOrdinal>(),
                        6,
                        13,
                        6,
                    ),
                    6 => (
                        2 * std::mem::size_of::<wire::ValueIds>() + 6 * std::mem::size_of::<u32>(),
                        4 * std::mem::size_of::<Box<[p::ValueId]>>()
                            + 12 * std::mem::size_of::<p::ValueId>(),
                        3,
                        12,
                        9,
                    ),
                    7 => (
                        3 * std::mem::size_of::<wire::ValueMapping>(),
                        6 * std::mem::size_of::<(p::ValueId, p::ValueId)>(),
                        1,
                        7,
                        6,
                    ),
                    _ => unreachable!(),
                };
                let inputs = if at <= 1 { 2 } else { 1 };
                let e_header = (inputs + 3) * std::mem::size_of::<u32>()
                    + inputs * std::mem::size_of::<wire::PhysicalProperties>();
                let d_header = 2
                    * (inputs * std::mem::size_of::<p::NodeId>()
                        + 3 * std::mem::size_of::<p::ValueId>()
                        + inputs * std::mem::size_of::<p::PhysicalProperties>());
                c.arm(None);
                let (_, facts) = encode_relational_node(
                    source_ref(at, &window, &source(at)),
                    values,
                    expressions,
                    SOURCE,
                    limits(),
                )
                .unwrap();
                assert_eq!(facts.allocation_requests_upper_bound, 3 + requests);
                assert_eq!(
                    facts.allocation_request_bytes_upper_bound,
                    e_header + extra_e
                );
                assert_eq!(facts.list_item_count, items);
                assert_eq!(facts.value_reference_count, refs);
                c.arm(None);
                let (_, facts) = decode_relational_node(
                    &expected(at),
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits(),
                )
                .unwrap();
                assert_eq!(facts.allocation_requests_upper_bound, 2 * (3 + requests));
                assert_eq!(
                    facts.allocation_request_bytes_upper_bound,
                    d_header + extra_d
                );
                // Three actual Window signatures each contribute one argument item
                // to the receiving node's original composition model.
                let decoded_items = if at == 5 { 16 } else { items };
                assert_eq!(facts.list_item_count, decoded_items);
                assert_eq!(facts.value_reference_count, refs);
            }
        },
    );
}
#[test]
fn relational_all_closed_join_set_and_partition_topn_enums_keep_raw_payloads() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            for (kind, raw) in [
                (p::JoinKind::Cross, wire::JoinKind::Cross),
                (p::JoinKind::LeftMark, wire::JoinKind::LeftMark),
                (p::JoinKind::Inner, wire::JoinKind::Inner),
                (p::JoinKind::LeftOuter, wire::JoinKind::LeftOuter),
                (p::JoinKind::RightOuter, wire::JoinKind::RightOuter),
                (p::JoinKind::FullOuter, wire::JoinKind::FullOuter),
                (p::JoinKind::LeftSemi, wire::JoinKind::LeftSemi),
                (p::JoinKind::RightSemi, wire::JoinKind::RightSemi),
                (p::JoinKind::LeftAnti, wire::JoinKind::LeftAnti),
                (p::JoinKind::RightAnti, wire::JoinKind::RightAnti),
                (
                    p::JoinKind::NullAwareLeftAnti,
                    wire::JoinKind::NullAwareLeftAnti,
                ),
            ] {
                for at in [0, 1] {
                    let mut node = source(at);
                    let mut wire = expected(at);
                    match &mut node.kind {
                        p::NodeKind::HashJoin { kind: target, .. }
                        | p::NodeKind::NestLoopJoin { kind: target, .. } => *target = kind,
                        _ => unreachable!(),
                    };
                    match wire.kind.as_mut() {
                        Some(wire::physical_node::Kind::HashJoin(v)) => v.kind = raw as i32,
                        Some(wire::physical_node::Kind::NestLoopJoin(v)) => v.kind = raw as i32,
                        _ => unreachable!(),
                    };
                    if matches!(kind, p::JoinKind::NullAwareLeftAnti | p::JoinKind::LeftMark)
                        && at == 0
                    {
                        if let p::NodeKind::HashJoin { condition, .. } = &mut node.kind {
                            let p::HashJoinCondition::Ordinary { keys, residual } =
                                condition.clone()
                            else {
                                unreachable!()
                            };
                            *condition = p::HashJoinCondition::NullAwareEqual {
                                value_equal_keys: keys,
                                scope_keys: Box::default(),
                                eligibility: residual,
                            };
                        }
                        if let Some(wire::physical_node::Kind::HashJoin(v)) = wire.kind.as_mut() {
                            let Some(wire::hash_join_node::Condition::Ordinary(old)) =
                                v.condition.take()
                            else {
                                unreachable!()
                            };
                            v.condition = Some(wire::hash_join_node::Condition::NullAwareEqual(
                                wire::NullAwareEqualHashJoinCondition {
                                    value_equal_keys: old.keys,
                                    scope_keys: vec![],
                                    eligibility_root: old.residual_root,
                                },
                            ));
                        }
                    }
                    c.arm(None);
                    assert_eq!(
                        encode_relational_node(&node, values, expressions, SOURCE, limits())
                            .unwrap()
                            .0,
                        wire
                    );
                    c.arm(None);
                    assert_eq!(
                        decode_relational_node(
                            &wire,
                            read,
                            functions,
                            aggregates,
                            binding_limits(),
                            SOURCE,
                            limits()
                        )
                        .unwrap()
                        .0,
                        node
                    );
                }
            }
            for (side, raw) in [
                (p::JoinSide::Left, wire::JoinSide::Left),
                (p::JoinSide::Right, wire::JoinSide::Right),
            ] {
                let mut node = source(0);
                let mut wire = expected(0);
                if let p::NodeKind::HashJoin { build_side, .. } = &mut node.kind {
                    *build_side = side;
                }
                if let Some(wire::physical_node::Kind::HashJoin(v)) = wire.kind.as_mut() {
                    v.build_side = raw as i32;
                }
                c.arm(None);
                assert_eq!(
                    encode_relational_node(&node, values, expressions, SOURCE, limits())
                        .unwrap()
                        .0,
                    wire
                );
                c.arm(None);
                assert_eq!(
                    decode_relational_node(
                        &wire,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    )
                    .unwrap()
                    .0,
                    node
                );
            }
            for (distribution, raw) in [
                (
                    p::JoinDistribution::Colocated,
                    wire::JoinDistribution::Colocated,
                ),
                (
                    p::JoinDistribution::Partitioned,
                    wire::JoinDistribution::Partitioned,
                ),
                (
                    p::JoinDistribution::BroadcastBuild,
                    wire::JoinDistribution::BroadcastBuild,
                ),
                (
                    p::JoinDistribution::Singleton,
                    wire::JoinDistribution::Singleton,
                ),
            ] {
                let mut node = source(0);
                let mut wire = expected(0);
                if let p::NodeKind::HashJoin {
                    distribution: target,
                    ..
                } = &mut node.kind
                {
                    *target = distribution;
                }
                if let Some(wire::physical_node::Kind::HashJoin(v)) = wire.kind.as_mut() {
                    v.distribution = raw as i32;
                }
                c.arm(None);
                assert_eq!(
                    encode_relational_node(&node, values, expressions, SOURCE, limits())
                        .unwrap()
                        .0,
                    wire
                );
                c.arm(None);
                assert_eq!(
                    decode_relational_node(
                        &wire,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    )
                    .unwrap()
                    .0,
                    node
                );
            }
            for (distribution, raw) in [
                (
                    p::NestLoopJoinDistribution::Singleton,
                    wire::NestLoopJoinDistribution::Singleton,
                ),
                (
                    p::NestLoopJoinDistribution::BroadcastRight,
                    wire::NestLoopJoinDistribution::BroadcastRight,
                ),
            ] {
                let mut node = source(1);
                let mut wire = expected(1);
                if let p::NodeKind::NestLoopJoin {
                    distribution: target,
                    ..
                } = &mut node.kind
                {
                    *target = distribution;
                }
                if let Some(wire::physical_node::Kind::NestLoopJoin(v)) = wire.kind.as_mut() {
                    v.distribution = raw as i32;
                }
                c.arm(None);
                assert_eq!(
                    encode_relational_node(&node, values, expressions, SOURCE, limits())
                        .unwrap()
                        .0,
                    wire
                );
                c.arm(None);
                assert_eq!(
                    decode_relational_node(
                        &wire,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    )
                    .unwrap()
                    .0,
                    node
                );
            }
            for (kind, raw) in [
                (
                    p::SetOperationKind::UnionAll,
                    wire::SetOperationKind::UnionAll,
                ),
                (
                    p::SetOperationKind::Intersect,
                    wire::SetOperationKind::Intersect,
                ),
                (p::SetOperationKind::Except, wire::SetOperationKind::Except),
            ] {
                let mut node = source(6);
                let mut wire = expected(6);
                if let p::NodeKind::SetOp { kind: target, .. } = &mut node.kind {
                    *target = kind;
                }
                if let Some(wire::physical_node::Kind::SetOperation(v)) = wire.kind.as_mut() {
                    v.kind = raw as i32;
                }
                c.arm(None);
                assert_eq!(
                    encode_relational_node(&node, values, expressions, SOURCE, limits())
                        .unwrap()
                        .0,
                    wire
                );
                c.arm(None);
                assert_eq!(
                    decode_relational_node(
                        &wire,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    )
                    .unwrap()
                    .0,
                    node
                );
            }
            for (kind, raw) in [
                (
                    p::PartitionTopNType::RowNumber,
                    wire::PartitionTopNType::RowNumber,
                ),
                (p::PartitionTopNType::Rank, wire::PartitionTopNType::Rank),
                (
                    p::PartitionTopNType::DenseRank,
                    wire::PartitionTopNType::DenseRank,
                ),
            ] {
                for limit in [0, 1 << 63, u64::MAX] {
                    let mut node = source(4);
                    let mut wire = expected(4);
                    if let p::NodeKind::Sort {
                        mode:
                            p::SortMode::PartitionTopN {
                                kind: target,
                                limit: target_limit,
                                ..
                            },
                        ..
                    } = &mut node.kind
                    {
                        *target = kind;
                        *target_limit = limit;
                    }
                    if let Some(wire::physical_node::Kind::Sort(v)) = wire.kind.as_mut()
                        && let Some(wire::sort_mode::Kind::PartitionTopN(top)) =
                            v.mode.as_mut().unwrap().kind.as_mut()
                    {
                        top.kind = raw as i32;
                        top.limit = limit;
                    }
                    c.arm(None);
                    assert_eq!(
                        encode_relational_node(&node, values, expressions, SOURCE, limits())
                            .unwrap()
                            .0,
                        wire
                    );
                    c.arm(None);
                    assert_eq!(
                        decode_relational_node(
                            &wire,
                            read,
                            functions,
                            aggregates,
                            binding_limits(),
                            SOURCE,
                            limits()
                        )
                        .unwrap()
                        .0,
                        node
                    );
                }
            }
        },
    );
}
fn ordinary(error: Error) -> bool {
    matches!(error, Error::InvalidShape(_) | Error::Properties(_))
}
#[test]
fn relational_unknown_enums_missing_required_payload_and_refs_fail() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            let mut cases = Vec::new();
            for bad in [0, i32::MAX] {
                for field in 0..3 {
                    let mut node = expected(0);
                    if let Some(wire::physical_node::Kind::HashJoin(v)) = node.kind.as_mut() {
                        match field {
                            0 => v.kind = bad,
                            1 => v.build_side = bad,
                            _ => v.distribution = bad,
                        }
                    }
                    cases.push(node);
                }
                let mut node = expected(1);
                if let Some(wire::physical_node::Kind::NestLoopJoin(v)) = node.kind.as_mut() {
                    v.distribution = bad;
                }
                cases.push(node);
                let mut node = expected(6);
                if let Some(wire::physical_node::Kind::SetOperation(v)) = node.kind.as_mut() {
                    v.kind = bad;
                }
                cases.push(node);
                let mut node = expected(4);
                if let Some(wire::physical_node::Kind::Sort(v)) = node.kind.as_mut()
                    && let Some(wire::sort_mode::Kind::PartitionTopN(top)) =
                        v.mode.as_mut().unwrap().kind.as_mut()
                {
                    top.kind = bad;
                }
                cases.push(node);
                for nulls in [false, true] {
                    let mut node = expected(2);
                    if let Some(wire::physical_node::Kind::Sort(v)) = node.kind.as_mut() {
                        if nulls {
                            v.order_by[0].null_ordering = bad;
                        } else {
                            v.order_by[0].direction = bad;
                        }
                    }
                    cases.push(node);
                }
            }
            for at in [0, 1, 2, 5, 6, 7] {
                let mut node = expected(at);
                match node.kind.as_mut() {
                    Some(wire::physical_node::Kind::HashJoin(v)) => {
                        ordinary_wire_keys_mut(v)[0].left_root = None
                    }
                    Some(wire::physical_node::Kind::NestLoopJoin(v)) => v.predicate_root = Some(99),
                    Some(wire::physical_node::Kind::Sort(v)) => v.order_by[0].root_ordinal = None,
                    Some(wire::physical_node::Kind::Window(v)) => v.calls[0].output_value_id = None,
                    Some(wire::physical_node::Kind::SetOperation(v)) => {
                        v.input_mappings[0].value_ids[0] = 99
                    }
                    Some(wire::physical_node::Kind::ExchangeSource(v)) => v.edge_id = None,
                    _ => unreachable!(),
                };
                cases.push(node);
            }
            let mut node = expected(2);
            if let Some(wire::physical_node::Kind::Sort(v)) = node.kind.as_mut() {
                v.mode = None;
            }
            cases.push(node);
            let mut node = expected(2);
            if let Some(wire::physical_node::Kind::Sort(v)) = node.kind.as_mut() {
                v.mode.as_mut().unwrap().kind = None;
            }
            cases.push(node);
            for source_missing in [true, false] {
                let mut node = expected(7);
                if let Some(wire::physical_node::Kind::ExchangeSource(v)) = node.kind.as_mut() {
                    if source_missing {
                        v.imports[0].source_value_id = None;
                    } else {
                        v.imports[0].destination_value_id = None;
                    }
                }
                cases.push(node);
            }
            let mut node = expected(0);
            node.output = None;
            cases.push(node);
            let mut node = expected(0);
            node.output_properties = None;
            cases.push(node);
            let mut node = expected(0);
            node.kind = None;
            cases.push(node);
            for node in cases {
                c.arm(None);
                assert!(ordinary(
                    decode_relational_node(
                        &node,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    )
                    .unwrap_err()
                ));
                assert!(!c.trace().is_empty());
            }
            for at in [0, 1, 2, 5, 6] {
                let mut node = source(at);
                match &mut node.kind {
                    p::NodeKind::HashJoin { condition, .. } => {
                        condition.keys_mut()[0].right = RootOrdinal::new(99)
                    }
                    p::NodeKind::NestLoopJoin { predicate, .. } => {
                        *predicate = Some(RootOrdinal::new(99))
                    }
                    p::NodeKind::Sort { order_by, .. } => order_by[0].expr = RootOrdinal::new(99),
                    p::NodeKind::Window(v) => v.calls[0].output = p::ValueId::new(99),
                    p::NodeKind::SetOp { input_mappings, .. } => {
                        input_mappings[0][0] = p::ValueId::new(99)
                    }
                    _ => unreachable!(),
                };
                if at == 5 {
                    let local = Control::default();
                    fixture.with_tokens(&node, &local, |values, expressions, _, _, _| {
                        local.arm(None);
                        assert!(ordinary(
                            encode_relational_node(&node, values, expressions, SOURCE, limits())
                                .unwrap_err()
                        ));
                    });
                } else {
                    c.arm(None);
                    assert!(ordinary(
                        encode_relational_node(&node, values, expressions, SOURCE, limits())
                            .unwrap_err()
                    ));
                }
            }
        },
    );
}
fn exact(facts: RelationalNodeProjectionFacts) -> RelationalNodeProjectionLimits {
    RelationalNodeProjectionLimits {
        max_input_nodes: facts.input_node_count,
        max_value_references: facts.value_reference_count,
        max_list_items: facts.list_item_count,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
        ..limits()
    }
}
fn under(mut limits: RelationalNodeProjectionLimits, at: usize) -> RelationalNodeProjectionLimits {
    match at {
        0 => limits.max_input_nodes -= 1,
        1 => limits.max_value_references -= 1,
        2 => limits.max_list_items -= 1,
        3 => limits.max_allocation_requests -= 1,
        4 => limits.max_allocation_request_bytes -= 1,
        5 => limits.max_coexisting_source_and_request_bytes -= 1,
        6 => limits.max_work -= 1,
        _ => unreachable!(),
    };
    limits
}
#[test]
fn relational_six_kinds_both_directions_require_all_seven_caps_and_actual_capacities() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            for at in 0..8 {
                c.arm(None);
                let (_, facts) = encode_relational_node(
                    source_ref(at, &window, &source(at)),
                    values,
                    expressions,
                    SOURCE,
                    limits(),
                )
                .unwrap();
                let cap = exact(facts);
                c.arm(None);
                assert!(
                    encode_relational_node(
                        source_ref(at, &window, &source(at)),
                        values,
                        expressions,
                        SOURCE,
                        cap
                    )
                    .is_ok()
                );
                for axis in 0..7 {
                    c.arm(None);
                    assert!(matches!(
                        encode_relational_node(
                            source_ref(at, &window, &source(at)),
                            values,
                            expressions,
                            SOURCE,
                            under(cap, axis)
                        ),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                }
                c.arm(None);
                let (_, facts) = decode_relational_node(
                    &expected(at),
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits(),
                )
                .unwrap();
                let cap = exact(facts);
                c.arm(None);
                assert!(
                    decode_relational_node(
                        &expected(at),
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        cap
                    )
                    .is_ok()
                );
                for axis in 0..7 {
                    c.arm(None);
                    assert!(matches!(
                        decode_relational_node(
                            &expected(at),
                            read,
                            functions,
                            aggregates,
                            binding_limits(),
                            SOURCE,
                            under(cap, axis)
                        ),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                }
            }
            for at in [0, 3, 5, 6, 7] {
                let mut node = expected(at);
                match node.kind.as_mut() {
                    Some(wire::physical_node::Kind::HashJoin(v)) => {
                        ordinary_wire_keys_mut(v).reserve_exact(SOURCE)
                    }
                    Some(wire::physical_node::Kind::Sort(v)) => {
                        if let Some(wire::sort_mode::Kind::Analytic(partition)) =
                            v.mode.as_mut().unwrap().kind.as_mut()
                        {
                            partition.partition_by.reserve_exact(SOURCE);
                        }
                    }
                    Some(wire::physical_node::Kind::Window(v)) => v.calls.reserve_exact(SOURCE),
                    Some(wire::physical_node::Kind::SetOperation(v)) => {
                        v.input_mappings[0].value_ids.reserve_exact(SOURCE)
                    }
                    Some(wire::physical_node::Kind::ExchangeSource(v)) => {
                        v.imports.reserve_exact(SOURCE)
                    }
                    _ => unreachable!(),
                };
                c.arm(None);
                assert!(matches!(
                    prepare_relational_node_decode(
                        &node,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    ),
                    Err(Error::InvalidShape(_))
                ));
            }
            c.arm(None);
            assert!(matches!(
                encode_relational_node(&source(0), values, expressions, 0, limits()),
                Err(Error::InvalidShape(_))
            ));
        },
    );
}
fn control_case(
    decode: bool,
    ordinary: bool,
    at: usize,
    stop: Option<(usize, CompileControlError)>,
) -> (Result<(), Error>, Vec<(CompilePhase, u32)>) {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    let result = fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            c.arm(stop);
            if decode {
                let mut node = expected(at);
                if ordinary {
                    node.output.as_mut().unwrap().value_ids[0] = 99;
                }
                decode_relational_node(
                    &node,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits(),
                )
                .map(|_| ())
            } else {
                let mut node = source(at);
                if ordinary {
                    node.output.columns[0] = p::ValueId::new(99);
                }
                encode_relational_node(
                    if ordinary {
                        &node
                    } else {
                        source_ref(at, &window, &node)
                    },
                    values,
                    expressions,
                    SOURCE,
                    limits(),
                )
                .map(|_| ())
            }
        },
    );
    (result, c.trace())
}
#[test]
fn relational_original_control_prefixes_and_nested_work_bound_are_not_replayed() {
    for decode in [false, true] {
        for fail in [false, true] {
            for at in [0, 1, 2, 5, 6, 7] {
                let (result, trace) = control_case(decode, fail, at, None);
                assert_eq!(result.is_err(), fail);
                assert_eq!(trace[0].1, 0);
                assert!(trace.last().is_some_and(|(_, units)| *units > 0));
                for stop in 0..trace.len() {
                    for cause in CAUSES {
                        let (result, actual) =
                            control_case(decode, fail, at, Some((stop, cause.clone())));
                        assert!(matches!(result,Err(Error::Control(actual))if actual==cause));
                        assert_eq!(actual, trace[..=stop]);
                    }
                }
            }
        }
    }
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(&window, &c,|values,expressions,read,functions,aggregates|{
        let mut node=source(6);if let p::NodeKind::SetOp{input_mappings,..}=&mut node.kind{*input_mappings=vec![vec![p::ValueId::new(7);320].into_boxed_slice()].into_boxed_slice();}let low=RelationalNodeProjectionLimits{max_work:1024,..limits()};c.arm(None);assert!(matches!(prepare_relational_node_encode(&node,values,expressions,SOURCE,low),Err(Error::Control(CompileControlError::ResourceExhausted))));assert!(!c.trace().iter().any(|(_,units)|*units==256));
        let mut wire=expected(6);if let Some(wire::physical_node::Kind::SetOperation(v))=wire.kind.as_mut(){v.input_mappings=vec![wire::ValueIds{value_ids:vec![7;320]}];}c.arm(None);assert!(matches!(prepare_relational_node_decode(&wire, read, functions, aggregates, binding_limits(), SOURCE, low),Err(Error::Control(CompileControlError::ResourceExhausted))));assert!(!c.trace().iter().any(|(_,units)|*units==256));
        if let p::NodeKind::SetOp{input_mappings,..}=&mut node.kind{*input_mappings=vec![Box::<[p::ValueId]>::default();320].into_boxed_slice();}c.arm(None);let(wire,_)=encode_relational_node(&node,values,expressions,SOURCE,limits()).unwrap();let trace=c.trace();let at=trace.iter().position(|(_,units)|*units==256).unwrap();for cause in CAUSES{c.arm(Some((at,cause.clone())));assert!(matches!(encode_relational_node(&node,values,expressions,SOURCE,limits()),Err(Error::Control(actual))if actual==cause));assert_eq!(c.trace(),trace[..=at]);}
        c.arm(None);decode_relational_node(&wire, read, functions, aggregates, binding_limits(), SOURCE, limits()).unwrap();let trace=c.trace();let at=trace.iter().position(|(_,units)|*units==256).unwrap();for cause in CAUSES{c.arm(Some((at,cause.clone())));assert!(matches!(decode_relational_node(&wire, read, functions, aggregates, binding_limits(), SOURCE, limits()),Err(Error::Control(actual))if actual==cause));assert_eq!(c.trace(),trace[..=at]);}
    });
}
#[test]
fn relational_empty_payloads_and_optional_predicates_remain_representation_only() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            for at in [0, 1, 2, 3, 4, 5, 6, 7] {
                let mut node = source(at);
                match &mut node.kind {
                    p::NodeKind::HashJoin {
                        condition,
                        null_extended,
                        ..
                    } => {
                        *condition.keys_mut() = Box::default();
                        *condition.residual_mut() = None;
                        *null_extended = Box::default();
                    }
                    p::NodeKind::NestLoopJoin {
                        predicate,
                        null_extended,
                        ..
                    } => {
                        *predicate = None;
                        *null_extended = Box::default();
                    }
                    p::NodeKind::Sort { order_by, mode } => {
                        *order_by = Box::default();
                        match mode {
                            p::SortMode::Global => (),
                            p::SortMode::Analytic { partition_by }
                            | p::SortMode::PartitionTopN { partition_by, .. } => {
                                *partition_by = Box::default()
                            }
                        }
                    }
                    p::NodeKind::Window(v) => {
                        v.partition_by = Box::default();
                        v.order_by = Box::default();
                        v.calls = Box::default();
                    }
                    p::NodeKind::SetOp { input_mappings, .. } => {
                        *input_mappings = vec![
                            Box::<[p::ValueId]>::default(),
                            vec![p::ValueId::new(7)].into_boxed_slice(),
                        ]
                        .into_boxed_slice()
                    }
                    p::NodeKind::ExchangeSource { imports, edge } => {
                        *imports = Box::default();
                        *edge = p::EdgeId::new(0);
                    }
                    _ => unreachable!(),
                };
                c.arm(None);
                let (wire, _) =
                    encode_relational_node(&node, values, expressions, SOURCE, limits()).unwrap();
                c.arm(None);
                assert_eq!(
                    decode_relational_node(
                        &wire,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    )
                    .unwrap()
                    .0,
                    node
                );
            }
            // Fragment still proves join-policy/input visibility, window functions,
            // SetOp widths/domains and exchange edge/source ValueOrigin agreement.
        },
    );
}

#[path = "owned_tests.rs"]
mod owned_tests;

fn naa_source() -> p::PhysicalNode {
    let mut node = source(0);
    let p::NodeKind::HashJoin {
        kind, condition, ..
    } = &mut node.kind
    else {
        unreachable!()
    };
    let p::HashJoinCondition::Ordinary { keys, residual } = condition.clone() else {
        unreachable!()
    };
    *kind = p::JoinKind::NullAwareLeftAnti;
    *condition = p::HashJoinCondition::NullAwareEqual {
        scope_keys: Box::from([keys[1].clone()]),
        value_equal_keys: keys,
        eligibility: residual,
    };
    node
}

#[test]
fn float_join_match_declaration_roundtrips_and_unknown_wire_values_are_rejected() {
    let c = Control::default();
    let fixture = Fixture::new();
    fixture.with_tokens(
        &source(5),
        &c,
        |values, expressions, read, functions, aggregates| {
            let mut original = source(0);
            let p::NodeKind::HashJoin {
                condition: p::HashJoinCondition::Ordinary { keys, .. },
                ..
            } = &mut original.kind
            else {
                unreachable!()
            };
            keys[0].match_relation = novarocks_type_contract::JoinKeyMatch::IeeeFloat;
            let (mut encoded, _) =
                encode_relational_node(&original, values, expressions, SOURCE, limits()).unwrap();
            let decode = |node: &wire::PhysicalNode| {
                decode_relational_node(
                    node,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits(),
                )
            };
            assert_eq!(decode(&encoded).unwrap().0, original);
            for invalid in [0, i32::MAX] {
                let Some(wire::physical_node::Kind::HashJoin(join)) = &mut encoded.kind else {
                    unreachable!()
                };
                ordinary_wire_keys_mut(join)[0].match_relation = invalid;
                assert!(decode(&encoded).is_err());
            }
            let Some(wire::physical_node::Kind::HashJoin(join)) = &mut encoded.kind else {
                unreachable!()
            };
            let key = &mut ordinary_wire_keys_mut(join)[0];
            key.match_relation = wire::JoinKeyMatch::IeeeFloat as i32;
            key.null_safe = true;
            assert!(decode(&encoded).is_err());
        },
    );
}

#[test]
fn null_aware_condition_roundtrip_preserves_independent_key_domains_and_resources() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            let node = naa_source();
            c.arm(None);
            let (wire_node, encode_facts) =
                encode_relational_node(&node, values, expressions, SOURCE, limits()).unwrap();
            let Some(wire::physical_node::Kind::HashJoin(join)) = wire_node.kind.as_ref() else {
                unreachable!()
            };
            let Some(wire::hash_join_node::Condition::NullAwareEqual(condition)) =
                join.condition.as_ref()
            else {
                unreachable!()
            };
            assert_eq!(condition.value_equal_keys.len(), 2);
            assert_eq!(condition.scope_keys.len(), 1);
            assert_eq!(condition.eligibility_root, Some(0));
            c.arm(None);
            let (decoded, decode_facts) = decode_relational_node(
                &wire_node,
                read,
                functions,
                aggregates,
                binding_limits(),
                SOURCE,
                limits(),
            )
            .unwrap();
            assert_eq!(decoded, node);
            for axis in 0..7 {
                c.arm(None);
                assert!(matches!(
                    encode_relational_node(
                        &node,
                        values,
                        expressions,
                        SOURCE,
                        under(exact(encode_facts), axis)
                    ),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                c.arm(None);
                assert!(matches!(
                    decode_relational_node(
                        &wire_node,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        under(exact(decode_facts), axis)
                    ),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
            }
        },
    );
}

#[test]
fn null_aware_condition_missing_empty_or_mixed_roles_are_rejected_without_defaults() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            c.arm(None);
            let (wire_node, _) =
                encode_relational_node(&naa_source(), values, expressions, SOURCE, limits())
                    .unwrap();
            for fault in 0..6 {
                let mut broken = wire_node.clone();
                let Some(wire::physical_node::Kind::HashJoin(join)) = broken.kind.as_mut() else {
                    unreachable!()
                };
                match fault {
                    0 => join.condition = None,
                    1 => join.kind = wire::JoinKind::Inner as i32,
                    2 => {
                        join.condition = Some(wire::hash_join_node::Condition::Ordinary(
                            wire::OrdinaryHashJoinCondition {
                                keys: vec![],
                                residual_root: None,
                            },
                        ))
                    }
                    3 => {
                        let Some(wire::hash_join_node::Condition::NullAwareEqual(condition)) =
                            join.condition.as_mut()
                        else {
                            unreachable!()
                        };
                        condition.value_equal_keys.clear();
                    }
                    4 => {
                        let Some(wire::hash_join_node::Condition::NullAwareEqual(condition)) =
                            join.condition.as_mut()
                        else {
                            unreachable!()
                        };
                        condition.scope_keys[0].left_root = Some(99);
                    }
                    _ => {
                        let Some(wire::hash_join_node::Condition::NullAwareEqual(condition)) =
                            join.condition.as_mut()
                        else {
                            unreachable!()
                        };
                        condition.eligibility_root = Some(99);
                    }
                }
                c.arm(None);
                assert!(matches!(
                    decode_relational_node(
                        &broken,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    ),
                    Err(Error::InvalidShape(_))
                ));
            }
            let mut broken = naa_source();
            let p::NodeKind::HashJoin { condition, .. } = &mut broken.kind else {
                unreachable!()
            };
            *condition.keys_mut() = Box::default();
            c.arm(None);
            assert!(matches!(
                encode_relational_node(&broken, values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        },
    );
}

#[test]
fn null_aware_scope_capacity_and_complete_nested_loop_predicate_are_accounted() {
    let fixture = Fixture::new();
    let c = Control::default();
    let window = source(5);
    fixture.with_tokens(
        &window,
        &c,
        |values, expressions, read, functions, aggregates| {
            c.arm(None);
            let (mut wire_node, _) =
                encode_relational_node(&naa_source(), values, expressions, SOURCE, limits())
                    .unwrap();
            c.arm(None);
            let (_, before) = decode_relational_node(
                &wire_node,
                read,
                functions,
                aggregates,
                binding_limits(),
                SOURCE,
                limits(),
            )
            .unwrap();
            let Some(wire::physical_node::Kind::HashJoin(join)) = wire_node.kind.as_mut() else {
                unreachable!()
            };
            let Some(wire::hash_join_node::Condition::NullAwareEqual(condition)) =
                join.condition.as_mut()
            else {
                unreachable!()
            };
            condition
                .scope_keys
                .reserve_exact(SOURCE / std::mem::size_of::<wire::JoinKey>() + 1);
            c.arm(None);
            assert!(matches!(
                decode_relational_node(
                    &wire_node,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    exact(before)
                ),
                Err(Error::InvalidShape(_))
            ));
            // Capacity belongs to the original source invoice. Supplying its
            // actual larger backing must also satisfy the original coexistence
            // author; it is not another allocation request by the decoder.
            c.arm(None);
            assert!(matches!(
                decode_relational_node(
                    &wire_node,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    2 * SOURCE,
                    exact(before)
                ),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            c.arm(None);
            assert!(
                decode_relational_node(
                    &wire_node,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    2 * SOURCE,
                    limits()
                )
                .is_ok()
            );
            let mut wire_node = expected(1);
            let Some(wire::physical_node::Kind::NestLoopJoin(join)) = wire_node.kind.as_mut()
            else {
                unreachable!()
            };
            join.kind = wire::JoinKind::NullAwareLeftAnti as i32;
            join.predicate_root = None;
            c.arm(None);
            assert!(matches!(
                decode_relational_node(
                    &wire_node,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits()
                ),
                Err(Error::InvalidShape(_))
            ));
            let mut node = source(1);
            let p::NodeKind::NestLoopJoin {
                kind, predicate, ..
            } = &mut node.kind
            else {
                unreachable!()
            };
            *kind = p::JoinKind::NullAwareLeftAnti;
            *predicate = None;
            c.arm(None);
            assert!(matches!(
                encode_relational_node(&node, values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        },
    );
}

#[test]
fn nested_loop_left_mark_roundtrips_the_complete_predicate() {
    let fixture = Fixture::new();
    let control = Control::default();
    let window = source(5);
    let mut node = source(1);
    node.kind = p::NodeKind::NestLoopJoin {
        kind: p::JoinKind::LeftMark,
        distribution: p::NestLoopJoinDistribution::Singleton,
        predicate: Some(RootOrdinal::new(2)),
        null_extended: Box::default(),
    };
    fixture.with_tokens(
        &window,
        &control,
        |values, expressions, read, functions, aggregates| {
            let (encoded, _) =
                encode_relational_node(&node, values, expressions, SOURCE, limits()).unwrap();
            let decode = |input| {
                decode_relational_node(
                    input,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits(),
                )
            };
            assert_eq!(decode(&encoded).unwrap().0, node);
            let mut missing_predicate = encoded.clone();
            let Some(wire::physical_node::Kind::NestLoopJoin(join)) =
                missing_predicate.kind.as_mut()
            else {
                unreachable!()
            };
            assert_eq!(join.kind, wire::JoinKind::LeftMark as i32);
            assert_eq!(
                join.distribution,
                wire::NestLoopJoinDistribution::Singleton as i32
            );
            assert_eq!(join.predicate_root, Some(2));
            join.predicate_root = None;
            assert!(matches!(
                decode(&missing_predicate),
                Err(Error::InvalidShape(_))
            ));
            let mut incomplete = node.clone();
            let p::NodeKind::NestLoopJoin { predicate, .. } = &mut incomplete.kind else {
                unreachable!()
            };
            *predicate = None;
            assert!(matches!(
                encode_relational_node(&incomplete, values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        },
    );
}

#[test]
fn relational_window_calls_keep_actual_order_frame_addresses_and_optional_aggregate() {
    use novarocks_type_contract::{
        AggregateStateArgumentContract, AggregateStateFormatId, FunctionArgumentType, FunctionKind,
        TypedFactSlice,
    };
    let fixture = Fixture::new();
    let mut node = source(5);
    let p::NodeKind::Window(spec) = &mut node.kind else {
        unreachable!()
    };
    let call = &mut spec.calls[0];
    call.distinct = true;
    call.ignore_nulls = true;
    call.arguments = Box::from([
        RootOrdinal::new(2),
        RootOrdinal::new(0),
        RootOrdinal::new(2),
    ]);
    call.binding.argument_types = TypedFactSlice::from_pure_boxed(
        vec![FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, true)); 3]
            .into_boxed_slice(),
    );
    call.order_by = Box::from([
        sort(0, p::SortDirection::Descending, p::NullOrdering::First),
        sort(2, p::SortDirection::Ascending, p::NullOrdering::Last),
    ]);
    // Preserve the original source addresses; Fragment owns frame-value semantics.
    call.frame = Some(p::WindowFrame {
        units: p::WindowFrameUnits::Groups,
        start: p::WindowBound::Preceding(reference(u32::MAX, 1)),
        end: p::WindowBound::Following(reference(u32::MAX, 2)),
        exclusion: p::WindowFrameExclusion::Ties,
    });
    let mut function = call.binding.clone();
    function.kind = FunctionKind::Aggregate;
    call.aggregate_binding = Some(Box::new(p::AggregateBinding {
        function,
        phase: p::AggregatePhase::Partial {
            sequence: p::AggregateSequenceId::new(u32::MAX),
        },
        logical_argument_count: 3,
        merge_state: Some(novarocks_physical_plan::AggregateMergeBinding {
            intermediate_type: FunctionValueType::new(DataType::Int64, true),
            state_argument_contract: AggregateStateArgumentContract::ExactSignature,
            state_format: AggregateStateFormatId::try_new("test.expression/state-v1").unwrap(),
            state_interpretation: None,
        }),
    }));
    let c = Control::default();
    fixture.with_tokens(
        &node,
        &c,
        |values, expressions, read, functions, aggregates| {
            c.arm(None);
            let (sent, _) =
                encode_relational_node(&node, values, expressions, SOURCE, limits()).unwrap();
            let wire::physical_node::Kind::Window(payload) = sent.kind.as_ref().unwrap() else {
                unreachable!()
            };
            let call = &payload.calls[0];
            assert_eq!(call.function_binding_id, Some(0));
            assert_eq!(call.aggregate_binding_id, Some(u32::MAX));
            assert!(call.distinct && call.ignore_nulls);
            assert_eq!(call.argument_roots, [2, 0, 2]);
            assert_eq!(call.output_value_id, Some(7));
            assert_eq!(
                call.order_by,
                vec![
                    ws(
                        0,
                        wire::SortDirection::Descending,
                        wire::NullOrdering::First
                    ),
                    ws(2, wire::SortDirection::Ascending, wire::NullOrdering::Last),
                ]
            );
            assert_eq!(
                call.frame,
                Some(wire::WindowFrame {
                    units: wire::WindowFrameUnits::Groups as i32,
                    start: Some(wire::WindowBound {
                        kind: Some(wire::window_bound::Kind::Preceding(
                            wire::ConstantReference {
                                pool_id: Some(u32::MAX),
                                row_ordinal: 1
                            }
                        ))
                    }),
                    end: Some(wire::WindowBound {
                        kind: Some(wire::window_bound::Kind::Following(
                            wire::ConstantReference {
                                pool_id: Some(u32::MAX),
                                row_ordinal: 2
                            }
                        ))
                    }),
                    exclusion: wire::WindowFrameExclusion::Ties as i32,
                })
            );
            assert!(payload.calls[1].aggregate_binding_id.is_none());
            assert!(payload.calls[1].frame.is_none());
            assert!(std::ptr::eq(expressions.pools(), &fixture.pools));
            assert!(std::ptr::eq(read.pools(), &fixture.pools));
            let source = fixture.pools.entries()[&p::ConstantPoolId::new(u32::MAX)]
                .array()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(source.value(1), -7);
            assert!(source.is_null(2));
            c.arm(None);
            let (received, _) = decode_relational_node(
                &sent,
                read,
                functions,
                aggregates,
                binding_limits(),
                SOURCE,
                limits(),
            )
            .unwrap();
            assert_eq!(received, node);
            for missing in [None, Some(99)] {
                let mut broken = sent.clone();
                let wire::physical_node::Kind::Window(payload) = broken.kind.as_mut().unwrap()
                else {
                    unreachable!()
                };
                payload.calls[0].function_binding_id = missing;
                c.arm(None);
                assert!(matches!(
                    decode_relational_node(
                        &broken,
                        read,
                        functions,
                        aggregates,
                        binding_limits(),
                        SOURCE,
                        limits()
                    ),
                    Err(Error::InvalidShape(_))
                ));
            }
            let mut broken = sent.clone();
            let wire::physical_node::Kind::Window(payload) = broken.kind.as_mut().unwrap() else {
                unreachable!()
            };
            payload.calls[0].aggregate_binding_id = Some(99);
            c.arm(None);
            assert!(matches!(
                decode_relational_node(
                    &broken,
                    read,
                    functions,
                    aggregates,
                    binding_limits(),
                    SOURCE,
                    limits()
                ),
                Err(Error::InvalidShape(_))
            ));
            // Equal signatures in another node are not this original emission.
            let foreign = node.clone();
            c.arm(None);
            assert!(matches!(
                encode_relational_node(&foreign, values, expressions, SOURCE, limits()),
                Err(Error::Binding(
                    crate::physical_binding_v2::BindingCodecError::InvalidShape(_)
                ))
            ));
        },
    );
}

#[test]
fn relational_window_binding_projection_rejects_foreign_complete_result_type() {
    let node = source(5);
    let p::NodeKind::Window(spec) = &node.kind else {
        unreachable!()
    };
    let c = Control::default();
    let roots = vec![
        (0, FunctionValueType::new(DataType::Int64, true)),
        (1, FunctionValueType::new(DataType::Int64, false)),
    ];
    let types = encode_type_table_sources(&roots, &[], types_limits(), &c).unwrap();
    let arguments = [crate::physical_binding_v2::ArgumentTypeIds::Value(0)];
    let input = [crate::physical_binding_v2::FunctionBindingInput {
        id: 0,
        source: crate::physical_binding_v2::BindingSource::Scalar(&spec.calls[0].binding),
        arguments: &arguments,
        result: crate::physical_binding_v2::ResultTypeIds::Scalar(1),
    }];
    c.arm(None);
    assert!(matches!(
        encode_function_bindings(&types, &input, 64 << 10, binding_limits(), &c),
        Err(crate::physical_binding_v2::BindingCodecError::InvalidShape(
            _
        ))
    ));
}
