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
use arrow::datatypes::{DataType, Field};
use novarocks_proto_models::physical_control_v2::Empty;
use novarocks_type_contract::{
    AggregateStateArgumentContract, AggregateStateFormatId, CompileControlError,
    FunctionArgumentType, FunctionId, FunctionKind, FunctionOverloadId, FunctionValueType,
    OrderedComparisonAlgorithm, PureCompileControl, SemanticParameters, ValueLogicalType,
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
fn limits() -> TopNNodeProjectionLimits {
    TopNNodeProjectionLimits {
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
        max_definitions: 0,
        max_payload_bytes: 0,
        max_allocation_requests: 0,
        max_allocation_request_bytes: 0,
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
fn node() -> p::PhysicalNode {
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
        kind: p::NodeKind::TopN {
            order_by: keys(),
            limit: u64::MAX,
            offset: 0,
            phase: p::TopNPhase::Partial {
                sequence: p::TopNSequenceId::new(0),
            },
            reduction: p::TopNReduction::GroupedStates {
                group_by: Box::from([
                    (p::ExprId::new(u32::MAX), p::ValueId::new(0)),
                    (p::ExprId::new(0), p::ValueId::new(u32::MAX)),
                    (p::ExprId::new(u32::MAX), p::ValueId::new(0)),
                ]),
                calls: Box::from([
                    p::AggregateCall {
                        id: p::AggregateCallId::new(0),
                        binding: aggregate(p::AggregatePhase::Intermediate {
                            sequence: p::AggregateSequenceId::new(0),
                        }),
                        arguments: Box::from([
                            p::ExprId::new(0),
                            p::ExprId::new(u32::MAX),
                            p::ExprId::new(0),
                        ]),
                        distinct: true,
                        order_by: Box::from([
                            p::SortExpr {
                                expr: p::ExprId::new(u32::MAX),
                                direction: p::SortDirection::Descending,
                                null_ordering: p::NullOrdering::First,
                            },
                            p::SortExpr {
                                expr: p::ExprId::new(0),
                                direction: p::SortDirection::Ascending,
                                null_ordering: p::NullOrdering::Last,
                            },
                        ]),
                        output: p::ValueId::new(u32::MAX),
                    },
                    p::AggregateCall {
                        id: p::AggregateCallId::new(u32::MAX),
                        binding: aggregate(p::AggregatePhase::Intermediate {
                            sequence: p::AggregateSequenceId::new(u32::MAX),
                        }),
                        arguments: Box::default(),
                        distinct: false,
                        order_by: Box::default(),
                        output: p::ValueId::new(0),
                    },
                ]),
                comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
            },
        },
    }
}
fn keys() -> Box<[p::SortExpr]> {
    Box::from([
        p::SortExpr {
            expr: p::ExprId::new(u32::MAX),
            direction: p::SortDirection::Descending,
            null_ordering: p::NullOrdering::First,
        },
        p::SortExpr {
            expr: p::ExprId::new(0),
            direction: p::SortDirection::Ascending,
            null_ordering: p::NullOrdering::Last,
        },
        p::SortExpr {
            expr: p::ExprId::new(u32::MAX),
            direction: p::SortDirection::Descending,
            null_ordering: p::NullOrdering::Last,
        },
    ])
}
fn calls(n: &mut p::PhysicalNode) -> &mut Box<[p::AggregateCall]> {
    match &mut n.kind {
        p::NodeKind::TopN {
            reduction: p::TopNReduction::GroupedStates { calls, .. },
            ..
        } => calls,
        _ => unreachable!(),
    }
}
fn rows() -> p::PhysicalNode {
    let mut n = node();
    if let p::NodeKind::TopN { reduction, .. } = &mut n.kind {
        *reduction = p::TopNReduction::Rows;
    }
    n
}
fn body(n: &mut wire::PhysicalNode) -> &mut wire::TopNNode {
    match n.kind.as_mut() {
        Some(wire::physical_node::Kind::TopN(v)) => v,
        _ => unreachable!(),
    }
}
fn grouped(n: &mut wire::PhysicalNode) -> &mut wire::GroupedStates {
    match body(n).reduction.as_mut().unwrap().kind.as_mut().unwrap() {
        wire::top_n_reduction::Kind::GroupedStates(g) => g,
        _ => unreachable!(),
    }
}
fn expected_keys() -> Vec<wire::SortExpression> {
    vec![
        wire::SortExpression {
            expr_id: Some(u32::MAX),
            direction: wire::SortDirection::Descending as i32,
            null_ordering: wire::NullOrdering::First as i32,
        },
        wire::SortExpression {
            expr_id: Some(0),
            direction: wire::SortDirection::Ascending as i32,
            null_ordering: wire::NullOrdering::Last as i32,
        },
        wire::SortExpression {
            expr_id: Some(u32::MAX),
            direction: wire::SortDirection::Descending as i32,
            null_ordering: wire::NullOrdering::Last as i32,
        },
    ]
}
fn expected() -> wire::PhysicalNode {
    wire::PhysicalNode {
        id: u32::MAX,
        input_node_ids: vec![0],
        required_inputs: vec![wp(true)],
        output_properties: Some(wp(false)),
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![0, u32::MAX, 0],
        }),
        kind: Some(wire::physical_node::Kind::TopN(wire::TopNNode {
            order_by: expected_keys(),
            limit: u64::MAX,
            offset: 0,
            phase: Some(wire::TopNPhase {
                kind: Some(wire::top_n_phase::Kind::PartialSequenceId(0)),
            }),
            reduction: Some(wire::TopNReduction {
                kind: Some(wire::top_n_reduction::Kind::GroupedStates(
                    wire::GroupedStates {
                        group_by: vec![
                            wire::ExpressionOutput {
                                expr_id: Some(u32::MAX),
                                value_id: Some(0),
                            },
                            wire::ExpressionOutput {
                                expr_id: Some(0),
                                value_id: Some(u32::MAX),
                            },
                            wire::ExpressionOutput {
                                expr_id: Some(u32::MAX),
                                value_id: Some(0),
                            },
                        ],
                        calls: vec![
                            wire::AggregateCall {
                                id: 0,
                                aggregate_binding_id: Some(0),
                                argument_expr_ids: vec![0, u32::MAX, 0],
                                distinct: true,
                                order_by: vec![
                                    wire::SortExpression {
                                        expr_id: Some(u32::MAX),
                                        direction: wire::SortDirection::Descending as i32,
                                        null_ordering: wire::NullOrdering::First as i32,
                                    },
                                    wire::SortExpression {
                                        expr_id: Some(0),
                                        direction: wire::SortDirection::Ascending as i32,
                                        null_ordering: wire::NullOrdering::Last as i32,
                                    },
                                ],
                                output_value_id: Some(u32::MAX),
                            },
                            wire::AggregateCall {
                                id: u32::MAX,
                                aggregate_binding_id: Some(u32::MAX),
                                argument_expr_ids: vec![],
                                distinct: false,
                                order_by: vec![],
                                output_value_id: Some(0),
                            },
                        ],
                        comparator: wire::OrderedComparisonAlgorithm::NativeScalarOrderV1 as i32,
                    },
                )),
            }),
        })),
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
        let values = [0, u32::MAX]
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
        let arena = p::ExprArena::try_from_definitions_observed(
            [0, u32::MAX].into_iter().map(|id| p::ExprNode {
                id: p::ExprId::new(id),
                owner: p::NodeId::new(u32::MAX),
                lambda_scope: None,
                ty: ty.clone(),
                kind: p::ExprKind::Value(p::ValueId::new(id)),
            }),
            &p::PlanLimits::default(),
            &Control::default(),
        )
        .unwrap();
        Self {
            roots: vec![(0, ty), (1, dictionary()), (2, nested()), (3, nominal())],
            values,
            arena,
            parameters: SemanticParameters::try_new([]).unwrap(),
            pools: p::ConstantPools::empty(),
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
        let payloads = encode_connector_payloads(&[], 16 << 10, pl(), c).unwrap();
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
        let (_, _, _, _, reduction) = physical(n).unwrap();
        let (_, calls) = physical_collections(reduction);
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
fn exact(mut l: TopNNodeProjectionLimits, f: TopNNodeProjectionFacts) -> TopNNodeProjectionLimits {
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
fn under(l: &mut TopNNodeProjectionLimits, axis: usize) {
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

#[test]
fn topn_rows_three_phases_preserve_header_ordered_keys_and_raw_bounds() {
    for (phase, expected_phase) in [
        (
            p::TopNPhase::Single,
            wire::top_n_phase::Kind::Single(Empty {}),
        ),
        (
            p::TopNPhase::Partial {
                sequence: p::TopNSequenceId::new(0),
            },
            wire::top_n_phase::Kind::PartialSequenceId(0),
        ),
        (
            p::TopNPhase::Final {
                sequence: p::TopNSequenceId::new(u32::MAX),
            },
            wire::top_n_phase::Kind::FinalSequenceId(u32::MAX),
        ),
    ] {
        for (limit, offset) in [(0, 0), (u64::MAX, 0), (0, u64::MAX), (u64::MAX, u64::MAX)] {
            let mut n = rows();
            if let p::NodeKind::TopN {
                phase: p,
                limit: l,
                offset: o,
                ..
            } = &mut n.kind
            {
                *p = phase;
                *l = limit;
                *o = offset;
            }
            let mut golden = expected();
            let b = body(&mut golden);
            b.phase = Some(wire::TopNPhase {
                kind: Some(expected_phase),
            });
            b.limit = limit;
            b.offset = offset;
            b.reduction = Some(wire::TopNReduction {
                kind: Some(wire::top_n_reduction::Kind::Rows(Empty {})),
            });
            Fixture::new().with(&n, &Control::default(), |v, e, d, a| {
                let (actual, facts) = encode_topn_node(&n, v, e, SOURCE, limits()).unwrap();
                assert_eq!(actual, golden);
                assert_eq!(facts.allocation_requests_upper_bound, 4);
                let (owned, facts) = decode_topn_node(&golden, d, a, SOURCE, limits()).unwrap();
                assert_eq!(owned, n);
                assert_eq!(facts.allocation_requests_upper_bound, 8);
            });
        }
    }
    // These include limit+offset overflow and nonzero Partial offset. This is
    // complete representation; original mandatory Fragment semantics reject
    // those combinations rather than this projection inventing a policy.
}

#[test]
fn topn_grouped_states_preserve_complete_independent_wire_and_owned_payloads() {
    let n = node();
    Fixture::new().with(&n, &Control::default(), |v, e, d, a| {
        let (actual, _) = encode_topn_node(&n, v, e, SOURCE, limits()).unwrap();
        assert_eq!(actual, expected());
        let (owned, _) = decode_topn_node(&expected(), d, a, SOURCE, limits()).unwrap();
        assert_eq!(owned, n);
        let (_, _, _, phase, reduction) = physical(&owned).unwrap();
        assert_eq!(
            phase,
            p::TopNPhase::Partial {
                sequence: p::TopNSequenceId::new(0)
            }
        );
        let (groups, calls) = physical_collections(reduction);
        assert_eq!(groups.len(), 3);
        assert_eq!(
            calls[0]
                .arguments
                .iter()
                .map(|e| e.get())
                .collect::<Vec<_>>(),
            [0, u32::MAX, 0]
        );
        assert!(calls[0].distinct);
        assert_eq!(calls[0].order_by.len(), 2);
        assert!(
            calls
                .iter()
                .all(|c| c.binding.function.legacy_metadata.is_none())
        );
        assert_eq!(calls[0].binding.state_format.as_str(), "state/v1");
        assert_eq!(
            calls[1].binding.phase,
            p::AggregatePhase::Intermediate {
                sequence: p::AggregateSequenceId::new(u32::MAX)
            }
        );
    });
    // Repeated grouping outputs, DISTINCT/ORDER, type domains and sequence
    // relations deliberately exercise representation, not a lawful Package.
}

#[test]
fn topn_owned_signatures_keep_lambda_nominal_metadata_and_original_aliases() {
    let mut n = node();
    calls(&mut n)[0].binding.function.argument_types =
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            FunctionArgumentType::Value(dictionary()),
            FunctionArgumentType::Lambda {
                parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    Box::from([nested(), FunctionValueType::new(DataType::Int64, true)]),
                ),
                result_type: nominal(),
            },
        ]));
    Fixture::new().with(&n, &Control::default(), |v, e, d, a| {
        let actual = encode_topn_node(&n, v, e, SOURCE, limits()).unwrap().0;
        // Alias 9 and ID0 point to the same original binding; original sparse
        // association selects the first ascending ID0, not a copied signature.
        assert_eq!(
            grouped(&mut actual.clone()).calls[0].aggregate_binding_id,
            Some(0)
        );
        let owned = decode_topn_node(&actual, d, a, SOURCE, limits()).unwrap().0;
        assert_eq!(owned, n);
        let first = &physical_collections(physical(&owned).unwrap().4).1[0].binding;
        assert_eq!(
            first.function.argument_types[0],
            FunctionArgumentType::Value(dictionary())
        );
        assert_eq!(first.intermediate_type, dictionary());
        match &first.function.argument_types[1] {
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => {
                assert_eq!(
                    parameter_types.as_ref(),
                    [nested(), FunctionValueType::new(DataType::Int64, true)]
                );
                assert_eq!(result_type, &nominal());
                if let DataType::Struct(fields) = &parameter_types[0].data_type() {
                    assert_eq!(fields[0].name(), "original-child");
                    assert!(fields[0].metadata().as_arc().is_none());
                    assert!(fields[0].is_nullable());
                } else {
                    panic!("original Lambda Struct was lost");
                }
            }
            _ => panic!("Lambda shape was lost"),
        }
        if let DataType::Struct(fields) = &first.function.result_type.data_type() {
            if let DataType::Struct(original) =
                &a.definitions()[0].1.function.result_type.data_type()
            {
                assert!(Arc::ptr_eq(&fields[0], &original[0]));
            } else {
                panic!("original result is not Struct");
            }
        } else {
            panic!("result is not Struct");
        }
    });
}

#[test]
fn topn_presence_comparator_sort_and_sparse_references_refuse_during_prepare() {
    let n = node();
    Fixture::new().with(&n, &Control::default(), |_, _, d, a| {
        let mut cases = Vec::new();
        let mut w = expected();
        body(&mut w).phase = None;
        cases.push(w);
        let mut w = expected();
        body(&mut w).phase.as_mut().unwrap().kind = None;
        cases.push(w);
        let mut w = expected();
        body(&mut w).reduction = None;
        cases.push(w);
        let mut w = expected();
        body(&mut w).reduction.as_mut().unwrap().kind = None;
        cases.push(w);
        for comparator in [0, 999] {
            let mut w = expected();
            grouped(&mut w).comparator = comparator;
            cases.push(w);
        }
        let mut w = expected();
        w.output = None;
        cases.push(w);
        let mut w = expected();
        w.output.as_mut().unwrap().node_id = None;
        cases.push(w);
        let mut w = expected();
        w.output_properties = None;
        cases.push(w);
        let mut w = expected();
        body(&mut w).order_by[0].expr_id = None;
        cases.push(w);
        let mut w = expected();
        body(&mut w).order_by[0].expr_id = Some(17);
        cases.push(w);
        let mut w = expected();
        body(&mut w).order_by[0].direction = 0;
        cases.push(w);
        let mut w = expected();
        body(&mut w).order_by[0].null_ordering = 999;
        cases.push(w);
        let mut w = expected();
        grouped(&mut w).group_by[0].expr_id = None;
        cases.push(w);
        let mut w = expected();
        grouped(&mut w).group_by[0].value_id = Some(17);
        cases.push(w);
        let mut w = expected();
        grouped(&mut w).calls[0].aggregate_binding_id = None;
        cases.push(w);
        let mut w = expected();
        grouped(&mut w).calls[0].aggregate_binding_id = Some(17);
        cases.push(w);
        let mut w = expected();
        grouped(&mut w).calls[0].output_value_id = None;
        cases.push(w);
        let mut w = expected();
        grouped(&mut w).calls[0].argument_expr_ids[0] = 17;
        cases.push(w);
        let mut w = expected();
        grouped(&mut w).calls[0].order_by[0].null_ordering = 0;
        cases.push(w);
        for w in cases {
            assert!(matches!(
                prepare_topn_node_decode(&w, d, a, SOURCE, limits()),
                Err(Error::InvalidShape(_)) | Err(Error::Properties(_))
            ));
        }
        let mut empty = expected();
        grouped(&mut empty).group_by.clear();
        grouped(&mut empty).calls.clear();
        body(&mut empty).order_by.clear();
        let owned = decode_topn_node(&empty, d, a, SOURCE, limits()).unwrap().0;
        let (keys, _, _, _, r) = physical(&owned).unwrap();
        let (g, c) = physical_collections(r);
        assert!(keys.is_empty() && g.is_empty() && c.is_empty());
    });
}

#[test]
fn topn_independent_layout_and_exact_seven_axes_cover_rows_and_inline_states() {
    for grouped_mode in [false, true] {
        let n = if grouped_mode { node() } else { rows() };
        Fixture::new().with(&n, &Control::default(), |v, e, d, a| {
            let wire = encode_topn_node(&n, v, e, SOURCE, limits()).unwrap().0;
            let sf = encode_topn_node(&n, v, e, SOURCE, limits()).unwrap().1;
            let rf = decode_topn_node(&wire, d, a, SOURCE, limits()).unwrap().1;
            let sender_header = layout::<u32>(1)
                + layout::<wire::PhysicalProperties>(1)
                + layout::<u32>(3)
                + layout::<wire::SortExpression>(3);
            let receiver_header = 2
                * (layout::<p::NodeId>(1)
                    + layout::<p::PhysicalProperties>(1)
                    + layout::<p::ValueId>(3)
                    + layout::<p::SortExpr>(3));
            let sender_extra = if grouped_mode {
                layout::<wire::ExpressionOutput>(3)
                    + layout::<wire::AggregateCall>(2)
                    + layout::<u32>(3)
                    + layout::<wire::SortExpression>(2)
            } else {
                0
            };
            let receiver_extra = if grouped_mode {
                2 * (layout::<(p::ExprId, p::ValueId)>(3)
                    + layout::<p::AggregateCall>(2)
                    + layout::<p::ExprId>(3)
                    + layout::<p::SortExpr>(2))
            } else {
                0
            };
            // Grouped inline state/function signatures share their original
            // immutable owners; only physical node collections allocate.
            assert_eq!(
                sf.allocation_requests_upper_bound,
                if grouped_mode { 8 } else { 4 }
            );
            assert_eq!(
                rf.allocation_requests_upper_bound,
                if grouped_mode { 16 } else { 8 }
            );
            assert_eq!(
                sf.allocation_request_bytes_upper_bound,
                sender_header + sender_extra
            );
            assert_eq!(
                rf.allocation_request_bytes_upper_bound,
                receiver_header + receiver_extra
            );
            assert_eq!(sf.value_reference_count, if grouped_mode { 8 } else { 3 });
            assert_eq!(rf.value_reference_count, sf.value_reference_count);
            let sl = exact(limits(), sf);
            let rl = exact(limits(), rf);
            assert!(encode_topn_node(&n, v, e, SOURCE, sl).is_ok());
            assert!(decode_topn_node(&wire, d, a, SOURCE, rl).is_ok());
            for axis in 0..7 {
                let mut l = sl;
                under(&mut l, axis);
                assert!(matches!(
                    encode_topn_node(&n, v, e, SOURCE, l),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                let mut l = rl;
                under(&mut l, axis);
                assert!(matches!(
                    decode_topn_node(&wire, d, a, SOURCE, l),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
            }
            if grouped_mode {
                let mut l = limits();
                l.binding.max_type_references = 5;
                assert!(matches!(
                    decode_topn_node(&wire, d, a, SOURCE, l),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                l.binding.max_type_references = 6;
                assert!(decode_topn_node(&wire, d, a, SOURCE, l).is_ok());
            }
            let mut over = wire.clone();
            body(&mut over).order_by.reserve(SOURCE);
            assert!(matches!(
                prepare_topn_node_decode(&over, d, a, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            if grouped_mode {
                let mut over = wire.clone();
                grouped(&mut over).calls[0]
                    .argument_expr_ids
                    .reserve(SOURCE);
                assert!(matches!(
                    prepare_topn_node_decode(&over, d, a, SOURCE, limits()),
                    Err(Error::InvalidShape(_))
                ));
            }
        });
    }
}

#[test]
fn topn_same_namespace_loans_reject_equal_foreign_binding_and_receiving_owners() {
    let n = node();
    let c = Control::default();
    let fixture = Fixture::new();
    fixture.with(&n, &c, |v, e, d, a| {
        assert!(matches!(
            encode_topn_node(&n.clone(), v, e, SOURCE, limits()),
            Err(Error::Binding(_))
        ));
        c.disarm();
        fixture.with(&n, &c, |_, _, other, _| {
            assert!(matches!(
                decode_topn_node(&expected(), other, a, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        });
        c.disarm();
        fixture.with(&n, &c, |_, _, _, other| {
            assert!(matches!(
                decode_topn_node(&expected(), d, other, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        });
    });
}

#[test]
fn topn_each_actual_small_success_ordinary_and_prepared_emit_prefix_keeps_three_causes() {
    for grouped_mode in [false, true] {
        let n = if grouped_mode { node() } else { rows() };
        let c = Control::default();
        Fixture::new().with(&n, &c, |v, e, d, a| {
            c.disarm();
            let wire = encode_topn_node(&n, v, e, SOURCE, limits()).unwrap().0;
            prefixes(&c, || {
                encode_topn_node(&n, v, e, SOURCE, limits()).map(|_| ())
            });
            prefixes(&c, || {
                decode_topn_node(&wire, d, a, SOURCE, limits()).map(|_| ())
            });
            let mut bad = wire.clone();
            body(&mut bad).phase.as_mut().unwrap().kind = None;
            prefixes(&c, || {
                decode_topn_node(&bad, d, a, SOURCE, limits()).map(|_| ())
            });
            c.disarm();
            let prepared = prepare_topn_node_encode(&n, v, e, SOURCE, limits()).unwrap();
            let sf = *prepared.facts();
            c.arm(None);
            assert_eq!(prepared.emit().unwrap().1, sf);
            let trace = c.trace();
            for at in 0..trace.len() {
                for cause in CAUSES {
                    c.disarm();
                    let p = prepare_topn_node_encode(&n, v, e, SOURCE, limits()).unwrap();
                    c.arm(Some((at, cause.clone())));
                    assert!(matches!(p.emit(),Err(Error::Control(actual)) if actual==cause));
                    assert_eq!(c.trace(), trace[..=at]);
                }
            }
            c.disarm();
            let prepared = prepare_topn_node_decode(&wire, d, a, SOURCE, limits()).unwrap();
            let rf = *prepared.facts();
            c.arm(None);
            assert_eq!(prepared.emit().unwrap().1, rf);
            let trace = c.trace();
            for at in 0..trace.len() {
                for cause in CAUSES {
                    c.disarm();
                    let p = prepare_topn_node_decode(&wire, d, a, SOURCE, limits()).unwrap();
                    c.arm(Some((at, cause.clone())));
                    assert!(matches!(p.emit(),Err(Error::Control(actual)) if actual==cause));
                    assert_eq!(c.trace(), trace[..=at]);
                }
            }
            c.disarm();
        });
    }
}

#[test]
fn topn_wide_actual_sort_and_nested_lists_observe_quantum_and_numeric_first_refusal() {
    for grouped_mode in [false, true] {
        let mut n = if grouped_mode { node() } else { rows() };
        if let p::NodeKind::TopN { order_by, .. } = &mut n.kind {
            *order_by = vec![keys()[0]; 320].into_boxed_slice();
        }
        if grouped_mode {
            calls(&mut n)[0].arguments = vec![p::ExprId::new(0); 320].into_boxed_slice();
        }
        let c = Control::default();
        Fixture::new().with(&n, &c, |v, e, d, a| {
            c.disarm();
            let wire = encode_topn_node(&n, v, e, SOURCE, limits()).unwrap().0;
            for receiving in [false, true] {
                let action = || {
                    if receiving {
                        decode_topn_node(&wire, d, a, SOURCE, limits()).map(|_| ())
                    } else {
                        encode_topn_node(&n, v, e, SOURCE, limits()).map(|_| ())
                    }
                };
                c.arm(None);
                action().unwrap();
                let trace = c.trace();
                let quantum = trace
                    .iter()
                    .position(|(_, units)| *units == 256)
                    .expect("actual sort/nested emission must observe its owned 320-item loop");
                for at in [0, quantum, trace.len() - 1] {
                    for cause in CAUSES {
                        c.arm(Some((at, cause.clone())));
                        assert!(matches!(action(),Err(Error::Control(actual)) if actual==cause));
                        assert_eq!(c.trace(), trace[..=at]);
                    }
                }
                let mut l = limits();
                l.node.max_work = 2048;
                for cause in CAUSES {
                    c.arm(Some((1, cause)));
                    let r = if receiving {
                        prepare_topn_node_decode(&wire, d, a, SOURCE, l).map(|_| ())
                    } else {
                        prepare_topn_node_encode(&n, v, e, SOURCE, l).map(|_| ())
                    };
                    assert!(matches!(
                        r,
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                    assert_eq!(c.trace(), trace[..1]);
                }
            }
            c.disarm();
        });
    }
}

// The caller owns one original scope, including success/ordinary footer.
fn caller_topn_encode(
    n: &p::PhysicalNode,
    v: &EncodedValues<'_, '_, '_>,
    e: &EncodedExpressions<'_, '_, '_>,
    l: TopNNodeProjectionLimits,
    c: &Control,
    parent: &mut NodeAdmit<'_>,
) -> Result<(wire::PhysicalNode, TopNNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(c, CompilePhase::Encode)?;
    let result = (|| {
        let token = prepare_topn_node_encode_in(n, v, e, SOURCE, l, parent, &mut w)?;
        token.emit_in(parent, &mut w)
    })();
    finish(w, result)
}
fn caller_topn_decode(
    raw: &wire::PhysicalNode,
    d: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    l: TopNNodeProjectionLimits,
    c: &Control,
    parent: &mut NodeAdmit<'_>,
) -> Result<(p::PhysicalNode, TopNNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
    let result = (|| {
        let token = prepare_topn_node_decode_in(raw, d, a, SOURCE, l, parent, &mut w)?;
        token.emit_in(parent, &mut w)
    })();
    finish(w, result)
}
fn caller_topn_axes(f: TopNNodeProjectionFacts) -> [usize; 7] {
    [
        f.input_node_count,
        f.value_reference_count,
        f.list_item_count,
        f.allocation_requests_upper_bound,
        f.allocation_request_bytes_upper_bound,
        f.coexisting_source_and_request_bytes_upper_bound,
        f.cumulative_work_upper_bound,
    ]
}
#[test]
fn topn_caller_owned_complete_loans_preserve_wire_and_seal_every_prefix() {
    let n = node();
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        let mut sent = Vec::new();
        let (wire, sf) = caller_topn_encode(&n, v, e, limits(), &c, &mut |f| {
            sent.push(*f);
            Ok(())
        })
        .unwrap();
        assert_eq!(wire, expected());
        let mut received = Vec::new();
        let (owned, rf) = caller_topn_decode(&wire, d, a, limits(), &c, &mut |f| {
            received.push(*f);
            Ok(())
        })
        .unwrap();
        assert_eq!(owned, n);
        for (prefixes, final_facts) in [(&sent, sf), (&received, rf)] {
            assert!(!prefixes.is_empty());
            for prefix in prefixes {
                assert!(
                    caller_topn_axes(*prefix)
                        .into_iter()
                        .zip(caller_topn_axes(final_facts))
                        .all(|(p, f)| p <= f)
                );
            }
        }
        for receive in [false, true] {
            let bound = if receive { rf } else { sf };
            let mut exact = limits();
            exact.node.max_input_nodes = bound.input_node_count;
            exact.node.max_value_references = bound.value_reference_count;
            exact.node.max_list_items = bound.list_item_count;
            exact.node.max_allocation_requests = bound.allocation_requests_upper_bound;
            exact.node.max_allocation_request_bytes = bound.allocation_request_bytes_upper_bound;
            exact.node.max_coexisting_source_and_request_bytes =
                bound.coexisting_source_and_request_bytes_upper_bound;
            exact.node.max_work = bound.cumulative_work_upper_bound;
            let actual = if receive {
                caller_topn_decode(&wire, d, a, exact, &c, &mut |_| Ok(())).map(|r| r.1)
            } else {
                caller_topn_encode(&n, v, e, exact, &c, &mut |_| Ok(())).map(|r| r.1)
            };
            assert_eq!(actual.unwrap(), bound);
        }
    });
}
#[test]
fn topn_caller_owned_actual_success_ordinary_and_foreign_control_prefixes() {
    let n = node();
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        prefixes(&c, || {
            caller_topn_encode(&n, v, e, limits(), &c, &mut |_| Ok(())).map(|_| ())
        });
        let raw = expected();
        prefixes(&c, || {
            caller_topn_decode(&raw, d, a, limits(), &c, &mut |_| Ok(())).map(|_| ())
        });
        let mut bad = raw.clone();
        match body(&mut bad)
            .reduction
            .as_mut()
            .unwrap()
            .kind
            .as_mut()
            .unwrap()
        {
            wire::top_n_reduction::Kind::GroupedStates(g) => g.calls[0].aggregate_binding_id = None,
            _ => panic!("fixture is GroupedStates"),
        }
        prefixes(&c, || {
            caller_topn_decode(&bad, d, a, limits(), &c, &mut |_| Ok(())).map(|_| ())
        });
        let foreign = Control::default();
        let mut calls = 0;
        let result = caller_topn_encode(&n, v, e, limits(), &foreign, &mut |_| {
            calls += 1;
            Ok(())
        });
        assert!(matches!(result, Err(Error::InvalidShape(_))));
        assert_eq!(calls, 0);
        assert!(matches!(
            caller_topn_encode(&n.clone(), v, e, limits(), &c, &mut |_| Ok(())),
            Err(Error::Binding(_))
        ));
    });
}
#[test]
fn topn_caller_owned_known_root_requests_win_at_pending255() {
    let n = node();
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        let raw = expected();
        for receive in [false, true] {
            for cause in CAUSES {
                c.arm(Some((1, cause)));
                let phase = if receive {
                    CompilePhase::Decode
                } else {
                    CompilePhase::Encode
                };
                let mut w = CompileCheckpoints::try_new(&c, phase).unwrap();
                for _ in 0..255 {
                    w.step().unwrap();
                }
                let mut parent = |f: &NodeProjectionFacts| {
                    if f.allocation_requests_upper_bound > 0 {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                };
                let result = if receive {
                    prepare_topn_node_decode_in(&raw, d, a, SOURCE, limits(), &mut parent, &mut w)
                        .map(|_| ())
                } else {
                    prepare_topn_node_encode_in(&n, v, e, SOURCE, limits(), &mut parent, &mut w)
                        .map(|_| ())
                };
                assert!(matches!(
                    result,
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(c.trace(), [(phase, 0)]);
            }
        }
        c.disarm();
    });
}

#[test]
fn topn_caller_parent_seven_axes_known_refusal_precedes_late_control() {
    let n = node();
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        let raw = expected();
        for receive in [false, true] {
            c.arm(None);
            let mut prefixes = Vec::new();
            let mut collect = |f: &NodeProjectionFacts| {
                prefixes.push((*f, c.trace().len()));
                Ok(())
            };
            let facts = if receive {
                caller_topn_decode(&raw, d, a, limits(), &c, &mut collect)
                    .unwrap()
                    .1
            } else {
                caller_topn_encode(&n, v, e, limits(), &c, &mut collect)
                    .unwrap()
                    .1
            };
            let trace = c.trace();
            for axis in 0..7 {
                let bound = caller_topn_axes(facts)[axis];
                assert!(bound > 0);
                let marker = prefixes
                    .iter()
                    .find(|(f, _)| caller_topn_axes(*f)[axis] > bound - 1)
                    .unwrap()
                    .1;
                for cause in CAUSES {
                    c.arm(Some((marker, cause)));
                    let mut parent = |f: &NodeProjectionFacts| {
                        if caller_topn_axes(*f)[axis] > bound - 1 {
                            Err(CompileControlError::ResourceExhausted)
                        } else {
                            Ok(())
                        }
                    };
                    let result = if receive {
                        caller_topn_decode(&raw, d, a, limits(), &c, &mut parent).map(|_| ())
                    } else {
                        caller_topn_encode(&n, v, e, limits(), &c, &mut parent).map(|_| ())
                    };
                    assert!(matches!(
                        result,
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                    assert_eq!(c.trace(), trace[..marker]);
                }
            }
        }
        c.disarm();
    });
}

#[test]
fn topn_caller_owned_actual320_ordered_rows_and_grouped_paths_share_same_parent() {
    for grouped_mode in [false, true] {
        let mut n = if grouped_mode { node() } else { rows() };
        if let p::NodeKind::TopN { order_by, .. } = &mut n.kind {
            *order_by = vec![keys()[0]; 320].into_boxed_slice();
        }
        if grouped_mode {
            calls(&mut n)[0].arguments = vec![p::ExprId::new(0); 320].into_boxed_slice();
        }
        let c = Control::default();
        Fixture::new().with(&n, &c, |v, e, d, a| {
            c.arm(None);
            let (wire, _) = caller_topn_encode(&n, v, e, limits(), &c, &mut |_| Ok(())).unwrap();
            assert_eq!(raw(&wire).unwrap().order_by, vec![expected_keys()[0]; 320]);
            let sent = c.trace();
            assert!(sent.iter().any(|(_, u)| *u == 256));
            c.arm(None);
            let (owned, _) =
                caller_topn_decode(&wire, d, a, limits(), &c, &mut |_| Ok(())).unwrap();
            assert_eq!(owned, n);
            let received = c.trace();
            assert!(received.iter().any(|(_, u)| *u == 256));
            for receive in [false, true] {
                let trace = if receive { &received } else { &sent };
                let at = trace.iter().position(|(_, u)| *u == 256).unwrap();
                for stop in [0, at, trace.len() - 1] {
                    for cause in CAUSES {
                        c.arm(Some((stop, cause.clone())));
                        let result = if receive {
                            caller_topn_decode(&wire, d, a, limits(), &c, &mut |_| Ok(()))
                                .map(|_| ())
                        } else {
                            caller_topn_encode(&n, v, e, limits(), &c, &mut |_| Ok(())).map(|_| ())
                        };
                        assert!(matches!(result,Err(Error::Control(actual)) if actual==cause));
                        assert_eq!(c.trace(), trace[..=stop]);
                    }
                }
            }
            c.disarm();
        });
    }
}
