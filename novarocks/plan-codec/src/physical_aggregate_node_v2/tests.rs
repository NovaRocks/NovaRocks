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
fn limits() -> AggregateNodeProjectionLimits {
    AggregateNodeProjectionLimits {
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
        kind: p::NodeKind::Aggregate {
            group_by: Box::from([
                (p::ExprId::new(u32::MAX), p::ValueId::new(0)),
                (p::ExprId::new(0), p::ValueId::new(u32::MAX)),
                (p::ExprId::new(u32::MAX), p::ValueId::new(0)),
            ]),
            calls: Box::from([
                p::AggregateCall {
                    id: p::AggregateCallId::new(0),
                    binding: aggregate(p::AggregatePhase::Partial {
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
                    binding: aggregate(p::AggregatePhase::Final {
                        sequence: p::AggregateSequenceId::new(u32::MAX),
                    }),
                    arguments: Box::default(),
                    distinct: false,
                    order_by: Box::default(),
                    output: p::ValueId::new(0),
                },
            ]),
            grouping: p::AggregateGrouping::Partial,
        },
    }
}
fn calls(n: &mut p::PhysicalNode) -> &mut Box<[p::AggregateCall]> {
    match &mut n.kind {
        p::NodeKind::Aggregate { calls, .. } => calls,
        _ => unreachable!(),
    }
}
fn body(n: &mut wire::PhysicalNode) -> &mut wire::AggregateNode {
    match n.kind.as_mut() {
        Some(wire::physical_node::Kind::Aggregate(v)) => v,
        _ => unreachable!(),
    }
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
        kind: Some(wire::physical_node::Kind::Aggregate(wire::AggregateNode {
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
            grouping: wire::AggregateGrouping::Partial as i32,
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
        let (_, calls, _) = physical(n).unwrap();
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
#[test]
fn aggregate_complete_payload_has_independent_wire_and_owned_oracles() {
    let n = node();
    Fixture::new().with(&n, &Control::default(), |v, e, d, a| {
        let wire = encode_aggregate_node(&n, v, e, SOURCE, limits()).unwrap().0;
        assert_eq!(wire, expected());
        let owned = decode_aggregate_node(&expected(), d, a, SOURCE, limits())
            .unwrap()
            .0;
        assert_eq!(owned, n);
        let (_, calls, _) = physical(&owned).unwrap();
        assert!(
            calls
                .iter()
                .all(|c| c.binding.function.legacy_metadata.is_none())
        );
        // Same outer original decoded Field Arc survives each observed FVT copy.
        if let DataType::Struct(fields) = &calls[0].binding.function.result_type.data_type() {
            if let DataType::Struct(original) =
                &a.definitions()[0].1.function.result_type.data_type()
            {
                assert!(Arc::ptr_eq(&fields[0], &original[0]));
            } else {
                panic!("source result is not Struct");
            }
        } else {
            panic!("owned result is not Struct");
        }
    });
}
#[test]
fn aggregate_all_phases_grouping_and_intentional_source_aliases_preserve_identity() {
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
        let mut n = node();
        calls(&mut n)[0].binding.phase = phase;
        if phase == p::AggregatePhase::Single {
            for call in calls(&mut n).iter_mut() {
                call.binding.function.argument_types =
                    novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                        FunctionArgumentType::Value(dictionary()),
                        FunctionArgumentType::Lambda {
                            parameter_types:
                                novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from(
                                    [nested(), FunctionValueType::new(DataType::Int64, true)],
                                )),
                            result_type: nominal(),
                        },
                    ]));
            }
        }
        if let p::NodeKind::Aggregate { grouping, .. } = &mut n.kind {
            *grouping = p::AggregateGrouping::Complete;
        }
        Fixture::new().with(&n, &Control::default(), |v, e, d, a| {
            let wire = encode_aggregate_node(&n, v, e, SOURCE, limits()).unwrap().0;
            let raw = raw(&wire).unwrap();
            assert_eq!(raw.grouping, wire::AggregateGrouping::Complete as i32);
            assert_eq!(raw.calls[0].aggregate_binding_id, Some(0));
            let own = decode_aggregate_node(&wire, d, a, SOURCE, limits())
                .unwrap()
                .0;
            assert_eq!(physical(&own).unwrap().1[0].binding.phase, phase);
            assert_eq!(own, n);
        });
    }
}
fn layout<T>(n: usize) -> usize {
    Layout::array::<T>(n).unwrap().size()
}
#[test]
fn aggregate_allocations_have_independent_layout_and_inline_binding_oracle() {
    let n = node();
    Fixture::new().with(&n, &Control::default(), |v, e, d, a| {
        let sf = encode_aggregate_node(&n, v, e, SOURCE, limits()).unwrap().1;
        let rf = decode_aggregate_node(&expected(), d, a, SOURCE, limits())
            .unwrap()
            .1;
        let sender = layout::<u32>(1)
            + layout::<wire::PhysicalProperties>(1)
            + layout::<u32>(3)
            + layout::<wire::ExpressionOutput>(3)
            + layout::<wire::AggregateCall>(2)
            + layout::<u32>(3)
            + layout::<wire::SortExpression>(2);
        assert_eq!(sf.allocation_requests_upper_bound, 7);
        assert_eq!(sf.allocation_request_bytes_upper_bound, sender);
        let root = 2
            * (layout::<p::NodeId>(1)
                + layout::<p::PhysicalProperties>(1)
                + layout::<p::ValueId>(3)
                + layout::<(p::ExprId, p::ValueId)>(3)
                + layout::<p::AggregateCall>(2)
                + layout::<p::ExprId>(3)
                + layout::<p::SortExpr>(2));
        // Each inline call shares its immutable signature and state identity.
        // The seven real output collections each have Vec/Box upper requests.
        assert_eq!(rf.allocation_requests_upper_bound, 14);
        assert_eq!(rf.allocation_request_bytes_upper_bound, root);
        assert_eq!(
            rf.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + root
        );
        assert_eq!(rf.value_reference_count, 8);
        assert_eq!(sf.value_reference_count, 8);
    });
}
fn exact(
    mut l: AggregateNodeProjectionLimits,
    f: AggregateNodeProjectionFacts,
) -> AggregateNodeProjectionLimits {
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
fn under(l: &mut AggregateNodeProjectionLimits, axis: usize) {
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
#[test]
fn aggregate_both_directions_accept_exact_seven_axes_and_refuse_each_under() {
    let n = node();
    Fixture::new().with(&n, &Control::default(), |v, e, d, a| {
        let sf = encode_aggregate_node(&n, v, e, SOURCE, limits()).unwrap().1;
        let rf = decode_aggregate_node(&expected(), d, a, SOURCE, limits())
            .unwrap()
            .1;
        let sl = exact(limits(), sf);
        let rl = exact(limits(), rf);
        assert!(encode_aggregate_node(&n, v, e, SOURCE, sl).is_ok());
        assert!(decode_aggregate_node(&expected(), d, a, SOURCE, rl).is_ok());
        for axis in 0..7 {
            let mut s = sl;
            under(&mut s, axis);
            assert!(matches!(
                encode_aggregate_node(&n, v, e, SOURCE, s),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            let mut r = rl;
            under(&mut r, axis);
            assert!(matches!(
                decode_aggregate_node(&expected(), d, a, SOURCE, r),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
        let mut r = limits();
        r.binding.max_type_references = 5;
        assert!(matches!(
            decode_aggregate_node(&expected(), d, a, SOURCE, r),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        r.binding.max_type_references = 6;
        assert!(decode_aggregate_node(&expected(), d, a, SOURCE, r).is_ok());
    });
}
#[test]
fn aggregate_shape_presence_enum_refs_and_received_capacities_are_not_defaults() {
    let n = node();
    Fixture::new().with(&n, &Control::default(), |_, _, d, a| {
        let mut cases = vec![];
        let mut w = expected();
        w.output = None;
        cases.push(w);
        let mut w = expected();
        w.output.as_mut().unwrap().node_id = None;
        assert!(matches!(
            prepare_aggregate_node_decode(&w, d, a, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        cases.push(w);
        let mut w = expected();
        body(&mut w).grouping = 0;
        cases.push(w);
        let mut w = expected();
        body(&mut w).grouping = 999;
        cases.push(w);
        let mut w = expected();
        body(&mut w).group_by[0].expr_id = None;
        cases.push(w);
        let mut w = expected();
        body(&mut w).group_by[0].value_id = Some(17);
        cases.push(w);
        let mut w = expected();
        body(&mut w).calls[0].aggregate_binding_id = None;
        cases.push(w);
        let mut w = expected();
        body(&mut w).calls[0].aggregate_binding_id = Some(17);
        cases.push(w);
        let mut w = expected();
        body(&mut w).calls[0].argument_expr_ids[0] = 17;
        cases.push(w);
        let mut w = expected();
        body(&mut w).calls[0].output_value_id = None;
        cases.push(w);
        let mut w = expected();
        body(&mut w).calls[0].order_by[0].direction = 0;
        cases.push(w);
        let mut w = expected();
        body(&mut w).calls[0].order_by[0].null_ordering = 999;
        cases.push(w);
        for w in cases {
            assert!(matches!(
                decode_aggregate_node(&w, d, a, SOURCE, limits()),
                Err(Error::InvalidShape(_)) | Err(Error::Properties(_))
            ));
        }
        let mut empty = expected();
        let b = body(&mut empty);
        b.group_by.clear();
        b.calls.clear();
        let owned = decode_aggregate_node(&empty, d, a, SOURCE, limits())
            .unwrap()
            .0;
        let (groups, calls, grouping) = physical(&owned).unwrap();
        assert!(groups.is_empty() && calls.is_empty());
        assert_eq!(grouping, p::AggregateGrouping::Partial);
        let mut over = expected();
        body(&mut over).calls[0].argument_expr_ids.reserve(SOURCE);
        assert!(matches!(
            decode_aggregate_node(&over, d, a, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}
#[test]
fn aggregate_equal_foreign_source_and_foreign_receiving_owners_are_refused() {
    let n = node();
    let f = Fixture::new();
    let c = Control::default();
    f.with(&n, &c, |v, e, d, a| {
        assert!(matches!(
            encode_aggregate_node(&n.clone(), v, e, SOURCE, limits()),
            Err(Error::Binding(_))
        ));
        c.disarm();
        f.with(&n, &c, |_, _, other, _| {
            assert!(matches!(
                decode_aggregate_node(&expected(), other, a, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            assert!(decode_aggregate_node(&expected(), d, a, SOURCE, limits()).is_ok());
        });
    });
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
fn aggregate_each_actual_small_success_and_ordinary_tail_preserves_three_causes() {
    let n = node();
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        prefixes(&c, || {
            encode_aggregate_node(&n, v, e, SOURCE, limits()).map(|_| ())
        });
        prefixes(&c, || {
            decode_aggregate_node(&expected(), d, a, SOURCE, limits()).map(|_| ())
        });
        let mut bad = expected();
        body(&mut bad).calls[0].aggregate_binding_id = None;
        prefixes(&c, || {
            decode_aggregate_node(&bad, d, a, SOURCE, limits()).map(|_| ())
        });
        c.arm(None);
        let prepared = prepare_aggregate_node_encode(&n, v, e, SOURCE, limits()).unwrap();
        let sf = *prepared.facts();
        assert_eq!(prepared.emit().unwrap().1, sf);
        c.arm(None);
        let wire = expected();
        let prepared = prepare_aggregate_node_decode(&wire, d, a, SOURCE, limits()).unwrap();
        let rf = *prepared.facts();
        assert_eq!(prepared.emit().unwrap().1, rf);
    });
}
#[test]
fn aggregate_wide_actual_inner_lists_observe_quantum_and_numeric_first_refusal() {
    let mut n = node();
    calls(&mut n)[0].arguments = vec![p::ExprId::new(0); 320].into_boxed_slice();
    let c = Control::default();
    Fixture::new().with(&n,&c,|v,e,d,a|{
        c.arm(None);let (w,_)=encode_aggregate_node(&n,v,e,SOURCE,limits()).unwrap();let baseline=c.trace();
        let quantum=baseline.iter().position(|(_,u)|*u==256).expect("actual 320 argument walk must reach quantum");
        for at in [0,quantum,baseline.len()-1]{for cause in CAUSES{c.arm(Some((at,cause.clone())));assert!(matches!(encode_aggregate_node(&n,v,e,SOURCE,limits()),Err(Error::Control(actual)) if actual==cause));assert_eq!(c.trace(),baseline[..=at]);}}
        c.arm(None);let own=decode_aggregate_node(&w,d,a,SOURCE,limits()).unwrap().0;assert_eq!(own,n);let read_trace=c.trace();let quantum=read_trace.iter().position(|(_,u)|*u==256).unwrap();
        for at in [0,quantum,read_trace.len()-1]{for cause in CAUSES{c.arm(Some((at,cause.clone())));assert!(matches!(decode_aggregate_node(&w,d,a,SOURCE,limits()),Err(Error::Control(actual)) if actual==cause));assert_eq!(c.trace(),read_trace[..=at]);}}
        let mut l=limits();l.node.max_work=2048;
        for cause in CAUSES{c.arm(Some((1,cause)));assert!(matches!(encode_aggregate_node(&n,v,e,SOURCE,l),Err(Error::Control(CompileControlError::ResourceExhausted))));assert_eq!(c.trace(),vec![(CompilePhase::Encode,0)]);}
        c.disarm();
    });
}

/// All observed prefixes precede an already known numerical refusal. Arm the
/// next actual callback to demonstrate that no footer/post-check can replace it.
fn numeric_prefix(c: &Control, action: impl Fn() -> Result<(), Error>) {
    c.arm(None);
    assert!(matches!(
        action(),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    let prefix = c.trace();
    for cause in CAUSES {
        c.arm(Some((prefix.len(), cause)));
        assert!(matches!(
            action(),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(c.trace(), prefix);
    }
    c.disarm();
}
#[test]
fn aggregate_real_signature_and_lambda_collections_obey_tighter_parent_before_walk() {
    for lambda in [false, true] {
        let mut n = node();
        calls(&mut n)[0].binding.function.argument_types = if lambda {
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Value(dictionary()),
                FunctionArgumentType::Lambda {
                    parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        vec![nested(); 320].into_boxed_slice(),
                    ),
                    result_type: nominal(),
                },
            ]))
        } else {
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                vec![FunctionArgumentType::Value(dictionary()); 320].into_boxed_slice(),
            )
        };
        let c = Control::default();
        Fixture::new().with(&n,&c,|_,_,d,a|{
            let wire=expected();
            let mut l=limits();
            // Original Node arrays contribute 14; actual signature counts are
            // 320 Value args or 2 args + 320 Lambda parameters before types.
            l.node.max_list_items=14+if lambda{321}else{319};
            numeric_prefix(&c,||prepare_aggregate_node_decode(&wire,d,a,4*SOURCE,l).map(|_|()));
            c.arm(None);
            assert!(matches!(prepare_aggregate_node_decode(&wire,d,a,4*SOURCE,l),Err(Error::Control(CompileControlError::ResourceExhausted))));
            assert!(!c.trace().iter().any(|(_,units)|*units==256),"classification/type walk ran before parent admission");
            c.arm(None);
            let prepared=prepare_aggregate_node_decode(&wire,d,a,4*SOURCE,limits()).unwrap();
            let facts=*prepared.facts();
            let trace=c.trace();
            // The Value-argument profile has a genuine 320-item counts loop.
            // Lambda len admission is O(1); its opaque type copies need not
            // produce a 256-unit callback and are sampled honestly below.
            if !lambda {assert!(trace.iter().any(|(_,units)|*units==256));}
            let mut samples=vec![0,trace.len()-1];
            if let Some(at)=trace.iter().position(|(_,units)|*units==256){samples.push(at);}
            for at in samples {for cause in CAUSES {
                c.arm(Some((at,cause.clone())));
                assert!(matches!(prepare_aggregate_node_decode(&wire,d,a,4*SOURCE,limits()),Err(Error::Control(actual)) if actual==cause));
                assert_eq!(c.trace(),trace[..=at]);
            }}
            c.disarm();
            let tight=exact(limits(),facts);
            let prepared=prepare_aggregate_node_decode(&wire,d,a,4*SOURCE,tight).unwrap();
            c.arm(None);
            let (owned,actual)=prepared.emit().unwrap();
            assert_eq!(actual,facts);
            let first=&physical(&owned).unwrap().1[0].binding;
            if lambda {
                match &first.function.argument_types[1] {
                    FunctionArgumentType::Lambda{parameter_types,result_type}=>{
                        assert_eq!(parameter_types.len(),320);
                        assert!(parameter_types.iter().all(|ty|ty==&nested()));
                        assert_eq!(result_type,&nominal());
                    }
                    _=>panic!("owned Lambda was lost"),
                }
            } else {
                assert_eq!(first.function.argument_types.len(),320);
                assert!(first.function.argument_types.iter().all(|arg|arg==&FunctionArgumentType::Value(dictionary())));
            }
            let emit=c.trace();
            for at in [0,emit.len()-1] {for cause in CAUSES {
                c.disarm();let prepared=prepare_aggregate_node_decode(&wire,d,a,4*SOURCE,tight).unwrap();
                c.arm(Some((at,cause.clone())));
                assert!(matches!(prepared.emit(),Err(Error::Control(actual)) if actual==cause));
                assert_eq!(c.trace(),emit[..=at]);
            }}
            c.disarm();
        });
    }
}
#[test]
fn aggregate_known_root_collection_requests_precede_shared_signature_work() {
    let n = node();
    let c = Control::default();
    Fixture::new().with(&n, &c, |_, _, d, a| {
        let mut wire = expected();
        wire.output.as_mut().unwrap().value_ids.clear();
        let b = body(&mut wire);
        b.group_by.clear();
        for call in &mut b.calls {
            call.argument_expr_ids.clear();
            call.order_by.clear();
        }
        c.arm(None);
        let prepared = prepare_aggregate_node_decode(&wire, d, a, SOURCE, limits()).unwrap();
        let facts = *prepared.facts();
        let success = c.trace();
        // Only the three real input/required-input/calls collections remain.
        // Dictionary encoding on a source never creates boxes in a logical
        // function signature; each inline signature shares its original owner.
        assert_eq!(facts.allocation_requests_upper_bound, 6);
        let root = 2
            * (layout::<p::NodeId>(1)
                + layout::<p::PhysicalProperties>(1)
                + layout::<p::AggregateCall>(2));
        assert_eq!(facts.allocation_request_bytes_upper_bound, root);
        let prefix = &success[..1];
        for axis in 0..3 {
            let mut l = limits();
            match axis {
                0 => l.node.max_allocation_requests = 5,
                1 => l.node.max_allocation_request_bytes = root - 1,
                2 => l.node.max_coexisting_source_and_request_bytes = SOURCE + root - 1,
                _ => unreachable!(),
            }
            for cause in CAUSES {
                c.arm(Some((1, cause)));
                assert!(matches!(
                    prepare_aggregate_node_decode(&wire, d, a, SOURCE, l),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(
                    c.trace(),
                    prefix,
                    "root collection refusal precedes namespace work"
                );
            }
        }
        c.disarm();
    });
}

#[test]
fn aggregate_known_container_layouts_precede_nested_quantum_and_late_three_causes() {
    const COUNT: usize = 64;
    let mut n = node();
    if let p::NodeKind::Aggregate {
        group_by, calls, ..
    } = &mut n.kind
    {
        *group_by = Box::default();
        *calls = (0..COUNT)
            .map(|i| p::AggregateCall {
                id: p::AggregateCallId::new(if i + 1 == COUNT { u32::MAX } else { i as u32 }),
                binding: aggregate(p::AggregatePhase::Partial {
                    sequence: p::AggregateSequenceId::new(0),
                }),
                arguments: Box::from([p::ExprId::new(0)]),
                distinct: false,
                order_by: Box::default(),
                output: p::ValueId::new(0),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
    }
    // Representation-only fixture: every namespace loan and ordered reference
    // is real; the mandatory Fragment retains aggregate/type/phase semantics.
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        c.disarm();
        let wire = encode_aggregate_node(&n, v, e, 4 * SOURCE, limits())
            .unwrap()
            .0;
        for receiving in [false, true] {
            c.arm(None);
            let facts = if receiving {
                *prepare_aggregate_node_decode(&wire, d, a, 4 * SOURCE, limits())
                    .unwrap()
                    .facts()
            } else {
                *prepare_aggregate_node_encode(&n, v, e, 4 * SOURCE, limits())
                    .unwrap()
                    .facts()
            };
            let success = c.trace();
            // Sixty-four actual nested containers walk enough owned call
            // counts to reach the original meter's first 256-unit callback.
            // No preloaded meter or synthetic estimated-byte loop is involved.
            assert_eq!(success[0].1, 0);
            assert_eq!(success[1].1, 256);
            let prefix = &success[..1];
            let (root_requests, root_bytes, first_bytes) = if receiving {
                (
                    8,
                    2 * (layout::<p::NodeId>(1)
                        + layout::<p::PhysicalProperties>(1)
                        + layout::<p::ValueId>(3)
                        + layout::<p::AggregateCall>(COUNT)),
                    2 * layout::<p::ExprId>(1),
                )
            } else {
                (
                    4,
                    layout::<u32>(1)
                        + layout::<wire::PhysicalProperties>(1)
                        + layout::<u32>(3)
                        + layout::<wire::AggregateCall>(COUNT),
                    layout::<u32>(1),
                )
            };
            assert!(facts.allocation_requests_upper_bound > root_requests);
            assert!(facts.allocation_request_bytes_upper_bound > root_bytes + first_bytes);
            assert_eq!(v.count(), 2);
            assert_eq!(e.aggregates().source_counts(), COUNT);
            assert_eq!(a.definitions().len(), COUNT);
            let binding_lookup = if receiving {
                3 * COUNT * (COUNT + 2)
            } else {
                2 * COUNT * COUNT
            };
            let expression_lookup = if receiving {
                d.lookup_work_upper_bound().unwrap()
            } else {
                e.lookup_work_upper_bound().unwrap()
            };
            for header_only in [true, false] {
                // The first actual nested request is admitted in addition to
                // all four header/call arrays (each Vec-to-Box doubled on read).
                let boundary_requests = root_requests
                    + if header_only {
                        0
                    } else if receiving {
                        2
                    } else {
                        1
                    };
                let boundary_bytes = root_bytes + if header_only { 0 } else { first_bytes };
                // Independently authored Node work: one input, 1 required
                // property + 3 output columns + COUNT calls, and 3 + COUNT
                // Value references. Two Values give bit_length(2)+1+32=35.
                // Only the first nested argument contributes an Expr lookup;
                // group_by is empty. Source bytes are not an invented grant.
                let nested_items = usize::from(!header_only);
                let boundary_work = 256
                    + 32 * (4 + COUNT + nested_items + 1)
                    + (3 + COUNT) * 35
                    + 4 * boundary_bytes
                    + binding_lookup
                    + nested_items * expression_lookup;
                assert!(facts.cumulative_work_upper_bound > boundary_work);
                for axis in 0..4 {
                    let mut l = limits();
                    match axis {
                        0 => l.node.max_allocation_requests = boundary_requests - 1,
                        1 => l.node.max_allocation_request_bytes = boundary_bytes - 1,
                        2 => {
                            l.node.max_coexisting_source_and_request_bytes =
                                4 * SOURCE + boundary_bytes - 1
                        }
                        3 => l.node.max_work = boundary_work - 1,
                        _ => unreachable!(),
                    }
                    for cause in CAUSES {
                        c.arm(Some((1, cause)));
                        let result = if receiving {
                            prepare_aggregate_node_decode(&wire, d, a, 4 * SOURCE, l).map(|_| ())
                        } else {
                            prepare_aggregate_node_encode(&n, v, e, 4 * SOURCE, l).map(|_| ())
                        };
                        assert!(matches!(
                            result,
                            Err(Error::Control(CompileControlError::ResourceExhausted))
                        ));
                        assert_eq!(
                            c.trace(),
                            prefix,
                            "known container Layout must precede the next nested callback"
                        );
                    }
                }
            }
        }
        c.disarm();
    });
}

#[test]
fn aggregate_collection_counts_preserve_parent_work_once_across_batches() {
    const PARENT_WORK: usize = 137;
    let mut n = node();
    calls(&mut n)[1].arguments = Box::from([p::ExprId::new(u32::MAX)]);
    let mut raw = expected();
    body(&mut raw).calls[1].argument_expr_ids = vec![u32::MAX];
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        let (groups, source_calls, _) = physical(&n).unwrap();
        let raw_calls = &match &raw.kind {
            Some(wire::physical_node::Kind::Aggregate(b)) => b,
            _ => unreachable!(),
        }
        .calls;
        assert_eq!(e.aggregates().source_counts(), 3);
        assert_eq!(a.definitions().len(), 3);
        for receiving in [false, true] {
            let lookup = if receiving {
                d.lookup_work_upper_bound().unwrap()
            } else {
                e.lookup_work_upper_bound().unwrap()
            };
            let binding_work = if receiving { 30 } else { 12 };
            let roots = if receiving {
                decode_collection_lookup_work(
                    &match &raw.kind {
                        Some(wire::physical_node::Kind::Aggregate(b)) => b,
                        _ => unreachable!(),
                    }
                    .group_by,
                    raw_calls,
                    d,
                    a,
                )
                .unwrap()
            } else {
                encode_collection_lookup_work(groups, source_calls, e).unwrap()
            };
            assert_eq!(roots, binding_work + 3 * lookup);
            let root_bytes = layout::<usize>(3)
                + if receiving {
                    2 * (layout::<p::NodeId>(1)
                        + layout::<p::PhysicalProperties>(1)
                        + layout::<p::ValueId>(3)
                        + layout::<(p::ExprId, p::ValueId)>(3)
                        + layout::<p::AggregateCall>(2))
                } else {
                    layout::<u32>(1)
                        + layout::<wire::PhysicalProperties>(1)
                        + layout::<u32>(3)
                        + layout::<wire::ExpressionOutput>(3)
                        + layout::<wire::AggregateCall>(2)
                };
            let nested_bytes = if receiving {
                2 * (layout::<p::ExprId>(3) + layout::<p::SortExpr>(2) + layout::<p::ExprId>(1))
            } else {
                layout::<u32>(3) + layout::<wire::SortExpression>(2) + layout::<u32>(1)
            };
            let initial = Model {
                inputs: 1,
                refs: 8,
                items: 9,
                requests: if receiving { 11 } else { 6 },
                requested: root_bytes,
                delegated_work: PARENT_WORK + roots,
            };
            let known = if receiving {
                wire_header_floor(&raw, raw.output.as_ref().unwrap()).unwrap()
                    + layout::<wire::ExpressionOutput>(3)
                    + layout::<wire::AggregateCall>(2)
            } else {
                physical_header_floor(&n).unwrap()
                    + layout::<(p::ExprId, p::ValueId)>(3)
                    + layout::<p::AggregateCall>(2)
            };
            let run = |split: bool| -> Result<Model, Error> {
                let mut w = CompileCheckpoints::try_new(
                    &c,
                    if receiving {
                        CompilePhase::Decode
                    } else {
                        CompilePhase::Encode
                    },
                )?;
                let result = (|| {
                    let mut model = initial;
                    model.numerical_facts(SOURCE, v.count(), limits().node)?;
                    if split {
                        let mut known = known;
                        for i in 0..2 {
                            let parent = CollectionProjection {
                                model: &mut model,
                                known,
                                source: SOURCE,
                                values: v.count(),
                                limits: limits().node,
                            };
                            if receiving {
                                parent.count_decode(&raw_calls[i..i + 1], d, &mut w)?;
                                known += layout::<u32>(raw_calls[i].argument_expr_ids.capacity())
                                    + layout::<wire::SortExpression>(
                                        raw_calls[i].order_by.capacity(),
                                    );
                            } else {
                                parent.count_encode(&source_calls[i..i + 1], e, &mut w)?;
                                known += layout::<p::ExprId>(source_calls[i].arguments.len())
                                    + layout::<p::SortExpr>(source_calls[i].order_by.len());
                            }
                        }
                    } else {
                        let parent = CollectionProjection {
                            model: &mut model,
                            known,
                            source: SOURCE,
                            values: v.count(),
                            limits: limits().node,
                        };
                        if receiving {
                            parent.count_decode(raw_calls, d, &mut w)?;
                        } else {
                            parent.count_encode(source_calls, e, &mut w)?;
                        }
                    }
                    Ok(model)
                })();
                finish(w, result)
            };
            c.arm(None);
            let whole = run(false).unwrap();
            let baseline = c.trace();
            c.arm(None);
            let batched = run(true).unwrap();
            assert_eq!(c.trace(), baseline);
            for actual in [whole, batched] {
                assert_eq!(
                    actual.delegated_work,
                    PARENT_WORK + binding_work + 9 * lookup
                );
                assert_eq!(actual.items, 15);
                assert_eq!(actual.requests, if receiving { 17 } else { 9 });
                assert_eq!(actual.requested, root_bytes + nested_bytes);
                assert_eq!(actual.refs, 8);
                assert_eq!(actual.inputs, 1);
                let facts = actual
                    .numerical_facts(SOURCE, v.count(), limits().node)
                    .unwrap();
                let work = 256
                    + 32 * (15 + 1)
                    + 8 * 35
                    + 4 * (root_bytes + nested_bytes)
                    + PARENT_WORK
                    + binding_work
                    + 9 * lookup;
                assert_eq!(facts.cumulative_work_upper_bound, work);
                assert_eq!(
                    facts.coexisting_source_and_request_bytes_upper_bound,
                    SOURCE + root_bytes + nested_bytes
                );
            }
        }
        c.disarm();
    });
}

#[test]
fn aggregate_collection_parent_tighter_gates_preserve_work_and_nested_capacity_errors() {
    const PARENT_WORK: usize = 137;
    let n = node();
    let raw = expected();
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        let (groups, source_calls, _) = physical(&n).unwrap();
        let b = match &raw.kind {
            Some(wire::physical_node::Kind::Aggregate(b)) => b,
            _ => unreachable!(),
        };
        for receiving in [false, true] {
            let lookup = if receiving {
                d.lookup_work_upper_bound().unwrap()
            } else {
                e.lookup_work_upper_bound().unwrap()
            };
            let binding_work = if receiving { 30 } else { 12 };
            let delegated = PARENT_WORK + binding_work + 3 * lookup;
            let root_bytes = layout::<usize>(3)
                + if receiving {
                    2 * (layout::<p::NodeId>(1)
                        + layout::<p::PhysicalProperties>(1)
                        + layout::<p::ValueId>(3)
                        + layout::<(p::ExprId, p::ValueId)>(3)
                        + layout::<p::AggregateCall>(2))
                } else {
                    layout::<u32>(1)
                        + layout::<wire::PhysicalProperties>(1)
                        + layout::<u32>(3)
                        + layout::<wire::ExpressionOutput>(3)
                        + layout::<wire::AggregateCall>(2)
                };
            let first_bytes = if receiving {
                2 * (layout::<p::ExprId>(3) + layout::<p::SortExpr>(2))
            } else {
                layout::<u32>(3) + layout::<wire::SortExpression>(2)
            };
            let initial = Model {
                inputs: 1,
                refs: 8,
                items: 9,
                requests: if receiving { 11 } else { 6 },
                requested: root_bytes,
                delegated_work: delegated,
            };
            let known = if receiving {
                wire_header_floor(&raw, raw.output.as_ref().unwrap()).unwrap()
                    + layout::<wire::ExpressionOutput>(3)
                    + layout::<wire::AggregateCall>(2)
            } else {
                physical_header_floor(&n).unwrap()
                    + layout::<(p::ExprId, p::ValueId)>(3)
                    + layout::<p::AggregateCall>(2)
            };
            let action = |l: NodeProjectionLimits| -> Result<(), Error> {
                let mut w = CompileCheckpoints::try_new(
                    &c,
                    if receiving {
                        CompilePhase::Decode
                    } else {
                        CompilePhase::Encode
                    },
                )?;
                let result = (|| {
                    let mut model = initial;
                    model.numerical_facts(SOURCE, v.count(), l)?;
                    let parent = CollectionProjection {
                        model: &mut model,
                        known,
                        source: SOURCE,
                        values: v.count(),
                        limits: l,
                    };
                    if receiving {
                        parent.count_decode(&b.calls, d, &mut w)?;
                    } else {
                        parent.count_encode(source_calls, e, &mut w)?;
                    }
                    Ok(())
                })();
                finish(w, result)
            };
            c.arm(None);
            action(limits().node).unwrap();
            let success = c.trace();
            assert_eq!(success.len(), 2);
            assert_eq!(success[0].1, 0);
            assert_eq!(success[1].1, 10);
            for axis in 0..4 {
                let mut l = limits().node;
                match axis {
                    0 => {
                        l.max_allocation_requests =
                            initial.requests + if receiving { 4 } else { 2 } - 1
                    }
                    1 => l.max_allocation_request_bytes = root_bytes + first_bytes - 1,
                    2 => {
                        l.max_coexisting_source_and_request_bytes =
                            SOURCE + root_bytes + first_bytes - 1
                    }
                    3 => {
                        l.max_work = 256
                            + 32 * (14 + 1)
                            + 8 * 35
                            + 4 * (root_bytes + first_bytes)
                            + delegated
                            + 5 * lookup
                            - 1
                    }
                    _ => unreachable!(),
                }
                for cause in CAUSES {
                    c.arm(Some((1, cause)));
                    assert!(matches!(
                        action(l),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                    assert_eq!(c.trace(), success[..1]);
                }
            }
        }
        // Actual receiving Vec capacity belongs to the source invoice. Its
        // omission remains an ordinary floor error, not a numeric cap failure.
        let mut over = b.calls.clone();
        over[0].argument_expr_ids.reserve(SOURCE);
        let known = wire_header_floor(&raw, raw.output.as_ref().unwrap()).unwrap()
            + layout::<wire::ExpressionOutput>(b.group_by.capacity())
            + layout::<wire::AggregateCall>(over.capacity());
        assert!(known + layout::<u32>(over[0].argument_expr_ids.capacity()) > SOURCE);
        let omitted_capacity = || {
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode)?;
            let mut model = Model {
                inputs: 1,
                refs: 8,
                items: 9,
                delegated_work: PARENT_WORK
                    + decode_collection_lookup_work(&b.group_by, &over, d, a)?,
                ..Model::default()
            };
            let result = CollectionProjection {
                model: &mut model,
                known,
                source: SOURCE,
                values: v.count(),
                limits: limits().node,
            }
            .count_decode(&over, d, &mut w);
            finish(w, result)
        };
        c.arm(None);
        assert!(matches!(omitted_capacity(), Err(Error::InvalidShape(_))));
        prefixes(&c, omitted_capacity);
        // The same root/group lookup author remains in the encoded direction.
        assert_eq!(
            encode_collection_lookup_work(groups, source_calls, e).unwrap(),
            12 + 3 * e.lookup_work_upper_bound().unwrap()
        );
        c.disarm();
    });
}

// The caller owns one original scope, including success/ordinary footer.
fn caller_aggregate_encode(
    n: &p::PhysicalNode,
    v: &EncodedValues<'_, '_, '_>,
    e: &EncodedExpressions<'_, '_, '_>,
    l: AggregateNodeProjectionLimits,
    c: &Control,
    parent: &mut NodeAdmit<'_>,
) -> Result<(wire::PhysicalNode, AggregateNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(c, CompilePhase::Encode)?;
    let result = (|| {
        let token = prepare_aggregate_node_encode_in(n, v, e, SOURCE, l, parent, &mut w)?;
        token.emit_in(parent, &mut w)
    })();
    finish(w, result)
}
fn caller_aggregate_decode(
    raw: &wire::PhysicalNode,
    d: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    l: AggregateNodeProjectionLimits,
    c: &Control,
    parent: &mut NodeAdmit<'_>,
) -> Result<(p::PhysicalNode, AggregateNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
    let result = (|| {
        let token = prepare_aggregate_node_decode_in(raw, d, a, SOURCE, l, parent, &mut w)?;
        token.emit_in(parent, &mut w)
    })();
    finish(w, result)
}
fn caller_aggregate_axes(f: AggregateNodeProjectionFacts) -> [usize; 7] {
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
fn aggregate_caller_owned_complete_loans_preserve_wire_and_seal_every_prefix() {
    let n = node();
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        let mut sent = Vec::new();
        let (wire, sf) = caller_aggregate_encode(&n, v, e, limits(), &c, &mut |f| {
            sent.push(*f);
            Ok(())
        })
        .unwrap();
        assert_eq!(wire, expected());
        let mut received = Vec::new();
        let (owned, rf) = caller_aggregate_decode(&wire, d, a, limits(), &c, &mut |f| {
            received.push(*f);
            Ok(())
        })
        .unwrap();
        assert_eq!(owned, n);
        for (prefixes, final_facts) in [(&sent, sf), (&received, rf)] {
            assert!(!prefixes.is_empty());
            for prefix in prefixes {
                assert!(
                    caller_aggregate_axes(*prefix)
                        .into_iter()
                        .zip(caller_aggregate_axes(final_facts))
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
                caller_aggregate_decode(&wire, d, a, exact, &c, &mut |_| Ok(())).map(|r| r.1)
            } else {
                caller_aggregate_encode(&n, v, e, exact, &c, &mut |_| Ok(())).map(|r| r.1)
            };
            assert_eq!(actual.unwrap(), bound);
        }
    });
}
#[test]
fn aggregate_caller_owned_actual_success_ordinary_and_foreign_control_prefixes() {
    let n = node();
    let c = Control::default();
    Fixture::new().with(&n, &c, |v, e, d, a| {
        prefixes(&c, || {
            caller_aggregate_encode(&n, v, e, limits(), &c, &mut |_| Ok(())).map(|_| ())
        });
        let raw = expected();
        prefixes(&c, || {
            caller_aggregate_decode(&raw, d, a, limits(), &c, &mut |_| Ok(())).map(|_| ())
        });
        let mut bad = raw.clone();
        body(&mut bad).calls[0].aggregate_binding_id = None;
        prefixes(&c, || {
            caller_aggregate_decode(&bad, d, a, limits(), &c, &mut |_| Ok(())).map(|_| ())
        });
        let foreign = Control::default();
        let mut calls = 0;
        let result = caller_aggregate_encode(&n, v, e, limits(), &foreign, &mut |_| {
            calls += 1;
            Ok(())
        });
        assert!(matches!(result, Err(Error::InvalidShape(_))));
        assert_eq!(calls, 0);
        assert!(matches!(
            caller_aggregate_encode(&n.clone(), v, e, limits(), &c, &mut |_| Ok(())),
            Err(Error::Binding(_))
        ));
    });
}
#[test]
fn aggregate_caller_owned_known_root_requests_win_at_pending255() {
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
                    prepare_aggregate_node_decode_in(
                        &raw,
                        d,
                        a,
                        SOURCE,
                        limits(),
                        &mut parent,
                        &mut w,
                    )
                    .map(|_| ())
                } else {
                    prepare_aggregate_node_encode_in(
                        &n,
                        v,
                        e,
                        SOURCE,
                        limits(),
                        &mut parent,
                        &mut w,
                    )
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
fn aggregate_caller_parent_seven_axes_known_refusal_precedes_late_control() {
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
                caller_aggregate_decode(&raw, d, a, limits(), &c, &mut collect)
                    .unwrap()
                    .1
            } else {
                caller_aggregate_encode(&n, v, e, limits(), &c, &mut collect)
                    .unwrap()
                    .1
            };
            let trace = c.trace();
            for axis in 0..7 {
                let bound = caller_aggregate_axes(facts)[axis];
                assert!(bound > 0);
                let marker = prefixes
                    .iter()
                    .find(|(f, _)| caller_aggregate_axes(*f)[axis] > bound - 1)
                    .unwrap()
                    .1;
                for cause in CAUSES {
                    c.arm(Some((marker, cause)));
                    let mut parent = |f: &NodeProjectionFacts| {
                        if caller_aggregate_axes(*f)[axis] > bound - 1 {
                            Err(CompileControlError::ResourceExhausted)
                        } else {
                            Ok(())
                        }
                    };
                    let result = if receive {
                        caller_aggregate_decode(&raw, d, a, limits(), &c, &mut parent).map(|_| ())
                    } else {
                        caller_aggregate_encode(&n, v, e, limits(), &c, &mut parent).map(|_| ())
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
fn aggregate_caller_owned_actual320_signatures_and_lambda_copy_preserve_source_profiles() {
    for lambda in [false, true] {
        let mut n = node();
        calls(&mut n)[0].binding.function.argument_types = if lambda {
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Value(dictionary()),
                FunctionArgumentType::Lambda {
                    parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        vec![nested(); 320].into_boxed_slice(),
                    ),
                    result_type: nominal(),
                },
            ]))
        } else {
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                vec![FunctionArgumentType::Value(dictionary()); 320].into_boxed_slice(),
            )
        };
        let c = Control::default();
        Fixture::new().with(&n, &c, |_, _, d, a| {
            let raw = expected();
            // This wide namespace was admitted with 8 * 128 KiB of original
            // source. Its retained index and both newly owned signature tables
            // coexist with that invoice; SOURCE alone omits those outputs.
            let namespace_floor = d
                .retained_floor_header_in()
                .unwrap()
                .checked_add(a.functions().retained_output_floor().unwrap())
                .unwrap()
                .checked_add(a.retained_output_floor().unwrap())
                .unwrap();
            assert!(namespace_floor > SOURCE);
            // Keep the original small node's conservative source envelope and
            // add the separately owned namespace outputs exactly once. This is
            // a fixture invoice, not a measured backing or allocation grant.
            let source = namespace_floor.checked_add(SOURCE).unwrap();
            let decode = |l| {
                let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode)?;
                let mut parent = |_: &NodeProjectionFacts| Ok(());
                let result = (|| {
                    let token = prepare_aggregate_node_decode_in(
                        &raw,
                        d,
                        a,
                        source,
                        l,
                        &mut parent,
                        &mut w,
                    )?;
                    token.emit_in(&mut parent, &mut w)
                })();
                finish(w, result)
            };
            c.arm(None);
            let (owned, facts) = decode(limits()).unwrap();
            let actual = &physical(&owned).unwrap().1[0].binding;
            assert_eq!(
                actual.function.argument_types,
                physical(&n).unwrap().1[0].binding.function.argument_types
            );
            assert_eq!(actual.phase, physical(&n).unwrap().1[0].binding.phase);
            let trace = c.trace();
            if !lambda {
                assert!(trace.iter().any(|(_, u)| *u == 256));
            }
            let mut samples = vec![0, trace.len() - 1];
            if let Some(at) = trace.iter().position(|(_, u)| *u == 256) {
                samples.push(at);
            }
            for at in samples {
                for cause in CAUSES {
                    c.arm(Some((at, cause.clone())));
                    assert!(
                        matches!(decode(limits()),Err(Error::Control(actual)) if actual==cause)
                    );
                    assert_eq!(c.trace(), trace[..=at]);
                }
            }
            c.disarm();
            let (_, replay) = decode(exact(limits(), facts)).unwrap();
            assert_eq!(replay, facts);
        });
    }
}
