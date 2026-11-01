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
        ExpressionProjectionLimits, ExpressionTypeIds, decode_expression_definitions,
        encode_expression_definitions,
    },
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, ValueSource, decode_values, encode_values},
};
use arrow::{
    array::{Array, BooleanArray, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
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
fn limits() -> ChangeEventNodeProjectionLimits {
    ChangeEventNodeProjectionLimits {
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
        max_definitions: 0,
        max_type_references: 0,
        max_request_bytes: 8192,
        max_allocation_requests: 1,
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
struct Fixture {
    roots: Vec<(u32, FunctionValueType)>,
    definitions: Vec<p::ValueDef>,
    arena: p::ExprArena,
    parameters: SemanticParameters,
    pools: p::ConstantPools,
}
impl Fixture {
    fn new() -> Self {
        let value_type = FunctionValueType::new(DataType::Int64, true);
        let effect_type = FunctionValueType::new(DataType::Int8, false);
        let predicate_type = FunctionValueType::new(DataType::Boolean, false);
        let definitions = [0, 7, u32::MAX]
            .into_iter()
            .map(|id| p::ValueDef {
                id: p::ValueId::new(id),
                ty: if id == 7 {
                    effect_type.clone()
                } else {
                    value_type.clone()
                },
                origin: p::ValueOrigin::NodeOutput {
                    node: p::NodeId::new(u32::MAX),
                    output_ordinal: match id {
                        7 => 0,
                        u32::MAX => 1,
                        _ => 2,
                    },
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
        pools
            .insert(
                p::ConstantPoolId::new(7),
                ConstantPool::try_new(
                    Arc::new(Field::new("original_predicate", DataType::Boolean, false)),
                    predicate_type.clone(),
                    BooleanArray::from(vec![false, true]).to_data(),
                    policy(),
                    CompilePhase::Validate,
                    &Control::default(),
                )
                .unwrap(),
            )
            .unwrap();
        let nodes = [
            p::ExprNode {
                id: p::ExprId::new(0),
                owner: p::NodeId::new(u32::MAX),
                lambda_scope: None,
                ty: value_type.clone(),
                kind: p::ExprKind::Constant(reference(u32::MAX, 1)),
            },
            p::ExprNode {
                id: p::ExprId::new(7),
                owner: p::NodeId::new(u32::MAX),
                lambda_scope: None,
                ty: value_type.clone(),
                kind: p::ExprKind::Value(p::ValueId::new(0)),
            },
            p::ExprNode {
                id: p::ExprId::new(u32::MAX),
                owner: p::NodeId::new(u32::MAX),
                lambda_scope: None,
                ty: predicate_type.clone(),
                kind: p::ExprKind::Constant(reference(7, 1)),
            },
        ];
        let arena = p::ExprArena::try_from_definitions_observed(
            nodes.into_iter(),
            &p::PlanLimits::default(),
            &Control::default(),
        )
        .unwrap();
        Self {
            roots: vec![(0, value_type), (1, effect_type), (2, predicate_type)],
            definitions,
            arena,
            parameters: SemanticParameters::try_new([]).unwrap(),
            pools,
        }
    }
    fn with_tokens<R>(
        &self,
        control: &Control,
        run: impl FnOnce(
            &EncodedValues<'_, '_, '_>,
            &EncodedExpressions<'_, '_, '_>,
            &DecodedExpressions<'_, '_, '_>,
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
                value_type_id: if source.id.get() == 7 { 1 } else { 0 },
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
        let functions =
            encode_function_bindings(&types, &[], 64 << 10, binding_limits(), control).unwrap();
        let aggregates = encode_aggregate_bindings(
            &types,
            &functions,
            &[],
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
            .arena
            .iter()
            .map(|(id, _)| ExpressionTypeIds {
                expr: *id,
                value_type_id: if id.get() == u32::MAX { 2 } else { 0 },
                lambda_parameter_type_ids: &[],
                function_binding_id: None,
                aggregate_binding_id: None,
            })
            .collect::<Vec<_>>();
        let expressions = encode_expression_definitions(
            &self.arena,
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
        let read = decode_expression_definitions(
            expressions.as_wire(),
            &read_values,
            &functions_read,
            &aggregates_read,
            &self.parameters,
            &self.pools,
            512 << 10,
            expression_limits(),
        )
        .unwrap();
        run(&values, &expressions, &read)
    }
}
fn property() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn source() -> p::PhysicalNode {
    p::PhysicalNode {
        id: p::NodeId::new(u32::MAX),
        inputs: Box::from([p::NodeId::new(0)]),
        required_inputs: Box::from([p::PhysicalProperties {
            distribution: p::Distribution::Unconstrained,
            ..property()
        }]),
        output_properties: property(),
        output: p::OutputPort {
            node: p::NodeId::new(u32::MAX),
            columns: Box::from([
                p::ValueId::new(7),
                p::ValueId::new(u32::MAX),
                p::ValueId::new(0),
            ]),
        },
        kind: p::NodeKind::ChangeEventExpand {
            events: Box::from([
                p::ChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Delete,
                    assignments: Box::from([
                        (p::ValueId::new(0), Some(p::ExprId::new(0))),
                        (p::ValueId::new(u32::MAX), None),
                    ]),
                },
                p::ChangeEventSpec {
                    predicate: Some(p::ExprId::new(u32::MAX)),
                    effect: ConnectorRowMutationEffect::Replace,
                    assignments: Box::from([
                        (p::ValueId::new(0), Some(p::ExprId::new(7))),
                        (p::ValueId::new(u32::MAX), Some(p::ExprId::new(0))),
                    ]),
                },
                p::ChangeEventSpec {
                    predicate: Some(p::ExprId::new(u32::MAX)),
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: Box::from([
                        (p::ValueId::new(u32::MAX), Some(p::ExprId::new(7))),
                        (p::ValueId::new(0), Some(p::ExprId::new(0))),
                    ]),
                },
            ]),
            effect_output: p::ValueId::new(7),
        },
    }
}
fn wire_property(unconstrained: bool) -> wire::PhysicalProperties {
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
fn expected() -> wire::PhysicalNode {
    wire::PhysicalNode {
        id: u32::MAX,
        input_node_ids: vec![0],
        required_inputs: vec![wire_property(true)],
        output_properties: Some(wire_property(false)),
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![7, u32::MAX, 0],
        }),
        kind: Some(wire::physical_node::Kind::ChangeEventExpand(
            wire::ChangeEventExpandNode {
                effect_output_value_id: Some(7),
                events: vec![
                    wire::ChangeEvent {
                        predicate_expr_id: None,
                        effect: wire::RowMutationEffect::Delete as i32,
                        assignments: vec![
                            wire::ChangeAssignment {
                                value_id: Some(0),
                                expr_id: Some(0),
                            },
                            wire::ChangeAssignment {
                                value_id: Some(u32::MAX),
                                expr_id: None,
                            },
                        ],
                    },
                    wire::ChangeEvent {
                        predicate_expr_id: Some(u32::MAX),
                        effect: wire::RowMutationEffect::Replace as i32,
                        assignments: vec![
                            wire::ChangeAssignment {
                                value_id: Some(0),
                                expr_id: Some(7),
                            },
                            wire::ChangeAssignment {
                                value_id: Some(u32::MAX),
                                expr_id: Some(0),
                            },
                        ],
                    },
                    wire::ChangeEvent {
                        predicate_expr_id: Some(u32::MAX),
                        effect: wire::RowMutationEffect::Insert as i32,
                        assignments: vec![
                            wire::ChangeAssignment {
                                value_id: Some(u32::MAX),
                                expr_id: Some(7),
                            },
                            wire::ChangeAssignment {
                                value_id: Some(0),
                                expr_id: Some(0),
                            },
                        ],
                    },
                ],
            },
        )),
    }
}
fn events(node: &mut p::PhysicalNode) -> &mut Box<[p::ChangeEventSpec]> {
    match &mut node.kind {
        p::NodeKind::ChangeEventExpand { events, .. } => events,
        _ => unreachable!(),
    }
}
fn wire_events(node: &mut wire::PhysicalNode) -> &mut wire::ChangeEventExpandNode {
    match node.kind.as_mut() {
        Some(wire::physical_node::Kind::ChangeEventExpand(v)) => v,
        _ => unreachable!(),
    }
}
#[test]
fn change_event_complete_independent_dto_preserves_three_effects_sparse_ids_and_absence() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        assert_eq!(
            encode_change_event_node(&source(), values, expressions, SOURCE, limits())
                .unwrap()
                .0,
            expected()
        );
        c.arm(None);
        assert_eq!(
            decode_change_event_node(&expected(), read, SOURCE, limits())
                .unwrap()
                .0,
            source()
        );
        // Representation preserves duplicates; the mandatory Fragment owner
        // rejects duplicate output assignments rather than this codec deduping.
        let mut node = source();
        events(&mut node)[0].assignments = Box::from([
            (p::ValueId::new(0), None),
            (p::ValueId::new(0), Some(p::ExprId::new(0))),
        ]);
        c.arm(None);
        let wire = encode_change_event_node(&node, values, expressions, SOURCE, limits())
            .unwrap()
            .0;
        let assignments = &match wire.kind.as_ref().unwrap() {
            wire::physical_node::Kind::ChangeEventExpand(v) => v,
            _ => unreachable!(),
        }
        .events[0]
            .assignments;
        assert_eq!(
            assignments,
            &[
                wire::ChangeAssignment {
                    value_id: Some(0),
                    expr_id: None
                },
                wire::ChangeAssignment {
                    value_id: Some(0),
                    expr_id: Some(0)
                }
            ]
        );
        c.arm(None);
        assert_eq!(
            decode_change_event_node(&wire, read, SOURCE, limits())
                .unwrap()
                .0,
            node
        );
    });
}
#[test]
fn change_event_prepared_loans_and_original_work_match_combined_emission() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        let node = source();
        let wire = expected();
        c.arm(None);
        let (_, combined) =
            encode_change_event_node(&node, values, expressions, SOURCE, limits()).unwrap();
        let work = c
            .trace()
            .iter()
            .map(|(_, units)| u64::from(*units))
            .sum::<u64>();
        c.arm(None);
        let prepared =
            prepare_change_event_node_encode(&node, values, expressions, SOURCE, limits()).unwrap();
        assert!(std::ptr::eq(prepared.input, &node));
        assert!(std::ptr::eq(prepared.values, values));
        assert!(std::ptr::eq(prepared.expressions, expressions));
        assert_eq!(prepared.facts(), &combined);
        assert_eq!(prepared.emit().unwrap(), (wire.clone(), combined));
        assert_eq!(
            c.trace()
                .iter()
                .map(|(_, units)| u64::from(*units))
                .sum::<u64>(),
            work
        );
        c.arm(None);
        let (_, combined) = decode_change_event_node(&wire, read, SOURCE, limits()).unwrap();
        let work = c
            .trace()
            .iter()
            .map(|(_, units)| u64::from(*units))
            .sum::<u64>();
        c.arm(None);
        let prepared = prepare_change_event_node_decode(&wire, read, SOURCE, limits()).unwrap();
        assert!(std::ptr::eq(prepared.input, &wire));
        assert!(std::ptr::eq(prepared.expressions, read));
        assert_eq!(prepared.facts(), &combined);
        assert_eq!(prepared.emit().unwrap(), (node, combined));
        assert_eq!(
            c.trace()
                .iter()
                .map(|(_, units)| u64::from(*units))
                .sum::<u64>(),
            work
        );
        let other_types =
            encode_type_table_sources(&fixture.roots, &[], types_limits(), &c).unwrap();
        let inputs = fixture
            .definitions
            .iter()
            .map(|source| ValueSource {
                source,
                value_type_id: if source.id.get() == 7 { 1 } else { 0 },
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
            encode_change_event_node(&source(), &other_values, expressions, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        let other_control = Control::default();
        let payloads =
            encode_connector_payloads(&[], 64 << 10, payload_limits(), &other_control).unwrap();
        let other_values = encode_values(
            &inputs,
            &payloads,
            values.types(),
            256 << 10,
            value_limits(),
        )
        .unwrap();
        c.arm(None);
        assert!(matches!(
            encode_change_event_node(&source(), &other_values, expressions, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}
#[test]
fn change_event_independent_layout_and_every_seven_caps_apply_in_both_directions() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let (_, encode) =
            encode_change_event_node(&source(), values, expressions, SOURCE, limits()).unwrap();
        c.arm(None);
        let (_, decode) = decode_change_event_node(&expected(), read, SOURCE, limits()).unwrap();
        assert_eq!(encode.input_node_count, 1);
        assert_eq!(encode.value_reference_count, 10);
        assert_eq!(encode.list_item_count, 13);
        assert_eq!(decode.input_node_count, 1);
        assert_eq!(decode.value_reference_count, 10);
        assert_eq!(decode.list_item_count, 13);
        assert_eq!(encode.allocation_requests_upper_bound, 7);
        assert_eq!(decode.allocation_requests_upper_bound, 14);
        assert_eq!(
            encode.allocation_request_bytes_upper_bound,
            4 * std::mem::size_of::<u32>()
                + std::mem::size_of::<wire::PhysicalProperties>()
                + 3 * std::mem::size_of::<wire::ChangeEvent>()
                + 6 * std::mem::size_of::<wire::ChangeAssignment>()
        );
        assert_eq!(
            decode.allocation_request_bytes_upper_bound,
            2 * (std::mem::size_of::<p::NodeId>()
                + std::mem::size_of::<p::PhysicalProperties>()
                + 3 * std::mem::size_of::<p::ValueId>()
                + 3 * std::mem::size_of::<p::ChangeEventSpec>()
                + 6 * std::mem::size_of::<(p::ValueId, Option<p::ExprId>)>())
        );
        for is_decode in [false, true] {
            let cap = exact(if is_decode { decode } else { encode });
            c.arm(None);
            if is_decode {
                assert!(decode_change_event_node(&expected(), read, SOURCE, cap).is_ok());
            } else {
                assert!(
                    encode_change_event_node(&source(), values, expressions, SOURCE, cap).is_ok()
                );
            }
            for axis in 0..7 {
                c.arm(None);
                let result = if is_decode {
                    decode_change_event_node(&expected(), read, SOURCE, under(cap, axis))
                        .map(|_| ())
                } else {
                    encode_change_event_node(
                        &source(),
                        values,
                        expressions,
                        SOURCE,
                        under(cap, axis),
                    )
                    .map(|_| ())
                };
                assert!(matches!(
                    result,
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
            }
        }
    });
}
fn exact(facts: ChangeEventNodeProjectionFacts) -> ChangeEventNodeProjectionLimits {
    ChangeEventNodeProjectionLimits {
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
fn under(mut cap: ChangeEventNodeProjectionLimits, axis: usize) -> ChangeEventNodeProjectionLimits {
    match axis {
        0 => cap.max_input_nodes -= 1,
        1 => cap.max_value_references -= 1,
        2 => cap.max_list_items -= 1,
        3 => cap.max_allocation_requests -= 1,
        4 => cap.max_allocation_request_bytes -= 1,
        5 => cap.max_coexisting_source_and_request_bytes -= 1,
        6 => cap.max_work -= 1,
        _ => unreachable!(),
    }
    cap
}
#[test]
fn change_event_unknown_effect_missing_payload_and_absent_references_fail() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for shape in 0..12 {
            let mut node = expected();
            match shape {
                0 => node.kind = None,
                1 => {
                    node.kind = Some(wire::physical_node::Kind::Filter(
                        wire::FilterNode::default(),
                    ))
                }
                2 => node.output = None,
                3 => node.output_properties = None,
                4 => node.output.as_mut().unwrap().node_id = None,
                5 => wire_events(&mut node).effect_output_value_id = None,
                6 => wire_events(&mut node).events[0].effect = 0,
                7 => wire_events(&mut node).events[0].effect = i32::MAX,
                8 => wire_events(&mut node).events[0].assignments[0].value_id = None,
                9 => wire_events(&mut node).events[0].assignments[0].value_id = Some(99),
                10 => wire_events(&mut node).events[0].predicate_expr_id = Some(99),
                11 => wire_events(&mut node).events[0].assignments[0].expr_id = Some(99),
                _ => unreachable!(),
            }
            c.arm(None);
            assert!(matches!(
                decode_change_event_node(&node, read, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        }
        for shape in 0..5 {
            let mut node = source();
            match shape {
                0 => {
                    node.kind = p::NodeKind::Filter {
                        predicates: Box::default(),
                    }
                }
                1 => {
                    if let p::NodeKind::ChangeEventExpand { effect_output, .. } = &mut node.kind {
                        *effect_output = p::ValueId::new(99);
                    }
                }
                2 => events(&mut node)[0].assignments[0].0 = p::ValueId::new(99),
                3 => events(&mut node)[0].predicate = Some(p::ExprId::new(99)),
                4 => events(&mut node)[0].assignments[0].1 = Some(p::ExprId::new(99)),
                _ => unreachable!(),
            }
            c.arm(None);
            assert!(matches!(
                encode_change_event_node(&node, values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        }
    });
}
#[test]
fn change_event_inner_capacity_and_initial_work_are_admitted_before_visiting_assignments() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for outer in [false, true] {
            let mut node = expected();
            if outer {
                wire_events(&mut node).events.reserve_exact(SOURCE);
            } else {
                wire_events(&mut node).events[0]
                    .assignments
                    .reserve_exact(SOURCE);
            }
            c.arm(None);
            assert!(matches!(
                prepare_change_event_node_decode(&node, read, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        }
        let low = ChangeEventNodeProjectionLimits {
            max_work: 1024,
            ..limits()
        };
        let mut node = source();
        events(&mut node)[0].assignments = vec![(p::ValueId::new(0), None); 320].into_boxed_slice();
        c.arm(None);
        assert!(matches!(
            prepare_change_event_node_encode(&node, values, expressions, SOURCE, low),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(!c.trace().iter().any(|(_, units)| *units == 256));
        let mut wire = expected();
        wire_events(&mut wire).events[0].assignments = vec![
            wire::ChangeAssignment {
                value_id: Some(0),
                expr_id: None
            };
            320
        ];
        c.arm(None);
        assert!(matches!(
            prepare_change_event_node_decode(&wire, read, SOURCE, low),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(!c.trace().iter().any(|(_, units)| *units == 256));
        c.arm(None);
        assert!(matches!(
            encode_change_event_node(&source(), values, expressions, 0, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}
fn small_source() -> p::PhysicalNode {
    let mut node = source();
    *events(&mut node) = Box::from([p::ChangeEventSpec {
        predicate: Some(p::ExprId::new(u32::MAX)),
        effect: ConnectorRowMutationEffect::Insert,
        assignments: Box::from([(p::ValueId::new(0), Some(p::ExprId::new(0)))]),
    }]);
    node
}
fn small_wire() -> wire::PhysicalNode {
    let mut node = expected();
    wire_events(&mut node).events = vec![wire::ChangeEvent {
        predicate_expr_id: Some(u32::MAX),
        effect: wire::RowMutationEffect::Insert as i32,
        assignments: vec![wire::ChangeAssignment {
            value_id: Some(0),
            expr_id: Some(0),
        }],
    }];
    node
}
fn control_case(
    decode: bool,
    ordinary: bool,
    stop: Option<(usize, CompileControlError)>,
) -> (Result<(), Error>, Vec<(CompilePhase, u32)>) {
    let fixture = Fixture::new();
    let c = Control::default();
    let result = fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(stop);
        if decode {
            let mut node = small_wire();
            if ordinary {
                wire_events(&mut node).events[0].effect = 0;
            }
            decode_change_event_node(&node, read, SOURCE, limits()).map(|_| ())
        } else {
            let mut node = small_source();
            if ordinary {
                events(&mut node)[0].assignments[0].0 = p::ValueId::new(99);
            }
            encode_change_event_node(&node, values, expressions, SOURCE, limits()).map(|_| ())
        }
    });
    (result, c.trace())
}
#[test]
fn change_event_all_small_original_control_prefixes_and_wide_real_quantum_are_primary() {
    for decode in [false, true] {
        for ordinary in [false, true] {
            let (result, trace) = control_case(decode, ordinary, None);
            assert_eq!(result.is_err(), ordinary);
            assert_eq!(
                trace[0],
                (
                    if decode {
                        CompilePhase::Decode
                    } else {
                        CompilePhase::Encode
                    },
                    0
                )
            );
            assert!(trace.last().is_some_and(|(_, units)| *units > 0));
            for stop in 0..trace.len() {
                for cause in CAUSES {
                    let (result, actual) =
                        control_case(decode, ordinary, Some((stop, cause.clone())));
                    assert!(matches!(result, Err(Error::Control(actual)) if actual == cause));
                    assert_eq!(actual, trace[..=stop]);
                }
            }
        }
    }
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        let mut node = small_source(); events(&mut node)[0].assignments = vec![(p::ValueId::new(0), None); 320].into_boxed_slice();
        c.arm(None); let wire = encode_change_event_node(&node, values, expressions, SOURCE, limits()).unwrap().0;
        let trace = c.trace(); let at = trace.iter().position(|(_, units)| *units == 256).unwrap();
        for cause in CAUSES { c.arm(Some((at, cause.clone()))); assert!(matches!(encode_change_event_node(&node, values, expressions, SOURCE, limits()), Err(Error::Control(actual)) if actual == cause)); assert_eq!(c.trace(), trace[..=at]); }
        c.arm(None); assert_eq!(decode_change_event_node(&wire, read, SOURCE, limits()).unwrap().0, node);
        let trace = c.trace(); let at = trace.iter().position(|(_, units)| *units == 256).unwrap();
        for cause in CAUSES { c.arm(Some((at, cause.clone()))); assert!(matches!(decode_change_event_node(&wire, read, SOURCE, limits()), Err(Error::Control(actual)) if actual == cause)); assert_eq!(c.trace(), trace[..=at]); }
    });
}
#[test]
fn change_event_empty_events_and_absent_expressions_are_representation_not_static_proof() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        let mut node = source();
        *events(&mut node) = Box::default();
        c.arm(None);
        let wire = encode_change_event_node(&node, values, expressions, SOURCE, limits())
            .unwrap()
            .0;
        assert!(wire_events(&mut wire.clone()).events.is_empty());
        c.arm(None);
        assert_eq!(
            decode_change_event_node(&wire, read, SOURCE, limits())
                .unwrap()
                .0,
            node
        );
        *events(&mut node) = Box::from([p::ChangeEventSpec {
            predicate: None,
            effect: ConnectorRowMutationEffect::Replace,
            assignments: Box::default(),
        }]);
        c.arm(None);
        let wire = encode_change_event_node(&node, values, expressions, SOURCE, limits())
            .unwrap()
            .0;
        c.arm(None);
        assert_eq!(
            decode_change_event_node(&wire, read, SOURCE, limits())
                .unwrap()
                .0,
            node
        );
    });
}

#[path = "owned_tests.rs"]
mod owned_tests;
