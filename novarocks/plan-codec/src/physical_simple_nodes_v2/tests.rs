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
    array::{Array, Int64Array},
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
fn limits() -> SimpleNodeProjectionLimits {
    SimpleNodeProjectionLimits {
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
        let ty = FunctionValueType::new(DataType::Int64, true);
        let definitions = [0, 7, u32::MAX]
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
            roots: vec![(0, ty)],
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
                value_type_id: 0,
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
fn source(at: usize) -> p::PhysicalNode {
    let kind = match at {
        0 => p::NodeKind::Filter {
            predicates: Box::from([
                p::ExprId::new(u32::MAX),
                p::ExprId::new(7),
                p::ExprId::new(0),
                p::ExprId::new(7),
            ]),
        },
        1 => p::NodeKind::Project {
            expressions: Box::from([
                (p::ExprId::new(7), p::ValueId::new(u32::MAX)),
                (p::ExprId::new(0), p::ValueId::new(0)),
                (p::ExprId::new(7), p::ValueId::new(7)),
            ]),
        },
        2 => p::NodeKind::Values {
            rows: Box::from([
                Box::from([p::ExprId::new(u32::MAX), p::ExprId::new(0)]),
                Box::from([p::ExprId::new(7), p::ExprId::new(7)]),
            ]),
        },
        3 => p::NodeKind::Limit {
            limit: None,
            offset: u64::MAX,
        },
        4 => p::NodeKind::Limit {
            limit: Some(0),
            offset: 1 << 63,
        },
        5 => p::NodeKind::Limit {
            limit: Some(u64::MAX),
            offset: 0,
        },
        6 => p::NodeKind::GenerateSeries {
            start: p::ExprId::new(0),
            stop: p::ExprId::new(u32::MAX),
            step: None,
        },
        7 => p::NodeKind::GenerateSeries {
            start: p::ExprId::new(u32::MAX),
            stop: p::ExprId::new(0),
            step: Some(p::ExprId::new(0)),
        },
        _ => panic!("unknown fixture"),
    };
    p::PhysicalNode {
        id: p::NodeId::new(u32::MAX),
        inputs: Box::from([p::NodeId::new(0)]),
        required_inputs: Box::from([p::passthrough_requirement(&property())]),
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
fn expected(at: usize) -> wire::PhysicalNode {
    let kind = match at {
        0 => wire::physical_node::Kind::Filter(wire::FilterNode {
            predicate_expr_ids: vec![u32::MAX, 7, 0, 7],
        }),
        1 => wire::physical_node::Kind::Project(wire::ProjectNode {
            expressions: vec![
                wire::ExpressionOutput {
                    expr_id: Some(7),
                    value_id: Some(u32::MAX),
                },
                wire::ExpressionOutput {
                    expr_id: Some(0),
                    value_id: Some(0),
                },
                wire::ExpressionOutput {
                    expr_id: Some(7),
                    value_id: Some(7),
                },
            ],
        }),
        2 => wire::physical_node::Kind::Values(wire::ValuesNode {
            rows: vec![
                wire::ExpressionIds {
                    expr_ids: vec![u32::MAX, 0],
                },
                wire::ExpressionIds {
                    expr_ids: vec![7, 7],
                },
            ],
        }),
        3 => wire::physical_node::Kind::Limit(wire::LimitNode {
            limit: None,
            offset: u64::MAX,
        }),
        4 => wire::physical_node::Kind::Limit(wire::LimitNode {
            limit: Some(0),
            offset: 1 << 63,
        }),
        5 => wire::physical_node::Kind::Limit(wire::LimitNode {
            limit: Some(u64::MAX),
            offset: 0,
        }),
        6 => wire::physical_node::Kind::GenerateSeries(wire::GenerateSeriesNode {
            start_expr_id: Some(0),
            stop_expr_id: Some(u32::MAX),
            step_expr_id: None,
        }),
        7 => wire::physical_node::Kind::GenerateSeries(wire::GenerateSeriesNode {
            start_expr_id: Some(u32::MAX),
            stop_expr_id: Some(0),
            step_expr_id: Some(0),
        }),
        _ => panic!("unknown fixture"),
    };
    wire::PhysicalNode {
        id: u32::MAX,
        input_node_ids: vec![0],
        required_inputs: vec![expected_property(true)],
        output_properties: Some(expected_property(false)),
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![7, u32::MAX, 0],
        }),
        kind: Some(kind),
    }
}
#[test]
fn simple_nodes_all_five_complete_envelopes_match_independent_wire_and_raw_presence() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for at in 0..8 {
            c.arm(None);
            let (wire, _) =
                encode_simple_node(&source(at), values, expressions, SOURCE, limits()).unwrap();
            assert_eq!(wire, expected(at));
            c.arm(None);
            let (node, _) = decode_simple_node(&expected(at), read, SOURCE, limits()).unwrap();
            assert_eq!(node, source(at));
        }
    });
}
#[test]
fn simple_nodes_prepared_emission_keeps_actual_namespace_loans_and_facts() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for at in [0, 1, 2, 3, 6] {
            let node = source(at);
            let wire = expected(at);
            c.arm(None);
            let prepared =
                prepare_simple_node_encode(&node, values, expressions, SOURCE, limits()).unwrap();
            let facts = *prepared.facts();
            let (output, emitted) = prepared.emit().unwrap();
            assert_eq!(output, wire);
            assert_eq!(facts, emitted);
            c.arm(None);
            let prepared = prepare_simple_node_decode(&wire, read, SOURCE, limits()).unwrap();
            let facts = *prepared.facts();
            let (output, emitted) = prepared.emit().unwrap();
            assert_eq!(output, node);
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
            encode_simple_node(&source(3), &other_values, expressions, SOURCE, limits()),
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
            encode_simple_node(&source(0), &other_values, expressions, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}
#[test]
fn simple_nodes_layout_oracles_count_each_nested_request_without_expression_value_conflation() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        let encode_header =
            std::mem::size_of::<u32>() * 4 + std::mem::size_of::<wire::PhysicalProperties>();
        let decode_header = 2
            * (std::mem::size_of::<p::NodeId>()
                + std::mem::size_of::<p::ValueId>() * 3
                + std::mem::size_of::<p::PhysicalProperties>());
        for at in [0, 1, 2, 3, 6] {
            let (extra_encode, extra_decode, requests, items, refs) = match at {
                0 => (
                    4 * std::mem::size_of::<u32>(),
                    8 * std::mem::size_of::<p::ExprId>(),
                    1,
                    8,
                    3,
                ),
                1 => (
                    3 * std::mem::size_of::<wire::ExpressionOutput>(),
                    6 * std::mem::size_of::<(p::ExprId, p::ValueId)>(),
                    1,
                    7,
                    6,
                ),
                2 => (
                    2 * std::mem::size_of::<wire::ExpressionIds>() + 4 * std::mem::size_of::<u32>(),
                    4 * std::mem::size_of::<Box<[p::ExprId]>>()
                        + 8 * std::mem::size_of::<p::ExprId>(),
                    3,
                    10,
                    3,
                ),
                3 => (0, 0, 0, 4, 3),
                6 => (0, 0, 0, 6, 3),
                _ => unreachable!(),
            };
            c.arm(None);
            let (_, facts) =
                encode_simple_node(&source(at), values, expressions, SOURCE, limits()).unwrap();
            assert_eq!(facts.allocation_requests_upper_bound, 3 + requests);
            assert_eq!(
                facts.allocation_request_bytes_upper_bound,
                encode_header + extra_encode
            );
            assert_eq!(facts.list_item_count, items);
            assert_eq!(facts.value_reference_count, refs);
            c.arm(None);
            let (_, facts) = decode_simple_node(&expected(at), read, SOURCE, limits()).unwrap();
            assert_eq!(facts.allocation_requests_upper_bound, 2 * (3 + requests));
            assert_eq!(
                facts.allocation_request_bytes_upper_bound,
                decode_header + extra_decode
            );
            assert_eq!(facts.value_reference_count, refs);
        }
    });
}
#[test]
fn simple_nodes_missing_closed_fields_refs_and_out_of_family_kinds_fail() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for at in [0, 1, 2, 6] {
            let mut node = source(at);
            match &mut node.kind {
                p::NodeKind::Filter { predicates } => predicates[0] = p::ExprId::new(99),
                p::NodeKind::Project { expressions } => expressions[0].1 = p::ValueId::new(99),
                p::NodeKind::Values { rows } => rows[0][0] = p::ExprId::new(99),
                p::NodeKind::GenerateSeries { start, .. } => *start = p::ExprId::new(99),
                _ => unreachable!(),
            };
            c.arm(None);
            assert!(matches!(
                encode_simple_node(&node, values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        }
        let mut cases = Vec::new();
        let mut node = expected(0);
        node.output = None;
        cases.push(node);
        let mut node = expected(0);
        node.output_properties = None;
        cases.push(node);
        let mut node = expected(0);
        node.output.as_mut().unwrap().node_id = None;
        cases.push(node);
        let mut node = expected(0);
        node.kind = None;
        cases.push(node);
        for missing_expr in [true, false] {
            let mut node = expected(1);
            if let Some(wire::physical_node::Kind::Project(project)) = node.kind.as_mut() {
                if missing_expr {
                    project.expressions[0].expr_id = None;
                } else {
                    project.expressions[0].value_id = None;
                }
            }
            cases.push(node);
        }
        for missing_start in [true, false] {
            let mut node = expected(6);
            if let Some(wire::physical_node::Kind::GenerateSeries(series)) = node.kind.as_mut() {
                if missing_start {
                    series.start_expr_id = None;
                } else {
                    series.stop_expr_id = None;
                }
            }
            cases.push(node);
        }
        let mut node = expected(0);
        node.kind = Some(wire::physical_node::Kind::Repeat(wire::RepeatNode {
            rollup_key_value_ids: vec![],
            grouping_sets: vec![],
            grouping_values: vec![],
            grouping_outputs: vec![],
        }));
        cases.push(node);
        for at in [0, 1, 2, 6] {
            let mut node = expected(at);
            match node.kind.as_mut() {
                Some(wire::physical_node::Kind::Filter(v)) => v.predicate_expr_ids[0] = 99,
                Some(wire::physical_node::Kind::Project(v)) => v.expressions[0].expr_id = Some(99),
                Some(wire::physical_node::Kind::Values(v)) => v.rows[0].expr_ids[0] = 99,
                Some(wire::physical_node::Kind::GenerateSeries(v)) => v.start_expr_id = Some(99),
                _ => unreachable!(),
            }
            cases.push(node);
        }
        for node in cases {
            c.arm(None);
            assert!(matches!(
                decode_simple_node(&node, read, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            assert!(!c.trace().is_empty());
        }
    });
}
fn exact(facts: SimpleNodeProjectionFacts) -> SimpleNodeProjectionLimits {
    SimpleNodeProjectionLimits {
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
fn under(mut limits: SimpleNodeProjectionLimits, at: usize) -> SimpleNodeProjectionLimits {
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
fn simple_nodes_both_directions_all_seven_exact_and_one_under_caps_are_required() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for at in [0, 1, 2, 3, 6] {
            c.arm(None);
            let (_, facts) =
                encode_simple_node(&source(at), values, expressions, SOURCE, limits()).unwrap();
            let cap = exact(facts);
            c.arm(None);
            assert!(encode_simple_node(&source(at), values, expressions, SOURCE, cap).is_ok());
            for which in 0..7 {
                c.arm(None);
                assert!(matches!(
                    encode_simple_node(&source(at), values, expressions, SOURCE, under(cap, which)),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
            }
            c.arm(None);
            let (_, facts) = decode_simple_node(&expected(at), read, SOURCE, limits()).unwrap();
            let cap = exact(facts);
            c.arm(None);
            assert!(decode_simple_node(&expected(at), read, SOURCE, cap).is_ok());
            for which in 0..7 {
                c.arm(None);
                assert!(matches!(
                    decode_simple_node(&expected(at), read, SOURCE, under(cap, which)),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
            }
        }
        let mut node = expected(2);
        if let Some(wire::physical_node::Kind::Values(values)) = node.kind.as_mut() {
            values.rows[0].expr_ids.reserve_exact(SOURCE / 2);
        }
        c.arm(None);
        assert!(matches!(
            decode_simple_node(&node, read, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        c.arm(None);
        assert!(matches!(
            encode_simple_node(&source(0), values, expressions, 0, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}
#[test]
fn simple_nodes_empty_and_ragged_representation_is_not_a_shadow_fragment_proof() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for rows in [
            Box::<[Box<[p::ExprId]>]>::default(),
            vec![
                Box::<[p::ExprId]>::default(),
                vec![p::ExprId::new(7)].into_boxed_slice(),
            ]
            .into_boxed_slice(),
        ] {
            let mut node = source(2);
            node.kind = p::NodeKind::Values { rows };
            c.arm(None);
            let (wire, _) =
                encode_simple_node(&node, values, expressions, SOURCE, limits()).unwrap();
            c.arm(None);
            let (decoded, _) = decode_simple_node(&wire, read, SOURCE, limits()).unwrap();
            assert_eq!(decoded, node);
        }
        for kind in [
            p::NodeKind::Filter {
                predicates: Box::default(),
            },
            p::NodeKind::Project {
                expressions: Box::default(),
            },
        ] {
            let mut node = source(0);
            node.kind = kind;
            c.arm(None);
            let (wire, facts) =
                encode_simple_node(&node, values, expressions, SOURCE, limits()).unwrap();
            assert_eq!(facts.allocation_requests_upper_bound, 3);
            c.arm(None);
            assert_eq!(
                decode_simple_node(&wire, read, SOURCE, limits()).unwrap().0,
                node
            );
        }
        let mut node = source(3);
        node.inputs = Box::default();
        node.required_inputs = Box::default();
        node.output.columns = Box::default();
        node.kind = p::NodeKind::Limit {
            limit: None,
            offset: 0,
        };
        c.arm(None);
        let (wire, facts) =
            encode_simple_node(&node, values, expressions, SOURCE, limits()).unwrap();
        assert_eq!(facts.allocation_requests_upper_bound, 0);
        assert_eq!(facts.allocation_request_bytes_upper_bound, 0);
        c.arm(None);
        assert_eq!(
            decode_simple_node(&wire, read, SOURCE, limits()).unwrap().0,
            node
        );
        // Filter expression 7 is a valid Value leaf, not necessarily a Boolean.
        // GenerateSeries and Values widths/roles likewise belong to Fragment.
    });
}
fn control_case(
    decode: bool,
    ordinary: bool,
    at: usize,
    stop: Option<(usize, CompileControlError)>,
) -> (Result<(), Error>, Vec<(CompilePhase, u32)>) {
    let fixture = Fixture::new();
    let c = Control::default();
    let result = fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(stop);
        if decode {
            let mut node = expected(at);
            if ordinary {
                node.output.as_mut().unwrap().value_ids[0] = 99;
            }
            decode_simple_node(&node, read, SOURCE, limits()).map(|_| ())
        } else {
            let mut node = source(at);
            if ordinary {
                node.output.columns[0] = p::ValueId::new(99);
            }
            encode_simple_node(&node, values, expressions, SOURCE, limits()).map(|_| ())
        }
    });
    (result, c.trace())
}
#[test]
fn simple_nodes_all_original_control_prefixes_and_nested_work_admission_are_observed() {
    for decode in [false, true] {
        for ordinary in [false, true] {
            for at in [0, 1, 2, 3, 6] {
                let (result, trace) = control_case(decode, ordinary, at, None);
                assert_eq!(result.is_err(), ordinary);
                assert_eq!(trace[0].1, 0);
                assert!(trace.last().is_some_and(|(_, units)| *units > 0));
                for stop in 0..trace.len() {
                    for cause in CAUSES {
                        let (result, actual) =
                            control_case(decode, ordinary, at, Some((stop, cause.clone())));
                        assert!(matches!(result,Err(Error::Control(actual))if actual==cause));
                        assert_eq!(actual, trace[..=stop]);
                    }
                }
            }
        }
    }
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c,|values,expressions,read|{
        let mut node=source(2);node.kind=p::NodeKind::Values{rows:Box::from([vec![p::ExprId::new(7);320].into_boxed_slice()])};let low=SimpleNodeProjectionLimits{max_work:1024,..limits()};
        c.arm(None);assert!(matches!(prepare_simple_node_encode(&node,values,expressions,SOURCE,low),Err(Error::Control(CompileControlError::ResourceExhausted))));assert!(!c.trace().iter().any(|(_,units)|*units==256));
        let mut wire=expected(2);if let Some(wire::physical_node::Kind::Values(v))=wire.kind.as_mut(){v.rows=vec![wire::ExpressionIds{expr_ids:vec![7;320]}];}
        c.arm(None);assert!(matches!(prepare_simple_node_decode(&wire,read,SOURCE,low),Err(Error::Control(CompileControlError::ResourceExhausted))));assert!(!c.trace().iter().any(|(_,units)|*units==256));
        // 320 real row headers, not a prescan or synthetic callback loop.
        node.kind=p::NodeKind::Values{rows:vec![Box::<[p::ExprId]>::default();320].into_boxed_slice()};
        c.arm(None);let(wire,_)=encode_simple_node(&node,values,expressions,SOURCE,limits()).unwrap();let trace=c.trace();let at=trace.iter().position(|(_,units)|*units==256).unwrap();
        for cause in CAUSES{c.arm(Some((at,cause.clone())));assert!(matches!(encode_simple_node(&node,values,expressions,SOURCE,limits()),Err(Error::Control(actual))if actual==cause));assert_eq!(c.trace(),trace[..=at]);}
        c.arm(None);decode_simple_node(&wire,read,SOURCE,limits()).unwrap();let trace=c.trace();let at=trace.iter().position(|(_,units)|*units==256).unwrap();
        for cause in CAUSES{c.arm(Some((at,cause.clone())));assert!(matches!(decode_simple_node(&wire,read,SOURCE,limits()),Err(Error::Control(actual))if actual==cause));assert_eq!(c.trace(),trace[..=at]);}
    });
}

#[path = "owned_tests.rs"]
mod owned_tests;
