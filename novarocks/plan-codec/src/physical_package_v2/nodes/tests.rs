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
        encode_aggregate_bindings, materialize_aggregate_bindings,
        prepare_aggregate_binding_headers, prepare_aggregate_bindings_materialization,
    },
    physical_binding_v2::{
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
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_provider_binding_v2::{
        ProviderBindingProjectionLimits, decode_provider_bindings, encode_provider_bindings,
    },
    physical_provider_read_v2::{
        ProviderReadProjectionLimits, decode_provider_reads, encode_provider_reads,
    },
    physical_relation_v2::{decode_relations, encode_relations},
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
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, SemanticParameters,
};
use std::sync::{Arc, Mutex};
// Declared conservative source invoice, not a measured backing/MEM receipt.
const SOURCE: usize = 8 << 20;
// Every invoked component request-byte cap is at most one MiB.
const COEX: usize = SOURCE + (1 << 20);
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
fn limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 8,
        max_value_references: 8192,
        max_list_items: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 1 << 20,
        max_coexisting_source_and_request_bytes: COEX,
        max_work: 64 << 20,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 8192,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 1 << 20,
            max_coexisting_source_and_request_bytes: COEX,
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
        max_coexisting_source_and_request_bytes: COEX,
        max_work: 1 << 20,
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 0,
        max_payload_bytes: 0,
        max_allocation_requests: 0,
        max_allocation_request_bytes: 0,
        max_coexisting_source_and_request_bytes: COEX,
        max_work: 1 << 20,
    }
}
fn value_limits() -> ValueProjectionLimits {
    ValueProjectionLimits {
        max_definitions: 8,
        max_origin_references: 32,
        max_allocation_requests: 64,
        max_allocation_request_bytes: 64 << 10,
        max_coexisting_source_and_request_bytes: COEX,
        max_work: 16 << 20,
        origins: ValueOriginProjectionLimits {
            max_allocation_requests: 16,
            max_allocation_request_bytes: 64 << 10,
            max_coexisting_source_and_request_bytes: COEX,
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
        max_coexisting_source_and_request_bytes: COEX,
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
        run: impl FnOnce(NodeEncodeContext<'_, '_, '_, '_>, NodeDecodeContext<'_, '_, '_, '_>) -> R,
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
        let bindings = encode_provider_bindings(&[], 64 << 10, provider_limits(), control).unwrap();
        let read_bindings =
            decode_provider_bindings(bindings.as_wire(), 64 << 10, provider_limits(), control)
                .unwrap();
        let reads =
            encode_provider_reads(&[], &bindings, &payloads, 256 << 10, read_limits()).unwrap();
        let read_reads = decode_provider_reads(
            reads.as_wire(),
            &read_bindings,
            &read_payloads,
            256 << 10,
            read_limits(),
        )
        .unwrap();
        let relations =
            encode_relations(&[], &reads, &types, 512 << 10, relation_limits()).unwrap();
        let read_relations = decode_relations(
            relations.as_wire(),
            &read_reads,
            &read_types,
            512 << 10,
            relation_limits(),
        )
        .unwrap();
        // Retain the original source invoice and the actual prepared header
        // stock. Passing its old raw-source invoice alone omits the header.
        let function_source = functions_read.retained_invoice_floor().unwrap();
        let materialized_functions = materialize_function_bindings(
            prepare_function_bindings_materialization(
                &functions_read,
                function_source,
                binding_limits(),
            )
            .unwrap(),
        )
        .unwrap();
        let aggregate_source = aggregates_read
            .retained_invoice_floor()
            .unwrap()
            .checked_add(materialized_functions.retained_output_floor().unwrap())
            .unwrap();
        let materialized_aggregates = materialize_aggregate_bindings(
            prepare_aggregate_bindings_materialization(
                &aggregates_read,
                &materialized_functions,
                aggregate_source,
                binding_limits(),
            )
            .unwrap(),
        )
        .unwrap();
        run(
            NodeEncodeContext {
                values: &values,
                expressions: &expressions,
                relations: &relations,
            },
            NodeDecodeContext {
                expressions: &read,
                relations: &read_relations,
                functions: &materialized_functions,
                aggregates: &materialized_aggregates,
            },
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

fn provider_limits() -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: 0,
        max_allocation_requests: 8,
        max_allocation_request_bytes: 65536,
        max_coexisting_source_and_request_bytes: COEX,
        max_work: 1 << 20,
    }
}
fn read_limits() -> ProviderReadProjectionLimits {
    ProviderReadProjectionLimits {
        max_definitions: 0,
        max_input_version_bytes: 0,
        max_allocation_requests: 8,
        max_allocation_request_bytes: 65536,
        max_coexisting_source_and_request_bytes: COEX,
        max_work: 1 << 20,
    }
}
fn relation_limits() -> RelationProjectionLimits {
    RelationProjectionLimits {
        max_definitions: 0,
        max_schema_fields: 0,
        max_predicate_guarantees: 0,
        max_metadata_kind_bytes: 0,
        max_coverage_bytes: 0,
        max_allocation_requests: 8,
        max_allocation_request_bytes: 65536,
        max_coexisting_source_and_request_bytes: COEX,
        max_work: 8 << 20,
        properties: limits().properties,
    }
}
fn dispatch_limits() -> NodeDispatchLimits {
    NodeDispatchLimits {
        node: limits(),
        binding: binding_limits(),
        relation: relation_limits(),
        writer_schema: WriterSchemaProjectionLimits {
            max_fields: 8,
            max_name_bytes: 1024,
            max_type_references: 16,
            max_allocation_requests: 32,
            max_allocation_request_bytes: 65536,
            max_coexisting_source_and_request_bytes: COEX,
            max_work: 64 << 20,
        },
    }
}
fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, NodeCodecError>,
) -> Result<T, NodeCodecError> {
    if matches!(&result, Err(NodeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn encode(
    n: &p::PhysicalNode,
    ctx: &NodeEncodeContext<'_, '_, '_, '_>,
    c: &Control,
    admit: &mut NodeAdmit<'_>,
) -> Result<(wire::PhysicalNode, NodeProjectionFacts), NodeCodecError> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::LowerProgram)?;
    let result = (|| {
        let token =
            prepare_node_encode_in(n, ctx, None, SOURCE, dispatch_limits(), admit, &mut work)?;
        let expected_facts = *token.facts();
        let result = token.emit_in(admit, &mut work)?;
        assert_eq!(result.1, expected_facts);
        Ok(result)
    })();
    finish(work, result)
}
fn decode(
    n: &wire::PhysicalNode,
    ctx: &NodeDecodeContext<'_, '_, '_, '_>,
    c: &Control,
    admit: &mut NodeAdmit<'_>,
) -> Result<(p::PhysicalNode, NodeProjectionFacts), NodeCodecError> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::LowerProgram)?;
    let result = (|| {
        let token = prepare_node_decode_in(n, ctx, SOURCE, dispatch_limits(), admit, &mut work)?;
        let expected_facts = *token.facts();
        let result = token.emit_in(admit, &mut work)?;
        assert_eq!(result.1, expected_facts);
        Ok(result)
    })();
    finish(work, result)
}

#[test]
fn dispatcher_simple_family_preserves_independent_sparse_wire_and_original_token_facts() {
    let c = Control::default();
    Fixture::new().with_tokens(&c, |send, receive| {
        for at in 0..8 {
            c.arm(None);
            let source = source(at);
            let mut snapshots = Vec::new();
            let output = encode(&source, &send, &c, &mut |f| {
                snapshots.push(*f);
                Ok(())
            })
            .unwrap();
            assert_eq!(output.0, expected(at));
            assert_eq!(snapshots.last(), Some(&output.1));
            c.arm(None);
            let output = decode(&expected(at), &receive, &c, &mut |_| Ok(())).unwrap();
            assert_eq!(output.0, source);
            assert!(
                c.trace()
                    .iter()
                    .all(|(phase, _)| *phase == CompilePhase::LowerProgram)
            );
        }
    });
}

#[test]
fn dispatcher_actual_repeat_uses_original_value_namespace_and_ordered_mapping() {
    let c = Control::default();
    Fixture::new().with_tokens(&c, |send, receive| {
        let mut n = source(3);
        n.kind = p::NodeKind::Repeat {
            rollup_keys: Box::from([p::ValueId::new(0)]),
            grouping_sets: Box::from([
                Box::from([p::ValueId::new(0)]),
                Box::<[p::ValueId]>::default(),
            ]),
            grouping_values: Box::from([(p::ValueId::new(0), p::ValueId::new(7))]),
            grouping_outputs: Box::from([p::GroupingOutput {
                output: p::ValueId::new(u32::MAX),
                arguments: Box::from([p::ValueId::new(0)]),
            }]),
        };
        let mut raw = expected(3);
        raw.kind = Some(wire::physical_node::Kind::Repeat(wire::RepeatNode {
            rollup_key_value_ids: vec![0],
            grouping_sets: vec![
                wire::ValueIds { value_ids: vec![0] },
                wire::ValueIds { value_ids: vec![] },
            ],
            grouping_values: vec![wire::ValueMapping {
                source_value_id: Some(0),
                destination_value_id: Some(7),
            }],
            grouping_outputs: vec![wire::GroupingOutput {
                output_value_id: Some(u32::MAX),
                argument_value_ids: vec![0],
            }],
        }));
        c.arm(None);
        assert_eq!(encode(&n, &send, &c, &mut |_| Ok(())).unwrap().0, raw);
        c.arm(None);
        assert_eq!(decode(&raw, &receive, &c, &mut |_| Ok(())).unwrap().0, n);
    });
}

#[test]
fn dispatcher_foreign_control_and_missing_wire_kind_refuse_without_parent_admission() {
    let c = Control::default();
    let foreign = Control::default();
    Fixture::new().with_tokens(&c, |send, receive| {
        foreign.arm(None);
        let mut parent_calls = 0;
        let error = encode(&source(3), &send, &foreign, &mut |_| {
            parent_calls += 1;
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(
            error,
            NodeCodecError::InvalidShape("node dispatcher uses a different original control")
        ));
        assert_eq!(parent_calls, 0);
        assert_eq!(foreign.trace().len(), 2); // Caller entry and ordinary footer.
        c.arm(None);
        let mut raw = expected(3);
        raw.kind = None;
        let error = decode(&raw, &receive, &c, &mut |_| {
            parent_calls += 1;
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(
            error,
            NodeCodecError::InvalidShape("node dispatcher has no node kind")
        ));
        assert_eq!(parent_calls, 0);
        assert_eq!(c.trace().len(), 2);
    });
}

#[test]
fn dispatcher_every_actual_success_and_ordinary_callback_preserves_caller_first_cause() {
    let c = Control::default();
    Fixture::new().with_tokens(&c, |send, receive| {
        for receiving in [false, true] {
            for malformed in [false, true] {
                let mut n = source(3);
                let mut raw = expected(3);
                if malformed {
                    n.output.columns[0] = p::ValueId::new(42);
                    raw.output.as_mut().unwrap().value_ids[0] = 42;
                }
                let invoke = || {
                    if receiving { decode(&raw, &receive, &c, &mut |_| Ok(())).map(|_| ()) }
                    else { encode(&n, &send, &c, &mut |_| Ok(())).map(|_| ()) }
                };
                c.arm(None);
                let baseline = invoke();
                assert_eq!(baseline.is_ok(), !malformed);
                assert!(!matches!(baseline, Err(NodeCodecError::Control(_))));
                let trace = c.trace();
                assert!(!trace.is_empty());
                for at in 0..trace.len() {
                    for cause in CAUSES {
                        c.arm(Some((at, cause.clone())));
                        assert!(matches!(invoke(), Err(NodeCodecError::Control(actual)) if actual == cause));
                        assert_eq!(c.trace(), trace[..=at]);
                    }
                }
            }
        }
    });
}
