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
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, ValueSource, decode_values, encode_values},
};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::{
    CompileControlError, FunctionArgumentType, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionValueType, PureCompileControl, SemanticParameters, ValueLogicalType,
};
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};
const SOURCE: usize = 8 << 20;
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
        max_type_references: 4096,
        max_request_bytes: 1 << 20,
        max_allocation_requests: 8192,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 1 << 30,
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

fn node_limits() -> TableFunctionNodeProjectionLimits {
    let mut node = limits();
    node.max_coexisting_source_and_request_bytes = 16 << 20;
    node.max_work = 2_000_000_000;
    node.properties.max_coexisting_source_and_request_bytes = 16 << 20;
    let mut binding = binding_limits();
    binding.max_coexisting_source_and_request_bytes = 16 << 20;
    binding.max_work = 2_000_000_000;
    TableFunctionNodeProjectionLimits { node, binding }
}
fn property() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
struct Fixture {
    node: p::PhysicalNode,
    roots: Vec<(u32, FunctionValueType)>,
    values: Vec<p::ValueDef>,
    arena: p::ExprArena,
    parameters: SemanticParameters,
    pools: p::ConstantPools,
    arguments: Vec<ArgumentTypeIds<'static>>,
}
impl Fixture {
    fn new(count: usize, complex: bool) -> Self {
        let value = FunctionValueType::new(DataType::Int64, true);
        let dict = FunctionValueType::new(
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        );
        let field = Arc::new(Field::new("preserved", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "unknown.tag".into(),
                "original".into(),
            )]),
        ));
        let structure = FunctionValueType::new(DataType::Struct(vec![field.clone()].into()), true);
        assert_eq!(field.metadata()["unknown.tag"], "original");
        let json =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let roots = vec![
            (0, value.clone()),
            (7, dict.clone()),
            (9, structure.clone()),
            (u32::MAX, json.clone()),
        ];
        let mut argument_types = vec![FunctionArgumentType::Value(value.clone()); count];
        let mut arguments = vec![ArgumentTypeIds::Value(0); count];
        let results = if complex {
            argument_types.push(FunctionArgumentType::Lambda {
                parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    Box::from([dict.clone(), structure.clone()]),
                ),
                result_type: json.clone(),
            });
            arguments.push(ArgumentTypeIds::Lambda {
                parameters: &[7, 9],
                result: u32::MAX,
            });
            Box::from([dict, structure, json])
        } else {
            Box::from([value.clone()])
        };
        let function = p::BoundTableFunction::from_exact_signature(
            FunctionId::try_new("test/table").unwrap(),
            FunctionOverloadId::try_new("test/table/profile").unwrap(),
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                argument_types.into_boxed_slice(),
            ),
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(results),
        );
        let node = p::PhysicalNode {
            id: p::NodeId::new(u32::MAX),
            inputs: Box::from([p::NodeId::new(0)]),
            required_inputs: Box::from([property()]),
            output_properties: property(),
            output: p::OutputPort {
                node: p::NodeId::new(u32::MAX),
                columns: Box::from([
                    p::ValueId::new(7),
                    p::ValueId::new(0),
                    p::ValueId::new(u32::MAX),
                ]),
            },
            kind: p::NodeKind::TableFunction {
                function,
                arguments: vec![p::ExprId::new(7); arguments.len()].into_boxed_slice(),
                outputs: Box::from([
                    p::TableFunctionOutput::PassThrough(p::ValueId::new(7)),
                    p::TableFunctionOutput::FunctionResult {
                        result_ordinal: 0,
                        value: p::ValueId::new(0),
                    },
                    p::TableFunctionOutput::FunctionResult {
                        result_ordinal: u32::MAX,
                        value: p::ValueId::new(u32::MAX),
                    },
                ]),
                left_outer: true,
            },
        };
        let values = [0, 7, u32::MAX]
            .into_iter()
            .map(|id| p::ValueDef {
                id: p::ValueId::new(id),
                ty: value.clone(),
                origin: p::ValueOrigin::NodeOutput {
                    node: p::NodeId::new(0),
                    output_ordinal: id,
                },
            })
            .collect();
        let arena = p::ExprArena::try_from_definitions_observed(
            [0, 7, u32::MAX].into_iter().map(|id| p::ExprNode {
                id: p::ExprId::new(id),
                owner: p::NodeId::new(u32::MAX),
                lambda_scope: None,
                ty: value.clone(),
                kind: p::ExprKind::Value(p::ValueId::new(0)),
            }),
            &p::PlanLimits::FROZEN,
            &Control::default(),
        )
        .unwrap();
        Self {
            node,
            roots,
            values,
            arena,
            arguments,
            parameters: SemanticParameters::try_new([]).unwrap(),
            pools: p::ConstantPools::empty(),
        }
    }
    fn with_tokens<R>(
        &self,
        control: &Control,
        run: impl FnOnce(
            &EncodedValues<'_, '_, '_>,
            &EncodedExpressions<'_, '_, '_>,
            &DecodedExpressions<'_, '_, '_>,
            &MaterializedFunctionBindings<'_, '_>,
        ) -> R,
    ) -> R {
        let types = encode_type_table_sources(&self.roots, &[], types_limits(), control).unwrap();
        let read_types = decode_type_table(types.as_wire(), types_limits(), control).unwrap();
        let payloads = encode_connector_payloads(&[], 64 << 10, payload_limits(), control).unwrap();
        let read_payloads =
            decode_connector_payloads(payloads.as_wire(), 64 << 10, payload_limits(), control)
                .unwrap();
        let inputs = self
            .values
            .iter()
            .map(|source| ValueSource {
                source,
                value_type_id: 0,
            })
            .collect::<Vec<_>>();
        let values = encode_values(&inputs, &payloads, &types, 256 << 10, value_limits()).unwrap();
        let read_values = decode_values(
            values.as_wire(),
            &read_payloads,
            &read_types,
            256 << 10,
            value_limits(),
        )
        .unwrap();
        let function = physical(&self.node).unwrap().function;
        let results: Vec<_> = if function.result_types.len() == 3 {
            vec![7, 9, u32::MAX]
        } else {
            vec![0]
        };
        let scalar = p::BoundFunction::from_exact_signature(
            FunctionId::try_new("test/scalar").unwrap(),
            FunctionOverloadId::try_new("test/scalar/profile").unwrap(),
            FunctionKind::Scalar,
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Value(self.roots[0].1.clone()),
            ])),
            self.roots[0].1.clone(),
        );
        // Intentional aliases retain the same original pointer; the original
        // encoder chooses the first ascending source definition.
        let binding_inputs = [
            FunctionBindingInput {
                id: 0,
                source: BindingSource::Table(function),
                arguments: &self.arguments,
                result: ResultTypeIds::Relation(&results),
            },
            FunctionBindingInput {
                id: 7,
                source: BindingSource::Scalar(&scalar),
                arguments: &[ArgumentTypeIds::Value(0)],
                result: ResultTypeIds::Scalar(0),
            },
            FunctionBindingInput {
                id: u32::MAX,
                source: BindingSource::Table(function),
                arguments: &self.arguments,
                result: ResultTypeIds::Relation(&results),
            },
        ];
        let functions = encode_function_bindings(
            &types,
            &binding_inputs,
            64 << 10,
            node_limits().binding,
            control,
        )
        .unwrap();
        let headers = prepare_function_binding_headers(
            functions.as_wire(),
            &read_types,
            64 << 10,
            node_limits().binding,
            control,
        )
        .unwrap();
        let materialized = materialize_function_bindings(
            prepare_function_bindings_materialization(&headers, 128 << 10, node_limits().binding)
                .unwrap(),
        )
        .unwrap();
        let mut empty_binding = binding_limits();
        empty_binding.max_definitions = 0;
        empty_binding.max_type_references = 0;
        let aggregates =
            encode_aggregate_bindings(&types, &functions, &[], 128 << 10, empty_binding, control)
                .unwrap();
        let aggregates_read = prepare_aggregate_binding_headers(
            aggregates.as_wire(),
            &headers,
            128 << 10,
            empty_binding,
        )
        .unwrap();
        let ids = self
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
            &ids,
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
            &headers,
            &aggregates_read,
            &self.parameters,
            &self.pools,
            512 << 10,
            expression_limits(),
        )
        .unwrap();
        run(&values, &expressions, &read, &materialized)
    }
}
fn encode(
    source: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    l: TableFunctionNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, NodeProjectionFacts), Error> {
    prepare_table_function_node_encode(source, values, expressions, SOURCE, l)?.emit()
}
fn decode(
    source: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    functions: &MaterializedFunctionBindings<'_, '_>,
    l: TableFunctionNodeProjectionLimits,
) -> Result<(p::PhysicalNode, NodeProjectionFacts), Error> {
    prepare_table_function_node_decode(source, expressions, functions, SOURCE, l)?.emit()
}
fn raw_mut(source: &mut wire::PhysicalNode) -> &mut wire::TableFunctionNode {
    match source.kind.as_mut().unwrap() {
        wire::physical_node::Kind::TableFunction(body) => body,
        _ => panic!(),
    }
}
fn assert_node(actual: &p::PhysicalNode, expected: &p::PhysicalNode) {
    assert_eq!(actual.id, expected.id);
    assert_eq!(actual.inputs, expected.inputs);
    assert_eq!(actual.output, expected.output);
    assert_eq!(actual.required_inputs, expected.required_inputs);
    assert_eq!(actual.output_properties, expected.output_properties);
    let actual = physical(actual).unwrap();
    let expected = physical(expected).unwrap();
    assert_eq!(actual.arguments, expected.arguments);
    assert_eq!(actual.outputs, expected.outputs);
    assert_eq!(actual.left_outer, expected.left_outer);
    assert_eq!(actual.function, expected.function);
    assert!(actual.function.legacy_metadata.is_none());
}
#[test]
fn complete_table_node_has_independent_sparse_ordered_wire_oracle() {
    let f = Fixture::new(1, false);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        let (wire, _) = encode(&f.node, values, expressions, node_limits()).unwrap();
        assert_eq!(wire.id, u32::MAX);
        assert_eq!(wire.input_node_ids, [0]);
        assert_eq!(wire.output.as_ref().unwrap().value_ids, [7, 0, u32::MAX]);
        let body = raw(&wire).unwrap();
        assert_eq!(body.function_binding_id, Some(0));
        assert_eq!(body.argument_expr_ids, [7]);
        assert!(body.left_outer);
        assert_eq!(
            body.outputs,
            vec![
                wire::TableFunctionOutput {
                    kind: Some(wire::table_function_output::Kind::PassthroughValueId(7))
                },
                wire::TableFunctionOutput {
                    kind: Some(wire::table_function_output::Kind::FunctionResult(
                        wire::TableFunctionResult {
                            result_ordinal: 0,
                            value_id: Some(0)
                        }
                    ))
                },
                wire::TableFunctionOutput {
                    kind: Some(wire::table_function_output::Kind::FunctionResult(
                        wire::TableFunctionResult {
                            result_ordinal: u32::MAX,
                            value_id: Some(u32::MAX)
                        }
                    ))
                }
            ]
        );
        let (actual, _) = decode(&wire, read, functions, node_limits()).unwrap();
        assert_node(&actual, &f.node);
    });
}
#[test]
fn full_relation_signature_preserves_lambda_dictionary_metadata_and_nominal_types() {
    let f = Fixture::new(1, true);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        let (wire, _) = encode(&f.node, values, expressions, node_limits()).unwrap();
        let (actual, _) = decode(&wire, read, functions, node_limits()).unwrap();
        assert_node(&actual, &f.node);
        let a = physical(&actual).unwrap().function;
        let original = match functions.definitions()[0].1.as_source() {
            BindingSource::Table(v) => v,
            _ => panic!(),
        };
        let DataType::Struct(fields) = &a.result_types[1].data_type() else {
            panic!()
        };
        let DataType::Struct(original_fields) = &original.result_types[1].data_type() else {
            panic!()
        };
        assert!(Arc::ptr_eq(&fields[0], &original_fields[0]));
        assert!(fields[0].metadata().as_arc().is_none());
        assert_eq!(fields[0].name(), "preserved");
        assert!(fields[0].is_nullable());
        assert_eq!(a.result_types[2].logical_type(), ValueLogicalType::Json);
        let FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } = &a.argument_types[1]
        else {
            panic!()
        };
        assert_eq!(parameter_types.len(), 2);
        assert_eq!(result_type.logical_type(), ValueLogicalType::Json);
    });
}
#[test]
fn zero_arguments_and_both_output_variants_are_preserved_without_defaults() {
    let f = Fixture::new(0, false);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        let (wire, _) = encode(&f.node, values, expressions, node_limits()).unwrap();
        assert!(raw(&wire).unwrap().argument_expr_ids.is_empty());
        let (actual, _) = decode(&wire, read, functions, node_limits()).unwrap();
        assert_node(&actual, &f.node);
    });
}
#[test]
fn missing_unknown_wrong_kind_and_foreign_equal_source_are_ordinary_refusals() {
    let f = Fixture::new(1, false);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        let foreign = f.node.clone();
        assert!(matches!(
            encode(&foreign, values, expressions, node_limits()),
            Err(Error::Binding(_))
        ));
        let (wire, _) = encode(&f.node, values, expressions, node_limits()).unwrap();
        for at in 0..11 {
            let mut bad = wire.clone();
            match at {
                0 => bad.output = None,
                1 => bad.output_properties = None,
                2 => raw_mut(&mut bad).function_binding_id = None,
                3 => raw_mut(&mut bad).function_binding_id = Some(7),
                4 => raw_mut(&mut bad).argument_expr_ids[0] = 8,
                5 => raw_mut(&mut bad).outputs[0].kind = None,
                6 => {
                    raw_mut(&mut bad).outputs[1].kind =
                        Some(wire::table_function_output::Kind::FunctionResult(
                            wire::TableFunctionResult {
                                result_ordinal: 0,
                                value_id: None,
                            },
                        ))
                }
                7 => bad.kind = None,
                8 => raw_mut(&mut bad).function_binding_id = Some(8),
                9 => bad.output.as_mut().unwrap().node_id = None,
                10 => {
                    raw_mut(&mut bad).outputs[0].kind =
                        Some(wire::table_function_output::Kind::PassthroughValueId(8))
                }
                _ => unreachable!(),
            };
            assert!(matches!(
                decode(&bad, read, functions, node_limits()),
                Err(Error::InvalidShape(_))
            ));
        }
    });
}
fn tight(f: NodeProjectionFacts) -> NodeProjectionLimits {
    let mut l = node_limits().node;
    l.max_input_nodes = f.input_node_count;
    l.max_value_references = f.value_reference_count;
    l.max_list_items = f.list_item_count;
    l.max_allocation_requests = f.allocation_requests_upper_bound;
    l.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound;
    l.max_coexisting_source_and_request_bytes = f.coexisting_source_and_request_bytes_upper_bound;
    l.max_work = f.cumulative_work_upper_bound;
    l
}
#[test]
fn independent_request_layout_golden_and_every_node_axis_exact_or_one_under() {
    let f = Fixture::new(1, false);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        let (wire, sent) = encode(&f.node, values, expressions, node_limits()).unwrap();
        let (_, received) = decode(&wire, read, functions, node_limits()).unwrap();
        let sent_bytes = Layout::array::<u32>(1).unwrap().size()
            + Layout::array::<wire::PhysicalProperties>(1).unwrap().size()
            + Layout::array::<u32>(3).unwrap().size()
            + Layout::array::<u32>(1).unwrap().size()
            + Layout::array::<wire::TableFunctionOutput>(3)
                .unwrap()
                .size();
        assert_eq!(sent.allocation_requests_upper_bound, 5);
        assert_eq!(sent.allocation_request_bytes_upper_bound, sent_bytes);
        assert_eq!(sent.value_reference_count, 6);
        // Only the five physical node collections are newly constructed.
        // Argument/results and identity holders retain the original signature.
        let receiver_bytes = 2
            * (Layout::array::<p::NodeId>(1).unwrap().size()
                + Layout::array::<p::PhysicalProperties>(1).unwrap().size()
                + Layout::array::<p::ValueId>(3).unwrap().size()
                + Layout::array::<p::ExprId>(1).unwrap().size()
                + Layout::array::<p::TableFunctionOutput>(3).unwrap().size());
        assert_eq!(received.allocation_requests_upper_bound, 10);
        assert_eq!(
            received.allocation_request_bytes_upper_bound,
            receiver_bytes
        );
        for (receive, facts) in [(false, sent), (true, received)] {
            let mut exact = node_limits();
            exact.node = tight(facts);
            if receive {
                decode(&wire, read, functions, exact).unwrap();
            } else {
                encode(&f.node, values, expressions, exact).unwrap();
            }
            for axis in 0..7 {
                let mut l = exact;
                let cap = match axis {
                    0 => &mut l.node.max_input_nodes,
                    1 => &mut l.node.max_value_references,
                    2 => &mut l.node.max_list_items,
                    3 => &mut l.node.max_allocation_requests,
                    4 => &mut l.node.max_allocation_request_bytes,
                    5 => &mut l.node.max_coexisting_source_and_request_bytes,
                    6 => &mut l.node.max_work,
                    _ => unreachable!(),
                };
                assert!(*cap > 0);
                *cap -= 1;
                let result = if receive {
                    decode(&wire, read, functions, l).map(|_| ())
                } else {
                    encode(&f.node, values, expressions, l).map(|_| ())
                };
                assert!(matches!(
                    result,
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
            }
        }
    });
}
#[test]
fn actual_source_floors_include_wire_capacity_and_foreign_receiving_headers_refuse() {
    let f = Fixture::new(1, false);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        let (mut wire, _) = encode(&f.node, values, expressions, node_limits()).unwrap();
        raw_mut(&mut wire).argument_expr_ids.reserve(1024);
        let known = wire_header_floor(&wire, wire.output.as_ref().unwrap()).unwrap()
            + bytes::<u32>(raw(&wire).unwrap().argument_expr_ids.capacity()).unwrap()
            + bytes::<wire::TableFunctionOutput>(raw(&wire).unwrap().outputs.capacity()).unwrap();
        assert!(matches!(
            prepare_table_function_node_decode(&wire, read, functions, known - 1, node_limits()),
            Err(Error::InvalidShape(_))
        ));
        let foreign_headers = prepare_function_binding_headers(
            read.functions().as_wire(),
            read.types(),
            64 << 10,
            node_limits().binding,
            &control,
        )
        .unwrap();
        let foreign = materialize_function_bindings(
            prepare_function_bindings_materialization(
                &foreign_headers,
                128 << 10,
                node_limits().binding,
            )
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            decode(&wire, read, &foreign, node_limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}
fn prefixes(control: &Control, mut invoke: impl FnMut() -> Result<(), Error>, success: bool) {
    control.arm(None);
    assert_eq!(invoke().is_ok(), success);
    let trace = control.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            control.arm(Some((at, cause.clone())));
            assert!(matches!(invoke(),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
    control.arm(None);
}
#[test]
fn every_actual_small_callback_preserves_three_causes_and_ordinary_footer() {
    let f = Fixture::new(1, false);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        let (wire, _) = encode(&f.node, values, expressions, node_limits()).unwrap();
        prefixes(
            &control,
            || encode(&f.node, values, expressions, node_limits()).map(|_| ()),
            true,
        );
        prefixes(
            &control,
            || decode(&wire, read, functions, node_limits()).map(|_| ()),
            true,
        );
        let mut bad = wire.clone();
        raw_mut(&mut bad).argument_expr_ids[0] = 8;
        control.arm(None);
        assert!(decode(&bad, read, functions, node_limits()).is_err());
        assert!(control.trace().last().unwrap().1 > 0);
        prefixes(
            &control,
            || decode(&bad, read, functions, node_limits()).map(|_| ()),
            false,
        );
    });
}
#[test]
fn wide_actual_counts_have_real_quantum_and_numeric_refusal_has_no_late_callback() {
    let f = Fixture::new(320, false);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        control.arm(None);
        let (wire, _) = encode(&f.node, values, expressions, node_limits()).unwrap();
        let sent = control.trace();
        assert!(sent.iter().any(|(_, units)| *units == 256));
        control.arm(None);
        decode(&wire, read, functions, node_limits()).unwrap();
        let received = control.trace();
        assert!(received.iter().any(|(_, units)| *units == 256));
        for receive in [false, true] {
            let trace = if receive { &received } else { &sent };
            let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
            for at in [0, quantum, trace.len() - 1] {
                for cause in CAUSES {
                    control.arm(Some((at, cause.clone())));
                    let result = if receive {
                        decode(&wire, read, functions, node_limits()).map(|_| ())
                    } else {
                        encode(&f.node, values, expressions, node_limits()).map(|_| ())
                    };
                    assert!(matches!(result,Err(Error::Control(actual)) if actual==cause));
                    assert_eq!(control.trace(), trace[..=at]);
                }
            }
        }
        let mut l = node_limits();
        l.node.max_list_items = 0;
        control.arm(Some((1, CompileControlError::Cancelled)));
        assert!(matches!(
            prepare_table_function_node_decode(&wire, read, functions, SOURCE, l),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(control.trace(), [(CompilePhase::Decode, 0)]);
    });
}

fn caller_table_encode(
    source: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    l: TableFunctionNodeProjectionLimits,
    control: &Control,
    parent: &mut NodeAdmit<'_>,
) -> Result<(wire::PhysicalNode, NodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let token = prepare_table_function_node_encode_in(
            source,
            values,
            expressions,
            SOURCE,
            l,
            parent,
            &mut work,
        )?;
        token.emit_in(parent, &mut work)
    })();
    finish(work, result)
}
fn caller_table_decode(
    source: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    functions: &MaterializedFunctionBindings<'_, '_>,
    l: TableFunctionNodeProjectionLimits,
    control: &Control,
    parent: &mut NodeAdmit<'_>,
) -> Result<(p::PhysicalNode, NodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let token = prepare_table_function_node_decode_in(
            source,
            expressions,
            functions,
            SOURCE,
            l,
            parent,
            &mut work,
        )?;
        token.emit_in(parent, &mut work)
    })();
    finish(work, result)
}
fn caller_table_axes(f: NodeProjectionFacts) -> [usize; 7] {
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
fn table_caller_owned_full_lambda_dictionary_relation_seals_prefixes_and_replays_exact() {
    let f = Fixture::new(1, true);
    let control = Control::default();
    f.with_tokens(&control,|values,expressions,read,functions| {
        let mut sent=Vec::new();
        let (wire,sf)=caller_table_encode(&f.node,values,expressions,node_limits(),&control,&mut |f|{sent.push(*f);Ok(())}).unwrap();
        let body=raw(&wire).unwrap();
        assert_eq!(body.function_binding_id,Some(0));
        assert_eq!(body.argument_expr_ids,[7,7]);
        assert!(body.left_outer);
        assert_eq!(body.outputs.len(),3);
        let mut received=Vec::new();
        let (owned,rf)=caller_table_decode(&wire,read,functions,node_limits(),&control,&mut |f|{received.push(*f);Ok(())}).unwrap();
        assert_node(&owned,&f.node);
        assert_eq!(physical(&owned).unwrap().function.result_types.len(),3);
        assert!(matches!(&physical(&owned).unwrap().function.argument_types[1],FunctionArgumentType::Lambda{parameter_types,result_type} if parameter_types.len()==2 && result_type.logical_type()==ValueLogicalType::Json));
        for (prefixes,final_facts) in [(&sent,sf),(&received,rf)] {
            assert!(!prefixes.is_empty());
            for prefix in prefixes {
                assert!(caller_table_axes(*prefix).into_iter().zip(caller_table_axes(final_facts)).all(|(p,f)|p<=f));
            }
        }
        for receive in [false,true] {
            let bound=if receive {rf}else{sf};let mut l=node_limits();
            l.node.max_input_nodes=bound.input_node_count;
            l.node.max_value_references=bound.value_reference_count;
            l.node.max_list_items=bound.list_item_count;
            l.node.max_allocation_requests=bound.allocation_requests_upper_bound;
            l.node.max_allocation_request_bytes=bound.allocation_request_bytes_upper_bound;
            l.node.max_coexisting_source_and_request_bytes=bound.coexisting_source_and_request_bytes_upper_bound;
            l.node.max_work=bound.cumulative_work_upper_bound;
            let replay=if receive {caller_table_decode(&wire,read,functions,l,&control,&mut |_|Ok(())).map(|r|r.1)} else {caller_table_encode(&f.node,values,expressions,l,&control,&mut |_|Ok(())).map(|r|r.1)};
            assert_eq!(replay.unwrap(),bound);
        }
    });
}
#[test]
fn table_caller_owned_actual_success_ordinary_prefixes_and_foreign_control() {
    let f = Fixture::new(1, false);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        let wire = caller_table_encode(
            &f.node,
            values,
            expressions,
            node_limits(),
            &control,
            &mut |_| Ok(()),
        )
        .unwrap()
        .0;
        prefixes(
            &control,
            || {
                caller_table_encode(
                    &f.node,
                    values,
                    expressions,
                    node_limits(),
                    &control,
                    &mut |_| Ok(()),
                )
                .map(|_| ())
            },
            true,
        );
        prefixes(
            &control,
            || {
                caller_table_decode(&wire, read, functions, node_limits(), &control, &mut |_| {
                    Ok(())
                })
                .map(|_| ())
            },
            true,
        );
        let mut bad = wire.clone();
        raw_mut(&mut bad).function_binding_id = Some(8);
        prefixes(
            &control,
            || {
                caller_table_decode(&bad, read, functions, node_limits(), &control, &mut |_| {
                    Ok(())
                })
                .map(|_| ())
            },
            false,
        );
        let foreign = Control::default();
        let mut callbacks = 0;
        assert!(matches!(
            caller_table_encode(
                &f.node,
                values,
                expressions,
                node_limits(),
                &foreign,
                &mut |_| {
                    callbacks += 1;
                    Ok(())
                }
            ),
            Err(Error::InvalidShape(_))
        ));
        assert_eq!(callbacks, 0);
        assert!(matches!(
            caller_table_encode(
                &f.node.clone(),
                values,
                expressions,
                node_limits(),
                &control,
                &mut |_| Ok(())
            ),
            Err(Error::Binding(_))
        ));
    });
}
#[test]
fn table_caller_owned_known_root_requests_win_at_pending255_and_wide_is_actual() {
    let f = Fixture::new(320, false);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        control.arm(None);
        let (wire, _) = caller_table_encode(
            &f.node,
            values,
            expressions,
            node_limits(),
            &control,
            &mut |_| Ok(()),
        )
        .unwrap();
        let sent = control.trace();
        assert!(sent.iter().any(|(_, u)| *u == 256));
        assert_eq!(raw(&wire).unwrap().argument_expr_ids, vec![7; 320]);
        control.arm(None);
        let (owned, _) =
            caller_table_decode(&wire, read, functions, node_limits(), &control, &mut |_| {
                Ok(())
            })
            .unwrap();
        let received = control.trace();
        assert!(received.iter().any(|(_, u)| *u == 256));
        assert_eq!(physical(&owned).unwrap().arguments.len(), 320);
        for receive in [false, true] {
            let trace = if receive { &received } else { &sent };
            let quantum = trace.iter().position(|(_, u)| *u == 256).unwrap();
            for at in [0, quantum, trace.len() - 1] {
                for cause in CAUSES {
                    control.arm(Some((at, cause.clone())));
                    let result = if receive {
                        caller_table_decode(
                            &wire,
                            read,
                            functions,
                            node_limits(),
                            &control,
                            &mut |_| Ok(()),
                        )
                        .map(|_| ())
                    } else {
                        caller_table_encode(
                            &f.node,
                            values,
                            expressions,
                            node_limits(),
                            &control,
                            &mut |_| Ok(()),
                        )
                        .map(|_| ())
                    };
                    assert!(matches!(result,Err(Error::Control(actual)) if actual==cause));
                    assert_eq!(control.trace(), trace[..=at]);
                }
            }
            for cause in CAUSES {
                control.arm(Some((1, cause)));
                let phase = if receive {
                    CompilePhase::Decode
                } else {
                    CompilePhase::Encode
                };
                let mut work = CompileCheckpoints::try_new(&control, phase).unwrap();
                for _ in 0..255 {
                    work.step().unwrap();
                }
                let mut parent = |f: &NodeProjectionFacts| {
                    if f.allocation_requests_upper_bound > 0 {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                };
                let result = if receive {
                    prepare_table_function_node_decode_in(
                        &wire,
                        read,
                        functions,
                        SOURCE,
                        node_limits(),
                        &mut parent,
                        &mut work,
                    )
                    .map(|_| ())
                } else {
                    prepare_table_function_node_encode_in(
                        &f.node,
                        values,
                        expressions,
                        SOURCE,
                        node_limits(),
                        &mut parent,
                        &mut work,
                    )
                    .map(|_| ())
                };
                assert!(matches!(
                    result,
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(control.trace(), [(phase, 0)]);
            }
        }
    });
}

#[test]
fn table_caller_parent_seven_axes_known_refusal_precedes_late_control() {
    let f = Fixture::new(1, true);
    let control = Control::default();
    f.with_tokens(&control, |values, expressions, read, functions| {
        control.arm(None);
        let wire = caller_table_encode(
            &f.node,
            values,
            expressions,
            node_limits(),
            &control,
            &mut |_| Ok(()),
        )
        .unwrap()
        .0;
        for receive in [false, true] {
            control.arm(None);
            let mut prefixes = Vec::new();
            let mut collect = |f: &NodeProjectionFacts| {
                prefixes.push((*f, control.trace().len()));
                Ok(())
            };
            let facts = if receive {
                caller_table_decode(
                    &wire,
                    read,
                    functions,
                    node_limits(),
                    &control,
                    &mut collect,
                )
                .unwrap()
                .1
            } else {
                caller_table_encode(
                    &f.node,
                    values,
                    expressions,
                    node_limits(),
                    &control,
                    &mut collect,
                )
                .unwrap()
                .1
            };
            let trace = control.trace();
            for axis in 0..7 {
                let bound = caller_table_axes(facts)[axis];
                assert!(bound > 0);
                let marker = prefixes
                    .iter()
                    .find(|(f, _)| caller_table_axes(*f)[axis] > bound - 1)
                    .unwrap()
                    .1;
                for cause in CAUSES {
                    control.arm(Some((marker, cause)));
                    let mut parent = |f: &NodeProjectionFacts| {
                        if caller_table_axes(*f)[axis] > bound - 1 {
                            Err(CompileControlError::ResourceExhausted)
                        } else {
                            Ok(())
                        }
                    };
                    let result = if receive {
                        caller_table_decode(
                            &wire,
                            read,
                            functions,
                            node_limits(),
                            &control,
                            &mut parent,
                        )
                        .map(|_| ())
                    } else {
                        caller_table_encode(
                            &f.node,
                            values,
                            expressions,
                            node_limits(),
                            &control,
                            &mut parent,
                        )
                        .map(|_| ())
                    };
                    assert!(matches!(
                        result,
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                    assert_eq!(control.trace(), trace[..marker]);
                }
            }
        }
    });
}
