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
    array::{Array, Int64Array, ListArray, MapArray, StringArray, StructArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_proto_models::physical_control_v2::Empty;
use novarocks_type_contract::{CompileControlError, FunctionValueType, SemanticParameters};
use std::sync::{Arc, Mutex};
const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
pub(crate) struct Control {
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
    pub(crate) fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.state.lock().unwrap() = (true, stop);
    }
    pub(crate) fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
pub(crate) fn limits() -> UnpivotNodeProjectionLimits {
    UnpivotNodeProjectionLimits {
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
pub(crate) struct Fixture {
    roots: Vec<(u32, FunctionValueType)>,
    definitions: Vec<p::ValueDef>,
    arena: p::ExprArena,
    parameters: SemanticParameters,
    pools: p::ConstantPools,
}
impl Fixture {
    pub(crate) fn new() -> Self {
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
            roots: vec![(0, ty)],
            definitions,
            arena,
            parameters: SemanticParameters::try_new([]).unwrap(),
            pools,
        }
    }
    pub(crate) fn with_tokens<R>(
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
fn source() -> p::PhysicalNode {
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
        kind: p::NodeKind::Unpivot {
            spec: p::UnpivotSpec {
                passthrough: Box::from([(p::ValueId::new(0), p::ValueId::new(7))]),
                value_output: p::ValueId::new(u32::MAX),
                literal_outputs: Box::from([
                    p::ValueId::new(0),
                    p::ValueId::new(7),
                    p::ValueId::new(u32::MAX),
                ]),
                mappings: Box::from([
                    p::UnpivotValueMapping {
                        input: p::ValueId::new(0),
                        constants: Box::from([
                            p::UnpivotConstant::Scalar(p::ExprId::new(0)),
                            p::UnpivotConstant::Int32List(reference(0, 1)),
                            p::UnpivotConstant::Utf8Map(reference(7, 1)),
                        ]),
                    },
                    p::UnpivotValueMapping {
                        input: p::ValueId::new(7),
                        constants: Box::from([
                            p::UnpivotConstant::Scalar(p::ExprId::new(u32::MAX)),
                            p::UnpivotConstant::Int32List(reference(0, 1)),
                            p::UnpivotConstant::Utf8Map(reference(7, 1)),
                        ]),
                    },
                ]),
                max_output_rows: u64::MAX,
                max_output_bytes: 1 << 63,
            },
        },
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
fn expected() -> wire::PhysicalNode {
    let constants = |id| {
        vec![
            wire::UnpivotConstant {
                kind: Some(wire::unpivot_constant::Kind::ScalarExprId(id)),
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
        ]
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
        kind: Some(wire::physical_node::Kind::Unpivot(wire::UnpivotNode {
            passthrough: vec![wire::ValueMapping {
                source_value_id: Some(0),
                destination_value_id: Some(7),
            }],
            value_output_id: Some(u32::MAX),
            literal_output_ids: vec![0, 7, u32::MAX],
            mappings: vec![
                wire::UnpivotMapping {
                    input_value_id: Some(0),
                    constants: constants(0),
                },
                wire::UnpivotMapping {
                    input_value_id: Some(7),
                    constants: constants(u32::MAX),
                },
            ],
            max_output_rows: u64::MAX,
            max_output_bytes: 1 << 63,
        })),
    }
}
fn spec_mut(node: &mut p::PhysicalNode) -> &mut p::UnpivotSpec {
    match &mut node.kind {
        p::NodeKind::Unpivot { spec } => spec,
        _ => panic!(),
    }
}
fn wire_spec_mut(node: &mut wire::PhysicalNode) -> &mut wire::UnpivotNode {
    match node.kind.as_mut() {
        Some(wire::physical_node::Kind::Unpivot(spec)) => spec,
        _ => panic!(),
    }
}

#[test]
fn unpivot_complete_header_and_ordered_reference_payload_match_independent_wire() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let (wire, facts) =
            encode_unpivot_node(&source(), values, expressions, SOURCE, limits()).unwrap();
        assert_eq!(wire, expected());
        assert_eq!(facts.input_node_count, 1);
        assert_eq!(facts.list_item_count, 16);
        assert_eq!(facts.value_reference_count, 11);
        c.arm(None);
        let (decoded, decode_facts) =
            decode_unpivot_node(&expected(), read, SOURCE, limits()).unwrap();
        assert_eq!(decoded, source());
        assert!(
            decode_facts.allocation_requests_upper_bound > facts.allocation_requests_upper_bound
        );
        assert!(
            decode_facts.allocation_request_bytes_upper_bound
                > facts.allocation_request_bytes_upper_bound
        );
        // Independent Layout oracle: eight nonempty emitted Vecs. Receiving
        // models each corresponding Vec plus the possible Box shrink request.
        let layout = |width: usize, count: usize| width * count;
        let encoded_bytes = layout(std::mem::size_of::<u32>(), 1 + 3 + 3)
            + layout(std::mem::size_of::<wire::PhysicalProperties>(), 1)
            + layout(std::mem::size_of::<wire::ValueMapping>(), 1)
            + layout(std::mem::size_of::<wire::UnpivotMapping>(), 2)
            + layout(std::mem::size_of::<wire::UnpivotConstant>(), 6);
        let decoded_bytes = 2
            * (layout(std::mem::size_of::<p::NodeId>(), 1)
                + layout(std::mem::size_of::<p::PhysicalProperties>(), 1)
                + layout(std::mem::size_of::<p::ValueId>(), 3 + 3)
                + layout(std::mem::size_of::<(p::ValueId, p::ValueId)>(), 1)
                + layout(std::mem::size_of::<p::UnpivotValueMapping>(), 2)
                + layout(std::mem::size_of::<p::UnpivotConstant>(), 6));
        assert_eq!(facts.allocation_requests_upper_bound, 8);
        assert_eq!(decode_facts.allocation_requests_upper_bound, 16);
        assert_eq!(facts.allocation_request_bytes_upper_bound, encoded_bytes);
        assert_eq!(
            decode_facts.allocation_request_bytes_upper_bound,
            decoded_bytes
        );
        assert!(std::ptr::eq(expressions.pools(), &fixture.pools));
        assert!(std::ptr::eq(read.pools(), &fixture.pools));
        let list = fixture.pools.entries()[&p::ConstantPoolId::new(0)]
            .value(1)
            .unwrap();
        assert_eq!(
            list.int32_list_observed(CompilePhase::Validate, &Control::default())
                .unwrap()
                .unwrap()
                .item(0, CompilePhase::Validate, &Control::default())
                .unwrap(),
            Some(-2)
        );
        let map = fixture.pools.entries()[&p::ConstantPoolId::new(7)]
            .array()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert_eq!(
            &map.keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            &"z"
        );
        assert_eq!(
            &map.values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            &"雪"
        );
    });
}
#[test]
fn unpivot_prepared_emission_keeps_original_loans_and_same_projection_facts() {
    let fixture = Fixture::new();
    let c = Control::default();
    let node = source();
    let wire = expected();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let prepared =
            prepare_unpivot_node_encode(&node, values, expressions, SOURCE, limits()).unwrap();
        let facts = *prepared.facts();
        let (output, emitted) = prepared.emit().unwrap();
        assert_eq!(facts, emitted);
        assert_eq!(output, wire);
        // Equal content in another immutable TypeTable is not this emission.
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
            prepare_unpivot_node_encode(&node, &other_values, expressions, SOURCE, limits()),
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
            prepare_unpivot_node_encode(&node, &other_values, expressions, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        c.arm(None);
        let prepared = prepare_unpivot_node_decode(&wire, read, SOURCE, limits()).unwrap();
        let facts = *prepared.facts();
        let (output, emitted) = prepared.emit().unwrap();
        assert_eq!(facts, emitted);
        assert_eq!(output, node);
    });
}
#[test]
fn unpivot_scalar_shape_and_special_selected_addresses_fail_without_reauthoring() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for id in [7, 99] {
            let mut node = source();
            spec_mut(&mut node).mappings[0].constants[0] =
                p::UnpivotConstant::Scalar(p::ExprId::new(id));
            c.arm(None);
            assert!(matches!(
                encode_unpivot_node(&node, values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            let mut wire = expected();
            wire_spec_mut(&mut wire).mappings[0].constants[0].kind =
                Some(wire::unpivot_constant::Kind::ScalarExprId(id));
            c.arm(None);
            assert!(matches!(
                decode_unpivot_node(&wire, read, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        }
        for reference in [reference(88, 0), reference(0, 2)] {
            let mut node = source();
            spec_mut(&mut node).mappings[0].constants[1] = p::UnpivotConstant::Int32List(reference);
            c.arm(None);
            assert!(matches!(
                encode_unpivot_node(&node, values, expressions, SOURCE, limits()),
                Err(Error::Constant(_))
            ));
            let mut wire = expected();
            wire_spec_mut(&mut wire).mappings[0].constants[1].kind = Some(
                wire::unpivot_constant::Kind::Int32List(encode_address(reference)),
            );
            c.arm(None);
            assert!(matches!(
                decode_unpivot_node(&wire, read, SOURCE, limits()),
                Err(Error::Constant(_))
            ));
        }
        // Address admission does not replace the Fragment collection/profile
        // author. This deliberately swapped carrier remains a checked address.
        let mut node = source();
        spec_mut(&mut node).mappings[0].constants[1] =
            p::UnpivotConstant::Int32List(reference(7, 1));
        c.arm(None);
        assert!(encode_unpivot_node(&node, values, expressions, SOURCE, limits()).is_ok());
    });
}
#[test]
fn unpivot_missing_header_value_and_closed_oneof_payloads_are_rejected() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |_, _, read| {
        let mut cases = Vec::new();
        let mut node = expected();
        node.output = None;
        cases.push(node);
        let mut node = expected();
        node.output_properties = None;
        cases.push(node);
        let mut node = expected();
        node.output.as_mut().unwrap().node_id = None;
        cases.push(node);
        let mut node = expected();
        wire_spec_mut(&mut node).value_output_id = None;
        cases.push(node);
        let mut node = expected();
        wire_spec_mut(&mut node).passthrough[0].source_value_id = None;
        cases.push(node);
        let mut node = expected();
        wire_spec_mut(&mut node).mappings[0].input_value_id = Some(99);
        cases.push(node);
        let mut node = expected();
        wire_spec_mut(&mut node).mappings[0].constants[0].kind = None;
        cases.push(node);
        let mut node = expected();
        wire_spec_mut(&mut node).mappings[0].constants[1].kind = Some(
            wire::unpivot_constant::Kind::Int32List(wire::ConstantReference {
                pool_id: None,
                row_ordinal: 1,
            }),
        );
        cases.push(node);
        for node in cases {
            c.arm(None);
            assert!(matches!(
                decode_unpivot_node(&node, read, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            assert!(!c.trace().is_empty());
        }
    });
}
#[test]
fn unpivot_all_seven_caps_exact_boundaries_and_actual_wire_capacity_are_gated() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let (_, facts) =
            encode_unpivot_node(&source(), values, expressions, SOURCE, limits()).unwrap();
        let exact = UnpivotNodeProjectionLimits {
            max_input_nodes: facts.input_node_count,
            max_value_references: facts.value_reference_count,
            max_list_items: facts.list_item_count,
            max_allocation_requests: facts.allocation_requests_upper_bound,
            max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: facts
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: facts.cumulative_work_upper_bound,
            ..limits()
        };
        c.arm(None);
        assert!(encode_unpivot_node(&source(), values, expressions, SOURCE, exact).is_ok());
        for at in 0..7 {
            let mut cap = exact;
            match at {
                0 => cap.max_input_nodes -= 1,
                1 => cap.max_value_references -= 1,
                2 => cap.max_list_items -= 1,
                3 => cap.max_allocation_requests -= 1,
                4 => cap.max_allocation_request_bytes -= 1,
                5 => cap.max_coexisting_source_and_request_bytes -= 1,
                6 => cap.max_work -= 1,
                _ => unreachable!(),
            };
            c.arm(None);
            assert!(matches!(
                encode_unpivot_node(&source(), values, expressions, SOURCE, cap),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
        c.arm(None);
        let (_, facts) = decode_unpivot_node(&expected(), read, SOURCE, limits()).unwrap();
        let exact = UnpivotNodeProjectionLimits {
            max_input_nodes: facts.input_node_count,
            max_value_references: facts.value_reference_count,
            max_list_items: facts.list_item_count,
            max_allocation_requests: facts.allocation_requests_upper_bound,
            max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: facts
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: facts.cumulative_work_upper_bound,
            ..limits()
        };
        c.arm(None);
        assert!(decode_unpivot_node(&expected(), read, SOURCE, exact).is_ok());
        for at in 0..7 {
            let mut cap = exact;
            match at {
                0 => cap.max_input_nodes -= 1,
                1 => cap.max_value_references -= 1,
                2 => cap.max_list_items -= 1,
                3 => cap.max_allocation_requests -= 1,
                4 => cap.max_allocation_request_bytes -= 1,
                5 => cap.max_coexisting_source_and_request_bytes -= 1,
                6 => cap.max_work -= 1,
                _ => unreachable!(),
            }
            c.arm(None);
            assert!(matches!(
                decode_unpivot_node(&expected(), read, SOURCE, cap),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
        let mut wide = source();
        spec_mut(&mut wide).mappings[0].constants =
            vec![p::UnpivotConstant::Scalar(p::ExprId::new(0)); 320].into_boxed_slice();
        let small_work = UnpivotNodeProjectionLimits {
            max_work: 1024,
            ..limits()
        };
        c.arm(None);
        assert!(matches!(
            prepare_unpivot_node_encode(&wide, values, expressions, SOURCE, small_work),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(
            !c.trace().iter().any(|(_, units)| *units == 256),
            "inner work must be admitted before walking constants"
        );
        let mut wide_wire = expected();
        wire_spec_mut(&mut wide_wire).mappings[0].constants = vec![
            wire::UnpivotConstant {
                kind: Some(wire::unpivot_constant::Kind::ScalarExprId(0))
            };
            320
        ];
        c.arm(None);
        assert!(matches!(
            prepare_unpivot_node_decode(&wide_wire, read, SOURCE, small_work),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(!c.trace().iter().any(|(_, units)| *units == 256));
        let mut wire = expected();
        wire_spec_mut(&mut wire).mappings[0]
            .constants
            .reserve_exact(100_000);
        c.arm(None);
        assert!(matches!(
            decode_unpivot_node(&wire, read, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        c.arm(None);
        assert!(matches!(
            encode_unpivot_node(&source(), values, expressions, 0, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
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
            let mut node = expected();
            if ordinary {
                wire_spec_mut(&mut node).mappings[0].constants[0].kind =
                    Some(wire::unpivot_constant::Kind::ScalarExprId(7));
            }
            decode_unpivot_node(&node, read, SOURCE, limits()).map(|_| ())
        } else {
            let mut node = source();
            if ordinary {
                spec_mut(&mut node).mappings[0].constants[0] =
                    p::UnpivotConstant::Scalar(p::ExprId::new(7));
            }
            encode_unpivot_node(&node, values, expressions, SOURCE, limits()).map(|_| ())
        }
    });
    (result, c.trace())
}
#[test]
fn unpivot_original_control_every_small_success_and_ordinary_prefix_and_real_quantum() {
    for decode in [false, true] {
        for ordinary in [false, true] {
            let (result, trace) = control_case(decode, ordinary, None);
            assert_eq!(result.is_err(), ordinary);
            assert_eq!(trace[0].1, 0);
            assert!(trace.last().is_some_and(|(_, units)| *units > 0));
            for at in 0..trace.len() {
                for cause in CAUSES {
                    let (result, actual) =
                        control_case(decode, ordinary, Some((at, cause.clone())));
                    assert!(matches!(result,Err(Error::Control(actual))if actual==cause));
                    assert_eq!(actual, trace[..=at]);
                }
            }
        }
    }
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c,|values,expressions,read|{
        let mut node=source();spec_mut(&mut node).mappings[0].constants=vec![p::UnpivotConstant::Scalar(p::ExprId::new(0));320].into_boxed_slice();
        c.arm(None);let(wire,_)=encode_unpivot_node(&node,values,expressions,SOURCE,limits()).unwrap();let trace=c.trace();let at=trace.iter().position(|(_,units)|*units==256).expect("actual source kind loop quantum");
        for cause in CAUSES{c.arm(Some((at,cause.clone())));assert!(matches!(encode_unpivot_node(&node,values,expressions,SOURCE,limits()),Err(Error::Control(actual))if actual==cause));assert_eq!(c.trace(),trace[..=at]);}
        c.arm(None);decode_unpivot_node(&wire,read,SOURCE,limits()).unwrap();assert!(c.trace().iter().any(|(_,units)|*units==256));
    });
}

#[path = "owned_tests.rs"]
mod owned_tests;
