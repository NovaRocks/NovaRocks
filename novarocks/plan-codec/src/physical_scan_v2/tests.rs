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
    physical_provider_binding_v2::{
        ProviderBindingProjectionLimits, decode_provider_bindings, encode_provider_bindings,
    },
    physical_provider_read_v2::{
        ProviderReadProjectionLimits, decode_provider_reads, encode_provider_reads,
    },
    physical_relation_v2::{RelationSource, decode_relations, encode_relations},
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, ValueSource, decode_values, encode_values},
};
use arrow::datatypes::DataType;
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorReadInputVersion,
    ConnectorReadRelationKind, ConnectorReadRelationPayload,
};
use novarocks_proto_models::physical_control_v2::Empty;
use novarocks_type_contract::{
    CompileControlError, FunctionValueType, PureCompileControl, SemanticParameters,
};
use std::sync::{Arc, Mutex};
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
fn property_limits() -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: 4096,
        max_allocation_requests: 128,
        max_allocation_request_bytes: 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn relation_limits() -> RelationProjectionLimits {
    RelationProjectionLimits {
        max_definitions: 1024,
        max_schema_fields: 4096,
        max_predicate_guarantees: 4096,
        max_metadata_kind_bytes: 4096,
        max_coverage_bytes: 1024 * 1024,
        max_allocation_requests: 32768,
        max_allocation_request_bytes: 16 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 32 * 1024 * 1024,
        max_work: 256 * 1024 * 1024,
        properties: property_limits(),
    }
}
fn provider_limits() -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 4096,
        max_payload_bytes: 1024 * 1024,
        max_allocation_requests: 65536,
        max_allocation_request_bytes: 8 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 16 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn read_limits() -> ProviderReadProjectionLimits {
    ProviderReadProjectionLimits {
        max_definitions: 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn types_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100000,
        max_expanded_nodes: 100000,
        max_string_bytes: 1024 * 1024,
    }
}
fn binding() -> ConnectorReadBinding {
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::try_from_canonical("lake").unwrap(),
        },
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("lake").unwrap(),
            CatalogVersion::from_bytes([0; 32]),
        ),
    )
}
fn payload(kind: ConnectorCodecCategory, body: &[u8]) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([0; 32]),
            ),
            kind,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        body.to_vec().into(),
    )
}
fn read() -> p::ProviderReadReference {
    p::ProviderReadReference {
        binding: binding(),
        input_version: ConnectorReadInputVersion::try_new(Arc::<[u8]>::from([0, 255].as_slice()))
            .unwrap(),
        relation: ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            payload(ConnectorCodecCategory::ReadTable, b"table"),
            payload(ConnectorCodecCategory::ReadView, b"view"),
        ),
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

fn limits() -> ScanProjectionLimits {
    ScanProjectionLimits {
        node: NodeProjectionLimits {
            max_input_nodes: 8,
            max_value_references: 8192,
            max_list_items: 8192,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 4 << 20,
            max_coexisting_source_and_request_bytes: 16 << 20,
            max_work: 256 << 20,
            properties: property_limits(),
        },
        relation: relation_limits(),
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
    let ty = FunctionValueType::new(DataType::Int64, true);
    let relation = p::Relation::Data(p::DataRelation {
        read: read(),

        selection_digest: [255; 32],
        schema: Box::from([p::RelationField {
            column: p::ProviderColumnReference {
                column_payload: payload(ConnectorCodecCategory::ReadColumn, b"column / \xc3\xa9"),
            },
            ty,
        }]),
        predicate_guarantees: Box::from([p::PredicateGuarantee {
            predicate: p::ExprId::new(u32::MAX),
            kind: p::PredicateGuaranteeKind::PruningOnly,
        }]),
        provided_properties: property(),
    });
    // Same contents, deliberately distinct original outer owner (the actual FE
    // author clones schema and provider output columns independently).
    let column = relation.schema()[0].column.clone();
    p::PhysicalNode {
        id: p::NodeId::new(u32::MAX),
        inputs: Box::from([p::NodeId::new(0)]),
        required_inputs: Box::from([property()]),
        output_properties: property(),
        output: p::OutputPort {
            node: p::NodeId::new(u32::MAX),
            columns: Box::from([p::ValueId::new(u32::MAX), p::ValueId::new(0)]),
        },
        kind: p::NodeKind::Scan {
            occurrence: p::ProviderReadOccurrenceId::new(0),
            relation: Box::new(relation),
            read_budget: p::ScanReadBudget {
                max_batch_rows: 0,
                max_batch_bytes: u64::MAX,
            },
            provider_outputs: Box::from([(column, p::ValueId::new(u32::MAX))]),
            residuals: Box::from([
                p::ExprId::new(u32::MAX),
                p::ExprId::new(0),
                p::ExprId::new(u32::MAX),
            ]),
            derived_values: Box::from([p::ValueId::new(0), p::ValueId::new(u32::MAX)]),
        },
    }
}
fn wire_property() -> wire::PhysicalProperties {
    wire::PhysicalProperties {
        distribution: Some(wire::Distribution {
            kind: Some(wire::distribution::Kind::Singleton(Empty {})),
        }),
        row_multiplicity: wire::RowMultiplicity::SingleCopy as i32,
        ordering: vec![],
    }
}
fn expected() -> wire::PhysicalNode {
    wire::PhysicalNode {
        id: u32::MAX,
        input_node_ids: vec![0],
        required_inputs: vec![wire_property()],
        output_properties: Some(wire_property()),
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![u32::MAX, 0],
        }),
        kind: Some(wire::physical_node::Kind::Scan(wire::ScanNode {
            occurrence_id: Some(0),
            relation_id: Some(u32::MAX),
            read_budget: Some(wire::ScanReadBudget {
                max_batch_rows: 0,
                max_batch_bytes: u64::MAX,
            }),
            provider_outputs: vec![wire::ProviderOutput {
                column_payload_id: Some(u32::MAX),
                value_id: Some(u32::MAX),
            }],
            residual_expr_ids: vec![u32::MAX, 0, u32::MAX],
            derived_value_ids: vec![0, u32::MAX],
        })),
    }
}
fn wire_scan(n: &mut wire::PhysicalNode) -> &mut wire::ScanNode {
    match n.kind.as_mut().unwrap() {
        wire::physical_node::Kind::Scan(v) => v,
        _ => unreachable!(),
    }
}
fn with_tokens<R>(
    node: &p::PhysicalNode,
    c: &Control,
    run: impl FnOnce(
        &EncodedRelations<'_, '_, '_>,
        &EncodedValues<'_, '_, '_>,
        &EncodedExpressions<'_, '_, '_>,
        &DecodedRelations<'_, '_, '_>,
        &DecodedExpressions<'_, '_, '_>,
    ) -> R,
) -> R {
    with_token_payload_mode(node, c, false, run)
}
fn with_token_payload_mode<R>(
    node: &p::PhysicalNode,
    c: &Control,
    foreign_payload_owner: bool,
    run: impl FnOnce(
        &EncodedRelations<'_, '_, '_>,
        &EncodedValues<'_, '_, '_>,
        &EncodedExpressions<'_, '_, '_>,
        &DecodedRelations<'_, '_, '_>,
        &DecodedExpressions<'_, '_, '_>,
    ) -> R,
) -> R {
    let scan = physical(node).unwrap();
    let ty = FunctionValueType::new(DataType::Int64, true);
    let roots = [(0, ty.clone())];
    let types = encode_type_table_sources(&roots, &[], types_limits(), c).unwrap();
    let rdtypes = decode_type_table(types.as_wire(), types_limits(), c).unwrap();
    let binding_inputs = [(0, &scan.relation.read().binding)];
    let bindings =
        encode_provider_bindings(&binding_inputs, 64 << 10, provider_limits(), c).unwrap();
    let rdbindings =
        decode_provider_bindings(bindings.as_wire(), 64 << 10, provider_limits(), c).unwrap();
    let mut payload_inputs = vec![
        (0, scan.relation.read().relation.table()),
        (1, scan.relation.read().relation.view()),
        (7, &scan.relation.schema()[0].column.column_payload),
    ];
    let foreign_columns = if foreign_payload_owner {
        Some(
            scan.columns
                .iter()
                .map(|(column, _)| column.clone())
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    for (i, (column, _)) in scan.columns.iter().enumerate() {
        let column = foreign_columns
            .as_ref()
            .map_or(column, |columns| &columns[i]);
        payload_inputs.push((u32::MAX - i as u32, &column.column_payload));
    }
    let payloads =
        encode_connector_payloads(&payload_inputs, 256 << 10, payload_limits(), c).unwrap();
    let rdpayloads =
        decode_connector_payloads(payloads.as_wire(), 256 << 10, payload_limits(), c).unwrap();
    let read_inputs = [(0, scan.relation.read())];
    let reads = encode_provider_reads(&read_inputs, &bindings, &payloads, 512 << 10, read_limits())
        .unwrap();
    let rdreads = decode_provider_reads(
        reads.as_wire(),
        &rdbindings,
        &rdpayloads,
        512 << 10,
        read_limits(),
    )
    .unwrap();
    let type_ids = [0];
    let relation_inputs = [RelationSource {
        id: u32::MAX,
        relation: scan.relation,
        value_type_ids: &type_ids,
    }];
    let relations =
        encode_relations(&relation_inputs, &reads, &types, 2 << 20, relation_limits()).unwrap();
    let rdrelations = decode_relations(
        relations.as_wire(),
        &rdreads,
        &rdtypes,
        2 << 20,
        relation_limits(),
    )
    .unwrap();
    let defs = [0, u32::MAX].map(|id| p::ValueDef {
        id: p::ValueId::new(id),
        ty: ty.clone(),
        origin: p::ValueOrigin::NodeOutput {
            node: p::NodeId::new(u32::MAX),
            output_ordinal: if id == 0 { 0 } else { 1 },
        },
    });
    let value_inputs = defs
        .iter()
        .map(|source| ValueSource {
            source,
            value_type_id: 0,
        })
        .collect::<Vec<_>>();
    let values = encode_values(&value_inputs, &payloads, &types, 1 << 20, value_limits()).unwrap();
    let rdvalues = decode_values(
        values.as_wire(),
        &rdpayloads,
        &rdtypes,
        1 << 20,
        value_limits(),
    )
    .unwrap();
    let functions = encode_function_bindings(&types, &[], 64 << 10, binding_limits(), c).unwrap();
    let aggregates =
        encode_aggregate_bindings(&types, &functions, &[], 128 << 10, binding_limits(), c).unwrap();
    let rdfns = prepare_function_binding_headers(
        functions.as_wire(),
        &rdtypes,
        64 << 10,
        binding_limits(),
        c,
    )
    .unwrap();
    let rdaggs = prepare_aggregate_binding_headers(
        aggregates.as_wire(),
        &rdfns,
        128 << 10,
        binding_limits(),
    )
    .unwrap();
    let nodes = [0, u32::MAX].map(|id| p::ExprNode {
        id: p::ExprId::new(id),
        owner: p::NodeId::new(u32::MAX),
        lambda_scope: None,
        ty: ty.clone(),
        kind: p::ExprKind::Value(p::ValueId::new(id)),
    });
    let arena = p::ExprArena::try_from_definitions_observed(
        nodes.into_iter(),
        &p::PlanLimits::default(),
        c,
    )
    .unwrap();
    let expr_inputs = arena
        .iter()
        .map(|(id, _)| ExpressionTypeIds {
            expr: *id,
            value_type_id: 0,
            lambda_parameter_type_ids: &[],
            function_binding_id: None,
            aggregate_binding_id: None,
        })
        .collect::<Vec<_>>();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let pools = p::ConstantPools::empty();
    let expressions = encode_expression_definitions(
        &arena,
        &expr_inputs,
        &types,
        &functions,
        &aggregates,
        &parameters,
        &pools,
        2 << 20,
        expression_limits(),
        c,
    )
    .unwrap();
    let rdexpr = decode_expression_definitions(
        expressions.as_wire(),
        &rdvalues,
        &rdfns,
        &rdaggs,
        &parameters,
        &pools,
        2 << 20,
        expression_limits(),
    )
    .unwrap();
    run(&relations, &values, &expressions, &rdrelations, &rdexpr)
}

#[test]
fn scan_independent_wire_preserves_distinct_equal_column_owners_and_raw_budget() {
    let node = source();
    let c = Control::default();
    with_tokens(
        &node,
        &c,
        |relations, values, expressions, rdrelations, rdexpr| {
            c.arm(None);
            assert_eq!(
                encode_scan_node(&node, relations, values, expressions, SOURCE, limits())
                    .unwrap()
                    .0,
                expected()
            );
            let definition = &relations.as_wire()[0];
            let schema = match definition.kind.as_ref().unwrap() {
                wire::relation_definition::Kind::Data(v) => &v.schema,
                _ => unreachable!(),
            };
            assert_eq!(schema[0].column_payload_id, Some(7));
            assert_eq!(
                wire_scan(&mut expected()).provider_outputs[0].column_payload_id,
                Some(u32::MAX)
            );
            let body = physical(&node).unwrap();
            assert!(!std::ptr::eq(
                &body.columns[0].0.column_payload,
                &body.relation.schema()[0].column.column_payload
            ));
            assert_eq!(
                body.columns[0].0.column_payload,
                body.relation.schema()[0].column.column_payload
            );
            c.arm(None);
            let decoded = decode_scan_node(&expected(), rdrelations, rdexpr, SOURCE, limits())
                .unwrap()
                .0;
            assert_eq!(decoded, node);
            assert_eq!(
                physical(&decoded).unwrap().relation.schema()[0].ty,
                FunctionValueType::new(DataType::Int64, true)
            );
            // Presence and raw integers are representation; positive budget is the
            // existing mandatory Fragment owner's responsibility.
            let mut raw = expected();
            wire_scan(&mut raw).occurrence_id = Some(u32::MAX);
            wire_scan(&mut raw).read_budget = Some(wire::ScanReadBudget {
                max_batch_rows: u64::MAX,
                max_batch_bytes: 0,
            });
            c.arm(None);
            let decoded = decode_scan_node(&raw, rdrelations, rdexpr, SOURCE, limits())
                .unwrap()
                .0;
            let body = physical(&decoded).unwrap();
            assert_eq!(body.occurrence.get(), u32::MAX);
            assert_eq!(body.budget.max_batch_rows, u64::MAX);
            assert_eq!(body.budget.max_batch_bytes, 0);
        },
    );
}
fn work_sum(c: &Control) -> u64 {
    c.trace().iter().map(|(_, u)| u64::from(*u)).sum()
}
#[test]
fn scan_metadata_relation_preserves_kind_coverage_read_purpose_and_complete_header() {
    let mut node = source();
    let data = match physical(&node).unwrap().relation {
        p::Relation::Data(data) => data.clone(),
        _ => unreachable!(),
    };
    let mut reference = data.read;
    reference.relation = ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::SystemTable,
        reference.relation.table().clone(),
        reference.relation.view().clone(),
    );
    let metadata = p::Relation::Metadata(p::MetadataRelation {
        kind: p::MetadataRelationKind::try_new("files / ✓").unwrap(),
        read: reference,

        selection_digest: data.selection_digest,
        schema: data.schema,
        predicate_guarantees: data.predicate_guarantees,
        provided_properties: data.provided_properties,
        coverage_evidence: Box::from([0, 255, 7]),
    });
    if let p::NodeKind::Scan { relation, .. } = &mut node.kind {
        **relation = metadata;
    }
    let c = Control::default();
    with_tokens(
        &node,
        &c,
        |relations, values, expressions, rdrelations, rdexpr| {
            c.arm(None);
            assert_eq!(
                encode_scan_node(&node, relations, values, expressions, SOURCE, limits())
                    .unwrap()
                    .0,
                expected()
            );
            let wire::relation_definition::Kind::Metadata(raw) =
                relations.as_wire()[0].kind.as_ref().unwrap()
            else {
                panic!("metadata relation kind lost");
            };
            assert_eq!(raw.kind, "files / ✓");
            assert_eq!(raw.coverage_evidence, [0, 255, 7]);
            assert_eq!(raw.schema[0].column_payload_id, Some(7));
            c.arm(None);
            let decoded = decode_scan_node(&expected(), rdrelations, rdexpr, SOURCE, limits())
                .unwrap()
                .0;
            assert_eq!(decoded, node);
            let p::Relation::Metadata(metadata) = physical(&decoded).unwrap().relation else {
                panic!("owned metadata relation kind lost");
            };
            assert_eq!(metadata.kind.as_str(), "files / ✓");
            assert_eq!(metadata.coverage_evidence.as_ref(), [0, 255, 7]);
            assert_eq!(
                metadata.read.relation.kind(),
                ConnectorReadRelationKind::SystemTable
            );
        },
    );
}
#[test]
fn scan_prepared_consumes_original_loans_without_repreparing_model() {
    let node = source();
    let raw = expected();
    let c = Control::default();
    with_tokens(
        &node,
        &c,
        |relations, values, expressions, rdrelations, rdexpr| {
            c.arm(None);
            let combined =
                encode_scan_node(&node, relations, values, expressions, SOURCE, limits()).unwrap();
            let units = work_sum(&c);
            c.arm(None);
            let prepared =
                prepare_scan_node_encode(&node, relations, values, expressions, SOURCE, limits())
                    .unwrap();
            assert!(std::ptr::eq(prepared.input, &node));
            assert!(std::ptr::eq(prepared.relations, relations));
            assert!(std::ptr::eq(prepared.values, values));
            assert!(std::ptr::eq(prepared.expressions, expressions));
            assert_eq!(prepared.facts(), &combined.1);
            assert_eq!(prepared.emit().unwrap(), combined);
            assert_eq!(work_sum(&c), units);
            c.arm(None);
            let combined = decode_scan_node(&raw, rdrelations, rdexpr, SOURCE, limits()).unwrap();
            let units = work_sum(&c);
            c.arm(None);
            let prepared =
                prepare_scan_node_decode(&raw, rdrelations, rdexpr, SOURCE, limits()).unwrap();
            assert!(std::ptr::eq(prepared.input, &raw));
            assert!(std::ptr::eq(prepared.expressions, rdexpr));
            assert_eq!(prepared.facts(), &combined.1);
            assert_eq!(prepared.emit().unwrap(), combined);
            assert_eq!(work_sum(&c), units);
        },
    );
}
fn exact(f: ScanNodeProjectionFacts) -> ScanProjectionLimits {
    ScanProjectionLimits {
        node: NodeProjectionLimits {
            max_input_nodes: f.input_node_count,
            max_value_references: f.value_reference_count,
            max_list_items: f.list_item_count,
            max_allocation_requests: f.allocation_requests_upper_bound,
            max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: f
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: f.cumulative_work_upper_bound,
            ..limits().node
        },
        ..limits()
    }
}
fn under(mut limits: ScanProjectionLimits, axis: usize) -> ScanProjectionLimits {
    let n = &mut limits.node;
    match axis {
        0 => n.max_input_nodes -= 1,
        1 => n.max_value_references -= 1,
        2 => n.max_list_items -= 1,
        3 => n.max_allocation_requests -= 1,
        4 => n.max_allocation_request_bytes -= 1,
        5 => n.max_coexisting_source_and_request_bytes -= 1,
        6 => n.max_work -= 1,
        _ => unreachable!(),
    };
    limits
}
// Locked bytes::Shared has a pointer, a usize length/capacity and an
// AtomicUsize reference count. Per-field padding plus final alignment bounds
// its repr(Rust) request without pretending its private layout is exact.
fn independent_shared_request() -> usize {
    use std::{
        alloc::Layout,
        mem::{align_of, size_of},
        sync::atomic::AtomicUsize,
    };
    let alignment = align_of::<*mut u8>()
        .max(align_of::<usize>())
        .max(align_of::<AtomicUsize>());
    Layout::from_size_align(
        size_of::<*mut u8>() + size_of::<usize>() + size_of::<AtomicUsize>() + 3 * (alignment - 1),
        alignment,
    )
    .unwrap()
    .pad_to_align()
    .size()
}
#[test]
fn scan_independent_layout_selected_child_and_all_seven_caps_in_both_directions() {
    let node = source();
    let raw = expected();
    let c = Control::default();
    with_tokens(
        &node,
        &c,
        |relations, values, expressions, rdrelations, rdexpr| {
            c.arm(None);
            let (_, e) =
                encode_scan_node(&node, relations, values, expressions, SOURCE, limits()).unwrap();
            c.arm(None);
            let (_, d) = decode_scan_node(&raw, rdrelations, rdexpr, SOURCE, limits()).unwrap();
            for f in [e, d] {
                assert_eq!(f.input_node_count, 1);
                assert_eq!(f.value_reference_count, 5);
                assert_eq!(f.list_item_count, 9);
                assert_eq!(
                    f.coexisting_source_and_request_bytes_upper_bound,
                    SOURCE + f.allocation_request_bytes_upper_bound
                );
            }
            assert_eq!(e.allocation_requests_upper_bound, 6);
            assert_eq!(
                e.allocation_request_bytes_upper_bound,
                8 * std::mem::size_of::<u32>()
                    + std::mem::size_of::<wire::PhysicalProperties>()
                    + std::mem::size_of::<wire::ProviderOutput>()
            );
            c.arm(None);
            let child = crate::physical_relation_v2::prepare_relation_materialization(
                rdrelations,
                u32::MAX,
                SOURCE,
                relation_limits(),
            )
            .unwrap();
            let child = *child.facts();
            let shared = independent_shared_request();
            let child_bytes = std::alloc::Layout::array::<usize>(1).unwrap().size()
                + std::mem::size_of::<p::Relation>()
                + 2 * std::mem::size_of::<p::RelationField>()
                + 2 * std::mem::size_of::<p::PredicateGuarantee>()
                + 3 * shared;
            assert_eq!(child.allocation_requests_upper_bound, 9);
            assert_eq!(child.allocation_request_bytes_upper_bound, child_bytes);
            assert_eq!(d.allocation_requests_upper_bound, 14 + 9);
            assert_eq!(
                d.allocation_request_bytes_upper_bound,
                2 * (std::mem::size_of::<p::NodeId>()
                    + std::mem::size_of::<p::PhysicalProperties>()
                    + 4 * std::mem::size_of::<p::ValueId>()
                    + 3 * std::mem::size_of::<p::ExprId>()
                    + std::mem::size_of::<(p::ProviderColumnReference, p::ValueId)>())
                    + std::mem::size_of::<p::Relation>()
                    + shared
                    + child_bytes
            );
            for decode in [false, true] {
                let cap = exact(if decode { d } else { e });
                c.arm(None);
                let result = if decode {
                    decode_scan_node(&raw, rdrelations, rdexpr, SOURCE, cap).map(|_| ())
                } else {
                    encode_scan_node(&node, relations, values, expressions, SOURCE, cap).map(|_| ())
                };
                assert!(result.is_ok(), "exact {decode}: {result:?}");
                for axis in 0..7 {
                    c.arm(None);
                    let result = if decode {
                        decode_scan_node(&raw, rdrelations, rdexpr, SOURCE, under(cap, axis))
                            .map(|_| ())
                    } else {
                        encode_scan_node(
                            &node,
                            relations,
                            values,
                            expressions,
                            SOURCE,
                            under(cap, axis),
                        )
                        .map(|_| ())
                    };
                    assert!(
                        matches!(
                            result,
                            Err(Error::Control(CompileControlError::ResourceExhausted))
                        ),
                        "direction {decode}, axis {axis}: {result:?}"
                    );
                }
            }
        },
    );
}
#[test]
fn scan_required_presence_and_unknown_references_reject_before_emission() {
    let node = source();
    let c = Control::default();
    with_tokens(&node, &c, |_, _, _, rdrelations, rdexpr| {
        for shape in 0..13 {
            let mut raw = expected();
            match shape {
                0 => raw.kind = None,
                1 => {
                    raw.kind = Some(wire::physical_node::Kind::Filter(
                        wire::FilterNode::default(),
                    ))
                }
                2 => raw.output = None,
                3 => raw.output_properties = None,
                4 => wire_scan(&mut raw).occurrence_id = None,
                5 => wire_scan(&mut raw).relation_id = None,
                6 => wire_scan(&mut raw).relation_id = Some(99),
                7 => wire_scan(&mut raw).read_budget = None,
                8 => wire_scan(&mut raw).provider_outputs[0].column_payload_id = None,
                9 => wire_scan(&mut raw).provider_outputs[0].column_payload_id = Some(99),
                10 => wire_scan(&mut raw).provider_outputs[0].value_id = None,
                11 => wire_scan(&mut raw).provider_outputs[0].value_id = Some(99),
                12 => wire_scan(&mut raw).residual_expr_ids[0] = 99,
                _ => unreachable!(),
            }
            c.arm(None);
            let result = decode_scan_node(&raw, rdrelations, rdexpr, SOURCE, limits());
            if shape == 6 {
                assert!(
                    matches!(result, Err(Error::Relation(_))),
                    "shape {shape}: {result:?}"
                );
            } else {
                assert!(
                    matches!(result, Err(Error::InvalidShape(_))),
                    "shape {shape}: {result:?}"
                );
            }
        }
    });
}
#[test]
fn scan_source_association_requires_original_relation_payload_and_type_namespace() {
    let node = source();
    let c = Control::default();
    with_tokens(&node, &c, |relations, values, expressions, _, _| {
        let clone = node.clone();
        c.arm(None);
        assert!(matches!(
            encode_scan_node(&clone, relations, values, expressions, SOURCE, limits()),
            Err(Error::Relation(_))
        ));
        // Publish the same relation under two emitted IDs. An exact same-source
        // association is ambiguous and must not silently select the first ID.
        let ids = [0];
        let relation = physical(&node).unwrap().relation;
        let duplicate = [
            RelationSource {
                id: 0,
                relation,
                value_type_ids: &ids,
            },
            RelationSource {
                id: u32::MAX,
                relation,
                value_type_ids: &ids,
            },
        ];
        c.arm(None);
        let aliases = encode_relations(
            &duplicate,
            relations.reads(),
            relations.types(),
            2 << 20,
            relation_limits(),
        )
        .unwrap();
        c.arm(None);
        assert!(matches!(
            encode_scan_node(&node, &aliases, values, expressions, SOURCE, limits()),
            Err(Error::Relation(_))
        ));
        let roots = [(0, FunctionValueType::new(DataType::Int64, true))];
        c.arm(None);
        let other_types = encode_type_table_sources(&roots, &[], types_limits(), &c).unwrap();
        let defs = [0, u32::MAX].map(|id| p::ValueDef {
            id: p::ValueId::new(id),
            ty: roots[0].1.clone(),
            origin: p::ValueOrigin::NodeOutput {
                node: p::NodeId::new(u32::MAX),
                output_ordinal: 0,
            },
        });
        let inputs = defs
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
            1 << 20,
            value_limits(),
        )
        .unwrap();
        c.arm(None);
        assert!(matches!(
            encode_scan_node(
                &node,
                relations,
                &other_values,
                expressions,
                SOURCE,
                limits()
            ),
            Err(Error::InvalidShape(_))
        ));
    });
    let c = Control::default();
    with_token_payload_mode(&node, &c, true, |relations, values, expressions, _, _| {
        c.arm(None);
        // All other loans are identical; equal-content replacement of the
        // actual provider output column is still a different source owner.
        assert!(matches!(
            encode_scan_node(&node, relations, values, expressions, SOURCE, limits()),
            Err(Error::Payload(_))
        ));
    });
}

#[test]
fn scan_namespace_counts_and_wire_capacity_are_gated_before_traversal() {
    let mut node = source();
    if let p::NodeKind::Scan { residuals, .. } = &mut node.kind {
        *residuals = vec![p::ExprId::new(0); 320].into_boxed_slice();
    }
    let c = Control::default();
    with_tokens(
        &node,
        &c,
        |relations, values, expressions, rdrelations, rdexpr| {
            let low = ScanProjectionLimits {
                node: NodeProjectionLimits {
                    max_work: 1024,
                    ..limits().node
                },
                ..limits()
            };
            c.arm(None);
            assert!(matches!(
                prepare_scan_node_encode(&node, relations, values, expressions, SOURCE, low),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert!(!c.trace().iter().any(|(_, u)| *u == 256));
            let mut raw = expected();
            wire_scan(&mut raw).residual_expr_ids = vec![0; 320];
            c.arm(None);
            assert!(matches!(
                prepare_scan_node_decode(&raw, rdrelations, rdexpr, SOURCE, low),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert!(!c.trace().iter().any(|(_, u)| *u == 256));
            let mut raw = expected();
            wire_scan(&mut raw).residual_expr_ids.reserve_exact(SOURCE);
            c.arm(None);
            assert!(matches!(
                prepare_scan_node_decode(&raw, rdrelations, rdexpr, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            c.arm(None);
            assert!(matches!(
                encode_scan_node(&node, relations, values, expressions, 0, limits()),
                Err(Error::InvalidShape(_))
            ));
            c.arm(None);
            assert!(matches!(
                decode_scan_node(&expected(), rdrelations, rdexpr, 0, limits()),
                Err(Error::InvalidShape(_))
            ));
        },
    );
}
#[test]
fn scan_every_small_original_control_prefix_and_real_wide_quantum_are_primary() {
    for decode in [false, true] {
        for ordinary in [false, true] {
            let mut node = source();
            if ordinary && let p::NodeKind::Scan { residuals, .. } = &mut node.kind {
                residuals[0] = p::ExprId::new(99);
            }
            let mut raw = expected();
            if ordinary {
                wire_scan(&mut raw).residual_expr_ids[0] = 99;
            }
            let c = Control::default();
            with_tokens(
                &node,
                &c,
                |relations, values, expressions, rdrelations, rdexpr| {
                    let invoke = || {
                        if decode {
                            decode_scan_node(&raw, rdrelations, rdexpr, SOURCE, limits())
                                .map(|_| ())
                        } else {
                            encode_scan_node(
                                &node,
                                relations,
                                values,
                                expressions,
                                SOURCE,
                                limits(),
                            )
                            .map(|_| ())
                        }
                    };
                    c.arm(None);
                    assert_eq!(invoke().is_err(), ordinary);
                    let trace = c.trace();
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
                    assert!(trace.last().is_some_and(|(_, u)| *u > 0));
                    for at in 0..trace.len() {
                        for cause in CAUSES {
                            c.arm(Some((at, cause.clone())));
                            assert!(matches!(invoke(),Err(Error::Control(actual))if actual==cause));
                            assert_eq!(c.trace(), trace[..=at]);
                        }
                    }
                },
            );
        }
    }
    let mut node = source();
    if let p::NodeKind::Scan { residuals, .. } = &mut node.kind {
        *residuals = vec![p::ExprId::new(0); 320].into_boxed_slice();
    }
    let c = Control::default();
    with_tokens(
        &node,
        &c,
        |relations, values, expressions, rdrelations, rdexpr| {
            c.arm(None);
            let raw = encode_scan_node(&node, relations, values, expressions, SOURCE, limits())
                .unwrap()
                .0;
            let trace = c.trace();
            let at = trace.iter().position(|(_, u)| *u == 256).unwrap();
            for cause in CAUSES {
                c.arm(Some((at, cause.clone())));
                assert!(
                    matches!(encode_scan_node(&node,relations,values,expressions,SOURCE,limits()),Err(Error::Control(actual))if actual==cause)
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
            c.arm(None);
            assert_eq!(
                decode_scan_node(&raw, rdrelations, rdexpr, SOURCE, limits())
                    .unwrap()
                    .0,
                node
            );
            let trace = c.trace();
            let at = trace.iter().position(|(_, u)| *u == 256).unwrap();
            for cause in CAUSES {
                c.arm(Some((at, cause.clone())));
                assert!(
                    matches!(decode_scan_node(&raw,rdrelations,rdexpr,SOURCE,limits()),Err(Error::Control(actual))if actual==cause)
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
        },
    );
}

#[test]
fn scan_parent_actual_loans_exact_axes_and_all_small_control_prefixes() {
    let c = Control::default();
    let node = source();
    let raw = expected();
    with_tokens(
        &node,
        &c,
        |relations, values, expressions, read_relations, read_expr| {
            for decode in [false, true] {
                let run = |l, snapshots: &mut Vec<NodeProjectionFacts>| {
                    let phase = if decode {
                        CompilePhase::Decode
                    } else {
                        CompilePhase::Encode
                    };
                    let owner = if decode {
                        read_expr.original_control()
                    } else {
                        values.original_control()
                    };
                    let mut work = CompileCheckpoints::try_new(owner, phase)?;
                    let mut admit = |f: &NodeProjectionFacts| {
                        snapshots.push(*f);
                        Ok(())
                    };
                    let result = if decode {
                        prepare_scan_node_decode_in(
                            &raw,
                            read_relations,
                            read_expr,
                            SOURCE,
                            l,
                            &mut admit,
                            &mut work,
                        )
                        .and_then(|p| p.emit_in(&mut admit, &mut work))
                        .map(|(out, f)| {
                            assert_eq!(out, node);
                            f
                        })
                    } else {
                        prepare_scan_node_encode_in(
                            &node,
                            relations,
                            values,
                            expressions,
                            SOURCE,
                            l,
                            &mut admit,
                            &mut work,
                        )
                        .and_then(|p| p.emit_in(&mut admit, &mut work))
                        .map(|(out, f)| {
                            assert_eq!(out, raw);
                            f
                        })
                    };
                    finish(work, result)
                };
                c.arm(None);
                let mut captured = vec![];
                let f = run(limits(), &mut captured).unwrap();
                let trace = c.trace();
                assert!(!captured.is_empty());
                for prefix in &captured {
                    assert!(
                        prefix.allocation_requests_upper_bound <= f.allocation_requests_upper_bound
                    );
                    assert!(
                        prefix.allocation_request_bytes_upper_bound
                            <= f.allocation_request_bytes_upper_bound
                    );
                    assert!(prefix.cumulative_work_upper_bound <= f.cumulative_work_upper_bound);
                    assert_eq!(
                        prefix.coexisting_source_and_request_bytes_upper_bound,
                        SOURCE + prefix.allocation_request_bytes_upper_bound
                    );
                }
                c.arm(None);
                assert_eq!(run(exact(f), &mut vec![]).unwrap(), f);
                for axis in 0..7 {
                    c.arm(None);
                    assert!(matches!(
                        run(under(exact(f), axis), &mut vec![]),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                }
                for at in 0..trace.len() {
                    for cause in CAUSES {
                        c.arm(Some((at, cause.clone())));
                        assert!(
                            matches!(run(limits(), &mut vec![]), Err(Error::Control(actual)) if actual == cause)
                        );
                        assert_eq!(c.trace(), trace[..=at]);
                    }
                }
            }
        },
    );
}

#[test]
fn scan_parent_known_header_precedes_pending_refusal_and_foreign_admission() {
    let c = Control::default();
    let node = source();
    let raw = expected();
    with_tokens(
        &node,
        &c,
        |relations, values, expressions, read_relations, read_expr| {
            for decode in [false, true] {
                for cause in CAUSES {
                    c.arm(Some((1, cause)));
                    let owner = if decode {
                        read_expr.original_control()
                    } else {
                        values.original_control()
                    };
                    let mut work =
                        CompileCheckpoints::try_new(owner, CompilePhase::Decode).unwrap();
                    for _ in 0..255 {
                        work.step().unwrap();
                    }
                    let mut l = limits();
                    l.node.max_allocation_requests = 0;
                    let mut admit = |_: &NodeProjectionFacts| -> Result<(), CompileControlError> {
                        panic!("own known cap must refuse before parent")
                    };
                    let result = if decode {
                        prepare_scan_node_decode_in(
                            &raw,
                            read_relations,
                            read_expr,
                            SOURCE,
                            l,
                            &mut admit,
                            &mut work,
                        )
                        .map(|_| ())
                    } else {
                        prepare_scan_node_encode_in(
                            &node,
                            relations,
                            values,
                            expressions,
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
                let foreign = Control::default();
                let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
                let mut admit = |_: &NodeProjectionFacts| -> Result<(), CompileControlError> {
                    panic!("foreign caller cannot reach parent")
                };
                let result = if decode {
                    prepare_scan_node_decode_in(
                        &raw,
                        read_relations,
                        read_expr,
                        SOURCE,
                        limits(),
                        &mut admit,
                        &mut work,
                    )
                    .map(|_| ())
                } else {
                    prepare_scan_node_encode_in(
                        &node,
                        relations,
                        values,
                        expressions,
                        SOURCE,
                        limits(),
                        &mut admit,
                        &mut work,
                    )
                    .map(|_| ())
                };
                assert!(matches!(finish(work, result), Err(Error::InvalidShape(_))));
            }
        },
    );
}

#[test]
fn scan_parent_ordinary_tail_and_real_320_residual_occurrences() {
    let c = Control::default();
    let mut node = source();
    if let p::NodeKind::Scan { residuals, .. } = &mut node.kind {
        *residuals = vec![p::ExprId::new(0); 320].into_boxed_slice();
    }
    with_tokens(
        &node,
        &c,
        |relations, values, expressions, read_relations, read_expr| {
            c.arm(None);
            let mut work =
                CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)
                    .unwrap();
            let mut admit = |_: &NodeProjectionFacts| Ok(());
            let prepared = prepare_scan_node_encode_in(
                &node,
                relations,
                values,
                expressions,
                SOURCE,
                limits(),
                &mut admit,
                &mut work,
            )
            .unwrap();
            let (raw, _) = prepared.emit_in(&mut admit, &mut work).unwrap();
            finish(work, Ok(())).unwrap();
            assert_eq!(
                match raw.kind.as_ref().unwrap() {
                    wire::physical_node::Kind::Scan(scan) => scan.residual_expr_ids.len(),
                    _ => panic!("expected actual Scan"),
                },
                320
            );
            c.arm(None);
            let mut work =
                CompileCheckpoints::try_new(read_expr.original_control(), CompilePhase::Decode)
                    .unwrap();
            let (decoded, _) = prepare_scan_node_decode_in(
                &raw,
                read_relations,
                read_expr,
                SOURCE,
                limits(),
                &mut admit,
                &mut work,
            )
            .and_then(|p| p.emit_in(&mut admit, &mut work))
            .unwrap();
            finish(work, Ok(())).unwrap();
            assert_eq!(decoded, node);
            assert!(c.trace().iter().any(|(_, units)| *units == 256));
            let mut invalid = expected();
            if let Some(wire::physical_node::Kind::Scan(scan)) = &mut invalid.kind {
                scan.residual_expr_ids.push(123456);
            }
            let run = || {
                let mut work = CompileCheckpoints::try_new(
                    read_expr.original_control(),
                    CompilePhase::Decode,
                )?;
                let result = prepare_scan_node_decode_in(
                    &invalid,
                    read_relations,
                    read_expr,
                    SOURCE,
                    limits(),
                    &mut |_: &NodeProjectionFacts| Ok(()),
                    &mut work,
                )
                .map(|_| ());
                finish(work, result)
            };
            c.arm(None);
            assert!(matches!(run(), Err(Error::InvalidShape(_))));
            let trace = c.trace();
            for at in 0..trace.len() {
                for cause in CAUSES {
                    c.arm(Some((at, cause.clone())));
                    assert!(matches!(run(), Err(Error::Control(actual)) if actual == cause));
                    assert_eq!(c.trace(), trace[..=at]);
                }
            }
        },
    );
}
