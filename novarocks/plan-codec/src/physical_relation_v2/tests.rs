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
    physical_connector_payload_v2::{
        ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
    },
    physical_provider_binding_v2::{
        ProviderBindingProjectionLimits, decode_provider_bindings, encode_provider_bindings,
    },
    physical_provider_read_v2::{
        ProviderReadProjectionLimits, decode_provider_reads, encode_provider_reads,
    },
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
};
use arrow::datatypes::{DataType, Field};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorReadInputVersion,
    ConnectorReadRelationKind, ConnectorReadRelationPayload,
};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::{Arc, Mutex};
const PRIOR: usize = 64 * 1024;
const SOURCE: usize = 2 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, p: CompilePhase, u: u32) -> Result<(), CompileControlError> {
        assert!(u <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        let s = self.stop.lock().unwrap().clone();
        if let Some((s, _)) = s {
            assert!(at <= s, "callback after refusal");
        }
        t.push((p, u));
        match s {
            Some((s, c)) if s == at => Err(c),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn arm(&self, s: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.stop.lock().unwrap() = s;
    }
}
fn trace(c: &Control) -> Vec<(CompilePhase, u32)> {
    c.trace.lock().unwrap().clone()
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
fn limits() -> RelationProjectionLimits {
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
fn binding_limits() -> ProviderBindingProjectionLimits {
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
        max_input_version_bytes: 1024 * 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn type_limits() -> TypeProjectionLimits {
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
fn props() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Broadcast,
        row_multiplicity: p::RowMultiplicity::Replicated,
        ordering: vec![
            p::OrderingKey {
                value: p::ValueId::new(u32::MAX),
                direction: p::SortDirection::Descending,
                null_ordering: p::NullOrdering::Last,
            },
            p::OrderingKey {
                value: p::ValueId::new(0),
                direction: p::SortDirection::Ascending,
                null_ordering: p::NullOrdering::First,
            },
        ]
        .into_boxed_slice(),
    }
}
fn fixture() -> (Vec<p::Relation>, Vec<(u32, FunctionValueType)>) {
    let values = vec![
        (
            0,
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ),
        (
            u32::MAX,
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ),
        (
            42,
            FunctionValueType::new(
                DataType::Struct(
                    vec![Arc::new(
                        Field::new("item", DataType::Int64, false).with_metadata(
                            std::collections::HashMap::<String, String>::from([(
                                "unknown".into(),
                                "exact".into(),
                            )]),
                        ),
                    )]
                    .into(),
                ),
                false,
            ),
        ),
    ];
    let fields = || {
        values
            .iter()
            .map(|(_, ty)| p::RelationField {
                column: p::ProviderColumnReference {
                    column_payload: payload(ConnectorCodecCategory::ReadColumn, b"column"),
                },
                ty: ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
    };
    let guarantees = || {
        vec![
            p::PredicateGuarantee {
                predicate: p::ExprId::new(u32::MAX),
                kind: p::PredicateGuaranteeKind::Exact,
            },
            p::PredicateGuarantee {
                predicate: p::ExprId::new(0),
                kind: p::PredicateGuaranteeKind::PruningOnly,
            },
            p::PredicateGuarantee {
                predicate: p::ExprId::new(u32::MAX),
                kind: p::PredicateGuaranteeKind::Exact,
            },
        ]
        .into_boxed_slice()
    };
    let data = p::Relation::Data(p::DataRelation {
        read: read(),

        selection_digest: [0; 32],
        schema: fields(),
        predicate_guarantees: guarantees(),
        provided_properties: props(),
    });
    let meta = p::Relation::Metadata(p::MetadataRelation {
        kind: p::MetadataRelationKind::try_new("files / ✓").unwrap(),
        read: read(),

        selection_digest: [255; 32],
        schema: fields(),
        predicate_guarantees: guarantees(),
        provided_properties: props(),
        coverage_evidence: vec![0, 255, 7].into_boxed_slice(),
    });
    (vec![data, meta], values)
}
fn expected_props() -> wire::PhysicalProperties {
    wire::PhysicalProperties {
        distribution: Some(wire::Distribution {
            kind: Some(wire::distribution::Kind::Broadcast(
                novarocks_proto_models::physical_control_v2::Empty {},
            )),
        }),
        row_multiplicity: 2,
        ordering: vec![
            wire::OrderingKey {
                value_id: Some(u32::MAX),
                direction: 2,
                null_ordering: 2,
            },
            wire::OrderingKey {
                value_id: Some(0),
                direction: 1,
                null_ordering: 1,
            },
        ],
    }
}
fn expected(i: usize) -> wire::RelationDefinition {
    let schema = (0..3)
        .map(|n| wire::RelationField {
            column_payload_id: Some(100 + (i * 3 + n) as u32),
            value_type_id: Some([0, u32::MAX, 42][n]),
        })
        .collect();
    let guarantees = vec![
        wire::PredicateGuarantee {
            predicate_expr_id: Some(u32::MAX),
            kind: 1,
        },
        wire::PredicateGuarantee {
            predicate_expr_id: Some(0),
            kind: 2,
        },
        wire::PredicateGuarantee {
            predicate_expr_id: Some(u32::MAX),
            kind: 1,
        },
    ];
    wire::RelationDefinition {
        id: if i == 0 { u32::MAX } else { 0 },
        kind: Some(if i == 0 {
            wire::relation_definition::Kind::Data(wire::DataRelation {
                read_reference_id: Some(0),

                selection_digest: vec![0; 32],
                schema,
                predicate_guarantees: guarantees,
                provided_properties: Some(expected_props()),
            })
        } else {
            wire::relation_definition::Kind::Metadata(wire::MetadataRelation {
                kind: "files / ✓".into(),
                read_reference_id: Some(1),

                selection_digest: vec![255; 32],
                schema,
                predicate_guarantees: guarantees,
                provided_properties: Some(expected_props()),
                coverage_evidence: vec![0, 255, 7],
            })
        }),
    }
}
fn with_sources(
    c: &Control,
    f: impl FnMut(
        &[p::Relation],
        &EncodedProviderReads<'_, '_>,
        &EncodedTypeTable<'_>,
    ) -> Result<(), Error>,
) -> Result<(), Error> {
    with_source_mode(c, false, f)
}
fn with_source_mode(
    c: &Control,
    wide: bool,
    mut f: impl FnMut(
        &[p::Relation],
        &EncodedProviderReads<'_, '_>,
        &EncodedTypeTable<'_>,
    ) -> Result<(), Error>,
) -> Result<(), Error> {
    let (mut relations, values) = fixture();
    if wide && let p::Relation::Data(data) = &mut relations[0] {
        data.schema = Vec::new().into_boxed_slice();
        data.predicate_guarantees = (0..320)
            .map(|id| p::PredicateGuarantee {
                predicate: p::ExprId::new(u32::MAX - id),
                kind: p::PredicateGuaranteeKind::Exact,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
    }
    let bis = relations
        .iter()
        .enumerate()
        .map(|(i, r)| (i as u32, &r.read().binding))
        .collect::<Vec<_>>();
    let mut pis = Vec::new();
    for (i, r) in relations.iter().enumerate() {
        pis.push((i as u32 * 2, r.read().relation.table()));
        pis.push((i as u32 * 2 + 1, r.read().relation.view()));
        for (n, field) in r.schema().iter().enumerate() {
            pis.push((100 + (i * 3 + n) as u32, &field.column.column_payload));
        }
    }
    let bs = encode_provider_bindings(&bis, PRIOR, binding_limits(), c).unwrap();
    let ps = encode_connector_payloads(&pis, PRIOR, payload_limits(), c).unwrap();
    let ris = relations
        .iter()
        .enumerate()
        .map(|(i, r)| (i as u32, r.read()))
        .collect::<Vec<_>>();
    let reads = encode_provider_reads(&ris, &bs, &ps, 256 * 1024, read_limits()).unwrap();
    let types = encode_type_table_sources(&values, &[], type_limits(), c).unwrap();
    f(&relations, &reads, &types)
}
fn inputs<'a>(relations: &'a [p::Relation], ids: &'a [u32]) -> [RelationSource<'a>; 2] {
    [
        RelationSource {
            id: u32::MAX,
            relation: &relations[0],
            value_type_ids: ids,
        },
        RelationSource {
            id: 0,
            relation: &relations[1],
            value_type_ids: ids,
        },
    ]
}
#[test]
fn relation_namespace_data_metadata_independent_expected_wire_and_full_type_receiving() {
    let c = Control::default();
    with_sources(&c, |sources, reads, types| {
        let ids = [0, u32::MAX, 42];
        let inputs = inputs(sources, &ids);
        let encoded = encode_relations(&inputs, reads, types, SOURCE, limits())?;
        assert_eq!(encoded.as_wire(), [expected(0), expected(1)]);
        assert!(std::ptr::eq(encoded.relation(0)?.unwrap(), &sources[1]));
        assert_eq!(encoded.source_id(&sources[0])?, u32::MAX);
        assert!(std::ptr::eq(encoded.reads(), reads));
        assert!(std::ptr::eq(encoded.types(), types));
        let bs = decode_provider_bindings(reads.bindings().as_wire(), PRIOR, binding_limits(), &c)
            .unwrap();
        let ps = decode_connector_payloads(reads.payloads().as_wire(), PRIOR, payload_limits(), &c)
            .unwrap();
        let rs =
            decode_provider_reads(reads.as_wire(), &bs, &ps, 256 * 1024, read_limits()).unwrap();
        let ts = decode_type_table(types.as_wire(), type_limits(), &c).unwrap();
        let raw = [expected(0), expected(1)];
        let decoded = decode_relations(&raw, &rs, &ts, SOURCE, limits())?;
        assert_eq!(decoded.relation(u32::MAX)?, Some(&sources[0]));
        assert_eq!(decoded.relation(0)?, Some(&sources[1]));
        assert!(decoded.relation(19)?.is_none());
        assert!(std::ptr::eq(decoded.as_wire(), raw.as_slice()));
        assert!(std::ptr::eq(decoded.reads(), &rs));
        let source_child = ts.value_type(42).unwrap();
        let output = &decoded.relation(0)?.unwrap().schema()[2].ty;
        if let (DataType::Struct(a), DataType::Struct(b)) =
            (&source_child.data_type(), &output.data_type())
        {
            assert!(Arc::ptr_eq(&a[0], &b[0]));
        } else {
            panic!("actual Struct carrier lost");
        }
        assert_eq!(
            decoded.relation(0)?.unwrap().schema()[0].ty.logical_type(),
            ValueLogicalType::Json
        );
        assert!(encoded.retained_invoice_floor()? > SOURCE);
        assert!(decoded.retained_invoice_floor()? > SOURCE);
        assert_eq!(
            encode_relations(&[], reads, types, SOURCE, limits())?.source_count(),
            0
        );
        assert_eq!(
            decode_relations(&[], &rs, &ts, SOURCE, limits())?.source_count(),
            0
        );
        Ok(())
    })
    .unwrap();
}
#[test]
fn relation_namespace_source_association_full_fvt_and_presence_never_guess_or_retag() {
    let c = Control::default();
    with_sources(&c, |sources, reads, types| {
        let mut ids = [0, u32::MAX, 42];
        let mut wrong = inputs(sources, &ids);
        wrong[0].value_type_ids = &[0];
        assert!(matches!(
            encode_relations(&wrong, reads, types, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        ids[0] = u32::MAX;
        assert!(matches!(
            encode_relations(&inputs(sources, &ids), reads, types, SOURCE, limits()),
            Err(Error::InvalidShape(
                "relation full source value type differs"
            ))
        ));
        ids[0] = 999;
        assert!(matches!(
            encode_relations(&inputs(sources, &ids), reads, types, SOURCE, limits()),
            Err(Error::InvalidShape("relation value type ID is unknown"))
        ));
        // Same carrier alone cannot bind a different nullable/root domain or
        // nested semantic definition from another lawful TypeTable emission.
        for which in 0..3 {
            let mut alternate = fixture().1;
            match which {
                0 => alternate[0].1 = alternate[0].1.with_nullable(false).unwrap(),
                1 => {
                    alternate[0].1 = FunctionValueType::new(
                        alternate[0].1.data_type().clone(),
                        alternate[0].1.nullable(),
                    )
                }
                _ => {
                    alternate[2].1 = FunctionValueType::new(
                        DataType::Struct(
                            vec![Arc::new(Field::new("different", DataType::Int64, false))].into(),
                        ),
                        alternate[2].1.nullable(),
                    )
                }
            }
            let other = encode_type_table_sources(&alternate, &[], type_limits(), &c).unwrap();
            let ids = [0, u32::MAX, 42];
            assert!(matches!(
                encode_relations(&inputs(sources, &ids), reads, &other, SOURCE, limits()),
                Err(Error::InvalidShape(
                    "relation full source value type differs"
                ))
            ));
        }
        let foreign = sources[0].clone();
        let one = [RelationSource {
            id: 0,
            relation: &foreign,
            value_type_ids: &[0, u32::MAX, 42],
        }];
        assert!(matches!(
            encode_relations(&one, reads, types, SOURCE, limits()),
            Err(Error::Read(_))
        ));
        let bs = decode_provider_bindings(reads.bindings().as_wire(), PRIOR, binding_limits(), &c)
            .unwrap();
        let ps = decode_connector_payloads(reads.payloads().as_wire(), PRIOR, payload_limits(), &c)
            .unwrap();
        let rs =
            decode_provider_reads(reads.as_wire(), &bs, &ps, 256 * 1024, read_limits()).unwrap();
        let ts = decode_type_table(types.as_wire(), type_limits(), &c).unwrap();
        for which in 0..10 {
            let mut def = expected(0);
            if which == 0 {
                def.kind = None;
            } else if let Some(wire::relation_definition::Kind::Data(v)) = &mut def.kind {
                match which {
                    1 => v.read_reference_id = None,
                    2 => v.read_reference_id = Some(999),
                    3 => v.schema[0].column_payload_id = None,
                    4 => v.schema[0].column_payload_id = Some(999),
                    5 => v.schema[0].value_type_id = None,
                    6 => v.schema[0].value_type_id = Some(999),
                    7 => v.predicate_guarantees[0].predicate_expr_id = None,
                    8 => v.provided_properties = None,
                    9 => v.selection_digest.pop().map(|_| ()).unwrap(),
                    _ => unreachable!(),
                }
            }
            assert!(
                decode_relations(&[def], &rs, &ts, SOURCE, limits()).is_err(),
                "bad field {which}"
            );
        }
        for kind in [0, 3, -1, i32::MAX] {
            let mut def = expected(0);
            if let Some(wire::relation_definition::Kind::Data(v)) = &mut def.kind {
                v.predicate_guarantees[0].kind = kind;
            }
            assert!(decode_relations(&[def], &rs, &ts, SOURCE, limits()).is_err());
        }
        let defs = [expected(0), expected(0)];
        assert!(matches!(
            decode_relations(&defs, &rs, &ts, SOURCE, limits()),
            Err(Error::Index(_))
        ));
        Ok(())
    })
    .unwrap();
}
fn exact(f: &RelationProjectionFacts) -> RelationProjectionLimits {
    RelationProjectionLimits {
        max_definitions: f.definition_count,
        max_schema_fields: f.schema_field_count,
        max_predicate_guarantees: f.predicate_guarantee_count,
        max_metadata_kind_bytes: f.metadata_kind_bytes,
        max_coverage_bytes: f.coverage_bytes,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
        properties: property_limits(),
    }
}
fn under(mut l: RelationProjectionLimits, n: usize) -> RelationProjectionLimits {
    match n {
        0 => l.max_definitions -= 1,
        1 => l.max_schema_fields -= 1,
        2 => l.max_predicate_guarantees -= 1,
        3 => l.max_metadata_kind_bytes -= 1,
        4 => l.max_coverage_bytes -= 1,
        5 => l.max_allocation_requests -= 1,
        6 => l.max_allocation_request_bytes -= 1,
        7 => l.max_coexisting_source_and_request_bytes -= 1,
        _ => l.max_work -= 1,
    };
    l
}
#[test]
fn relation_namespace_all_nine_caps_dictionary_clone_requests_and_alias_safe_source_floors() {
    let c = Control::default();
    with_sources(&c, |sources, reads, types| {
        let ids = [0, u32::MAX, 42];
        let input = inputs(sources, &ids);
        let e = encode_relations(&input, reads, types, SOURCE, limits())?;
        let l = exact(e.facts());
        encode_relations(&input, reads, types, SOURCE, l)?;
        for i in 0..9 {
            assert!(matches!(
                encode_relations(&input, reads, types, SOURCE, under(l, i)),
                Err(Error::InvalidShape(_))
            ));
        }
        let bs = decode_provider_bindings(reads.bindings().as_wire(), PRIOR, binding_limits(), &c)
            .unwrap();
        let ps = decode_connector_payloads(reads.payloads().as_wire(), PRIOR, payload_limits(), &c)
            .unwrap();
        let rs =
            decode_provider_reads(reads.as_wire(), &bs, &ps, 256 * 1024, read_limits()).unwrap();
        let ts = decode_type_table(types.as_wire(), type_limits(), &c).unwrap();
        let defs = [expected(0), expected(1)];
        let d = decode_relations(&defs, &rs, &ts, SOURCE, limits())?;
        let l = exact(d.facts());
        decode_relations(&defs, &rs, &ts, SOURCE, l)?;
        for i in 0..9 {
            assert!(matches!(
                decode_relations(&defs, &rs, &ts, SOURCE, under(l, i)),
                Err(Error::InvalidShape(_))
            ));
        }
        // All validated type roots share immutable binding handles; no source
        // encoding Boxes or Field metadata are copied into the value namespace.
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let f = physical_type_v2::preflight_value_type_clone(
            ts.value_type(u32::MAX).unwrap(),
            &mut work,
        )
        .unwrap();
        assert_eq!(f.allocation_requests_upper_bound(), 0);
        assert_eq!(f.allocation_request_bytes_upper_bound(), 0);
        assert_eq!(f.work_upper_bound(), 2);
        work.finish().unwrap();
        assert!(bytes::<p::RelationField>(usize::MAX).is_err());
        assert!(own_work(usize::MAX, 1, 1, 1, 1, 1).is_err());
        assert!(add(usize::MAX, 1).is_err());
        assert!(matches!(
            encode_relations(&input, reads, types, 0, limits()),
            Err(Error::InvalidShape(_))
        ));
        Ok(())
    })
    .unwrap();
}
fn run(
    c: &Control,
    stop: Option<(usize, CompileControlError)>,
    decode: bool,
    bad: bool,
) -> Result<(), Error> {
    with_sources(c, |sources, reads, types| {
        if decode {
            let bs =
                decode_provider_bindings(reads.bindings().as_wire(), PRIOR, binding_limits(), c)
                    .unwrap();
            let ps =
                decode_connector_payloads(reads.payloads().as_wire(), PRIOR, payload_limits(), c)
                    .unwrap();
            let rs = decode_provider_reads(reads.as_wire(), &bs, &ps, 256 * 1024, read_limits())
                .unwrap();
            let ts = decode_type_table(types.as_wire(), type_limits(), c).unwrap();
            let mut raw = expected(1);
            if bad && let Some(wire::relation_definition::Kind::Metadata(v)) = &mut raw.kind {
                v.kind.clear();
            }
            let defs = [raw];
            c.arm(stop.clone());
            decode_relations(&defs, &rs, &ts, SOURCE, limits()).map(|_| ())
        } else {
            let ids = if bad {
                [u32::MAX, u32::MAX, 42]
            } else {
                [0, u32::MAX, 42]
            };
            let input = inputs(sources, &ids);
            c.arm(stop.clone());
            encode_relations(&input, reads, types, SOURCE, limits()).map(|_| ())
        }
    })
}
#[test]
fn relation_namespace_success_ordinary_and_native_kind_failure_every_actual_callback_three_causes()
{
    for decode in [false, true] {
        for bad in [false, true] {
            let c = Control::default();
            let r = run(&c, None, decode, bad);
            assert_eq!(r.is_ok(), !bad);
            let positive = trace(&c);
            assert!(!positive.is_empty());
            for at in 0..positive.len() {
                for cause in CAUSES {
                    let refusal = Control::default();
                    let r = run(&refusal, Some((at, cause.clone())), decode, bad);
                    assert!(matches!(&r,Err(Error::Control(c)) if c==&cause), "{r:?}");
                    assert_eq!(trace(&refusal), positive[..=at]);
                }
            }
        }
    }
}
#[test]
fn relation_namespace_real_occurrences_cross_quantum_preserve_sparse_layout_without_dedup() {
    let c = Control::default();
    with_source_mode(&c, true, |sources, reads, types| {
        let input = (0..8).map(|n| RelationSource {
            id: u32::MAX-n, relation: &sources[0], value_type_ids: &[],
        }).collect::<Vec<_>>();
        c.arm(None);
        let encoded = encode_relations(&input, reads, types, SOURCE, limits())?;
        assert_eq!(encoded.source_count(), 8);
        assert_eq!(encoded.facts().predicate_guarantee_count, 2560);
        for def in encoded.as_wire() {
            if let Some(wire::relation_definition::Kind::Data(data)) = &def.kind {
                assert_eq!(data.predicate_guarantees.len(), 320);
                assert_eq!(data.predicate_guarantees[319].predicate_expr_id, Some(u32::MAX-319));
            } else { panic!("actual Data source changed"); }
        }
        let positive = trace(&c);
        let at = positive.iter().position(|(_,u)| *u==256).expect("real relation operations quantum");
        for cause in CAUSES {
            c.arm(Some((at,cause.clone())));
            assert!(matches!(encode_relations(&input, reads, types, SOURCE, limits()), Err(Error::Control(c)) if c==cause));
            assert_eq!(trace(&c), positive[..=at]);
        }
        c.arm(None);
        let bs = decode_provider_bindings(reads.bindings().as_wire(), PRIOR, binding_limits(), &c).unwrap();
        let ps = decode_connector_payloads(reads.payloads().as_wire(), PRIOR, payload_limits(), &c).unwrap();
        let rs = decode_provider_reads(reads.as_wire(), &bs, &ps, 256*1024, read_limits()).unwrap();
        let ts = decode_type_table(types.as_wire(), type_limits(), &c).unwrap();
        c.arm(None);
        let decoded = decode_relations(encoded.as_wire(), &rs, &ts, SOURCE, limits())?;
        assert_eq!(decoded.source_count(), 8);
        let positive = trace(&c);
        let at = positive.iter().position(|(_,u)| *u==256).expect("real receiving guarantee quantum");
        for cause in CAUSES {
            c.arm(Some((at,cause.clone())));
            assert!(matches!(decode_relations(encoded.as_wire(), &rs, &ts, SOURCE, limits()), Err(Error::Control(c)) if c==cause));
            assert_eq!(trace(&c), positive[..=at]);
        }
        Ok(())
    }).unwrap();
}

fn check_original_owned_backing_floor(dictionary: bool) {
    let c = Control::default();
    let value_type = FunctionValueType::new(
        if dictionary {
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8))
        } else {
            DataType::Utf8
        },
        true,
    );
    let relation = p::Relation::Metadata(p::MetadataRelation {
        kind: p::MetadataRelationKind::try_new("original.metadata").unwrap(),
        read: read(),

        selection_digest: [7; 32],
        schema: (0..16)
            .map(|_| p::RelationField {
                column: p::ProviderColumnReference {
                    column_payload: payload(ConnectorCodecCategory::ReadColumn, b"column"),
                },
                ty: value_type.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        predicate_guarantees: (0..16)
            .map(|n| p::PredicateGuarantee {
                predicate: p::ExprId::new(n),
                kind: p::PredicateGuaranteeKind::Exact,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        provided_properties: props(),
        coverage_evidence: vec![255; 512 * 1024].into_boxed_slice(),
    });
    let bindings = [(0, &relation.read().binding)];
    let bs = encode_provider_bindings(&bindings, PRIOR, binding_limits(), &c).unwrap();
    let mut payloads = vec![
        (0, relation.read().relation.table()),
        (1, relation.read().relation.view()),
    ];
    payloads.extend(
        relation
            .schema()
            .iter()
            .enumerate()
            .map(|(n, field)| (100 + n as u32, &field.column.column_payload)),
    );
    let ps = encode_connector_payloads(&payloads, PRIOR, payload_limits(), &c).unwrap();
    let read_sources = [(0, relation.read())];
    let reads = encode_provider_reads(&read_sources, &bs, &ps, 256 * 1024, read_limits()).unwrap();
    let values = [(0, value_type)];
    let types = encode_type_table_sources(&values, &[], type_limits(), &c).unwrap();
    let ids = vec![0; relation.schema().len()];
    let inputs = [
        RelationSource {
            id: 0,
            relation: &relation,
            value_type_ids: &ids,
        },
        RelationSource {
            id: u32::MAX,
            relation: &relation,
            value_type_ids: &ids,
        },
    ];
    // The coverage Box alone is an independent visible lower fact, regardless
    // of the helper's exact formula or the shared read/type namespace invoice.
    let omitted = 512 * 1024 - 1;
    assert!(reads.retained_invoice_floor().unwrap() < omitted);
    assert!(matches!(
        encode_relations(&inputs, &reads, &types, omitted, limits()),
        Err(Error::InvalidShape(
            "relation source invoice omits original backing"
        ))
    ));

    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    let (minimum, _) = individual_relation_floor(&relation, &mut work).unwrap();
    work.finish().unwrap();
    assert!(minimum > 512 * 1024);
    if dictionary {
        let p::Relation::Metadata(metadata) = &relation else {
            unreachable!()
        };
        // Normalized value bindings borrow immutable backing; dictionary
        // boxes belong to source encoding and are absent from this declaration.
        // The independent structural floor still refuses an omitted byte.
        let structural_floor = size_of::<p::Relation>()
            + std::mem::size_of_val(relation.schema())
            + std::mem::size_of_val(relation.predicate_guarantees())
            + std::mem::size_of_val(&*metadata.provided_properties.ordering)
            + metadata.kind.as_str().len()
            + metadata.coverage_evidence.len();
        assert_eq!(minimum, structural_floor);
        assert!(matches!(
            encode_relations(&inputs, &reads, &types, structural_floor - 1, limits()),
            Err(Error::InvalidShape(
                "relation source invoice omits original backing"
            ))
        ));
        assert_eq!(relation.schema()[0].ty.data_type(), &DataType::Utf8);
    }
    // This tests only the necessary floor. It does not attest a complete host
    // invoice for all shared backing. Two IDs must not charge the same owned
    // relation schema/guarantee/metadata Boxes twice.
    let encoded = encode_relations(&inputs, &reads, &types, minimum, limits()).unwrap();
    assert_eq!(encoded.source_count(), 2);
    assert_eq!(encoded.facts().schema_field_count, 32);
    assert_eq!(encoded.facts().predicate_guarantee_count, 32);
    assert_eq!(encoded.facts().coverage_bytes, 1024 * 1024);
    assert!(
        encoded
            .relation(0)
            .unwrap()
            .is_some_and(|r| std::ptr::eq(r, &relation))
    );
    assert!(
        encoded
            .relation(u32::MAX)
            .unwrap()
            .is_some_and(|r| std::ptr::eq(r, &relation))
    );
    assert!(matches!(
        encode_relations(&inputs, &reads, &types, minimum - 1, limits()),
        Err(Error::InvalidShape(
            "relation source invoice omits original backing"
        ))
    ));
}

#[test]
fn relation_namespace_original_owned_backing_floor_rejects_omission_without_alias_billing() {
    check_original_owned_backing_floor(false);
}

#[test]
fn relation_namespace_normalized_type_keeps_original_structural_floor_without_alias_charge() {
    check_original_owned_backing_floor(true);
}

mod materialization_tests;

mod caller_owned_tests {
    include!("caller_owned_tests.rs");
}
