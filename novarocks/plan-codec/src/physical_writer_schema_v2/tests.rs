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
use crate::physical_node_v2::NodeProjectionFacts;
use crate::physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::ValueLogicalType;
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

fn limits() -> WriterSchemaProjectionLimits {
    WriterSchemaProjectionLimits {
        max_fields: 4096,
        max_name_bytes: 1 << 20,
        max_type_references: 4096,
        max_allocation_requests: 16384,
        max_allocation_request_bytes: 16 << 20,
        max_coexisting_source_and_request_bytes: 32 << 20,
        max_work: 1 << 30,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 128,
        max_expanded_nodes: 2048,
        max_string_bytes: 1 << 20,
    }
}
#[allow(
    deprecated,
    reason = "Frozen field identity retains original Arrow dictionary IDs."
)]
fn roots() -> Vec<(u32, FunctionValueType)> {
    vec![
        (0, FunctionValueType::new(DataType::Int64, false)),
        (
            u32::MAX,
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ),
        (
            7,
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        ),
        (
            9,
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ),
        (
            11,
            FunctionValueType::new(
                DataType::List(Arc::new(
                    Field::new_dict(
                        "nested / é",
                        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                        true,
                        93,
                        true,
                    )
                    .with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "source.key".into(),
                            "exact / 值".into(),
                        )]),
                    ),
                )),
                true,
            ),
        ),
        (12, FunctionValueType::new(DataType::Utf8, true)),
        (
            13,
            FunctionValueType::new(DataType::FixedSizeBinary(16), false),
        ),
    ]
}
fn source_schema(roots: &[(u32, FunctionValueType)]) -> p::WriterRelationSchema {
    let roles = [
        p::WriterRelationFieldRole::Kind,
        p::WriterRelationFieldRole::TargetOrdinal,
        p::WriterRelationFieldRole::RowCount,
        p::WriterRelationFieldRole::CommitFragment,
        p::WriterRelationFieldRole::Auxiliary,
    ];
    let names = ["kind / é", "ordinal", "rows", "commit", "辅助"];
    p::WriterRelationSchema {
        revision: u32::MAX,
        fields: roots
            .iter()
            .take(5)
            .enumerate()
            .map(|(i, (_, ty))| p::WriterRelationField {
                value: p::ValueId::new(if i % 2 == 0 { 0 } else { u32::MAX }),
                name: names[i].into(),
                ty: ty.clone(),
                role: roles[i],
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }
}
fn expected_schema() -> wire::WriterRelationSchema {
    let fields = [
        (0, "kind / é", 0, wire::WriterRelationFieldRole::Kind),
        (
            u32::MAX,
            "ordinal",
            u32::MAX,
            wire::WriterRelationFieldRole::TargetOrdinal,
        ),
        (0, "rows", 7, wire::WriterRelationFieldRole::RowCount),
        (
            u32::MAX,
            "commit",
            9,
            wire::WriterRelationFieldRole::CommitFragment,
        ),
        (0, "辅助", 11, wire::WriterRelationFieldRole::Auxiliary),
    ]
    .into_iter()
    .map(|(value, name, ty, role)| wire::WriterRelationField {
        value_id: Some(value),
        name: name.into(),
        value_type_id: Some(ty),
        role: role as i32,
    })
    .collect();
    wire::WriterRelationSchema {
        revision: u32::MAX,
        fields,
    }
}
fn source_targets(roots: &[(u32, FunctionValueType)]) -> Box<[p::WriterTargetField]> {
    Box::from([
        p::WriterTargetField {
            token: ConnectorWriteFieldToken::from_bytes([0; 32]),
            provider_name: "first / é".into(),
            input: p::ValueId::new(0),
            ty: roots[3].1.clone(),
            hidden: false,
        },
        p::WriterTargetField {
            token: ConnectorWriteFieldToken::from_bytes([255; 32]),
            provider_name: "second / 值".into(),
            input: p::ValueId::new(u32::MAX),
            ty: roots[1].1.clone(),
            hidden: true,
        },
    ])
}
fn expected_targets() -> Vec<wire::WriterTargetField> {
    vec![
        wire::WriterTargetField {
            token: vec![0; 32],
            provider_name: "first / é".into(),
            input_value_id: Some(0),
            value_type_id: Some(9),
            hidden: false,
        },
        wire::WriterTargetField {
            token: vec![255; 32],
            provider_name: "second / 值".into(),
            input_value_id: Some(u32::MAX),
            value_type_id: Some(u32::MAX),
            hidden: true,
        },
    ]
}
fn with_types<R>(
    c: &Control,
    run: impl FnOnce(&[(u32, FunctionValueType)], &EncodedTypeTable<'_>, &DecodedTypeTable) -> R,
) -> R {
    let roots = roots();
    let encoded = encode_type_table_sources(&roots, &[], type_limits(), c).unwrap();
    let decoded = decode_type_table(encoded.as_wire(), type_limits(), c).unwrap();
    run(&roots, &encoded, &decoded)
}
fn units(c: &Control) -> u64 {
    c.trace().iter().map(|(_, n)| u64::from(*n)).sum()
}
#[test]
#[allow(
    deprecated,
    reason = "Source encoding attributes are deliberately absent from value layout."
)]
fn writer_schema_complete_independent_wire_preserves_roles_names_revision_and_full_types() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        let ids = [0, u32::MAX, 7, 9, 11];
        c.arm(None);
        assert_eq!(
            encode_writer_schema(&schema, &ids, types, SOURCE, limits(), &c)
                .unwrap()
                .0,
            expected_schema()
        );
        c.arm(None);
        let decoded = decode_writer_schema(&expected_schema(), read, SOURCE, limits(), &c)
            .unwrap()
            .0;
        assert_eq!(decoded, schema);
        assert_eq!(decoded.fields[1].ty.logical_type(), ValueLogicalType::Json);
        assert_eq!(
            decoded.fields[2].ty.logical_type(),
            ValueLogicalType::LargeInt
        );
        if let DataType::List(field) = &decoded.fields[4].ty.data_type() {
            assert_eq!(field.name(), "item");
            assert_eq!(field.data_type(), &DataType::Utf8);
            assert!(field.is_nullable());
            assert_eq!(field.dict_id(), None);
            assert_eq!(field.dict_is_ordered(), None);
            assert_eq!(field.metadata().get("source.key").map(String::as_str), None);
            let original = match &read.value_type(11).unwrap().data_type() {
                DataType::List(f) => f,
                _ => unreachable!(),
            };
            assert!(Arc::ptr_eq(field, original));
        } else {
            panic!("lost nested full source type");
        }
    });
}
#[test]
fn writer_target_fields_complete_independent_wire_preserves_tokens_hidden_and_nullable() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let fields = source_targets(roots);
        let ids = [9, u32::MAX];
        c.arm(None);
        assert_eq!(
            encode_writer_target_fields(&fields, &ids, types, SOURCE, limits(), &c)
                .unwrap()
                .0,
            expected_targets()
        );
        c.arm(None);
        assert_eq!(
            decode_writer_target_fields(&expected_targets(), read, SOURCE, limits(), &c)
                .unwrap()
                .0,
            fields
        );
        // Input membership and its nullability are not rebound by this component:
        // the declared target field type comes only from its authored type ID.
        let input = vec![wire::WriterTargetField {
            token: vec![7; 32],
            provider_name: "".into(),
            input_value_id: Some(u32::MAX),
            value_type_id: Some(0),
            hidden: true,
        }];
        c.arm(None);
        let decoded = decode_writer_target_fields(&input, read, SOURCE, limits(), &c)
            .unwrap()
            .0;
        assert!(!decoded[0].ty.nullable());
        assert_eq!(decoded[0].input.get(), u32::MAX);
        assert_eq!(decoded[0].token.to_bytes(), [7; 32]);
    });
}
#[test]
fn writer_prepared_original_loans_and_positive_work_equal_combined_for_all_four_routes() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        let ids = [0, u32::MAX, 7, 9, 11];
        let raw = expected_schema();
        let targets = source_targets(roots);
        let target_ids = [9, u32::MAX];
        let rawtargets = expected_targets();
        c.arm(None);
        let combined = encode_writer_schema(&schema, &ids, types, SOURCE, limits(), &c).unwrap();
        let work = units(&c);
        c.arm(None);
        let prepared =
            prepare_writer_schema_encode(&schema, &ids, types, SOURCE, limits(), &c).unwrap();
        assert!(std::ptr::eq(prepared.input, &schema));
        assert!(std::ptr::eq(prepared.types(), types));
        assert!(std::ptr::eq(
            prepared.control,
            &c as &dyn PureCompileControl
        ));
        assert_eq!(prepared.facts(), &combined.1);
        assert_eq!(prepared.emit().unwrap(), combined);
        assert_eq!(units(&c), work);
        c.arm(None);
        let combined = decode_writer_schema(&raw, read, SOURCE, limits(), &c).unwrap();
        let work = units(&c);
        c.arm(None);
        let prepared = prepare_writer_schema_decode(&raw, read, SOURCE, limits(), &c).unwrap();
        assert!(std::ptr::eq(prepared.input, &raw));
        assert!(std::ptr::eq(prepared.types(), read));
        assert_eq!(prepared.emit().unwrap(), combined);
        assert_eq!(units(&c), work);
        c.arm(None);
        let combined =
            encode_writer_target_fields(&targets, &target_ids, types, SOURCE, limits(), &c)
                .unwrap();
        let work = units(&c);
        c.arm(None);
        let prepared =
            prepare_writer_target_fields_encode(&targets, &target_ids, types, SOURCE, limits(), &c)
                .unwrap();
        assert!(std::ptr::eq(prepared.input, targets.as_ref()));
        assert_eq!(prepared.emit().unwrap(), combined);
        assert_eq!(units(&c), work);
        c.arm(None);
        let combined =
            decode_writer_target_fields(&rawtargets, read, SOURCE, limits(), &c).unwrap();
        let work = units(&c);
        c.arm(None);
        let prepared =
            prepare_writer_target_fields_decode(&rawtargets, read, SOURCE, limits(), &c).unwrap();
        assert_eq!(prepared.emit().unwrap(), combined);
        assert_eq!(units(&c), work);
    });
}
fn exact(f: WriterSchemaProjectionFacts) -> WriterSchemaProjectionLimits {
    WriterSchemaProjectionLimits {
        max_fields: f.field_count,
        max_name_bytes: f.name_bytes,
        max_type_references: f.type_reference_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
fn under(mut l: WriterSchemaProjectionLimits, axis: usize) -> WriterSchemaProjectionLimits {
    match axis {
        0 => l.max_fields -= 1,
        1 => l.max_name_bytes -= 1,
        2 => l.max_type_references -= 1,
        3 => l.max_allocation_requests -= 1,
        4 => l.max_allocation_request_bytes -= 1,
        5 => l.max_coexisting_source_and_request_bytes -= 1,
        6 => l.max_work -= 1,
        _ => unreachable!(),
    };
    l
}
#[test]
fn writer_independent_layout_dictionary_clone_and_seven_caps_all_four_routes() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        let ids = [0, u32::MAX, 7, 9, 11];
        let targets = source_targets(roots);
        let target_ids = [9, u32::MAX];
        let raw = expected_schema();
        let rt = expected_targets();
        let names = raw.fields.iter().map(|f| f.name.len()).sum::<usize>();
        let targetnames = rt.iter().map(|f| f.provider_name.len()).sum::<usize>();
        c.arm(None);
        let se = encode_writer_schema(&schema, &ids, types, SOURCE, limits(), &c)
            .unwrap()
            .1;
        c.arm(None);
        let sd = decode_writer_schema(&raw, read, SOURCE, limits(), &c)
            .unwrap()
            .1;
        c.arm(None);
        let te = encode_writer_target_fields(&targets, &target_ids, types, SOURCE, limits(), &c)
            .unwrap()
            .1;
        c.arm(None);
        let td = decode_writer_target_fields(&rt, read, SOURCE, limits(), &c)
            .unwrap()
            .1;
        assert_eq!(se.allocation_requests_upper_bound, 6);
        assert_eq!(
            se.allocation_request_bytes_upper_bound,
            5 * size_of::<wire::WriterRelationField>() + names
        );
        // Value-type clones retain existing immutable backing; only actual
        // field/name containers allocate on the receiving path.
        assert_eq!(sd.allocation_requests_upper_bound, 12);
        assert_eq!(
            sd.allocation_request_bytes_upper_bound,
            2 * 5 * size_of::<p::WriterRelationField>() + 2 * names
        );
        assert_eq!(te.allocation_requests_upper_bound, 5);
        assert_eq!(
            te.allocation_request_bytes_upper_bound,
            2 * size_of::<wire::WriterTargetField>() + 64 + targetnames
        );
        assert_eq!(td.allocation_requests_upper_bound, 6);
        assert_eq!(
            td.allocation_request_bytes_upper_bound,
            2 * 2 * size_of::<p::WriterTargetField>() + 2 * targetnames
        );
        for route in 0..4 {
            let f = [se, sd, te, td][route];
            let invoke = |l| match route {
                0 => encode_writer_schema(&schema, &ids, types, SOURCE, l, &c).map(|_| ()),
                1 => decode_writer_schema(&raw, read, SOURCE, l, &c).map(|_| ()),
                2 => encode_writer_target_fields(&targets, &target_ids, types, SOURCE, l, &c)
                    .map(|_| ()),
                3 => decode_writer_target_fields(&rt, read, SOURCE, l, &c).map(|_| ()),
                _ => unreachable!(),
            };
            c.arm(None);
            assert!(invoke(exact(f)).is_ok(), "exact route {route}");
            for axis in 0..7 {
                c.arm(None);
                assert!(
                    matches!(
                        invoke(under(exact(f), axis)),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ),
                    "route {route} axis {axis}"
                );
            }
        }
    });
}
#[test]
fn writer_exact_type_required_presence_closed_roles_and_tokens_reject_without_defaults() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        for ids in [
            vec![0],
            vec![0, u32::MAX, 7, 9, 99],
            vec![0, 0, 7, 9, 11],
            vec![0, u32::MAX, 0, 9, 11],
            vec![0, 12, 7, 9, 11], // JSON versus identical physical Utf8.
            vec![0, u32::MAX, 13, 9, 11], // LargeInt versus physical fixed-16.
        ] {
            c.arm(None);
            assert!(encode_writer_schema(&schema, &ids, types, SOURCE, limits(), &c).is_err());
        }
        let mut metadata_source = schema.clone();
        if let DataType::List(field) = &metadata_source.fields[4].ty.data_type() {
            metadata_source.fields[4].ty = FunctionValueType::new(
                DataType::List(Arc::new(
                    field.as_ref().clone().with_nullable(!field.is_nullable()),
                )),
                metadata_source.fields[4].ty.nullable(),
            );
        }
        c.arm(None);
        assert!(
            encode_writer_schema(
                &metadata_source,
                &[0, u32::MAX, 7, 9, 11],
                types,
                SOURCE,
                limits(),
                &c
            )
            .is_err()
        );
        for shape in 0..5 {
            let mut raw = expected_schema();
            match shape {
                0 => raw.fields[0].value_id = None,
                1 => raw.fields[0].value_type_id = None,
                2 => raw.fields[0].value_type_id = Some(99),
                3 => raw.fields[0].role = 0,
                4 => raw.fields[0].role = i32::MAX,
                _ => unreachable!(),
            };
            c.arm(None);
            assert!(decode_writer_schema(&raw, read, SOURCE, limits(), &c).is_err());
        }
        for shape in 0..5 {
            let mut raw = expected_targets();
            match shape {
                0 => raw[0].input_value_id = None,
                1 => raw[0].value_type_id = None,
                2 => raw[0].value_type_id = Some(99),
                3 => {
                    raw[0].token.pop();
                }
                4 => raw[0].token.push(0),
                _ => unreachable!(),
            };
            c.arm(None);
            assert!(decode_writer_target_fields(&raw, read, SOURCE, limits(), &c).is_err());
        }
        let empty = p::WriterRelationSchema {
            revision: 0,
            fields: Box::default(),
        };
        c.arm(None);
        let raw = encode_writer_schema(&empty, &[], types, SOURCE, limits(), &c)
            .unwrap()
            .0;
        assert_eq!(
            raw,
            wire::WriterRelationSchema {
                revision: 0,
                fields: vec![]
            }
        );
        c.arm(None);
        assert_eq!(
            decode_writer_schema(&raw, read, SOURCE, limits(), &c)
                .unwrap()
                .0,
            empty
        );
    });
}

fn small_schema() -> p::WriterRelationSchema {
    p::WriterRelationSchema {
        revision: 0,
        fields: Box::from([p::WriterRelationField {
            value: p::ValueId::new(0),
            name: "a".into(),
            ty: FunctionValueType::new(DataType::Int64, false),
            role: p::WriterRelationFieldRole::Auxiliary,
        }]),
    }
}
fn small_raw() -> wire::WriterRelationSchema {
    wire::WriterRelationSchema {
        revision: 0,
        fields: vec![wire::WriterRelationField {
            value_id: Some(0),
            name: "a".into(),
            value_type_id: Some(0),
            role: wire::WriterRelationFieldRole::Auxiliary as i32,
        }],
    }
}
fn small_target() -> Box<[p::WriterTargetField]> {
    Box::from([p::WriterTargetField {
        token: ConnectorWriteFieldToken::from_bytes([7; 32]),
        provider_name: "a".into(),
        input: p::ValueId::new(0),
        ty: FunctionValueType::new(DataType::Int64, false),
        hidden: false,
    }])
}
fn small_raw_target() -> Vec<wire::WriterTargetField> {
    vec![wire::WriterTargetField {
        token: vec![7; 32],
        provider_name: "a".into(),
        input_value_id: Some(0),
        value_type_id: Some(0),
        hidden: false,
    }]
}
#[test]
fn writer_actual_capacity_source_and_count_work_prefix_are_admitted_before_outputs() {
    let c = Control::default();
    with_types(&c, |_, types, read| {
        let mut raw = small_raw();
        raw.fields
            .reserve_exact(SOURCE / size_of::<wire::WriterRelationField>() + 1);
        c.arm(None);
        assert!(decode_writer_schema(&raw, read, SOURCE, limits(), &c).is_err());
        assert!(!c.trace().iter().any(|(_, u)| *u == 256));
        let mut targets = small_raw_target();
        targets.reserve_exact(SOURCE / size_of::<wire::WriterTargetField>() + 1);
        c.arm(None);
        assert!(decode_writer_target_fields(&targets, read, SOURCE, limits(), &c).is_err());
        assert!(!c.trace().iter().any(|(_, u)| *u == 256));
        let mut raw = small_raw();
        raw.fields[0].name.reserve_exact(SOURCE);
        c.arm(None);
        assert!(decode_writer_schema(&raw, read, SOURCE, limits(), &c).is_err());
        let mut targets = small_raw_target();
        targets[0].token.reserve_exact(SOURCE);
        c.arm(None);
        assert!(decode_writer_target_fields(&targets, read, SOURCE, limits(), &c).is_err());
        let mut wide = small_schema();
        wide.fields = vec![wide.fields[0].clone(); 320].into_boxed_slice();
        let ids = vec![0; 320];
        let mut wire = small_raw();
        wire.fields = vec![wire.fields[0].clone(); 320];
        let low = WriterSchemaProjectionLimits {
            max_work: 1024,
            ..limits()
        };
        c.arm(None);
        assert!(matches!(
            encode_writer_schema(&wide, &ids, types, SOURCE, low, &c),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(!c.trace().iter().any(|(_, u)| *u == 256));
        c.arm(None);
        assert!(matches!(
            decode_writer_schema(&wire, read, SOURCE, low, &c),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(!c.trace().iter().any(|(_, u)| *u == 256));
        c.arm(None);
        assert!(encode_writer_schema(&small_schema(), &[0], types, 0, limits(), &c).is_err());
        c.arm(None);
        assert!(decode_writer_target_fields(&small_raw_target(), read, 0, limits(), &c).is_err());
    });
}
#[test]
fn writer_all_small_callback_prefixes_and_wide_actual_name_copy_preserve_primary() {
    let c = Control::default();
    with_types(&c, |_, types, read| {
        for route in 0..4 {
            for ordinary in [false, true] {
                let schema = small_schema();
                let mut raw = small_raw();
                let target = small_target();
                let mut rt = small_raw_target();
                let id = if ordinary { 99 } else { 0 };
                if ordinary {
                    raw.fields[0].role = 0;
                    rt[0].token.pop();
                }
                let invoke = || match route {
                    0 => encode_writer_schema(&schema, &[id], types, SOURCE, limits(), &c)
                        .map(|_| ()),
                    1 => decode_writer_schema(&raw, read, SOURCE, limits(), &c).map(|_| ()),
                    2 => encode_writer_target_fields(&target, &[id], types, SOURCE, limits(), &c)
                        .map(|_| ()),
                    3 => decode_writer_target_fields(&rt, read, SOURCE, limits(), &c).map(|_| ()),
                    _ => unreachable!(),
                };
                c.arm(None);
                assert_eq!(invoke().is_err(), ordinary);
                let trace = c.trace();
                assert_eq!(
                    trace[0],
                    (
                        if route == 0 || route == 2 {
                            CompilePhase::Encode
                        } else {
                            CompilePhase::Decode
                        },
                        0
                    )
                );
                if !ordinary && (route == 1 || route == 3) {
                    // Vec-to-Box opaque exit and the successful finish both
                    // observe zero pending units; no synthetic step is added.
                    assert_eq!(trace.last(), Some(&(CompilePhase::Decode, 0)));
                } else {
                    assert!(trace.last().is_some_and(|(_, u)| *u > 0));
                }
                for at in 0..trace.len() {
                    for cause in CAUSES {
                        c.arm(Some((at, cause.clone())));
                        assert!(
                            matches!(invoke(),Err(Error::Control(actual))if actual==cause),
                            "route {route}, ordinary {ordinary}, at {at}"
                        );
                        assert_eq!(c.trace(), trace[..=at]);
                    }
                }
            }
        }
        let mut schema = small_schema();
        schema.fields[0].name = "é值".repeat(65536).into_boxed_str();
        let mut raw = small_raw();
        raw.fields[0].name = "é值".repeat(65536);
        for decode in [false, true] {
            let invoke = || {
                if decode {
                    decode_writer_schema(&raw, read, SOURCE, limits(), &c).map(|_| ())
                } else {
                    encode_writer_schema(&schema, &[0], types, SOURCE, limits(), &c).map(|_| ())
                }
            };
            c.arm(None);
            assert!(invoke().is_ok());
            let trace = c.trace();
            let at = trace.iter().position(|(_, u)| *u == 256).unwrap();
            for cause in CAUSES {
                c.arm(Some((at, cause.clone())));
                assert!(matches!(invoke(),Err(Error::Control(actual))if actual==cause));
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
        // Separate genuine 320-field source, sampled at its actual quantum. The
        // source invoice is an explicit whole fixture union, not a MEM grant.
        let mut wide = small_schema();
        wide.fields = vec![wide.fields[0].clone(); 320].into_boxed_slice();
        let ids = vec![0; 320];
        let mut wire = small_raw();
        wire.fields = vec![wire.fields[0].clone(); 320];
        for decode in [false, true] {
            let invoke = || {
                if decode {
                    decode_writer_schema(&wire, read, 128 << 10, limits(), &c).map(|_| ())
                } else {
                    encode_writer_schema(&wide, &ids, types, 128 << 10, limits(), &c).map(|_| ())
                }
            };
            c.arm(None);
            assert!(invoke().is_ok());
            let trace = c.trace();
            let at = trace.iter().position(|(_, u)| *u == 256).unwrap();
            for cause in CAUSES {
                c.arm(Some((at, cause.clone())));
                assert!(matches!(invoke(),Err(Error::Control(actual))if actual==cause));
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    });
}

#[test]
fn writer_known_name_limit_precedes_next_real_public_quantum_for_both_directions() {
    let c = Control::default();
    with_types(&c, |_, types, read| {
        // Before the final field, the original sender has completed 510 units:
        // its ten-unit header/count prefix and fifty ten-unit field prefixes.
        let mut schema = small_schema();
        schema.fields = vec![schema.fields[0].clone(); 51].into_boxed_slice();
        let ids = vec![0; 51];
        let send_limits = WriterSchemaProjectionLimits {
            max_name_bytes: 50,
            ..limits()
        };
        let send_prefix = vec![(CompilePhase::Encode, 0), (CompilePhase::Encode, 256)];
        c.arm(None);
        assert!(matches!(
            prepare_writer_schema_encode(&schema, &ids, types, SOURCE, send_limits, &c),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(c.trace(), send_prefix);
        for cause in CAUSES {
            // A later refusal must never be requested after the known limit.
            c.arm(Some((send_prefix.len(), cause)));
            assert!(matches!(
                prepare_writer_schema_encode(&schema, &ids, types, SOURCE, send_limits, &c),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), send_prefix);
        }

        // The receiver completes nine initial units plus 78 * 13 field units
        // before the last name is known to exceed the original name budget.
        let mut raw = small_raw();
        raw.fields = vec![raw.fields[0].clone(); 79];
        let receive_limits = WriterSchemaProjectionLimits {
            max_name_bytes: 78,
            ..limits()
        };
        let receive_prefix = vec![
            (CompilePhase::Decode, 0),
            (CompilePhase::Decode, 256),
            (CompilePhase::Decode, 256),
            (CompilePhase::Decode, 256),
        ];
        c.arm(None);
        assert!(matches!(
            prepare_writer_schema_decode(&raw, read, SOURCE, receive_limits, &c),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(c.trace(), receive_prefix);
        for cause in CAUSES {
            c.arm(Some((receive_prefix.len(), cause)));
            assert!(matches!(
                prepare_writer_schema_decode(&raw, read, SOURCE, receive_limits, &c),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), receive_prefix);
        }
    });
}

#[test]
fn writer_count_all_known_non_name_limits_precede_pending_255_callback() {
    let c = Control::default();
    let mut count = Count::new(1);
    count.model.request::<u8>(1, 1).unwrap();
    let original = count
        .model
        .numerical_facts(128, 1, limits().resources())
        .unwrap();
    let admitted = WriterSchemaProjectionLimits {
        max_fields: original.list_item_count,
        max_name_bytes: 0,
        max_type_references: original.value_reference_count,
        max_allocation_requests: original.allocation_requests_upper_bound,
        max_allocation_request_bytes: original.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: original
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: original.cumulative_work_upper_bound,
    };
    for axis in [0, 2, 3, 4, 5, 6] {
        for cause in CAUSES {
            c.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            // This is a private numeric-gate unit test, not a public source
            // traversal claim. Each comparison is completed on the real meter.
            for value in 0..255usize {
                assert!(value < 255);
                work.step().unwrap();
            }
            assert!(matches!(
                count.facts(128, 1, under(admitted, axis), &mut work),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), vec![(CompilePhase::Encode, 0)], "axis {axis}");
            // Direct primary errors do not receive an ordinary/success footer.
        }
    }
}

#[test]
fn writer_sole_clone_ceiling_is_admitted_before_walk_and_retained_for_exact_replay() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let ceiling = physical_type_v2::value_type_clone_preflight_work_upper_bound();
        assert_eq!(ceiling, 2);
        // Plain and normalized encoded/nested sources all use the same two
        // strong-handle clone work units, with no new type allocation.
        for id in [0, 9, 11] {
            let ty = &roots
                .iter()
                .find(|(candidate, _)| *candidate == id)
                .unwrap()
                .1;
            let schema = p::WriterRelationSchema {
                revision: 0,
                fields: vec![p::WriterRelationField {
                    value: p::ValueId::new(0),
                    name: "a".into(),
                    ty: ty.clone(),
                    role: p::WriterRelationFieldRole::Auxiliary,
                }]
                .into_boxed_slice(),
            };
            let raw = wire::WriterRelationSchema {
                revision: 0,
                fields: vec![wire::WriterRelationField {
                    value_id: Some(0),
                    name: "a".into(),
                    value_type_id: Some(id),
                    role: wire::WriterRelationFieldRole::Auxiliary as i32,
                }],
            };
            let roots_count = types.source_counts().0;
            let mut send_initial = Count::new(1);
            send_initial.model.delegated_work = roots_count + 8 + ceiling;
            let send_floor = send_initial
                .model
                .numerical_facts(SOURCE, roots_count, limits().resources())
                .unwrap()
                .cumulative_work_upper_bound;
            let mut receive_initial = Count::new(1);
            receive_initial.model.delegated_work =
                2 * crate::btree_resources_v2::lookup_work(read.value_types().len()).unwrap()
                    + 2 * ceiling;
            let receive_floor = receive_initial
                .model
                .numerical_facts(SOURCE, read.value_types().len(), limits().resources())
                .unwrap()
                .cumulative_work_upper_bound;
            for (decode, early_floor) in [(false, send_floor), (true, receive_floor)] {
                let low = WriterSchemaProjectionLimits {
                    max_work: early_floor - 1,
                    ..limits()
                };
                for cause in CAUSES {
                    c.arm(Some((1, cause)));
                    let refused = if decode {
                        prepare_writer_schema_decode(&raw, read, SOURCE, low, &c).map(|_| ())
                    } else {
                        prepare_writer_schema_encode(&schema, &[id], types, SOURCE, low, &c)
                            .map(|_| ())
                    };
                    assert!(matches!(
                        refused,
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ));
                    assert_eq!(
                        c.trace(),
                        vec![(
                            if decode {
                                CompilePhase::Decode
                            } else {
                                CompilePhase::Encode
                            },
                            0
                        )],
                        "id {id}, decode {decode}: rejected before type walk"
                    );
                }
            }

            // The expected final work is independently assembled with the
            // original numeric and borrowed-type authors. The clone ceiling
            // must remain in it even when the actual clone topology is small.
            c.arm(None);
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            let encoded_ty = types.value_type_observed(id, &mut work).unwrap().unwrap();
            let compared = verify_type_binding(
                &schema.fields[0].ty,
                encoded_ty,
                SOURCE,
                limits().max_work,
                &mut work,
            )
            .unwrap();
            assert!(compared.matches());
            send_initial
                .model
                .request::<wire::WriterRelationField>(1, 1)
                .unwrap();
            send_initial.model.request::<u8>(1, 1).unwrap();
            send_initial.model.delegated_work += compared.work_upper_bound();
            let expected_send = send_initial
                .model
                .numerical_facts(SOURCE, roots_count, limits().resources())
                .unwrap()
                .cumulative_work_upper_bound;

            c.arm(None);
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            let decoded_ty = read.value_type(id).unwrap();
            let compared = preflight_type_binding(
                decoded_ty,
                decoded_ty,
                SOURCE,
                limits().max_work,
                &mut work,
            )
            .unwrap();
            let clone =
                physical_type_v2::preflight_value_type_clone(decoded_ty, &mut work).unwrap();
            assert_eq!(clone.work_upper_bound(), ceiling);
            assert_eq!(clone.allocation_requests_upper_bound(), 0);
            receive_initial
                .model
                .request::<p::WriterRelationField>(1, 2)
                .unwrap();
            receive_initial.model.request::<u8>(1, 2).unwrap();
            receive_initial.model.requests += clone.allocation_requests_upper_bound();
            receive_initial.model.requested += clone.allocation_request_bytes_upper_bound();
            receive_initial.model.delegated_work += compared.work_upper_bound();
            let expected_receive = receive_initial
                .model
                .numerical_facts(SOURCE, read.value_types().len(), limits().resources())
                .unwrap()
                .cumulative_work_upper_bound;

            c.arm(None);
            let send = encode_writer_schema(&schema, &[id], types, SOURCE, limits(), &c).unwrap();
            assert_eq!(send.1.cumulative_work_upper_bound, expected_send);
            c.arm(None);
            let receive = decode_writer_schema(&raw, read, SOURCE, limits(), &c).unwrap();
            assert_eq!(receive.1.cumulative_work_upper_bound, expected_receive);
            assert_eq!(receive.0, schema);
            c.arm(None);
            assert_eq!(
                encode_writer_schema(&schema, &[id], types, SOURCE, exact(send.1), &c).unwrap(),
                send
            );
            c.arm(None);
            assert_eq!(
                decode_writer_schema(&raw, read, SOURCE, exact(receive.1), &c).unwrap(),
                receive
            );
            c.arm(None);
            assert!(matches!(
                encode_writer_schema(&schema, &[id], types, SOURCE, under(exact(send.1), 6), &c),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            c.arm(None);
            assert!(matches!(
                decode_writer_schema(&raw, read, SOURCE, under(exact(receive.1), 6), &c),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    });
}

fn parent_limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 64,
        max_value_references: 4096,
        max_list_items: 8192,
        max_allocation_requests: 32768,
        max_allocation_request_bytes: 32 << 20,
        max_coexisting_source_and_request_bytes: 64 << 20,
        max_work: 1 << 32,
        properties: limits().resources().properties,
    }
}
fn parent_base() -> Model {
    Model {
        inputs: 1,
        refs: 2,
        items: 3,
        requests: 1,
        requested: 8,
        delegated_work: 19,
    }
}
fn projection(base: Model, node: NodeProjectionLimits) -> WriterSchemaProjection {
    WriterSchemaProjection {
        source: SOURCE,
        limits: limits(),
        parent: WriterSchemaNodeAdmission {
            base,
            values: 2,
            limits: node,
        },
    }
}

#[test]
fn writer_observed_two_schemas_accumulate_same_node_without_type_value_or_source_duplication() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        let raw = expected_schema();
        let ids = [0, u32::MAX, 7, 9, 11];
        let targets = source_targets(roots);
        let rawtargets = expected_targets();
        for decode in [false, true] {
            c.arm(None);
            let standalone = if decode {
                prepare_writer_schema_decode(&raw, read, SOURCE, limits(), &c)
                    .unwrap()
                    .facts
            } else {
                prepare_writer_schema_encode(&schema, &ids, types, SOURCE, limits(), &c)
                    .unwrap()
                    .facts
            };
            let original_trace = c.trace();
            c.arm(None);
            let phase = if decode {
                CompilePhase::Decode
            } else {
                CompilePhase::Encode
            };
            let mut w = CompileCheckpoints::try_new(&c, phase).unwrap();
            let first = if decode {
                preflight_schema_decode_observed(
                    &raw,
                    read,
                    projection(parent_base(), parent_limits()),
                    &mut w,
                )
                .unwrap()
            } else {
                preflight_schema_encode_observed(
                    &schema,
                    &ids,
                    types,
                    projection(parent_base(), parent_limits()),
                    &mut w,
                )
                .unwrap()
            };
            finish(w, Ok(())).unwrap();
            assert_eq!(first, standalone);
            assert_eq!(
                c.trace(),
                original_trace,
                "same original meter, no nested entry/footer"
            );
            let admission = WriterSchemaNodeAdmission {
                base: parent_base(),
                values: 2,
                limits: parent_limits(),
            };
            let after_first = admission.merge(first).unwrap();
            c.arm(None);
            let mut w = CompileCheckpoints::try_new(&c, phase).unwrap();
            let second = if decode {
                preflight_targets_decode_observed(
                    &rawtargets,
                    read,
                    projection(after_first, parent_limits()),
                    &mut w,
                )
                .unwrap()
            } else {
                preflight_targets_encode_observed(
                    &targets,
                    &[9, u32::MAX],
                    types,
                    projection(after_first, parent_limits()),
                    &mut w,
                )
                .unwrap()
            };
            let merged = WriterSchemaNodeAdmission {
                base: after_first,
                ..admission
            }
            .merge(second)
            .unwrap();
            let f = merged.numerical_facts(SOURCE, 2, parent_limits()).unwrap();
            // Actual Value occurrences belong to the caller; seven type references
            // are not seven additional Value lookups. B occurs exactly once.
            assert_eq!(
                (
                    f.input_node_count,
                    f.value_reference_count,
                    f.list_item_count
                ),
                (1, 2, 10)
            );
            assert_eq!(
                f.allocation_requests_upper_bound,
                1 + first.allocation_requests_upper_bound + second.allocation_requests_upper_bound
            );
            let bytes = 8
                + first.allocation_request_bytes_upper_bound
                + second.allocation_request_bytes_upper_bound;
            assert_eq!(f.allocation_request_bytes_upper_bound, bytes);
            assert_eq!(
                f.coexisting_source_and_request_bytes_upper_bound,
                SOURCE + bytes
            );
            assert_eq!(
                f.cumulative_work_upper_bound,
                256 + 32 * 11
                    + 2 * 35
                    + 4 * bytes
                    + 19
                    + first.cumulative_work_upper_bound
                    + second.cumulative_work_upper_bound
            );
            finish(w, Ok(())).unwrap();
            let exact = NodeProjectionLimits {
                max_list_items: 10,
                max_allocation_requests: f.allocation_requests_upper_bound,
                max_allocation_request_bytes: bytes,
                max_coexisting_source_and_request_bytes: SOURCE + bytes,
                max_work: f.cumulative_work_upper_bound,
                ..parent_limits()
            };
            c.arm(None);
            let mut w = CompileCheckpoints::try_new(&c, phase).unwrap();
            let result = if decode {
                preflight_targets_decode_observed(
                    &rawtargets,
                    read,
                    projection(after_first, exact),
                    &mut w,
                )
            } else {
                preflight_targets_encode_observed(
                    &targets,
                    &[9, u32::MAX],
                    types,
                    projection(after_first, exact),
                    &mut w,
                )
            };
            assert_eq!(finish(w, result).unwrap(), second);
            c.arm(None);
            let mut w = CompileCheckpoints::try_new(&c, phase).unwrap();
            let under = NodeProjectionLimits {
                max_work: exact.max_work - 1,
                ..exact
            };
            let result = if decode {
                preflight_targets_decode_observed(
                    &rawtargets,
                    read,
                    projection(after_first, under),
                    &mut w,
                )
            } else {
                preflight_targets_encode_observed(
                    &targets,
                    &[9, u32::MAX],
                    types,
                    projection(after_first, under),
                    &mut w,
                )
            };
            assert!(matches!(
                finish(w, result),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    });
}

#[test]
fn writer_observed_name_requests_gate_containing_node_before_late_three_causes() {
    let c = Control::default();
    with_types(&c, |_, types, read| {
        let schema = p::WriterRelationSchema {
            revision: 0,
            fields: Box::from([p::WriterRelationField {
                value: p::ValueId::new(0),
                name: "x".repeat(300).into(),
                ty: FunctionValueType::new(DataType::Int64, false),
                role: p::WriterRelationFieldRole::Kind,
            }]),
        };
        let raw = wire::WriterRelationSchema {
            revision: 0,
            fields: vec![wire::WriterRelationField {
                value_id: Some(0),
                name: "x".repeat(300),
                value_type_id: Some(0),
                role: wire::WriterRelationFieldRole::Kind as i32,
            }],
        };
        for decode in [false, true] {
            let copies = if decode { 2 } else { 1 };
            let field_bytes = if decode {
                size_of::<p::WriterRelationField>()
            } else {
                size_of::<wire::WriterRelationField>()
            };
            let bytes = 8 + copies * (field_bytes + 300);
            let requests = 1 + copies * 2;
            let roots = if decode {
                read.value_types().len()
            } else {
                types.source_counts().0
            };
            let lookup = if decode {
                2 * crate::btree_resources_v2::lookup_work(roots).unwrap()
            } else {
                roots + 8
            };
            let ceiling = copies * physical_type_v2::value_type_clone_preflight_work_upper_bound();
            let height = (usize::BITS - roots.leading_zeros()) as usize + 1;
            let childwork =
                256 + 32 + height + 32 + 4 * copies * (field_bytes + 300) + lookup + ceiling;
            let parentwork = 256 + 32 * 5 + 2 * 35 + 4 * bytes + 19 + childwork;
            for axis in 0..4 {
                let mut low = parent_limits();
                match axis {
                    0 => low.max_allocation_requests = requests - 1,
                    1 => low.max_allocation_request_bytes = bytes - 1,
                    2 => low.max_coexisting_source_and_request_bytes = SOURCE + bytes - 1,
                    3 => low.max_work = parentwork - 1,
                    _ => unreachable!(),
                }
                for cause in CAUSES {
                    c.arm(Some((1, cause)));
                    let phase = if decode {
                        CompilePhase::Decode
                    } else {
                        CompilePhase::Encode
                    };
                    let mut w = CompileCheckpoints::try_new(&c, phase).unwrap();
                    let result = if decode {
                        preflight_schema_decode_observed(
                            &raw,
                            read,
                            projection(parent_base(), low),
                            &mut w,
                        )
                    } else {
                        preflight_schema_encode_observed(
                            &schema,
                            &[0],
                            types,
                            projection(parent_base(), low),
                            &mut w,
                        )
                    };
                    assert!(
                        matches!(
                            finish(w, result),
                            Err(Error::Control(CompileControlError::ResourceExhausted))
                        ),
                        "decode{decode} axis{axis}"
                    );
                    assert_eq!(
                        c.trace(),
                        vec![(phase, 0)],
                        "real name update, before next observer"
                    );
                }
            }
        }
    });
}

#[test]
fn writer_observed_parent_remaining_limits_original_borrowed_metadata_visits() {
    let c = Control::default();
    // Use the original actual metadata source and both original type namespaces.
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        let raw = expected_schema();
        let ids = [0, u32::MAX, 7, 9, 11];
        for decode in [false, true] {
            let phase = if decode {
                CompilePhase::Decode
            } else {
                CompilePhase::Encode
            };
            c.arm(None);
            let mut w = CompileCheckpoints::try_new(&c, phase).unwrap();
            let first = if decode {
                preflight_schema_decode_observed(
                    &raw,
                    read,
                    projection(parent_base(), parent_limits()),
                    &mut w,
                )
            } else {
                preflight_schema_encode_observed(
                    &schema,
                    &ids,
                    types,
                    projection(parent_base(), parent_limits()),
                    &mut w,
                )
            };
            let first = finish(w, first).unwrap();
            let success = c.trace();
            let whole = WriterSchemaNodeAdmission {
                base: parent_base(),
                values: 2,
                limits: parent_limits(),
            }
            .merge(first)
            .unwrap()
            .numerical_facts(SOURCE, 2, parent_limits())
            .unwrap();
            let low = NodeProjectionLimits {
                max_work: whole.cumulative_work_upper_bound - 1,
                ..parent_limits()
            };
            c.arm(None);
            let mut w = CompileCheckpoints::try_new(&c, phase).unwrap();
            let result = if decode {
                preflight_schema_decode_observed(&raw, read, projection(parent_base(), low), &mut w)
            } else {
                preflight_schema_encode_observed(
                    &schema,
                    &ids,
                    types,
                    projection(parent_base(), low),
                    &mut w,
                )
            };
            assert!(matches!(
                finish(w, result),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            let prefix = c.trace();
            assert!(
                prefix.len() > 4,
                "not merely entry/initial-ceiling admission"
            );
            assert!(
                prefix.len() < success.len(),
                "original walk stops before complete successful trace"
            );
            assert_eq!(prefix, success[..prefix.len()]);
            for cause in CAUSES {
                c.arm(Some((prefix.len(), cause)));
                let mut w = CompileCheckpoints::try_new(&c, phase).unwrap();
                let result = if decode {
                    preflight_schema_decode_observed(
                        &raw,
                        read,
                        projection(parent_base(), low),
                        &mut w,
                    )
                } else {
                    preflight_schema_encode_observed(
                        &schema,
                        &ids,
                        types,
                        projection(parent_base(), low),
                        &mut w,
                    )
                };
                assert!(matches!(
                    finish(w, result),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(c.trace(), prefix);
            }
        }
    });
}

#[test]
fn writer_schema_parent_prefix_replacement_exact_replay_and_actual_callbacks() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        let raw = expected_schema();
        let ids = [0, u32::MAX, 7, 9, 11];
        for decode in [false, true] {
            let run = |node_limits, snapshots: &mut Vec<NodeProjectionFacts>| {
                let phase = if decode {
                    CompilePhase::Decode
                } else {
                    CompilePhase::Encode
                };
                let mut work = CompileCheckpoints::try_new(&c, phase)?;
                let mut admit = |f: &NodeProjectionFacts| {
                    snapshots.push(*f);
                    Ok(())
                };
                let result = (|| {
                    let f = if decode {
                        preflight_schema_decode_in(
                            &raw,
                            read,
                            projection(parent_base(), node_limits),
                            &mut admit,
                            &mut work,
                        )?
                    } else {
                        preflight_schema_encode_in(
                            &schema,
                            &ids,
                            types,
                            projection(parent_base(), node_limits),
                            &mut admit,
                            &mut work,
                        )?
                    };
                    if decode {
                        assert_eq!(read_schema(&raw, read, &mut work)?, schema);
                    } else {
                        assert_eq!(emit_schema(&schema, &ids, &mut work)?, raw);
                    }
                    Ok(f)
                })();
                finish(work, result)
            };
            c.arm(None);
            let mut snapshots = vec![];
            let f = run(parent_limits(), &mut snapshots).unwrap();
            let trace = c.trace();
            let merged = WriterSchemaNodeAdmission {
                base: parent_base(),
                values: 2,
                limits: parent_limits(),
            }
            .merge(f)
            .unwrap();
            let complete = merged.numerical_facts(SOURCE, 2, parent_limits()).unwrap();
            // Base is one original contribution, and B is counted once.
            assert_eq!(
                complete.allocation_requests_upper_bound,
                1 + f.allocation_requests_upper_bound
            );
            assert_eq!(
                complete.allocation_request_bytes_upper_bound,
                8 + f.allocation_request_bytes_upper_bound
            );
            for prefix in snapshots {
                assert!(
                    prefix.allocation_requests_upper_bound
                        <= complete.allocation_requests_upper_bound
                );
                assert!(prefix.cumulative_work_upper_bound <= complete.cumulative_work_upper_bound);
                assert_eq!(
                    prefix.coexisting_source_and_request_bytes_upper_bound,
                    SOURCE + prefix.allocation_request_bytes_upper_bound
                );
            }
            let mut exact = parent_limits();
            exact.max_input_nodes = complete.input_node_count;
            exact.max_value_references = complete.value_reference_count;
            exact.max_list_items = complete.list_item_count;
            exact.max_allocation_requests = complete.allocation_requests_upper_bound;
            exact.max_allocation_request_bytes = complete.allocation_request_bytes_upper_bound;
            exact.max_coexisting_source_and_request_bytes =
                complete.coexisting_source_and_request_bytes_upper_bound;
            exact.max_work = complete.cumulative_work_upper_bound;
            c.arm(None);
            assert_eq!(run(exact, &mut vec![]).unwrap(), f);
            for at in 0..trace.len() {
                for cause in CAUSES {
                    c.arm(Some((at, cause.clone())));
                    assert!(
                        matches!(run(parent_limits(), &mut vec![]), Err(Error::Control(actual)) if actual == cause)
                    );
                    assert_eq!(c.trace(), trace[..=at]);
                }
            }
        }
    });
}

#[test]
fn writer_schema_parent_actual_name_requests_precede_lookup_completion() {
    let c = Control::default();
    with_types(&c, |roots, _, read| {
        let raw = wire::WriterRelationSchema {
            revision: 0,
            fields: vec![wire::WriterRelationField {
                value_id: Some(0),
                name: "a".into(),
                value_type_id: Some(9),
                role: role(p::WriterRelationFieldRole::CommitFragment),
            }],
        };
        assert!(matches!(
            roots.iter().find(|r| r.0 == 9).unwrap().1.data_type(),
            DataType::Utf8
        ));
        // One base request and the actual Vec+Box fields/name requests
        // precede type lookup; there are no type-clone Box requests.
        let before = 1 + 2 + 1;
        // Actual name Vec/Box request pair is gated before the type lookup;
        // the immutable value-type clone adds no allocation.
        let complete_requests = before + 1;
        let mut at_capture = None;
        c.arm(None);
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let mut admit = |f: &NodeProjectionFacts| {
            if f.allocation_requests_upper_bound == complete_requests && at_capture.is_none() {
                at_capture = Some(c.trace().len());
            }
            Ok(())
        };
        let f = preflight_schema_decode_in(
            &raw,
            read,
            projection(parent_base(), parent_limits()),
            &mut admit,
            &mut work,
        )
        .unwrap();
        finish(work, Ok(())).unwrap();
        assert_eq!(f.allocation_requests_upper_bound, 2 + 2);
        let capture = at_capture.unwrap();
        for cause in CAUSES {
            c.arm(Some((capture, cause)));
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            let mut tight = parent_limits();
            tight.max_allocation_requests = before;
            let mut admit = |f: &NodeProjectionFacts| {
                assert!(f.allocation_requests_upper_bound <= before);
                Ok(())
            };
            let result = preflight_schema_decode_in(
                &raw,
                read,
                projection(parent_base(), tight),
                &mut admit,
                &mut work,
            );
            assert!(matches!(
                finish(work, result),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert!(c.trace().len() <= capture);
        }
    });
}

#[test]
fn writer_schema_captured_comparison_prefix_numeric_first_and_final_peak() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = p::WriterRelationSchema {
            revision: 0,
            fields: Box::from([p::WriterRelationField {
                value: p::ValueId::new(0),
                name: "a".into(),
                ty: roots[3].1.clone(),
                role: p::WriterRelationFieldRole::CommitFragment,
            }]),
        };
        let raw = wire::WriterRelationSchema {
            revision: 0,
            fields: vec![wire::WriterRelationField {
                value_id: Some(0),
                name: "a".into(),
                value_type_id: Some(9),
                role: role(p::WriterRelationFieldRole::CommitFragment),
            }],
        };
        for decode in [false, true] {
            let run = |node_limits, snapshots: &mut Vec<(usize, NodeProjectionFacts)>| {
                let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode)?;
                let mut admit = |f: &NodeProjectionFacts| {
                    snapshots.push((c.trace().len(), *f));
                    Ok(())
                };
                let result = if decode {
                    preflight_schema_decode_in(
                        &raw,
                        read,
                        projection(parent_base(), node_limits),
                        &mut admit,
                        &mut work,
                    )
                } else {
                    preflight_schema_encode_in(
                        &schema,
                        &[9],
                        types,
                        projection(parent_base(), node_limits),
                        &mut admit,
                        &mut work,
                    )
                };
                finish(work, result)
            };
            c.arm(None);
            let mut snapshots = vec![];
            let f = run(parent_limits(), &mut snapshots).unwrap();
            // The comparison's fixed scratch initialization is real work;
            // retained source bytes are a separate floor, never a multiplier.
            let comparison_prefix = type_binding_prefix_work_upper_bound_in(
                &schema.fields[0].ty,
                &schema.fields[0].ty,
                SOURCE,
            )
            .unwrap()
            .work_upper_bound();
            let (at, captured) = snapshots
                .iter()
                .copied()
                .find(|(_, f)| f.cumulative_work_upper_bound >= comparison_prefix)
                .unwrap();
            let final_node = WriterSchemaNodeAdmission {
                base: parent_base(),
                values: 2,
                limits: parent_limits(),
            }
            .merge(f)
            .unwrap()
            .numerical_facts(SOURCE, 2, parent_limits())
            .unwrap();
            assert!(final_node.cumulative_work_upper_bound >= captured.cumulative_work_upper_bound);
            for cause in CAUSES {
                c.arm(Some((at, cause)));
                let mut tight = parent_limits();
                tight.max_work = captured.cumulative_work_upper_bound - 1;
                assert!(matches!(
                    run(tight, &mut vec![]),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert!(c.trace().len() <= at);
            }
        }
    });
    // Actual count-author seam: no datatype walk or allocation is needed to
    // test the sole captured-root arithmetic and its pending-tail priority.
    let ty = FunctionValueType::new(DataType::Int64, false);
    let prefix = type_binding_prefix_work_upper_bound_in(&ty, &ty, SOURCE)
        .unwrap()
        .work_upper_bound();
    // Overflow genuine cumulative delegated-work addition, independently of
    // source bytes. This private arithmetic seam performs no fake allocation.
    for delegated in [usize::MAX, usize::MAX - prefix + 1] {
        for cause in CAUSES {
            c.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut callback =
                |_: &WriterSchemaProjectionFacts| -> Result<(), CompileControlError> {
                    panic!("known numeric overflow precedes parent")
                };
            let mut count = Count::new(1);
            count.admit = Some(&mut callback);
            count.model.delegated_work = delegated;
            let result = count.comparison_prefix(&ty, &ty, SOURCE, 1, limits());
            assert!(matches!(
                finish(work, result),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), vec![(CompilePhase::Decode, 0)]);
        }
    }
}
