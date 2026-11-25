// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use arrow::datatypes::{IntervalUnit, TimeUnit, UnionFields, UnionMode};
use novarocks_proto_models::{plan, value_type as logical};
use novarocks_type_contract::{MAX_VALUE_TYPE_DEPTH, NR_LOGICAL_TYPE_KEY};
use prost::Message;
use std::{collections::HashMap, sync::Mutex};
use wire::carrier_type_definition::Kind;

#[derive(Default)]
struct Control {
    fail: Option<(usize, CompileControlError)>,
    events: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut events = self.events.lock().unwrap();
        events.push((phase, units));
        if let Some((at, error)) = self.fail.clone()
            && at == events.len()
        {
            return Err(error);
        }
        Ok(())
    }
}
fn limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 100_000,
        max_string_bytes: 100_000,
    }
}
fn c(id: u32, kind: Kind) -> wire::CarrierTypeDefinition {
    wire::CarrierTypeDefinition {
        id,
        kind: Some(kind),
    }
}
fn f(id: u32, carrier: u32) -> wire::FieldDefinition {
    wire::FieldDefinition {
        id,
        name: "x".into(),
        nullable: true,
        carrier_type_id: Some(carrier),
        logical_type_id: None,

        value_binding_id: None,
    }
}
fn v(id: u32, carrier: u32) -> wire::ValueTypeDefinition {
    wire::ValueTypeDefinition {
        id,
        carrier_type_id: Some(carrier),
        logical_type_id: None,
    }
}
fn scalar_semantic(id: u32, kind: logical::ScalarKind) -> logical::LogicalTypeDefinition {
    logical::LogicalTypeDefinition {
        id: Some(id),
        nullable: true,
        kind: Some(logical::logical_type_definition::Kind::Scalar(
            logical::ScalarValue {
                kind: kind as i32,
                ..Default::default()
            },
        )),
    }
}
fn list_semantic(id: u32, item: u32) -> logical::LogicalTypeDefinition {
    logical::LogicalTypeDefinition {
        id: Some(id),
        nullable: true,
        kind: Some(logical::logical_type_definition::Kind::ListItemTypeId(item)),
    }
}
fn sparse_list() -> wire::TypeTable {
    // Four explicit namespaces deliberately reuse zero and MAX.
    let mut item = f(0, 0);
    item.name = "item".into();
    item.logical_type_id = Some(0);
    let mut root = v(u32::MAX, u32::MAX);
    root.logical_type_id = Some(u32::MAX);
    wire::TypeTable {
        logical_types: vec![
            scalar_semantic(0, logical::ScalarKind::Int32),
            list_semantic(u32::MAX, 0),
        ],
        carriers: vec![c(u32::MAX, Kind::ListFieldId(0)), c(0, Kind::Primitive(5))],
        fields: vec![item],
        value_types: vec![root],
    }
}
fn reject(table: &wire::TypeTable) {
    assert!(decode_type_table(table, limits(), &Control::default()).is_err());
}
// A source occurrence is authored independently of the execution namespace.
fn source_table(mut shape: wire::TypeTable) -> wire::SourceFieldTable {
    shape.value_types.clear();
    shape.logical_types.clear();
    for field in &mut shape.fields {
        field.logical_type_id = None;
        field.value_binding_id = None;
    }
    let field_facts = shape
        .fields
        .iter()
        .map(|field| wire::SourceArrowFieldFacts {
            field_id: Some(field.id),
            metadata: vec![],
            dictionary_id: None,
            dictionary_is_ordered: None,
        })
        .collect();
    wire::SourceFieldTable {
        shape: Some(shape),
        field_facts,
    }
}
fn decode_source(
    table: &wire::SourceFieldTable,
    projection: TypeProjectionLimits,
) -> Result<DecodedSourceFieldTable, TypeCodecError> {
    use crate::host_projection_v2::ProjectionFailure;
    use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
    let shape = table.shape.as_ref().unwrap();
    // These fixtures create fresh public Vec/String backing; count its actual capacity.
    let source = std::mem::size_of_val(table)
        + std::mem::size_of_val(shape)
        + shape.carriers.capacity() * std::mem::size_of::<wire::CarrierTypeDefinition>()
        + shape.fields.capacity() * std::mem::size_of::<wire::FieldDefinition>()
        + shape.value_types.capacity() * std::mem::size_of::<wire::ValueTypeDefinition>()
        + shape.logical_types.capacity() * std::mem::size_of::<logical::LogicalTypeDefinition>()
        + shape
            .fields
            .iter()
            .map(|field| field.name.capacity())
            .sum::<usize>()
        + shape
            .carriers
            .iter()
            .map(|carrier| match &carrier.kind {
                Some(Kind::Timestamp(timestamp)) => {
                    timestamp.timezone.as_ref().map_or(0, String::capacity)
                }
                _ => 0,
            })
            .sum::<usize>()
        + table.field_facts.capacity() * std::mem::size_of::<wire::SourceArrowFieldFacts>()
        + table
            .field_facts
            .iter()
            .map(|facts| {
                facts.metadata.capacity() * std::mem::size_of::<plan::ArrowFieldMetadataEntry>()
                    + facts
                        .metadata
                        .iter()
                        .map(|entry| entry.key.capacity() + entry.value.capacity())
                        .sum::<usize>()
            })
            .sum::<usize>();
    let mut exact = |_: std::alloc::Layout| Ok::<_, CompileControlError>(());
    let mut opaque = |_: usize| Ok::<_, CompileControlError>(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode)?;
    let decoded = decode_source_field_table_with_creator(
        table,
        source,
        PackageTypeProjectionLimits {
            max_definitions: projection.max_definitions,
            max_expanded_nodes: projection.max_expanded_nodes,
            max_string_bytes: projection.max_string_bytes,
            max_allocation_requests: 10000,
            max_allocation_request_bytes: 1024 * 1024,
            max_coexisting_source_and_request_bytes: 2 * 1024 * 1024,
            max_work: 4 * 1024 * 1024,
        },
        &mut creation,
        &mut work,
    )
    .map_err(|error| match error {
        ProjectionFailure::Codec(error) => error,
        ProjectionFailure::Host(error) => TypeCodecError::Control(error),
    })?;
    work.finish()?;
    Ok(decoded)
}
fn source_field<'a>(
    decoded: &'a DecodedSourceFieldTable,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> &'a Arc<Field> {
    decoded.field_observed(id, work).unwrap().unwrap()
}
fn roundtrip(values: &[(u32, FunctionValueType)]) -> (wire::TypeTable, DecodedTypeTable) {
    let dto = encode_type_table(values, limits(), &Control::default()).unwrap();
    let decoded = decode_type_table(&dto, limits(), &Control::default()).unwrap();
    assert_eq!(decoded.value_types().len(), values.len());
    for (id, value) in values {
        assert_eq!(decoded.value_type(*id), Some(value));
    }
    (dto, decoded)
}

#[test]
fn handauthored_sparse_list_resolves_exact_separate_namespaces() {
    let table = sparse_list();
    let decoded = decode_type_table(&table, limits(), &Control::default()).unwrap();
    let child = Arc::new(Field::new("item", DataType::Int32, true));
    let expected = FunctionValueType::new(DataType::List(child.clone()), true);
    assert_eq!(decoded.value_type(u32::MAX), Some(&expected));
    assert_eq!(decoded.carrier(0), Some(&DataType::Int32));
    assert!(novarocks_type_contract::arrow_fields_exact(
        decoded.field(0).unwrap(),
        &child
    ));
    assert!(decoded.carrier(1).is_none());
    assert!(decoded.field(u32::MAX).is_none());
}

#[test]
fn fixed_binary_sixteen_preserves_three_explicit_root_identities_without_inference() {
    let table = wire::TypeTable {
        logical_types: vec![
            logical::LogicalTypeDefinition {
                kind: Some(logical::logical_type_definition::Kind::Scalar(
                    logical::ScalarValue {
                        fixed_binary_width: Some(16),
                        ..logical::ScalarValue {
                            kind: logical::ScalarKind::FixedSizeBinary as i32,
                            ..Default::default()
                        }
                    },
                )),
                ..scalar_semantic(0, logical::ScalarKind::FixedSizeBinary)
            },
            logical::LogicalTypeDefinition {
                id: Some(9),
                nullable: true,
                kind: Some(logical::logical_type_definition::Kind::Nominal(
                    logical::NominalValue::LargeInt as i32,
                )),
            },
            logical::LogicalTypeDefinition {
                id: Some(u32::MAX),
                nullable: true,
                kind: Some(logical::logical_type_definition::Kind::Nominal(
                    logical::NominalValue::Uuid as i32,
                )),
            },
        ],
        carriers: vec![c(u32::MAX, Kind::FixedSizeBinary(16))],
        fields: vec![],
        value_types: vec![
            wire::ValueTypeDefinition {
                logical_type_id: Some(0),
                ..v(0, u32::MAX)
            },
            wire::ValueTypeDefinition {
                logical_type_id: Some(9),
                ..v(9, u32::MAX)
            },
            wire::ValueTypeDefinition {
                logical_type_id: Some(u32::MAX),
                ..v(u32::MAX, u32::MAX)
            },
        ],
    };
    let decoded = decode_type_table(&table, limits(), &Control::default()).unwrap();
    for (id, logical) in [
        (0, ValueLogicalType::Physical),
        (9, ValueLogicalType::LargeInt),
        (u32::MAX, ValueLogicalType::Uuid),
    ] {
        let expected =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                .unwrap();
        assert_eq!(decoded.value_type(id), Some(&expected));
    }
    assert_ne!(decoded.value_type(0), decoded.value_type(9));
    assert_ne!(decoded.value_type(9), decoded.value_type(u32::MAX));
}

#[test]
#[allow(deprecated)]
fn normalized_value_layout_strips_encoding_and_source_metadata_preserving_union_order() {
    let json = Arc::new(
        Field::new("json", DataType::Utf8, true).with_metadata(HashMap::from([
            (NR_LOGICAL_TYPE_KEY.into(), "json".into()),
            ("provider".into(), "authored".into()),
        ])),
    );
    let dict = Arc::new(
        Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            false,
            -99,
            true,
        )
        .with_metadata(HashMap::from([
            ("z".into(), "last".into()),
            ("a".into(), "first".into()),
        ])),
    );
    let pair = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                json.clone(),
            ]
            .into(),
        ),
        false,
    ));
    let ends = Arc::new(Field::new("run_ends", DataType::Int32, false));
    let union: UnionFields = [(7, dict.clone()), (1, json.clone())].into_iter().collect();
    let types = vec![
        DataType::List(json.clone()),
        DataType::ListView(json.clone()),
        DataType::LargeList(json.clone()),
        DataType::LargeListView(json.clone()),
        DataType::FixedSizeList(json.clone(), 3),
        DataType::Struct(vec![json.clone(), dict.clone(), json.clone()].into()),
        DataType::Union(union.clone(), UnionMode::Sparse),
        DataType::Union(union, UnionMode::Dense),
        dict.data_type().clone(),
        DataType::Map(pair, true),
        DataType::RunEndEncoded(ends, json),
    ];
    let values: Vec<_> = types
        .into_iter()
        .enumerate()
        .map(|(i, t)| (i as u32, FunctionValueType::new(t, i % 2 == 0)))
        .collect();
    let (dto, decoded) = roundtrip(&values);
    assert!(
        dto.fields
            .iter()
            .all(|field| field.carrier_type_id.is_some())
    );
    assert!(dto.carriers.iter().all(|carrier| !matches!(
        carrier.kind,
        Some(Kind::Dictionary(_)) | Some(Kind::RunEndEncoded(_))
    )));
    for id in [6, 7] {
        let DataType::Union(fields, _) = decoded.value_type(id).unwrap().data_type() else {
            panic!("expected union");
        };
        assert_eq!(fields.iter().map(|(id, _)| id).collect::<Vec<_>>(), [7, 1]);
    }
    let DataType::Struct(fields) = decoded.value_type(5).unwrap().data_type() else {
        panic!("expected struct");
    };
    assert!(novarocks_type_contract::arrow_fields_exact(
        &fields[0], &fields[2]
    ));
}

#[test]
fn complete_scalar_logical_domains_and_temporal_facts_survive_public_table_apis() {
    let mut values = Vec::new();
    for (logical, carrier) in [
        (ValueLogicalType::Json, DataType::LargeBinary),
        (ValueLogicalType::Variant, DataType::LargeBinary),
        (ValueLogicalType::Hll, DataType::Binary),
        (ValueLogicalType::Bitmap, DataType::Binary),
        (ValueLogicalType::Object, DataType::LargeBinary),
        (ValueLogicalType::Percentile, DataType::Binary),
    ] {
        values.push((
            values.len() as u32,
            FunctionValueType::try_with_logical_type(carrier, true, logical).unwrap(),
        ));
    }
    for ty in [
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
        DataType::Timestamp(TimeUnit::Second, Some("Asia/Shanghai".into())),
        DataType::Decimal32(9, -128),
        DataType::Decimal64(18, -5),
        DataType::Decimal128(38, -46),
        DataType::Decimal256(76, 76),
        DataType::Duration(TimeUnit::Millisecond),
        DataType::Interval(IntervalUnit::MonthDayNano),
    ] {
        values.push((values.len() as u32, FunctionValueType::new(ty, false)));
    }
    roundtrip(&values);
}

#[test]
fn duplicates_missing_kind_and_required_references_are_rejected_in_each_namespace() {
    let table = sparse_list();
    for namespace in 0..3 {
        let mut bad = table.clone();
        match namespace {
            0 => bad.carriers.push(bad.carriers[0].clone()),
            1 => bad.fields.push(bad.fields[0].clone()),
            _ => bad.value_types.push(bad.value_types[0]),
        }
        reject(&bad);
    }
    let mut bad = table.clone();
    bad.carriers[0].kind = None;
    reject(&bad);
    let mut bad = table.clone();
    bad.fields[0].carrier_type_id = None;
    reject(&bad);
    let mut bad = table.clone();
    bad.value_types[0].carrier_type_id = None;
    reject(&bad);
    let mut bad = table.clone();
    bad.carriers[0].kind = Some(Kind::FixedSizeList(wire::FixedSizeList {
        item_field_id: None,
        length: 1,
    }));
    reject(&bad);
    let mut bad = table.clone();
    bad.carriers[0].kind = Some(Kind::Dictionary(wire::DictionaryTypes {
        key_type_id: None,
        value_type_id: Some(0),
    }));
    reject(&bad);
    let mut bad = table.clone();
    bad.carriers[0].kind = Some(Kind::Map(wire::MapField {
        entries_field_id: None,
        ordered: false,
    }));
    reject(&bad);
    let mut bad = table;
    bad.carriers[0].kind = Some(Kind::RunEndEncoded(wire::RunEndEncodedFields {
        run_ends_field_id: Some(0),
        values_field_id: None,
    }));
    reject(&bad);
}

#[test]
fn cross_namespace_ids_cannot_satisfy_missing_carrier_or_field_references() {
    let mut table = sparse_list();
    table.carriers[0].kind = Some(Kind::ListFieldId(u32::MAX));
    // A carrier and a value with this ID exist, but the field namespace does not.
    reject(&table);
    let mut table = sparse_list();
    table.fields[0].carrier_type_id = Some(31);
    table.value_types.push(v(31, 0));
    reject(&table);
    let mut table = sparse_list();
    table.value_types[0].carrier_type_id = Some(31);
    table.fields.push(f(31, 0));
    reject(&table);
}

#[test]
fn carrier_field_and_dictionary_cycles_fail_before_materialization() {
    let mut table = sparse_list();
    table.fields[0].carrier_type_id = Some(u32::MAX);
    reject(&table);
    let table = wire::TypeTable {
        logical_types: Vec::new(),
        carriers: vec![
            c(0, Kind::Primitive(5)),
            c(
                u32::MAX,
                Kind::Dictionary(wire::DictionaryTypes {
                    key_type_id: Some(0),
                    value_type_id: Some(u32::MAX),
                }),
            ),
        ],
        fields: vec![],
        value_types: vec![v(0, u32::MAX)],
    };
    reject(&table);
}

fn nested_list(depth: usize) -> wire::TypeTable {
    let mut table = wire::TypeTable {
        logical_types: vec![scalar_semantic(0, logical::ScalarKind::Int32)],
        carriers: vec![c(0, Kind::Primitive(5))],
        fields: vec![],
        value_types: vec![wire::ValueTypeDefinition {
            logical_type_id: Some((depth - 1) as u32),
            ..v(0, (depth - 1) as u32)
        }],
    };
    for id in 1..depth {
        let mut field = f(id as u32, (id - 1) as u32);
        field.name = "item".into();
        field.logical_type_id = Some((id - 1) as u32);
        table.fields.push(field);
        table
            .logical_types
            .push(list_semantic(id as u32, (id - 1) as u32));
        table
            .carriers
            .push(c(id as u32, Kind::ListFieldId(id as u32)));
    }
    table
}
#[test]
fn actual_type_depth_sixty_four_is_valid_and_sixty_five_is_rejected() {
    let decoded = decode_type_table(
        &nested_list(MAX_VALUE_TYPE_DEPTH),
        limits(),
        &Control::default(),
    )
    .unwrap();
    let mut cursor = decoded.value_type(0).unwrap().data_type();
    for _ in 1..MAX_VALUE_TYPE_DEPTH {
        let DataType::List(field) = cursor else {
            panic!("missing list layer");
        };
        cursor = field.data_type();
    }
    assert_eq!(cursor, &DataType::Int32);
    reject(&nested_list(MAX_VALUE_TYPE_DEPTH + 1));
}
fn shared_doubling(levels: usize) -> wire::TypeTable {
    let mut table = wire::TypeTable {
        logical_types: vec![scalar_semantic(0, logical::ScalarKind::Int32)],
        carriers: vec![c(0, Kind::Primitive(5))],
        fields: vec![],
        value_types: vec![wire::ValueTypeDefinition {
            logical_type_id: Some(levels as u32),
            ..v(0, levels as u32)
        }],
    };
    for id in 1..=levels {
        let mut field = f(id as u32, (id - 1) as u32);
        field.logical_type_id = Some((id - 1) as u32);
        table.fields.push(field);
        table.logical_types.push(logical::LogicalTypeDefinition {
            id: Some(id as u32),
            nullable: true,
            kind: Some(logical::logical_type_definition::Kind::StructValue(
                logical::StructValue {
                    fields: vec![
                        logical::NamedValueField {
                            name: "x".into(),
                            value_type_id: Some((id - 1) as u32)
                        };
                        2
                    ],
                },
            )),
        });
        table.carriers.push(c(
            id as u32,
            Kind::StructType(wire::StructFields {
                field_ids: vec![id as u32; 2],
            }),
        ));
    }
    table
}
#[test]
fn repeated_reference_dag_expansion_enforces_owner_node_bound_before_constructing_arrow_tree() {
    let near = shared_doubling(11); // 4095 expanded type nodes, despite only 12 carriers.
    decode_type_table(&near, limits(), &Control::default()).unwrap();
    reject(&shared_doubling(12)); // 8191 expanded type nodes.
}

#[test]
fn projection_definition_limits_count_all_four_namespaces_including_unused_definitions() {
    let base = sparse_list();
    let exact = TypeProjectionLimits {
        max_definitions: 6,
        ..limits()
    };
    decode_type_table(&base, exact, &Control::default()).unwrap();
    assert!(
        decode_type_table(
            &base,
            TypeProjectionLimits {
                max_definitions: 5,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    for namespace in 0..3 {
        let mut table = base.clone();
        match namespace {
            0 => table.carriers.push(c(31, Kind::Primitive(5))),
            1 => table.fields.push(f(31, 0)),
            _ => table.value_types.push(v(31, 0)),
        }
        assert!(decode_type_table(&table, exact, &Control::default()).is_err());
    }
    let values = [(
        u32::MAX,
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("x", DataType::Int32, true))),
            true,
        ),
    )];
    assert_eq!(
        encode_type_table(&values, exact, &Control::default())
            .unwrap()
            .carriers
            .len(),
        2
    );
    assert!(
        encode_type_table(
            &values,
            TypeProjectionLimits {
                max_definitions: 5,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn projection_expanded_limit_counts_carrier_field_and_value_roots_with_repeated_children() {
    let base = sparse_list(); // Physical roots total six; semantic Int32/List definitions total three.
    decode_type_table(
        &base,
        TypeProjectionLimits {
            max_expanded_nodes: 9,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        decode_type_table(
            &base,
            TypeProjectionLimits {
                max_expanded_nodes: 8,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    let mut repeated = base;
    repeated.logical_types[1].kind = Some(logical::logical_type_definition::Kind::StructValue(
        logical::StructValue {
            fields: vec![
                logical::NamedValueField {
                    name: "item".into(),
                    value_type_id: Some(0)
                };
                2
            ],
        },
    ));
    repeated.carriers[0].kind = Some(Kind::StructType(wire::StructFields {
        field_ids: vec![0, 0],
    })); // 1+3+1+3=8.
    decode_type_table(
        &repeated,
        TypeProjectionLimits {
            max_expanded_nodes: 12,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        decode_type_table(
            &repeated,
            TypeProjectionLimits {
                max_expanded_nodes: 11,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    let values = [(
        0,
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("x", DataType::Int32, true))),
            true,
        ),
    )];
    encode_type_table(
        &values,
        TypeProjectionLimits {
            max_expanded_nodes: 9,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        encode_type_table(
            &values,
            TypeProjectionLimits {
                max_expanded_nodes: 8,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
}

#[test]
#[allow(deprecated)]
fn dictionary_field_requires_both_optional_attributes_and_rejects_attributes_on_other_carriers() {
    let table = wire::TypeTable {
        logical_types: Vec::new(),
        carriers: vec![
            c(0, Kind::Primitive(5)),
            c(1, Kind::Primitive(19)),
            c(
                2,
                Kind::Dictionary(wire::DictionaryTypes {
                    key_type_id: Some(0),
                    value_type_id: Some(1),
                }),
            ),
            c(3, Kind::ListFieldId(0)),
        ],
        fields: vec![wire::FieldDefinition { ..f(0, 2) }],
        value_types: vec![],
    };
    let mut table = source_table(table);
    table.field_facts[0].dictionary_id = Some(i64::MIN);
    table.field_facts[0].dictionary_is_ordered = Some(false);
    let result = decode_source(&table, limits()).unwrap();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    assert_eq!(
        source_field(&result, 0, &mut work).dict_id(),
        Some(i64::MIN)
    );
    assert_eq!(
        source_field(&result, 0, &mut work).dict_is_ordered(),
        Some(false)
    );
    for missing in 0..3 {
        let mut bad = table.clone();
        if missing != 1 {
            bad.field_facts[0].dictionary_id = None;
        }
        if missing != 0 {
            bad.field_facts[0].dictionary_is_ordered = None;
        }
        assert!(decode_source(&bad, limits()).is_err());
    }
    for extra in 0..3 {
        let mut bad = source_table(sparse_list());
        if extra != 1 {
            bad.field_facts[0].dictionary_id = Some(0);
        }
        if extra != 0 {
            bad.field_facts[0].dictionary_is_ordered = Some(false);
        }
        assert!(decode_source(&bad, limits()).is_err());
    }
}

#[test]
fn field_metadata_must_be_sorted_unique_and_retains_complete_source_entries() {
    let entry = |key: &str, value: &str| plan::ArrowFieldMetadataEntry {
        key: key.into(),
        value: value.into(),
    };
    let mut valid = source_table(sparse_list());
    valid.field_facts[0].metadata = vec![entry("a", "first"), entry("z", "last")];
    let decoded = decode_source(&valid, limits()).unwrap();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    assert_eq!(
        source_field(&decoded, 0, &mut work)
            .metadata()
            .get("z")
            .map(String::as_str),
        Some("last")
    );
    let mut unsorted = valid.clone();
    unsorted.field_facts[0].metadata.reverse();
    assert!(decode_source(&unsorted, limits()).is_err());
    let mut duplicate = valid;
    duplicate.field_facts[0].metadata = vec![entry("a", "first"), entry("a", "last")];
    assert!(decode_source(&duplicate, limits()).is_err());
}

#[test]
fn logical_references_are_required_and_legacy_tags_cannot_repair_or_override_them() {
    for legacy in [0, -1, i32::MAX, 2, 8, 9] {
        let mut table = sparse_list();
        let mut bytes = table.value_types[0].encode_to_vec();
        prost::encoding::int32::encode(4, &legacy, &mut bytes);
        prost::encoding::bool::encode(3, &false, &mut bytes);
        table.value_types[0] = wire::ValueTypeDefinition::decode(bytes.as_slice()).unwrap();
        let decoded = decode_type_table(&table, limits(), &Control::default()).unwrap();
        assert!(decoded.value_type(u32::MAX).unwrap().nullable());
        table.value_types[0].logical_type_id = None;
        reject(&table);
    }
    for reference in [None, Some(37)] {
        let mut bad = sparse_list();
        bad.value_types[0].logical_type_id = reference;
        reject(&bad);
    }
    let mut bad = sparse_list();
    bad.logical_types[0].kind = Some(logical::logical_type_definition::Kind::Nominal(
        logical::NominalValue::Json as i32,
    ));
    reject(&bad);
    for metadata in ["json", "unknown"] {
        let mut source = source_table(sparse_list());
        source.field_facts[0].metadata = vec![plan::ArrowFieldMetadataEntry {
            key: NR_LOGICAL_TYPE_KEY.into(),
            value: metadata.into(),
        }];
        // An opaque source label is retained but cannot repair a missing execution declaration.
        decode_source(&source, limits()).unwrap();
        let mut missing = sparse_list();
        missing.value_types[0].logical_type_id = None;
        reject(&missing);
    }
}

#[test]
fn original_control_covers_real_wide_type_work_entry_256_tail_and_final_publication() {
    let values: Vec<_> = (0..320)
        .map(|id| (id, FunctionValueType::new(DataType::Int32, false)))
        .collect();
    let encode_control = Control::default();
    let dto = encode_type_table(&values, limits(), &encode_control).unwrap();
    let encode_trace = encode_control.events.into_inner().unwrap();
    let decode_control = Control::default();
    decode_type_table(&dto, limits(), &decode_control).unwrap();
    let decode_trace = decode_control.events.into_inner().unwrap();
    for trace in [&encode_trace, &decode_trace] {
        assert_eq!(trace[0].1, 0);
        assert!(trace.iter().any(|(_, units)| *units == 256));
        assert!(trace.iter().any(|(_, units)| *units > 0 && *units < 256));
    }
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=encode_trace.len() {
            let stop = Control {
                fail: Some((at, error.clone())),
                events: Mutex::default(),
            };
            assert!(
                matches!(encode_type_table(&values,limits(),&stop),Err(TypeCodecError::Control(actual)) if actual==error)
            );
            assert_eq!(stop.events.lock().unwrap().len(), at);
        }
        for at in 1..=decode_trace.len() {
            let stop = Control {
                fail: Some((at, error.clone())),
                events: Mutex::default(),
            };
            assert!(
                matches!(decode_type_table(&dto,limits(),&stop),Err(TypeCodecError::Control(actual)) if actual==error)
            );
            assert_eq!(stop.events.lock().unwrap().len(), at);
        }
    }
}

#[test]
fn malformed_tail_still_observes_original_control_without_publishing_a_prefix() {
    let mut dto = wire::TypeTable {
        logical_types: Vec::new(),
        carriers: (0..320).map(|id| c(id, Kind::Primitive(5))).collect(),
        fields: vec![],
        value_types: vec![v(0, 0)],
    };
    dto.carriers[319].kind = None;
    let good = Control::default();
    assert!(decode_type_table(&dto, limits(), &good).is_err());
    let trace = good.events.into_inner().unwrap();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let stop = Control {
                fail: Some((at, error.clone())),
                events: Mutex::default(),
            };
            assert!(
                matches!(decode_type_table(&dto,limits(),&stop),Err(TypeCodecError::Control(actual)) if actual==error)
            );
        }
    }
    let mut values: Vec<_> = (0..320)
        .map(|id| (id, FunctionValueType::new(DataType::Int32, false)))
        .collect();
    assert!(
        FunctionValueType::try_with_logical_type(DataType::Int32, false, ValueLogicalType::Json)
            .is_err()
    );
    // A valid immutable type remains valid. A duplicate root identity supplies
    // the late ordinary codec refusal without fabricating an invalid binding.
    values[319].0 = values[318].0;
    let good = Control::default();
    assert!(encode_type_table(&values, limits(), &good).is_err());
    let trace = good.events.into_inner().unwrap();
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let stop = Control {
                fail: Some((at, error.clone())),
                events: Mutex::default(),
            };
            assert!(
                matches!(encode_type_table(&values,limits(),&stop),Err(TypeCodecError::Control(actual)) if actual==error)
            );
        }
    }
}

#[test]
fn projection_string_byte_bound_counts_flat_names_metadata_and_timestamp_zone_exactly() {
    let mut table = sparse_list();
    table.value_types.clear();
    table.logical_types.clear();
    table.fields[0].logical_type_id = None;
    table.carriers[1].kind = Some(Kind::Timestamp(plan::ArrowTimestampType {
        unit: 4,
        timezone: Some("UTC".into()),
    }));
    let mut table = source_table(table);
    table.field_facts[0].metadata = vec![plan::ArrowFieldMetadataEntry {
        key: "a".into(),
        value: "b".into(),
    }];
    // Flat definition bytes: zone UTC (3) + name item (4) + metadata a:b (2).
    decode_source(
        &table,
        TypeProjectionLimits {
            max_string_bytes: 9,
            ..limits()
        },
    )
    .unwrap();
    assert!(
        decode_source(
            &table,
            TypeProjectionLimits {
                max_string_bytes: 8,
                ..limits()
            }
        )
        .is_err()
    );
    let field = Arc::new(
        Field::new(
            "x",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )
        .with_metadata(HashMap::from([("a".into(), "b".into())])),
    );
    let values = [(0, FunctionValueType::new(DataType::List(field), true))];
    encode_type_table(
        &values,
        TypeProjectionLimits {
            max_string_bytes: 10,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        encode_type_table(
            &values,
            TypeProjectionLimits {
                max_string_bytes: 9,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    let mut unicode = sparse_list();
    unicode.value_types.clear();
    unicode.logical_types.clear();
    unicode.fields[0].logical_type_id = None;
    unicode.fields[0].name = "é".into();
    let mut unicode = source_table(unicode);
    unicode.field_facts[0].metadata = vec![plan::ArrowFieldMetadataEntry {
        key: "a".into(),
        value: "漢".into(),
    }];
    // Three characters occupy six UTF-8 bytes: 2 + 1 + 3.
    decode_source(
        &unicode,
        TypeProjectionLimits {
            max_string_bytes: 6,
            ..limits()
        },
    )
    .unwrap();
    assert!(
        decode_source(
            &unicode,
            TypeProjectionLimits {
                max_string_bytes: 5,
                ..limits()
            }
        )
        .is_err()
    );
    let field = Arc::new(
        Field::new("é", DataType::Int32, true)
            .with_metadata(HashMap::from([("a".into(), "漢".into())])),
    );
    let values = [(0, FunctionValueType::new(DataType::List(field), true))];
    encode_type_table(
        &values,
        TypeProjectionLimits {
            max_string_bytes: 4,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        encode_type_table(
            &values,
            TypeProjectionLimits {
                max_string_bytes: 3,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn many_independent_valid_values_do_not_acquire_a_global_4096_type_table_limit() {
    let values: Vec<_> = (0..4100)
        .map(|id| (id, FunctionValueType::new(DataType::Int32, false)))
        .collect();
    let (dto, decoded) = roundtrip(&values);
    assert_eq!(dto.value_types.len(), 4100);
    assert_eq!(decoded.value_types().len(), 4100);
}

#[test]
fn composing_value_clone_shares_the_actual_semantic_and_layout_holders() {
    let nested = Arc::new(Field::new("shared", DataType::Utf8, false));
    for carrier in [
        DataType::Int32,
        DataType::List(nested),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
    ] {
        let original = FunctionValueType::new(carrier, true);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let facts = preflight_value_type_clone(&original, &mut work).unwrap();
        assert_eq!(facts.allocation_requests_upper_bound(), 0);
        assert_eq!(facts.allocation_request_bytes_upper_bound(), 0);
        assert_eq!(facts.work_upper_bound(), 2);
        let cloned = clone_value_type_observed(&original, &mut work).unwrap();
        work.finish().unwrap();
        assert_eq!(cloned, original);
        assert!(std::ptr::eq(
            cloned.value_type().kind(),
            original.value_type().kind()
        ));
        assert!(std::ptr::eq(cloned.data_type(), original.data_type()));
        // One preflight inspection plus the two actual handle increments.
        assert_eq!(
            control
                .events
                .lock()
                .unwrap()
                .iter()
                .map(|(_, n)| *n as usize)
                .sum::<usize>(),
            3
        );
    }
}

#[test]
fn composing_value_clone_observes_each_original_dictionary_request_boundary() {
    fn run(control: &Control) -> Result<FunctionValueType, TypeCodecError> {
        let value = FunctionValueType::new(
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        );
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = (|| {
            preflight_value_type_clone(&value, &mut work)?;
            clone_value_type_observed(&value, &mut work)
        })();
        finish(work, result)
    }
    let base = Control::default();
    run(&base).unwrap();
    let expected = base.events.lock().unwrap().clone();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=expected.len() {
            let control = Control {
                fail: Some((at, cause.clone())),
                events: Mutex::default(),
            };
            assert!(matches!(run(&control), Err(TypeCodecError::Control(got)) if got==cause));
            assert_eq!(*control.events.lock().unwrap(), expected[..at]);
        }
    }
}

#[test]
fn schema_field_ports_use_actual_sparse_namespace_and_necessary_inline_floor() {
    let mut raw = sparse_list();
    raw.fields.push(f(u32::MAX, 0));
    let decoded = decode_type_table(&raw, limits(), &Control::default()).unwrap();
    assert_eq!(decoded.field_count(), 2);
    let independent_floor = std::mem::size_of::<DecodedTypeTable>()
        + 2 * (std::mem::size_of::<u32>() + std::mem::size_of::<Arc<Field>>());
    assert_eq!(
        decoded.necessary_fields_retained_floor().unwrap(),
        independent_floor
    );
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    for id in [0, u32::MAX, 0] {
        let found = decoded.field_observed(id, &mut work).unwrap().unwrap();
        assert!(Arc::ptr_eq(found, decoded.field(id).unwrap()));
    }
    assert!(decoded.field_observed(1, &mut work).unwrap().is_none());
    work.finish().unwrap();
    let trace = control.events.lock().unwrap();
    assert_eq!(
        trace.iter().map(|(_, units)| *units).collect::<Vec<_>>(),
        [0, 0, 1, 0, 1, 0, 1, 0, 1, 0]
    );
}

#[test]
fn schema_field_lookup_preserves_every_actual_opaque_boundary_control_cause() {
    let decoded = decode_type_table(&sparse_list(), limits(), &Control::default()).unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [2, 3] {
            let control = Control {
                fail: Some((at, cause.clone())),
                ..Default::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            assert!(
                matches!(decoded.field_observed(0, &mut work), Err(TypeCodecError::Control(actual)) if actual==cause)
            );
            assert!(matches!(work.finish(), Err(actual) if actual==cause));
            assert_eq!(control.events.lock().unwrap().len(), at);
        }
    }
}

#[test]
fn captured_original_root_lookup_keeps_sparse_identity_and_plain_control_trace() {
    let roots = [
        (
            u32::MAX,
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int64)),
                true,
            ),
        ),
        (0, FunctionValueType::new(DataType::Int64, true)),
    ];
    let table = encode_type_table_sources(&roots, &[], limits(), &Control::default()).unwrap();
    for id in [0, u32::MAX, 3] {
        let plain = Control::default();
        let mut work = CompileCheckpoints::try_new(&plain, CompilePhase::LowerProgram).unwrap();
        let expected = table.value_type_observed(id, &mut work).unwrap();
        work.finish().unwrap();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        let mut captured = None;
        let actual = table
            .value_type_captured::<TypeCodecError>(
                id,
                &mut |value, _| {
                    captured = Some(value);
                    Ok(())
                },
                &mut work,
            )
            .unwrap();
        work.finish().unwrap();
        let source = roots
            .iter()
            .find(|(candidate, _)| *candidate == id)
            .map(|(_, value)| value);
        assert!(match (actual, source) {
            (Some(actual), Some(source)) => std::ptr::eq(actual, source),
            (None, None) => true,
            _ => false,
        });
        assert!(match (actual, expected, captured) {
            (Some(a), Some(b), Some(c)) => std::ptr::eq(a, b) && std::ptr::eq(a, c),
            (None, None, None) => true,
            _ => false,
        });
        assert_eq!(
            *control.events.lock().unwrap(),
            *plain.events.lock().unwrap()
        );
    }
}
#[test]
fn captured_root_known_refusal_precedes_matched_completed_quantum() {
    let roots = [(
        u32::MAX,
        FunctionValueType::new(
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int64)),
            true,
        ),
    )];
    let table = encode_type_table_sources(&roots, &[], limits(), &Control::default()).unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            fail: Some((2, cause.clone())),
            ..Control::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        // A pending caller seam, not synthetic completed type-comparison work.
        for _ in 0..255 {
            work.step().unwrap();
        }
        let result = table.value_type_captured::<TypeCodecError>(
            u32::MAX,
            &mut |value, _| {
                assert!(std::ptr::eq(value, &roots[0].1));
                Err(CompileControlError::ResourceExhausted.into())
            },
            &mut work,
        );
        assert!(matches!(
            result,
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(
            *control.events.lock().unwrap(),
            [(CompilePhase::LowerProgram, 0)]
        );
        let control = Control {
            fail: Some((2, cause.clone())),
            ..Control::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let missing = table.value_type_captured::<TypeCodecError>(
            0,
            &mut |_, _| panic!("unmatched root must not be captured"),
            &mut work,
        );
        assert!(matches!(missing, Err(TypeCodecError::Control(actual)) if actual == cause));
        assert_eq!(
            *control.events.lock().unwrap(),
            [
                (CompilePhase::LowerProgram, 0),
                (CompilePhase::LowerProgram, 256)
            ]
        );
    }
}
