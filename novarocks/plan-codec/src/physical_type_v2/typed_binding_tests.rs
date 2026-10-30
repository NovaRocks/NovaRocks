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
use crate::host_projection_v2::{AdmissionRefusal, ProjectionFailure};
use novarocks_proto_models::{physical_package_v2 as package, plan, value_type as logical};
use novarocks_type_contract::{
    ValueField, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageHold,
    ValueTypeStorageLifetime,
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 1_000_000,
    }
}
fn package_limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 1_000_000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: 1_000_000_000,
        max_coexisting_source_and_request_bytes: 1_000_000_000,
        max_work: usize::MAX / 4,
    }
}
fn value(kind: ValueKind, nullable: bool) -> ValueTypeRef {
    ValueTypeRef::try_new(kind, nullable, ValueTypeStorageLifetime::default()).unwrap()
}
fn fixture() -> FunctionValueType {
    let json = value(ValueKind::Nominal(ValueLogicalType::Json), true);
    let list = value(ValueKind::List(json), false);
    let map = value(
        ValueKind::Map {
            key: value(ValueKind::String, false),
            value: value(ValueKind::Nominal(ValueLogicalType::Hll), true),
        },
        false,
    );
    let fields = vec![
        ValueField::new("payload", list, ValueTypeStorageLifetime::default()),
        ValueField::new("states", map, ValueTypeStorageLifetime::default()),
    ]
    .into_boxed_slice();
    let semantic = value(ValueKind::Struct(fields), true);
    let layout = DataType::Struct(
        vec![
            Arc::new(Field::new(
                "payload",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            )),
            Arc::new(Field::new(
                "states",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Arc::new(Field::new("key", DataType::Utf8, false)),
                                Arc::new(Field::new("value", DataType::Binary, true)),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    true,
                ),
                false,
            )),
        ]
        .into(),
    );
    FunctionValueType::try_from_value(
        semantic,
        ValueLayout::try_new(&layout, ValueTypeStorageLifetime::default()).unwrap(),
    )
    .unwrap()
}
fn encode(values: &[(u32, FunctionValueType)], fields: &[(u32, Arc<Field>)]) -> wire::TypeTable {
    encode_type_table_with_fields(values, fields, limits(), &Control).unwrap()
}
#[test]
fn typed_semantic_complete_recursive_binding_and_physical_only_map_wrapper_round_trip() {
    let expected = fixture();
    let table = encode(&[(u32::MAX, expected.clone())], &[]);
    assert!(!table.logical_types.is_empty());
    assert!(table.value_types[0].logical_type_id.is_some());
    for field in &table.fields {
        assert!(field.value_binding_id.is_none());
        if field.name == "entries" {
            assert!(field.logical_type_id.is_none());
        } else {
            assert!(field.logical_type_id.is_some());
        }
    }
    let decoded = decode_type_table_pure(&table, limits(), &Control).unwrap();
    for field in decoded.fields.values() {
        assert!(field.metadata().as_arc().is_none());
        #[allow(deprecated)]
        {
            assert_eq!(field.dict_id(), None);
            assert_eq!(field.dict_is_ordered(), None);
        }
    }
    assert_eq!(decoded.value_type(u32::MAX), Some(&expected));
    let mut wrong = table.clone();
    let item = wrong
        .fields
        .iter_mut()
        .find(|field| field.name == "item")
        .unwrap();
    item.logical_type_id = table.value_types[0].logical_type_id;
    assert!(decode_type_table_pure(&wrong, limits(), &Control).is_err());
}
#[test]
#[allow(deprecated)]
fn typed_semantic_external_metadata_and_dictionary_descriptors_remain_independent_facts() {
    let source = Arc::new(
        Field::new_dict(
            "source",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            -29,
            true,
        )
        .with_metadata(HashMap::from([(
            novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
            "unknown-source-attribute".into(),
        )])),
    );
    let expected =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let table = encode(&[(0, expected.clone())], &[]);
    let decoded = decode_type_table_pure(&table, limits(), &Control).unwrap();
    assert_eq!(decoded.value_type(0), Some(&expected));
    assert!(decoded.field(u32::MAX).is_none());
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let source_wire =
        encode_source_field_table_pure_in(&[(u32::MAX, &source)], limits(), &mut work).unwrap();
    work.finish().unwrap();
    let wire_source = source_wire
        .shape
        .as_ref()
        .unwrap()
        .fields
        .iter()
        .find(|field| field.id == u32::MAX)
        .unwrap();
    assert!(wire_source.logical_type_id.is_none());
    assert!(wire_source.value_binding_id.is_none());
    assert_eq!(source_wire.field_facts[0].dictionary_id, Some(-29));
    assert_eq!(source_wire.field_facts[0].dictionary_is_ordered, Some(true));
    assert_eq!(
        source_wire.field_facts[0].metadata[0].value,
        "unknown-source-attribute"
    );
    let mut request = |_: std::alloc::Layout| Ok::<(), CompileControlError>(());
    let mut opaque = |_: usize| Ok::<(), CompileControlError>(());
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut opaque,
    );
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let source_decoded = decode_source_field_table_with_creator(
        &source_wire,
        65536,
        package_limits(),
        &mut creation,
        &mut work,
    )
    .unwrap();
    assert!(novarocks_type_contract::arrow_fields_exact(
        source_decoded
            .field_observed(u32::MAX, &mut work)
            .unwrap()
            .unwrap(),
        &source
    ));
    work.finish().unwrap();
    assert!(decoded.field_execution_type(u32::MAX).is_none());
}
#[test]
fn typed_semantic_one_logical_definition_allows_multiple_exact_binding_layouts() {
    let small = FunctionValueType::new(DataType::Utf8, false);
    let large = FunctionValueType::new(DataType::LargeUtf8, false);
    let mut table = encode(
        &[(0, small.clone()), (1, large.clone())],
        &[
            (
                90,
                Arc::new(Field::new("large", DataType::LargeUtf8, false)),
            ),
            (91, Arc::new(Field::new("small", DataType::Utf8, false))),
        ],
    );
    let shared = table.value_types[0].logical_type_id;
    table.value_types[1].logical_type_id = shared;
    for field in &mut table.fields {
        field.logical_type_id = shared;
        field.value_binding_id = Some(if field.id == 90 { 1 } else { 0 });
    }
    let decoded = decode_type_table_pure(&table, limits(), &Control).unwrap();
    assert_eq!(decoded.field_execution_type(90), Some(&large));
    assert_eq!(decoded.field_execution_type(91), Some(&small));
    table
        .fields
        .iter_mut()
        .find(|field| field.id == 90)
        .unwrap()
        .value_binding_id = Some(0);
    assert!(decode_type_table_pure(&table, limits(), &Control).is_err());
}
#[test]
fn typed_semantic_unused_definitions_and_unbound_field_parameters_are_validated() {
    let table = encode(&[(0, FunctionValueType::new(DataType::Int64, false))], &[]);
    let mut bad = table.clone();
    bad.logical_types.push(logical::LogicalTypeDefinition {
        id: Some(u32::MAX),
        nullable: false,
        kind: Some(logical::logical_type_definition::Kind::ListItemTypeId(
            u32::MAX,
        )),
    });
    assert!(decode_type_table_pure(&bad, limits(), &Control).is_err());
    let mut bad = table.clone();
    bad.carriers.push(wire::CarrierTypeDefinition {
        id: 77,
        kind: Some(wire::carrier_type_definition::Kind::Primitive(
            plan::ArrowPrimitiveType::Int32 as i32,
        )),
    });
    bad.fields.push(wire::FieldDefinition {
        id: 77,
        name: "unused".into(),
        nullable: false,
        carrier_type_id: Some(77),
        logical_type_id: table.value_types[0].logical_type_id,
        ..Default::default()
    });
    assert!(decode_type_table_pure(&bad, limits(), &Control).is_err());
}
#[test]
fn typed_semantic_production_decode_holds_the_actual_creator_until_last_child_or_layout_exit() {
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let table = encode(&[(0, fixture())], &[]);
    let dto = package::FragmentPackage {
        types: Some(table),
        source_fields: Some(wire::SourceFieldTable {
            shape: Some(wire::TypeTable::default()),
            field_facts: vec![],
        }),
        ..Default::default()
    };
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let decoded = decode_package_type_table_with_storage_observed(
        &dto,
        65536,
        ValueTypeStorageLifetime::held(Arc::new(Hold(dropped.clone()))),
        package_limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let binding = decoded.value_type(0).unwrap().clone();
    let child = binding.value_type().child_type_at(0).unwrap().clone();
    let layout = binding.typed_binding().layout().clone();
    drop(decoded);
    drop(binding);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(child);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(layout);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
#[test]
fn typed_semantic_writer_pairs_its_explicit_binding_while_preserving_source_metadata() {
    use novarocks_connector_contract as c;
    let expected =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let source = Field::new("input", DataType::Utf8, false).with_metadata(HashMap::from([(
        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
        "external-fact".into(),
    )]));
    let binding = c::ConnectorWriteFieldBinding::try_new_typed(
        c::ConnectorWriteFieldToken::from_bytes([3; 32]),
        source.clone(),
        expected.clone(),
    )
    .unwrap();
    let recipe = super::sender_tests::draft(c::ConnectorWriteInputShape::Data {
        fields: vec![binding],
    });
    let ids = [u32::MAX];
    let value_ids = [0];
    let roots = [WriterTypeSource::new_with_value_ids(
        &recipe, &ids, &value_ids,
    )];
    let values = [(0, &expected)];
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let encoded = encode_borrowed_type_namespaces_sources_in(
        &values,
        &[],
        &[],
        &roots,
        65536,
        package_limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let (table, source_table) = encoded.into_namespaces();
    let source_table = source_table.unwrap();
    work.finish().unwrap();
    let field = source_table
        .shape
        .as_ref()
        .unwrap()
        .fields
        .iter()
        .find(|field| field.id == u32::MAX)
        .unwrap();
    assert!(field.logical_type_id.is_none() && field.value_binding_id.is_none());
    assert_eq!(
        source_table
            .field_facts
            .iter()
            .find(|facts| facts.field_id == Some(u32::MAX))
            .unwrap()
            .metadata[0]
            .value,
        "external-fact"
    );
    let dto = package::FragmentPackage {
        types: Some(table),
        source_fields: Some(source_table),
        writes: vec![package::FrozenWriterRecipe {
            input: Some(package::ConnectorWriteInputShape {
                kind: Some(package::connector_write_input_shape::Kind::Data(
                    package::ConnectorWriteDataInput {
                        fields: vec![package::ConnectorWriteFieldBinding {
                            source_field_id: Some(u32::MAX),
                            value_type_id: Some(0),
                            field_token: vec![3; 32],
                        }],
                    },
                )),
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let decoded = decode_package_type_table_with_storage_observed(
        &dto,
        65536,
        ValueTypeStorageLifetime::default(),
        package_limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(decoded.value_type(0), Some(&expected));
    assert!(decoded.field_execution_type(u32::MAX).is_none());
    assert!(novarocks_type_contract::arrow_fields_exact(
        decoded.source_field(u32::MAX).unwrap(),
        &source
    ));
}
#[test]
fn typed_semantic_shared_requests_preserve_every_original_host_refusal_without_publishing() {
    let value = fixture();
    let roots = [(0, &value)];
    let mut seen = Vec::new();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    encode_borrowed_type_table_writer_sources_with_host_in(
        &roots,
        &[],
        &[],
        65536,
        package_limits(),
        &mut |facts| {
            seen.push(*facts);
            Ok::<_, AdmissionRefusal<usize>>(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    for stop in 0..seen.len() {
        let mut actual = Vec::new();
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let result = encode_borrowed_type_table_writer_sources_with_host_in(
            &roots,
            &[],
            &[],
            65536,
            package_limits(),
            &mut |facts| {
                let at = actual.len();
                actual.push(*facts);
                if at == stop {
                    Err(AdmissionRefusal::Host(stop))
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        assert!(matches!(result,Err(ProjectionFailure::Host(at)) if at==stop));
        assert_eq!(actual, &seen[..=stop]);
    }
}
