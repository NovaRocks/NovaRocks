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
use crate::host_projection_v2::ProjectionFailure;
use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
use std::alloc::Layout;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 32,
        max_expanded_nodes: 256,
        max_string_bytes: 4096,
        max_allocation_requests: 10000,
        max_allocation_request_bytes: 1024 * 1024,
        max_coexisting_source_and_request_bytes: 2 * 1024 * 1024,
        max_work: 4 * 1024 * 1024,
    }
}
fn table() -> wire::SourceFieldTable {
    wire::SourceFieldTable {
        shape: Some(wire::TypeTable {
            carriers: vec![wire::CarrierTypeDefinition {
                id: u32::MAX,
                kind: Some(wire::carrier_type_definition::Kind::Primitive(
                    novarocks_proto_models::arrow_type::ArrowPrimitiveType::Utf8 as i32,
                )),
            }],
            fields: vec![wire::FieldDefinition {
                id: 0,
                name: String::from("source"),
                nullable: true,
                carrier_type_id: Some(u32::MAX),
                logical_type_id: None,
                value_binding_id: None,
            }],
            value_types: vec![],
            logical_types: vec![],
        }),
        field_facts: vec![wire::SourceArrowFieldFacts {
            field_id: Some(0),
            metadata: vec![
                novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry {
                    key: String::from("nr.logical_type"),
                    value: String::from("unknown-provider-attribute"),
                },
            ],
            dictionary_id: None,
            dictionary_is_ordered: None,
        }],
    }
}
// This fixture owns fresh public Vec/String backing. Their actual capacities
// form the original input union; metadata visible length is never an invoice.
pub(crate) fn source_bytes(table: &wire::SourceFieldTable) -> usize {
    let shape = table.shape.as_ref().unwrap();
    std::mem::size_of_val(table)+std::mem::size_of_val(shape)
        +shape.carriers.capacity()*std::mem::size_of::<wire::CarrierTypeDefinition>()
        +shape.fields.capacity()*std::mem::size_of::<wire::FieldDefinition>()
        +shape.fields.iter().map(|f|f.name.capacity()).sum::<usize>()
        +shape.value_types.capacity()*std::mem::size_of::<wire::ValueTypeDefinition>()
        +shape.logical_types.capacity()*std::mem::size_of::<novarocks_proto_models::value_type::LogicalTypeDefinition>()
        +shape.carriers.iter().map(|carrier| match &carrier.kind {
            Some(wire::carrier_type_definition::Kind::StructType(fields)) => fields.field_ids.capacity()*std::mem::size_of::<u32>(),
            Some(wire::carrier_type_definition::Kind::UnionType(fields)) => fields.fields.capacity()*std::mem::size_of::<wire::UnionField>(),
            Some(wire::carrier_type_definition::Kind::Timestamp(timestamp)) => timestamp.timezone.as_ref().map_or(0,String::capacity),
            _ => 0,
        }).sum::<usize>()
        +table.field_facts.capacity()*std::mem::size_of::<wire::SourceArrowFieldFacts>()
        +table.field_facts.iter().map(|f|f.metadata.capacity()*std::mem::size_of::<novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry>()+f.metadata.iter().map(|e|e.key.capacity()+e.value.capacity()).sum::<usize>()).sum::<usize>()
}
fn decode(
    table: &wire::SourceFieldTable,
) -> Result<DecodedSourceFieldTable, ProjectionFailure<TypeCodecError, &'static str>> {
    decode_with_limits(table, limits())
}
fn decode_with_limits(
    table: &wire::SourceFieldTable,
    profile: PackageTypeProjectionLimits,
) -> Result<DecodedSourceFieldTable, ProjectionFailure<TypeCodecError, &'static str>> {
    let mut request = |_: Layout| Ok(());
    let mut opaque = |_: usize| Ok(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    decode_source_field_table_with_creator(
        table,
        source_bytes(table),
        profile,
        &mut creation,
        &mut work,
    )
}
#[test]
fn sparse_zero_source_fields_preserve_attributes_without_semantic_identity() {
    let table = table();
    let decoded = decode(&table).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let field = decoded.field_observed(0, &mut work).unwrap().unwrap();
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(field.name(), "source");
    assert!(field.is_nullable());
    assert_eq!(
        field.metadata().get("nr.logical_type").unwrap(),
        "unknown-provider-attribute"
    );
    assert!(table.shape.as_ref().unwrap().value_types.is_empty());
    assert!(
        table.shape.as_ref().unwrap().fields[0]
            .logical_type_id
            .is_none()
    );
    assert!(
        table.shape.as_ref().unwrap().fields[0]
            .value_binding_id
            .is_none()
    );
}
#[test]
fn all_source_fields_reject_execution_refs_even_when_unreferenced() {
    for logical in [true, false] {
        let mut table = table();
        if logical {
            table.shape.as_mut().unwrap().fields[0].logical_type_id = Some(0);
        } else {
            table.shape.as_mut().unwrap().fields[0].value_binding_id = Some(0);
        }
        assert!(matches!(
            decode(&table),
            Err(ProjectionFailure::Codec(TypeCodecError::InvalidShape(
                "source field owner cannot reference execution identity"
            )))
        ));
    }
}
#[test]
fn source_field_namespace_rejects_semantic_entries() {
    let mut table = table();
    table
        .shape
        .as_mut()
        .unwrap()
        .logical_types
        .push(novarocks_proto_models::value_type::LogicalTypeDefinition::default());
    assert!(matches!(
        decode(&table),
        Err(ProjectionFailure::Codec(TypeCodecError::InvalidShape(
            "source field owner cannot contain execution definitions"
        )))
    ));
}
#[test]
fn source_field_host_refusal_is_original_and_precedes_materialization() {
    let table = table();
    let mut request = |_: Layout| panic!("no exact child request after refusal");
    let mut calls = 0;
    let mut opaque = |_: usize| {
        calls += 1;
        Err("original-host-cause")
    };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    assert!(matches!(
        decode_source_field_table_with_creator(
            &table,
            source_bytes(&table),
            limits(),
            &mut creation,
            &mut work
        ),
        Err(ProjectionFailure::Host("original-host-cause"))
    ));
    assert_eq!(calls, 1);
}
#[test]
fn unreferenced_source_graph_cycles_are_rejected() {
    let mut table = table();
    table
        .shape
        .as_mut()
        .unwrap()
        .carriers
        .push(wire::CarrierTypeDefinition {
            id: 7,
            kind: Some(wire::carrier_type_definition::Kind::ListFieldId(9)),
        });
    let mut field = table.shape.as_ref().unwrap().fields[0].clone();
    field.id = 9;
    field.carrier_type_id = Some(7);
    table.shape.as_mut().unwrap().fields.push(field);
    table.field_facts.push(wire::SourceArrowFieldFacts {
        field_id: Some(9),
        ..Default::default()
    });
    assert!(matches!(
        decode(&table),
        Err(ProjectionFailure::Codec(TypeCodecError::InvalidShape(
            "cyclic type table references"
        )))
    ));
}

#[test]
fn package_creator_source_fields_hold_the_real_construction_until_last_loan() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Hold(Arc<AtomicUsize>);
    impl novarocks_type_contract::ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(dropped.clone())));
    let package = novarocks_proto_models::physical_package_v2::FragmentPackage {
        types: Some(wire::TypeTable::default()),
        source_fields: Some(table()),
        ..Default::default()
    };
    // This fixture owns a fresh package shell and the exact public Vec/String
    // backing counted above. It has no other package-owned heap allocations.
    let bytes =
        std::mem::size_of_val(&package) + source_bytes(package.source_fields.as_ref().unwrap());
    let mut exact = |_: Layout| Ok::<_, &'static str>(());
    let mut opaque = |_: usize| Ok::<_, &'static str>(());
    let mut creation = ValueSchemaCreation::new(storage, &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let decoded = decode_package_type_table_with_creator_in(
        &package,
        bytes,
        limits(),
        &mut creation,
        &mut work,
    )
    .unwrap();
    assert!(decoded.value_types().next().is_none());
    assert_eq!(
        decoded
            .source_field(0)
            .unwrap()
            .metadata()
            .get("nr.logical_type")
            .unwrap(),
        "unknown-provider-attribute"
    );
    let source_loan = decoded.source_field_borrow(0).unwrap().storage_lifetime();
    drop(creation);
    drop(decoded);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(source_loan);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn package_creator_returns_the_first_host_payload_before_graph_work() {
    let package = novarocks_proto_models::physical_package_v2::FragmentPackage {
        types: Some(wire::TypeTable::default()),
        source_fields: Some(table()),
        ..Default::default()
    };
    let bytes =
        std::mem::size_of_val(&package) + source_bytes(package.source_fields.as_ref().unwrap());
    let mut exact = |_: Layout| panic!("no exact request after original refusal");
    let mut calls = 0usize;
    let mut opaque = |_: usize| {
        calls += 1;
        Err((7usize, "actual host cause"))
    };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    assert!(matches!(
        decode_package_type_table_with_creator_in(
            &package,
            bytes,
            limits(),
            &mut creation,
            &mut work
        ),
        Err(ProjectionFailure::Host((7, "actual host cause")))
    ));
    assert_eq!(calls, 1);
}

fn associated_value() -> FunctionValueType {
    use novarocks_type_contract::{ValueField, ValueKind, ValueLayout, ValueTypeRef};
    let storage = ValueTypeStorageLifetime::pure();
    let child = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        false,
        storage.clone(),
    )
    .unwrap();
    let value = ValueTypeRef::try_new(
        ValueKind::Struct(
            vec![ValueField::new("payload", child, storage.clone())].into_boxed_slice(),
        ),
        true,
        storage.clone(),
    )
    .unwrap();
    let layout = ValueLayout::try_new(
        &DataType::Struct(vec![Arc::new(Field::new("payload", DataType::Utf8, false))].into()),
        storage,
    )
    .unwrap();
    FunctionValueType::try_from_value(value, layout).unwrap()
}
#[test]
fn execution_field_association_preserves_nested_domain_and_source_separation() {
    let ty = associated_value();
    let execution = Arc::new(Field::new("output", ty.data_type().clone(), ty.nullable()));
    let source = Arc::new(Field::new("source", DataType::Utf8, true));
    let values = [(u32::MAX, &ty)];
    let fields = [
        BorrowedFieldTypeRoot {
            id: 0,
            field: &execution,
            value_type_id: Some(u32::MAX),
        },
        BorrowedFieldTypeRoot {
            id: u32::MAX,
            field: &source,
            value_type_id: None,
        },
    ];
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let encoded = encode_borrowed_type_table_associated_sources_pure_in(
        &values,
        &fields,
        TypeProjectionLimits {
            max_definitions: 32,
            max_expanded_nodes: 256,
            max_string_bytes: 4096,
        },
        &mut work,
    )
    .unwrap();
    let wire = encoded.as_wire();
    let output = wire.fields.iter().find(|field| field.id == 0).unwrap();
    assert_eq!(output.value_binding_id, Some(u32::MAX));
    assert_eq!(output.logical_type_id, Some(u32::MAX));
    let source_shape_field = wire
        .fields
        .iter()
        .find(|field| field.id == u32::MAX)
        .unwrap();
    assert_eq!(
        (
            source_shape_field.value_binding_id,
            source_shape_field.logical_type_id
        ),
        (None, None)
    );
    let source_fields = [(u32::MAX, &source)];
    let source_wire = encode_source_field_table_pure_in(
        &source_fields,
        TypeProjectionLimits {
            max_definitions: 32,
            max_expanded_nodes: 256,
            max_string_bytes: 4096,
        },
        &mut work,
    )
    .unwrap();
    assert!(
        source_wire
            .shape
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .all(|field| field.logical_type_id.is_none() && field.value_binding_id.is_none())
    );
    assert!(std::ptr::eq(
        encoded
            .field_execution_type_observed(0, &mut work)
            .unwrap()
            .unwrap(),
        &ty
    ));
    assert!(
        encoded
            .field_execution_type_observed(u32::MAX, &mut work)
            .unwrap()
            .is_none()
    );
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let decoded = decode::decode(
        wire,
        TypeProjectionLimits {
            max_definitions: 32,
            max_expanded_nodes: 256,
            max_string_bytes: 4096,
        },
        &mut work,
    );
    // A generic pure TypeTable owner validates the same explicit semantic refs.
    let decoded = decoded.unwrap();
    assert_eq!(decoded.value_type(u32::MAX), Some(&ty));
    assert_eq!(decoded.field_execution_type(0), Some(&ty));
    decoded
        .validate_source_field_root_observed(u32::MAX, &mut work)
        .unwrap();
    assert!(
        decoded
            .validate_source_field_root_observed(0, &mut work)
            .is_err()
    );
}
#[test]
fn execution_field_association_refuses_dangling_child_nullability_and_attributes() {
    let ty = associated_value();
    let values = [(0, &ty)];
    for (field, binding) in [
        (
            Field::new("output", ty.data_type().clone(), true),
            Some(u32::MAX),
        ),
        (
            Field::new(
                "output",
                DataType::Struct(
                    vec![Arc::new(Field::new("payload", DataType::Utf8, true))].into(),
                ),
                true,
            ),
            Some(0),
        ),
        (
            Field::new("output", ty.data_type().clone(), true).with_metadata(
                std::collections::HashMap::from([(
                    String::from("nr.logical_type"),
                    String::from("anything"),
                )]),
            ),
            Some(0),
        ),
    ] {
        let field = Arc::new(field);
        let fields = [BorrowedFieldTypeRoot {
            id: 0,
            field: &field,
            value_type_id: binding,
        }];
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        assert!(matches!(
            encode_borrowed_type_table_associated_sources_pure_in(
                &values,
                &fields,
                TypeProjectionLimits {
                    max_definitions: 32,
                    max_expanded_nodes: 256,
                    max_string_bytes: 4096
                },
                &mut work
            ),
            Err(TypeCodecError::InvalidShape(_))
        ));
    }
}

#[test]
fn source_role_refuses_nested_execution_ref_below_unpaired_root() {
    let ty = associated_value();
    let field = Arc::new(Field::new("output", ty.data_type().clone(), true));
    let values = [(u32::MAX, &ty)];
    let fields = [BorrowedFieldTypeRoot {
        id: 0,
        field: &field,
        value_type_id: Some(u32::MAX),
    }];
    let limits = TypeProjectionLimits {
        max_definitions: 32,
        max_expanded_nodes: 256,
        max_string_bytes: 4096,
    };
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let mut table =
        encode_borrowed_type_table_associated_sources_pure_in(&values, &fields, limits, &mut work)
            .unwrap()
            .into_wire();
    let root = table.fields.iter_mut().find(|field| field.id == 0).unwrap();
    root.logical_type_id = None;
    root.value_binding_id = None;
    // The actual child reference remains legitimate in this mixed namespace;
    // this root cannot lend that graph as independent source-shape facts.
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let decoded = decode::decode(&table, limits, &mut work).unwrap();
    assert!(decoded.field_execution_type(0).is_none());
    assert!(matches!(
        decoded.validate_source_field_root_observed(0, &mut work),
        Err(TypeCodecError::InvalidShape(
            "source-only field graph contains execution references"
        ))
    ));
}

#[test]
fn input_derived_source_fields_keep_attributes_and_original_host_refusal() {
    let raw = table();
    let receiving = decode(&raw).unwrap();
    let mut exact = |_: Layout| Ok::<_, &'static str>(());
    let mut opaque = |_: usize| Ok::<_, &'static str>(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let derived = decode_source_field_table_input_derived_with_creator(
        &raw,
        source_bytes(&raw),
        &mut creation,
        &mut work,
    )
    .unwrap();
    assert_eq!(
        derived.field_observed(0, &mut work).unwrap(),
        receiving.field_observed(0, &mut work).unwrap()
    );
    assert!(derived.creation_request_bytes_upper_bound().unwrap() > 0);
    let mut calls = 0;
    let mut refusal = |_: usize| {
        calls += 1;
        assert_eq!(calls, 1, "callback after original host refusal");
        Err("original source creator")
    };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut refusal);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let error = decode_source_field_table_input_derived_with_creator(
        &raw,
        source_bytes(&raw),
        &mut creation,
        &mut work,
    )
    .err()
    .unwrap();
    assert!(matches!(
        error,
        ProjectionFailure::Host("original source creator")
    ));
}

#[test]
fn source_facts_require_exact_present_identity_cover_including_unused_fields() {
    for mutation in 0..5 {
        let mut t = table();
        match mutation {
            0 => t.field_facts.clear(),
            1 => {
                t.field_facts.push(t.field_facts[0].clone());
            }
            2 => t.field_facts[0].field_id = Some(u32::MAX),
            3 => t.field_facts[0].field_id = None,
            _ => t.shape.as_mut().unwrap().fields[0].value_binding_id = Some(0),
        }
        assert!(
            matches!(
                decode(&t),
                Err(ProjectionFailure::Codec(TypeCodecError::InvalidShape(_)))
            ),
            "mutation {mutation}"
        );
    }
}
#[test]
fn source_facts_dictionary_descriptors_remain_separate_from_execution_identity() {
    let mut t = table();
    let shape = t.shape.as_mut().unwrap();
    shape.carriers.extend([
        wire::CarrierTypeDefinition {
            id: 1,
            kind: Some(wire::carrier_type_definition::Kind::Primitive(
                novarocks_proto_models::arrow_type::ArrowPrimitiveType::Int32 as i32,
            )),
        },
        wire::CarrierTypeDefinition {
            id: 2,
            kind: Some(wire::carrier_type_definition::Kind::Dictionary(
                wire::DictionaryTypes {
                    key_type_id: Some(1),
                    value_type_id: Some(u32::MAX),
                },
            )),
        },
    ]);
    shape.fields[0].carrier_type_id = Some(2);
    t.field_facts[0].dictionary_id = Some(0);
    t.field_facts[0].dictionary_is_ordered = Some(false);
    let decoded = decode(&t).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let field = decoded.field_observed(0, &mut work).unwrap().unwrap();
    #[allow(deprecated)]
    {
        assert_eq!(field.dict_id(), Some(0));
        assert_eq!(field.dict_is_ordered(), Some(false));
    }
    assert_eq!(
        field.data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    );
    t.field_facts[0].dictionary_is_ordered = None;
    assert!(decode(&t).is_err());
}
#[test]
fn each_actual_source_constructor_refusal_preserves_first_payload_and_has_no_tail() {
    fn execute(
        t: &wire::SourceFieldTable,
        refuse: Option<usize>,
    ) -> (
        Result<DecodedSourceFieldTable, ProjectionFailure<TypeCodecError, usize>>,
        usize,
    ) {
        use std::cell::Cell;
        let calls = Cell::new(0usize);
        let tick = || {
            let n = calls.get();
            assert!(refuse.is_none_or(|at| n <= at), "callback after refusal");
            calls.set(n + 1);
            if refuse == Some(n) { Err(n) } else { Ok(()) }
        };
        let mut exact = |_: Layout| tick();
        // Both exact and opaque participate in the original linear order.
        let mut opaque = |_: usize| {
            let n = calls.get();
            assert!(refuse.is_none_or(|at| n <= at));
            calls.set(n + 1);
            if refuse == Some(n) { Err(n) } else { Ok(()) }
        };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
        let result = decode_source_field_table_with_creator(
            t,
            source_bytes(t),
            limits(),
            &mut creation,
            &mut work,
        );
        (result, calls.get())
    }
    let t = table();
    let (success, count) = execute(&t, None);
    assert!(success.is_ok());
    assert!(count > 1);
    for refusal in 0..count {
        let (result, calls) = execute(&t, Some(refusal));
        assert!(matches!(result,Err(ProjectionFailure::Host(at)) if at==refusal));
        assert_eq!(calls, refusal + 1);
    }
}

#[test]
fn actual_wide_empty_metadata_constructor_fits_original_work_envelope() {
    let mut t = table();
    let shape = t.shape.as_mut().unwrap();
    shape.fields.clear();
    t.field_facts.clear();
    for id in 0..400 {
        shape.fields.push(wire::FieldDefinition {
            id,
            name: String::from("source"),
            nullable: true,
            carrier_type_id: Some(u32::MAX),
            logical_type_id: None,
            value_binding_id: None,
        });
        t.field_facts.push(wire::SourceArrowFieldFacts {
            field_id: Some(id),
            ..Default::default()
        });
    }
    // This fixture's original public Vec genuinely owns spare backing. Its
    // capacity invoice is source coexistence, not work on each empty map.
    t.field_facts.reserve_exact(64 * 1024);
    let source = source_bytes(&t);
    assert!(source > 2 * 1024 * 1024);
    let structural = TypeProjectionLimits::for_validated_source_root_count(401).unwrap();
    let mut limit = limits();
    limit.max_definitions = structural.max_definitions;
    limit.max_expanded_nodes = structural.max_expanded_nodes;
    limit.max_coexisting_source_and_request_bytes = source
        .checked_add(limit.max_allocation_request_bytes)
        .unwrap();
    limit.max_work = 2_000_000_000; // The preserved failure envelope; not raised.
    let mut exact = |_: Layout| Ok::<_, &'static str>(());
    let mut opaque = |_: usize| Ok::<_, &'static str>(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let decoded =
        decode_source_field_table_with_creator(&t, source, limit, &mut creation, &mut work)
            .unwrap();
    for id in 0..400 {
        let field = decoded.field_observed(id, &mut work).unwrap().unwrap();
        assert!(field.metadata().as_arc().is_none());
        assert_eq!(field.data_type(), &DataType::Utf8);
    }
}

// The original provider-root positive formerly used execution FieldDefinition
// annotations. SourceFieldTable is now the sole owner of these opaque facts.
#[test]
#[allow(deprecated)]
fn original_nested_provider_source_attributes_and_dictionary_identity() {
    use wire::carrier_type_definition::Kind;
    let table = wire::SourceFieldTable {
        shape: Some(wire::TypeTable {
            carriers: vec![
                wire::CarrierTypeDefinition {
                    id: 1,
                    kind: Some(Kind::Primitive(
                        novarocks_proto_models::arrow_type::ArrowPrimitiveType::Int16 as i32,
                    )),
                },
                wire::CarrierTypeDefinition {
                    id: 2,
                    kind: Some(Kind::Primitive(
                        novarocks_proto_models::arrow_type::ArrowPrimitiveType::Utf8 as i32,
                    )),
                },
                wire::CarrierTypeDefinition {
                    id: 3,
                    kind: Some(Kind::Dictionary(wire::DictionaryTypes {
                        key_type_id: Some(1),
                        value_type_id: Some(2),
                    })),
                },
                wire::CarrierTypeDefinition {
                    id: 4,
                    kind: Some(Kind::ListFieldId(10)),
                },
                wire::CarrierTypeDefinition {
                    id: 5,
                    kind: Some(Kind::StructType(wire::StructFields {
                        field_ids: vec![9, 11],
                    })),
                },
            ],
            fields: vec![
                wire::FieldDefinition {
                    id: 9,
                    name: "dictionary".into(),
                    nullable: false,
                    carrier_type_id: Some(3),
                    logical_type_id: None,
                    value_binding_id: None,
                },
                wire::FieldDefinition {
                    id: 10,
                    name: "item".into(),
                    nullable: true,
                    carrier_type_id: Some(3),
                    logical_type_id: None,
                    value_binding_id: None,
                },
                wire::FieldDefinition {
                    id: 11,
                    name: "nested".into(),
                    nullable: true,
                    carrier_type_id: Some(4),
                    logical_type_id: None,
                    value_binding_id: None,
                },
                wire::FieldDefinition {
                    id: 17,
                    name: "original_root".into(),
                    nullable: false,
                    carrier_type_id: Some(5),
                    logical_type_id: None,
                    value_binding_id: None,
                },
            ],
            value_types: vec![],
            logical_types: vec![],
        }),
        field_facts: vec![
            wire::SourceArrowFieldFacts {
                field_id: Some(9),
                metadata: vec![
                    novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry {
                        key: "provider.field_id".into(),
                        value: "17".into(),
                    },
                ],
                dictionary_id: Some(-37),
                dictionary_is_ordered: Some(true),
            },
            wire::SourceArrowFieldFacts {
                field_id: Some(10),
                metadata: vec![
                    novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry {
                        key: "provider.source".into(),
                        value: "nested-original".into(),
                    },
                ],
                dictionary_id: Some(0),
                dictionary_is_ordered: Some(false),
            },
            wire::SourceArrowFieldFacts {
                field_id: Some(11),
                metadata: vec![],
                dictionary_id: None,
                dictionary_is_ordered: None,
            },
            wire::SourceArrowFieldFacts {
                field_id: Some(17),
                metadata: vec![
                    novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry {
                        key: "provider.schema".into(),
                        value: "frozen".into(),
                    },
                ],
                dictionary_id: None,
                dictionary_is_ordered: None,
            },
        ],
    };
    let decoded = decode_with_limits(
        &table,
        PackageTypeProjectionLimits {
            max_definitions: 64,
            max_expanded_nodes: 128,
            max_string_bytes: 65536,
            max_allocation_requests: 16384,
            max_allocation_request_bytes: 16 * 1024 * 1024,
            max_coexisting_source_and_request_bytes: 32 * 1024 * 1024,
            max_work: 128 * 1024 * 1024,
        },
    )
    .unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
    let root = decoded.field_observed(17, &mut work).unwrap().unwrap();
    assert_eq!(root.name(), "original_root");
    assert!(!root.is_nullable());
    assert_eq!(root.metadata()["provider.schema"], "frozen");
    let DataType::Struct(children) = root.data_type() else {
        panic!("source Struct")
    };
    assert_eq!(children[0].name(), "dictionary");
    assert!(!children[0].is_nullable());
    assert_eq!(children[0].metadata()["provider.field_id"], "17");
    assert_eq!(children[0].dict_id(), Some(-37));
    assert_eq!(children[0].dict_is_ordered(), Some(true));
    assert_eq!(children[1].name(), "nested");
    assert!(children[1].is_nullable());
    let DataType::List(item) = children[1].data_type() else {
        panic!("source List")
    };
    assert_eq!(item.name(), "item");
    assert!(item.is_nullable());
    assert_eq!(item.metadata()["provider.source"], "nested-original");
    assert_eq!(item.data_type(), children[0].data_type());
    assert!(novarocks_type_contract::arrow_fields_exact(
        children[0].as_ref(),
        decoded
            .field_observed(9, &mut work)
            .unwrap()
            .unwrap()
            .as_ref()
    ));
    assert!(table.shape.as_ref().unwrap().value_types.is_empty());
    assert!(table.shape.as_ref().unwrap().logical_types.is_empty());
    assert!(
        table
            .shape
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .all(|field| field.logical_type_id.is_none() && field.value_binding_id.is_none())
    );
    work.finish().unwrap();
}
