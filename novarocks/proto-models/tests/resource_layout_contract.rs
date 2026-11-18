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

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    mem::{align_of, align_of_val, size_of, size_of_val},
};

use bytes::Bytes;
use novarocks_proto_models::{
    FILE_DESCRIPTOR_SET, common, generated_resource_layouts, novarocks, plan,
    resource_layout::{
        Cardinality, FieldLayout, GeneratedResourceLayout, ObjectKind, ObjectLayout, RustLayout,
        Storage, WireField, WireKind,
    },
};
use prost_reflect::{
    Cardinality as DescriptorCardinality, DescriptorPool, FieldDescriptor, Kind, OneofDescriptor,
};

fn pool() -> DescriptorPool {
    DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("canonical descriptor set")
}

fn is_synthetic(oneof: &OneofDescriptor) -> bool {
    oneof
        .fields()
        .any(|field| field.field_descriptor_proto().proto3_optional == Some(true))
}

fn wire_kind(kind: &Kind) -> WireKind {
    match kind {
        Kind::Double => WireKind::Double,
        Kind::Float => WireKind::Float,
        Kind::Int64 => WireKind::Int64,
        Kind::Uint64 => WireKind::Uint64,
        Kind::Int32 => WireKind::Int32,
        Kind::Fixed64 => WireKind::Fixed64,
        Kind::Fixed32 => WireKind::Fixed32,
        Kind::Bool => WireKind::Bool,
        Kind::String => WireKind::String,
        Kind::Message(_) => WireKind::Message,
        Kind::Bytes => WireKind::Bytes,
        Kind::Uint32 => WireKind::Uint32,
        Kind::Enum(_) => WireKind::Enum,
        Kind::Sfixed32 => WireKind::Sfixed32,
        Kind::Sfixed64 => WireKind::Sfixed64,
        Kind::Sint32 => WireKind::Sint32,
        Kind::Sint64 => WireKind::Sint64,
    }
}

fn check_wire(wire: &WireField, descriptor: &FieldDescriptor) {
    let context = descriptor.full_name();
    assert_eq!(wire.name, descriptor.name(), "{context}");
    assert_eq!(wire.number, descriptor.number(), "{context}");
    let kind = descriptor.kind();
    assert_eq!(wire.kind, wire_kind(&kind), "{context}");
    let cardinality = match descriptor.cardinality() {
        DescriptorCardinality::Repeated => Cardinality::Repeated,
        DescriptorCardinality::Required => Cardinality::Required,
        DescriptorCardinality::Optional if descriptor.supports_presence() => Cardinality::Optional,
        DescriptorCardinality::Optional => Cardinality::Singular,
    };
    assert_eq!(wire.cardinality, cardinality, "{context}");
    assert_eq!(wire.packed, descriptor.is_packed(), "{context}");
    let target = match &kind {
        Kind::Message(message) => Some(message.full_name()),
        Kind::Enum(enumeration) => Some(enumeration.full_name()),
        _ => None,
    };
    assert_eq!(wire.target, target, "{context}");
    if descriptor.is_map() {
        let Kind::Message(entry) = kind else {
            panic!("map entry must be a message: {context}");
        };
        assert!(entry.is_map_entry(), "{context}");
        let manifest = wire.map_entry.expect("map-entry wire manifest");
        assert_eq!(manifest.len(), entry.fields().len(), "{context}");
        let mut covered = BTreeSet::new();
        for child in manifest {
            assert!(covered.insert(child.number), "duplicate map-entry wire tag");
            check_wire(
                child,
                &entry.get_field(child.number).expect("map-entry tag"),
            );
        }
        assert_eq!(
            covered,
            entry.fields().map(|field| field.number()).collect(),
            "{context}"
        );
    } else {
        assert!(wire.map_entry.is_none(), "unexpected map entry: {context}");
    }
}

fn dimensions<T>(layout: &RustLayout) {
    assert_eq!(layout.size, size_of::<T>());
    assert_eq!(layout.alignment, align_of::<T>());
}

fn check_leaf(
    rust: &RustLayout,
    descriptor: &FieldDescriptor,
    layouts: &BTreeMap<&str, &ObjectLayout>,
) {
    if let Storage::Box(child) = rust.storage {
        assert!(matches!(descriptor.kind(), Kind::Message(_)));
        dimensions::<Box<()>>(rust);
        check_leaf(child, descriptor, layouts);
        return;
    }
    match descriptor.kind() {
        Kind::Message(target) => {
            let Storage::Message(actual) = rust.storage else {
                panic!("{} must retain message storage", descriptor.full_name());
            };
            assert_eq!(actual, target.full_name());
            let target = layouts[actual];
            assert_eq!(target.kind, ObjectKind::Message);
            assert_eq!(rust.size, target.size);
            assert_eq!(rust.alignment, target.alignment);
        }
        Kind::String => {
            assert!(matches!(rust.storage, Storage::String));
            dimensions::<String>(rust);
        }
        Kind::Bytes => match rust.storage {
            Storage::Bytes => dimensions::<Bytes>(rust),
            Storage::Vec(child) => {
                dimensions::<Vec<u8>>(rust);
                assert!(matches!(child.storage, Storage::Scalar));
                dimensions::<u8>(child);
            }
            _ => panic!("{} must retain bytes storage", descriptor.full_name()),
        },
        kind => {
            assert!(matches!(rust.storage, Storage::Scalar));
            match kind {
                Kind::Double => dimensions::<f64>(rust),
                Kind::Float => dimensions::<f32>(rust),
                Kind::Int64 | Kind::Sfixed64 | Kind::Sint64 => dimensions::<i64>(rust),
                Kind::Uint64 | Kind::Fixed64 => dimensions::<u64>(rust),
                Kind::Int32 | Kind::Sfixed32 | Kind::Sint32 | Kind::Enum(_) => {
                    dimensions::<i32>(rust)
                }
                Kind::Uint32 | Kind::Fixed32 => dimensions::<u32>(rust),
                Kind::Bool => dimensions::<bool>(rust),
                Kind::String | Kind::Message(_) | Kind::Bytes => unreachable!(),
            }
        }
    }
}

fn check_field_storage(
    field: &FieldLayout,
    descriptor: &FieldDescriptor,
    kind: ObjectKind,
    layouts: &BTreeMap<&str, &ObjectLayout>,
) {
    if descriptor.is_map() {
        let (key, value) = match field.rust.storage {
            Storage::HashMap { key, value } | Storage::BTreeMap { key, value } => (key, value),
            _ => panic!(
                "{} must retain a real map container",
                descriptor.full_name()
            ),
        };
        let Kind::Message(entry) = descriptor.kind() else {
            unreachable!();
        };
        assert!(!layouts.contains_key(entry.full_name()));
        check_leaf(key, &entry.map_entry_key_field(), layouts);
        check_leaf(value, &entry.map_entry_value_field(), layouts);
        return;
    }
    if kind == ObjectKind::Message
        && let Some(oneof) = descriptor
            .containing_oneof()
            .filter(|oneof| !is_synthetic(oneof))
    {
        let Storage::Option(child) = field.rust.storage else {
            panic!("real parent oneof must use Option");
        };
        let Storage::Oneof(target) = child.storage else {
            panic!("real parent oneof must name its actual enum");
        };
        assert_eq!(target, oneof.full_name());
        let target = layouts[target];
        assert_eq!(target.kind, ObjectKind::Oneof);
        assert_eq!(child.size, target.size);
        assert_eq!(child.alignment, target.alignment);
        assert_eq!(
            field
                .wire
                .iter()
                .map(|wire| wire.number)
                .collect::<BTreeSet<_>>(),
            oneof.fields().map(|member| member.number()).collect()
        );
        return;
    }
    assert_eq!(field.wire.len(), 1);
    let leaf = match (
        kind,
        descriptor.cardinality(),
        descriptor.supports_presence(),
    ) {
        (ObjectKind::Message, DescriptorCardinality::Repeated, _) => {
            let Storage::Vec(child) = field.rust.storage else {
                panic!("repeated field must use Vec");
            };
            dimensions::<Vec<()>>(&field.rust);
            child
        }
        (ObjectKind::Message, DescriptorCardinality::Optional, true) => {
            let Storage::Option(child) = field.rust.storage else {
                panic!("optional field must use Option");
            };
            child
        }
        _ => &field.rust,
    };
    check_leaf(leaf, descriptor, layouts);
}

fn check_object(
    object: &ObjectLayout,
    descriptors: impl IntoIterator<Item = FieldDescriptor>,
    layouts: &BTreeMap<&str, &ObjectLayout>,
) {
    assert!(object.alignment.is_power_of_two(), "{}", object.schema_id);
    assert_eq!(object.size % object.alignment, 0, "{}", object.schema_id);
    let expected = descriptors
        .into_iter()
        .map(|field| (field.number(), field))
        .collect::<BTreeMap<_, _>>();
    let mut covered = BTreeSet::new();
    let mut names = BTreeSet::new();
    for field in object.fields {
        assert!(!field.rust_name.is_empty());
        assert!(names.insert(field.rust_name), "duplicate actual Rust field");
        assert!(!field.wire.is_empty(), "{}", object.schema_id);
        for wire in field.wire {
            assert!(covered.insert(wire.number), "duplicate field tag");
            check_wire(
                wire,
                expected.get(&wire.number).expect("descriptor field tag"),
            );
        }
        check_field_storage(
            field,
            &expected[&field.wire[0].number],
            object.kind,
            layouts,
        );
    }
    assert_eq!(
        covered,
        expected.keys().copied().collect(),
        "{}",
        object.schema_id
    );
}

#[test]
fn descriptor_messages_oneofs_and_all_fields_have_exact_resource_manifests() {
    let pool = pool();
    let mut layouts = BTreeMap::new();
    for object in generated_resource_layouts() {
        assert!(
            layouts.insert(object.schema_id, object).is_none(),
            "duplicate layout identity"
        );
    }
    let mut expected = BTreeSet::new();
    for message in pool.all_messages() {
        if message.is_map_entry() {
            assert!(!layouts.contains_key(message.full_name()));
            continue;
        }
        assert!(expected.insert(message.full_name().to_owned()));
        let object = layouts[message.full_name()];
        assert_eq!(object.kind, ObjectKind::Message);
        assert_eq!(
            object.reserved,
            message
                .reserved_ranges()
                .map(|range| (range.start, range.end))
                .collect::<Vec<_>>(),
            "{} reserved numbers differ from the descriptor",
            message.full_name()
        );
        check_object(object, message.fields(), &layouts);
        for oneof in message.oneofs() {
            if is_synthetic(&oneof) {
                assert!(!layouts.contains_key(oneof.full_name()));
                continue;
            }
            assert!(expected.insert(oneof.full_name().to_owned()));
            let object = layouts[oneof.full_name()];
            assert_eq!(object.kind, ObjectKind::Oneof);
            assert!(
                object.reserved.is_empty(),
                "oneof has no message-local reserved namespace"
            );
            check_object(object, oneof.fields(), &layouts);
        }
    }
    assert_eq!(
        layouts
            .keys()
            .map(|id| (*id).to_owned())
            .collect::<BTreeSet<_>>(),
        expected
    );
}

fn actual_object<T: GeneratedResourceLayout>(value: &T) -> &'static ObjectLayout {
    let object = generated_resource_layouts()
        .find(|object| object.schema_id == T::SCHEMA_ID)
        .expect("actual DTO must be registered");
    assert_eq!(object.schema_id, T::SCHEMA_ID);
    assert_eq!(object.kind, T::RESOURCE_LAYOUT.kind);
    assert_eq!(object.size, size_of_val(value));
    assert_eq!(object.alignment, align_of_val(value));
    object
}

fn field_named(object: &ObjectLayout, descriptor_name: &str) -> &'static FieldLayout {
    // Join on descriptor names, never a guessed Rust case conversion or tag.
    object
        .fields
        .iter()
        .find(|field| field.wire.iter().any(|wire| wire.name == descriptor_name))
        .expect("descriptor field manifest")
}

fn actual_field<T>(layout: &RustLayout, value: &T) {
    assert_eq!(layout.size, size_of_val(value));
    assert_eq!(layout.alignment, align_of_val(value));
}

#[test]
fn target_layouts_follow_real_hash_maps_bytes_and_automatic_recursive_boxing() {
    let reference = common::TypeDesc {
        value_binding_id: Some(0),
    };
    let object = actual_object(&reference);
    assert_eq!(object.fields.len(), 1);
    let binding = field_named(object, "value_binding_id");
    actual_field(&binding.rust, &reference.value_binding_id);
    let Storage::Option(value) = binding.rust.storage else {
        panic!("binding reference must preserve presence");
    };
    dimensions::<u32>(value);

    let stats = novarocks::FileScanRange::default();
    let _: &HashMap<i32, novarocks::FilePruningMinMaxValue> = &stats.file_pruning_min_max_values;
    let stats_field = field_named(actual_object(&stats), "file_pruning_min_max_values");
    actual_field(&stats_field.rust, &stats.file_pruning_min_max_values);
    let Storage::HashMap { key, value } = stats_field.rust.storage else {
        panic!("real HashMap cannot be modeled as another container");
    };
    dimensions::<i32>(key);
    assert!(
        matches!(value.storage, Storage::Message(target) if target == novarocks::FilePruningMinMaxValue::SCHEMA_ID)
    );
    let child = novarocks::FilePruningMinMaxValue::default();
    let target = actual_object(&child);
    assert_eq!(
        (value.size, value.alignment),
        (target.size, target.alignment)
    );

    let request = novarocks::CreateTaskRequest::default();
    let _: &Bytes = &request.frozen_fragment;
    let request_object = actual_object(&request);
    for (name, value) in [
        ("frozen_fragment", &request.frozen_fragment),
        ("creation_metadata", &request.creation_metadata),
    ] {
        let field = field_named(request_object, name);
        assert!(matches!(field.rust.storage, Storage::Bytes));
        actual_field(&field.rust, value);
    }
    let result = novarocks::FetchResultResponse::default();
    let field = field_named(actual_object(&result), "result_arrow_ipc");
    let _: &Bytes = &result.result_arrow_ipc;
    assert!(matches!(field.rust.storage, Storage::Bytes));
    actual_field(&field.rust, &result.result_arrow_ipc);

    let field = plan::ArrowPhysicalField::default();
    let field_layout = actual_object(&field);
    assert!(
        !field_layout
            .fields
            .iter()
            .flat_map(|field| field.wire)
            .any(|wire| matches!(wire.number, 4..=6))
    );
    let logical = field_named(field_layout, "logical_type_id");
    actual_field(&logical.rust, &field.logical_type_id);

    let _: &Option<Box<plan::ArrowPhysicalType>> = &field.r#type;
    let recursive = field_named(actual_object(&field), "type");
    actual_field(&recursive.rust, &field.r#type);
    let Storage::Option(boxed) = recursive.rust.storage else {
        panic!("recursive optional message must retain Option");
    };
    dimensions::<Box<plan::ArrowPhysicalType>>(boxed);
    let Storage::Box(child) = boxed.storage else {
        panic!("automatic boxing must be part of the resource model");
    };
    assert!(
        matches!(child.storage, Storage::Message(target) if target == plan::ArrowPhysicalType::SCHEMA_ID)
    );
    dimensions::<plan::ArrowPhysicalType>(child);

    let recursive = plan::arrow_physical_type::Kind::List(Box::new(field));
    let variant = field_named(actual_object(&recursive), "list");
    let plan::arrow_physical_type::Kind::List(value) = &recursive else {
        unreachable!();
    };
    actual_field(&variant.rust, value);
    let Storage::Box(child) = variant.rust.storage else {
        panic!("recursive oneof variant must retain Box");
    };
    assert!(
        matches!(child.storage, Storage::Message(target) if target == plan::ArrowPhysicalField::SCHEMA_ID)
    );
    actual_field(child, value.as_ref());

    let arrow_type = plan::ArrowPhysicalType::default();
    let parent = field_named(actual_object(&arrow_type), "list");
    actual_field(&parent.rust, &arrow_type.kind);
    let Storage::Option(child) = parent.rust.storage else {
        panic!("parent oneof must retain Option");
    };
    assert!(
        matches!(child.storage, Storage::Oneof(target) if target == plan::arrow_physical_type::Kind::SCHEMA_ID)
    );
    dimensions::<plan::arrow_physical_type::Kind>(child);
}
