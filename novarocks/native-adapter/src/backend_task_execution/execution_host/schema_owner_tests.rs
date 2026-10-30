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
use crate::native_type_construction::{
    NativeTypeConstruction, NativeTypeConstructionError, NativeTypeFactory,
};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_plan_codec::{
    host_projection_v2::ProjectionFailure,
    physical_package_v2::prepare_package_wire_in,
    physical_public_source_schema_v2::decode_public_source_schemas_with_factory_in,
    physical_schema_v2::{SchemaCodecError, decode_schemas_with_factory_in},
    physical_type_v2::decode_package_type_table_with_factory_in,
};
use novarocks_proto_models::{
    arrow_type::ArrowFieldMetadataEntry, physical_package_v2 as wire, physical_type_v2 as ty,
};
use novarocks_type_contract::{CompileCheckpoints, ValueSchemaCreation};

fn authored_schema_package(unrelated: bool) -> (Vec<u8>, u32) {
    let (bytes, _) = select_one_producer();
    let mut package = wire::FragmentPackage::decode(bytes.as_slice()).unwrap();
    // Preserve the real FE expression/request/control/constant package. These
    // additional unused source-shape declarations are still fully validated.
    let types = package.types.as_mut().unwrap();
    let carrier = types
        .carriers
        .iter()
        .find_map(|carrier| match carrier.kind {
            Some(ty::carrier_type_definition::Kind::Primitive(value))
                if value
                    == novarocks_proto_models::arrow_type::ArrowPrimitiveType::Int64 as i32 =>
            {
                Some(carrier.id)
            }
            _ => None,
        })
        .expect("SELECT 1 has an explicitly authored Int64 carrier");
    let first = types
        .fields
        .iter()
        .map(|field| field.id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .unwrap();
    let schema = package
        .schemas
        .iter()
        .map(|schema| schema.id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .unwrap();
    let source = package
        .public_source_schemas
        .iter()
        .filter_map(|source| source.id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .unwrap();
    let count = if unrelated { 2 } else { 1 };
    for ordinal in 0..count {
        let field_id = first + ordinal;
        types.fields.push(ty::FieldDefinition {
            id: field_id,
            name: format!("source_shape_{ordinal}"),
            nullable: false,
            carrier_type_id: Some(carrier),
            logical_type_id: None,
            value_binding_id: None,
        });
        package.schemas.push(wire::SchemaDefinition {
            id: schema + ordinal,
            field_ids: vec![field_id],
        });
        package
            .public_source_schemas
            .push(wire::PublicSourceSchemaFactsDefinition {
                id: Some(source + ordinal),
                source_shape_schema_id: Some(schema + ordinal),
                schema_attributes: vec![ArrowFieldMetadataEntry {
                    key: "provider.schema".into(),
                    value: format!("source {ordinal}"),
                }],
                field_facts: vec![wire::PublicSourceFieldFactsDefinition {
                    field_path: vec![0],
                    // Provider identity is independent of the shape namespace ID.
                    field_id: Some(70 + i32::try_from(ordinal).unwrap()),
                    initial_default: Some(wire::SourceInitialDefault {
                        json: Some("7".into()),
                    }),
                    attributes: vec![ArrowFieldMetadataEntry {
                        key: "provider.field".into(),
                        value: "source original".into(),
                    }],
                }],
            });
    }
    (package.encode_to_vec(), source)
}

fn selected_schema_after_exit(
    scratch: usize,
    unrelated: bool,
) -> (
    novarocks_connector_contract::ConnectorSourceSchemaFacts,
    Arc<MemTracker>,
) {
    let (bytes, selected_id) = authored_schema_package(unrelated);
    let tracker = MemTracker::new_root("candidate selected source schema");
    let actual = NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
    actual
        .request_layout(std::alloc::Layout::array::<u64>(scratch).unwrap())
        .unwrap();
    let scratch = Vec::<u64>::with_capacity(scratch);
    let mut work = CompileCheckpoints::try_new(&Unbounded, CompilePhase::Decode).unwrap();
    let model = FragmentDecodeResourceModel::try_new(&Unbounded).unwrap();
    let prepared = prepare_package_wire_in(
        &bytes,
        &model,
        decode_limits().wire,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let usage = prepared.projection().usage;
    actual
        .request_opaque_upper_bound(usage.peak_requested_heap_bytes_upper)
        .unwrap();
    let package = prepared.materialize_in(&mut |_| Ok(()), &mut work).unwrap();
    let mut source = usage
        .input_bytes
        .checked_add(usage.root_inline_bytes)
        .unwrap()
        .checked_add(usage.peak_requested_heap_bytes_upper)
        .unwrap();
    let factory = NativeTypeFactory::new(&actual);
    let mut exact = |layout| actual.request_layout(layout);
    let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
    let mut creator = ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
    let types = decode_package_type_table_with_factory_in(
        &package,
        source,
        decode_limits().types,
        &mut creator,
        &factory,
        &mut work,
    )
    .unwrap();
    source = source
        .checked_add(types.creation_request_bytes_upper_bound().unwrap())
        .unwrap()
        .checked_add(std::mem::size_of_val(&types))
        .unwrap();
    let schemas = decode_schemas_with_factory_in(
        &package.schemas,
        &types,
        source,
        decode_limits().node,
        &factory,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    source = schemas
        .facts()
        .coexisting_source_and_request_bytes_upper_bound;
    let public = decode_public_source_schemas_with_factory_in(
        &package.public_source_schemas,
        &schemas,
        source,
        decode_limits().node,
        &factory,
        &mut work,
    )
    .unwrap();
    let selected = public
        .lookup_observed(selected_id, &mut work)
        .unwrap()
        .unwrap()
        .clone();
    assert_eq!(
        selected
            .schema_attributes()
            .get("provider.schema")
            .map(String::as_str),
        Some("source 0")
    );
    assert_eq!(
        selected.field_facts()[0]
            .attributes()
            .get("provider.field")
            .map(String::as_str),
        Some("source original")
    );
    drop(public);
    drop(schemas);
    drop(types);
    drop(creator);
    {
        let _factory = factory;
    }
    drop(package);
    work.finish().unwrap();
    drop(scratch);
    drop(actual);
    (selected, tracker)
}

#[test]
fn candidate_selected_schema_keeps_only_its_fields_after_namespace_and_main_exit() {
    let (minimal, minimal_tracker) = selected_schema_after_exit(0, false);
    let (selected, tracker) = selected_schema_after_exit(1024, true);
    assert_eq!(selected, minimal);
    let shape = selected.shape().clone();
    assert_eq!(shape.schema().fields().len(), 1);
    assert_eq!(shape.schema().fields()[0].name(), "source_shape_0");
    assert!(shape.schema().metadata().as_arc().is_none());
    assert!(shape.schema().fields()[0].metadata().as_arc().is_none());
    assert!(tracker.current() > 0);
    assert_eq!(
        tracker.current(),
        minimal_tracker.current(),
        "selected schema cannot retain another schema, type namespace, index, emission scratch or unrelated main allowance"
    );
    let before = tracker.current();
    drop(selected);
    assert!(
        tracker.current() < before,
        "facts/attribute backing exits independently of the shared selected schema"
    );
    assert_eq!(
        shape.schema().fields()[0].data_type(),
        &arrow::datatypes::DataType::Int64
    );
    drop(shape);
    assert_eq!(tracker.current(), 0);
    drop(minimal);
    assert_eq!(minimal_tracker.current(), 0);
}

#[test]
fn candidate_source_field_fact_and_default_do_not_pin_schema_or_namespace() {
    let (minimal, minimal_tracker) = selected_schema_after_exit(0, false);
    let (selected, tracker) = selected_schema_after_exit(512, true);
    let field = selected.field_facts()[0].clone();
    let plain_field = minimal.field_facts()[0].clone();
    let default = field.initial_default().unwrap().clone();
    let plain_default = plain_field.initial_default().unwrap().clone();
    let before = tracker.current();
    drop(selected);
    drop(minimal);
    assert!(tracker.current() < before);
    assert_eq!(tracker.current(), minimal_tracker.current());
    assert_eq!(field.field_path(), &[0]);
    assert_eq!(field.field_id(), Some(70));
    assert_eq!(
        field.attributes().get("provider.field").map(String::as_str),
        Some("source original")
    );
    let before = tracker.current();
    drop(field);
    drop(plain_field);
    assert!(
        tracker.current() < before,
        "path/field attributes and FieldFacts holder exit before the standalone default"
    );
    assert_eq!(tracker.current(), minimal_tracker.current());
    assert_eq!(default.as_json(), "7");
    drop(default);
    drop(plain_default);
    assert_eq!(tracker.current(), 0);
    assert_eq!(minimal_tracker.current(), 0);
}

#[test]
fn candidate_schema_original_namespace_gate_precedes_first_real_scope_refusal() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Control(AtomicUsize);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let (bytes, _) = authored_schema_package(true);
    let control = Control(AtomicUsize::new(0));
    let tracker = MemTracker::new_root("schema first real scope refusal");
    let actual = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let model = FragmentDecodeResourceModel::try_new(&control).unwrap();
    let prepared = prepare_package_wire_in(
        &bytes,
        &model,
        decode_limits().wire,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let usage = prepared.projection().usage;
    actual
        .request_opaque_upper_bound(usage.peak_requested_heap_bytes_upper)
        .unwrap();
    let package = prepared.materialize_in(&mut |_| Ok(()), &mut work).unwrap();
    let source = usage
        .input_bytes
        .checked_add(usage.root_inline_bytes)
        .unwrap()
        .checked_add(usage.peak_requested_heap_bytes_upper)
        .unwrap();
    let factory = NativeTypeFactory::new(&actual);
    let mut exact = |layout| actual.request_layout(layout);
    let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
    let mut creator = ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
    let types = decode_package_type_table_with_factory_in(
        &package,
        source,
        decode_limits().types,
        &mut creator,
        &factory,
        &mut work,
    )
    .unwrap();
    let source = source
        .checked_add(types.creation_request_bytes_upper_bound().unwrap())
        .unwrap()
        .checked_add(std::mem::size_of_val(&types))
        .unwrap();
    let before = tracker.current();
    let allocated = tracker.allocated();
    let deallocated = tracker.deallocated();
    tracker.install_limit_once(before).unwrap();
    let mut limits = decode_limits().node;
    limits.max_list_items = 0;
    let result = decode_schemas_with_factory_in(
        &package.schemas,
        &types,
        source,
        limits,
        &factory,
        &mut |_| Ok(()),
        &mut work,
    );
    assert!(matches!(
        result,
        Err(ProjectionFailure::Codec(SchemaCodecError::Control(
            CompileControlError::ResourceExhausted
        )))
    ));
    assert_eq!(tracker.current(), before);
    assert_eq!(
        tracker.allocated(),
        allocated,
        "original header/whole-namespace cap refusal makes no scope request"
    );
    assert_eq!(tracker.deallocated(), deallocated);
    let result = decode_schemas_with_factory_in(
        &package.schemas,
        &types,
        source,
        decode_limits().node,
        &factory,
        &mut |_| Ok(()),
        &mut work,
    );
    assert!(
        matches!(result,Err(ProjectionFailure::Host(NativeTypeConstructionError::Capacity(ref error))) if error.detail().contains("schema first real scope refusal"))
    );
    let stopped = control.0.load(Ordering::SeqCst);
    drop(result);
    assert_eq!(
        control.0.load(Ordering::SeqCst),
        stopped,
        "returned first typed H has no finish/control tail"
    );
    let header = crate::native_type_construction::NativeTypeScope::reservation_allocation_layout()
        .unwrap()
        .size() as i64;
    assert_eq!(tracker.allocated() - allocated, header);
    assert_eq!(tracker.deallocated() - deallocated, header);
    assert_eq!(tracker.current(), before);
    drop(types);
    drop(creator);
    {
        let _factory = factory;
    }
    drop(actual);
    assert_eq!(tracker.current(), 0);
}
