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

//! Caller-owned construction for the same receiving Schema namespace body.
use super::*;

/// The token seals the original creator lifetime. Emission consumes that same
/// prepared token; no second creator can retag its structural responsibility.
pub fn prepare_schemas_decode_with_creator_in<'loan, 'control, H>(
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    original_source_bytes: usize,
    limits: NodeProjectionLimits,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedSchemasDecode<'loan, 'control>, ProjectionFailure<Error, H>> {
    prepare_schemas_decode_core(
        definitions,
        types,
        original_source_bytes,
        limits,
        &mut |_| Ok(()),
        Some(creation),
        work,
    )
}

/// All identities, lengths and format bytes have passed the original numerical
/// and shape preflight before this author runs. Observe each known construction
/// request once, before the first allocation. HashMap's locked private backing
/// and Vec-to-Box shrink remain separate operation upper bounds, never Layout
/// invoices or claims that the opaque operation necessarily allocates.
pub(super) fn observe_schema_construction<H>(
    definitions: &[wire::SchemaDefinition],
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProjectionFailure<Error, H>> {
    fn vector<T, H>(
        count: usize,
        creation: &mut ValueSchemaCreation<'_, H>,
    ) -> Result<(), ProjectionFailure<Error, H>> {
        let layout =
            Layout::array::<T>(count).map_err(|_| CompileControlError::ResourceExhausted)?;
        if layout.size() != 0 {
            creation
                .request_layout(layout)
                .map_err(ProjectionFailure::Host)?;
        }
        Ok(())
    }
    work.flush()?;
    vector::<usize, H>(definitions.len(), creation)?;
    vector::<(u32, MaterializedSchema), H>(definitions.len(), creation)?;
    for source in definitions {
        vector::<Arc<Field>, H>(source.field_ids.len(), creation)?;
        creation
            .request_layout(fields_layout(source.field_ids.len())?)
            .map_err(ProjectionFailure::Host)?;
        work.step()?;
    }
    let boxed_upper = bytes::<(u32, MaterializedSchema)>(definitions.len())?;
    if boxed_upper != 0 {
        creation
            .observe_opaque_upper_bound(boxed_upper)
            .map_err(ProjectionFailure::Host)?;
    }
    work.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
    use crate::physical_public_source_schema_v2::{
        decode_public_source_schemas_with_creator_in, encoded_public_source_schemas_backing_bytes,
    };
    use crate::physical_type_v2::{
        PackageTypeProjectionLimits, decode_package_type_table_with_creator_in,
    };
    use novarocks_proto_models::arrow_type::ArrowFieldMetadataEntry;
    use novarocks_type_contract::ValueTypeStorageHold;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct Control(AtomicBool);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            assert!(
                !self.0.load(Ordering::SeqCst),
                "callback after original host refusal"
            );
            Ok(())
        }
    }
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn limits() -> NodeProjectionLimits {
        NodeProjectionLimits {
            max_input_nodes: 4096,
            max_value_references: 4096,
            max_list_items: 16384,
            max_allocation_requests: 16384,
            max_allocation_request_bytes: 1024 * 1024,
            max_coexisting_source_and_request_bytes: 4 * 1024 * 1024,
            max_work: 16 * 1024 * 1024,
            properties: PhysicalPropertyProjectionLimits {
                max_value_references: 4096,
                max_allocation_requests: 16384,
                max_allocation_request_bytes: 1024 * 1024,
                max_coexisting_source_and_request_bytes: 4 * 1024 * 1024,
                max_work: 16 * 1024 * 1024,
            },
        }
    }
    fn sources(
        control: &Control,
        source_storage: ValueTypeStorageLifetime,
    ) -> (DecodedTypeTable, Vec<wire::SchemaDefinition>, usize) {
        use novarocks_proto_models::{arrow_type::ArrowPrimitiveType, physical_type_v2 as t};
        let package = wire::FragmentPackage {
            types: Some(t::TypeTable {
                carriers: vec![t::CarrierTypeDefinition {
                    id: 0,
                    kind: Some(t::carrier_type_definition::Kind::Primitive(
                        ArrowPrimitiveType::Utf8 as i32,
                    )),
                }],
                fields: vec![t::FieldDefinition {
                    id: u32::MAX,
                    name: "provider.source".into(),
                    nullable: true,
                    carrier_type_id: Some(0),
                    logical_type_id: None,
                    value_binding_id: None,
                }],
                value_types: vec![],
                logical_types: vec![],
            }),
            source_fields: Some(t::SourceFieldTable {
                shape: Some(t::TypeTable {
                    carriers: vec![t::CarrierTypeDefinition {
                        id: 0,
                        kind: Some(t::carrier_type_definition::Kind::Primitive(
                            ArrowPrimitiveType::Utf8 as i32,
                        )),
                    }],
                    fields: vec![t::FieldDefinition {
                        id: u32::MAX,
                        name: "provider.source".into(),
                        nullable: true,
                        carrier_type_id: Some(0),
                        logical_type_id: None,
                        value_binding_id: None,
                    }],
                    value_types: vec![],
                    logical_types: vec![],
                }),
                field_facts: vec![t::SourceArrowFieldFacts {
                    field_id: Some(u32::MAX),
                    metadata: vec![ArrowFieldMetadataEntry {
                        key: "provider.attribute".into(),
                        value: "original value".into(),
                    }],
                    dictionary_id: None,
                    dictionary_is_ordered: None,
                }],
            }),
            ..Default::default()
        };
        let source = package.source_fields.as_ref().unwrap();
        let table = source.shape.as_ref().unwrap();
        // This fresh source author reports actual public Vec/String capacities,
        // never a decoded floor or invented allocator geometry.
        let execution = package.types.as_ref().unwrap();
        let original_source_bytes = std::mem::size_of_val(&package)
            + execution.carriers.capacity() * std::mem::size_of::<t::CarrierTypeDefinition>()
            + execution.fields.capacity() * std::mem::size_of::<t::FieldDefinition>()
            + execution
                .fields
                .iter()
                .map(|field| field.name.capacity())
                .sum::<usize>()
            + table.carriers.capacity() * std::mem::size_of::<t::CarrierTypeDefinition>()
            + table.fields.capacity() * std::mem::size_of::<t::FieldDefinition>()
            + table
                .fields
                .iter()
                .map(|field| field.name.capacity())
                .sum::<usize>()
            + source.field_facts.capacity() * std::mem::size_of::<t::SourceArrowFieldFacts>()
            + source
                .field_facts
                .iter()
                .map(|facts| {
                    facts.metadata.capacity() * std::mem::size_of::<ArrowFieldMetadataEntry>()
                        + facts
                            .metadata
                            .iter()
                            .map(|entry| entry.key.capacity() + entry.value.capacity())
                            .sum::<usize>()
                })
                .sum::<usize>();
        let mut exact = |_: Layout| Ok::<(), ()>(());
        let mut opaque = |_: usize| Ok::<(), ()>(());
        let mut creation = ValueSchemaCreation::new(source_storage, &mut exact, &mut opaque);
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode).unwrap();
        let decoded = decode_package_type_table_with_creator_in(
            &package,
            original_source_bytes,
            PackageTypeProjectionLimits {
                max_definitions: 32,
                max_expanded_nodes: 256,
                max_string_bytes: 4096,
                max_allocation_requests: 10000,
                max_allocation_request_bytes: 1024 * 1024,
                max_coexisting_source_and_request_bytes: 2 * 1024 * 1024,
                max_work: 4 * 1024 * 1024,
            },
            &mut creation,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        let definitions = vec![
            wire::SchemaDefinition {
                id: u32::MAX,
                field_ids: vec![u32::MAX, u32::MAX],
            },
            // A distinct, genuine zero-column source shape authors root attributes.
            wire::SchemaDefinition {
                id: 0,
                field_ids: vec![],
            },
        ];
        let definition_bytes = std::mem::size_of_val(definitions.as_slice())
            + definitions
                .iter()
                .map(|definition| definition.field_ids.capacity() * std::mem::size_of::<u32>())
                .sum::<usize>();
        let invoice = definition_bytes
            + original_source_bytes
            + decoded.creation_request_bytes_upper_bound().unwrap()
            + std::mem::size_of_val(&decoded);
        (decoded, definitions, invoice)
    }

    #[test]
    fn independent_schema_owner_keeps_original_fields_root_provenance_and_creation_until_last_exit()
    {
        let control = Control(AtomicBool::new(false));
        let source_dropped = Arc::new(AtomicUsize::new(0));
        let source_storage = ValueTypeStorageLifetime::held(Arc::new(Hold(source_dropped.clone())));
        let (types, definitions, invoice) = sources(&control, source_storage);
        let field_weak = Arc::downgrade(types.field(u32::MAX).unwrap());
        let original_source_field = types.source_field(u32::MAX).unwrap().clone();
        let original_source_storage = types
            .source_field_borrow(u32::MAX)
            .unwrap()
            .storage_lifetime();
        let source_field_weak = Arc::downgrade(&original_source_field);
        let namespace = types.source_metadata_namespace().unwrap();
        let loan = namespace
            .fields()
            .iter()
            .find(|loan| loan.lends(&original_source_field))
            .unwrap();
        assert!(loan.original_backing_observed::<novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError>(
            &original_source_field, &mut || Ok(())
        ).unwrap().unwrap() > 0);
        let dropped = Arc::new(AtomicUsize::new(0));
        let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(dropped.clone())));
        let requests = AtomicUsize::new(0);
        let mut exact = |layout: Layout| {
            requests.fetch_add(layout.size(), Ordering::SeqCst);
            Ok::<(), ()>(())
        };
        let mut opaque = |bytes: usize| {
            requests.fetch_add(bytes, Ordering::SeqCst);
            Ok::<(), ()>(())
        };
        let mut creation = ValueSchemaCreation::new(storage, &mut exact, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let prepared = prepare_schemas_decode_with_creator_in(
            &definitions,
            &types,
            invoice,
            limits(),
            &mut creation,
            &mut work,
        )
        .unwrap();
        let decoded = prepared
            .emit_observed_in(&mut |_| Ok(()), &mut work)
            .unwrap();
        let paired = decoded
            .materialized_schema_observed(u32::MAX, &mut work)
            .unwrap()
            .unwrap();
        assert!(paired.schema().metadata().as_arc().is_none());
        assert!(
            paired
                .metadata_origin()
                .table_request()
                .unwrap()
                .layout
                .is_none()
        );
        let source_definitions = vec![wire::PublicSourceSchemaFactsDefinition {
            id: Some(u32::MAX),
            source_shape_schema_id: Some(0),
            schema_attributes: vec![ArrowFieldMetadataEntry {
                key: "source.schema.attribute".into(),
                value: "original annotation".into(),
            }],
            field_facts: vec![],
        }];
        let source_invoice = encoded_public_source_schemas_backing_bytes(&source_definitions)
            .unwrap()
            + invoice
            + requests.load(Ordering::SeqCst)
            + std::mem::size_of_val(&decoded);
        let source_facts = decode_public_source_schemas_with_creator_in(
            &source_definitions,
            &decoded,
            source_invoice,
            limits(),
            &mut creation,
            &mut work,
        )
        .unwrap();
        assert_eq!(
            source_facts
                .lookup_observed(u32::MAX, &mut work)
                .unwrap()
                .unwrap()
                .schema_attributes()["source.schema.attribute"],
            "original annotation"
        );
        let owned = decoded.into_owned_definitions();
        work.finish().unwrap();
        drop((creation, types, definitions));
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert_eq!(source_dropped.load(Ordering::SeqCst), 0);
        let paired = &owned.definitions()[0].1;
        let fields = paired.schema().fields();
        assert!(Arc::ptr_eq(&fields[0], &fields[1]));
        assert!(fields[0].metadata().as_arc().is_none());
        assert_eq!(
            original_source_field.metadata()["provider.attribute"],
            "original value"
        );
        assert!(field_weak.upgrade().is_some());
        drop(owned);
        assert!(field_weak.upgrade().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert_eq!(source_dropped.load(Ordering::SeqCst), 0);
        drop(source_facts);
        assert_eq!(source_dropped.load(Ordering::SeqCst), 0);
        assert!(source_field_weak.upgrade().is_some());
        drop((original_source_field, original_source_storage));
        assert!(source_field_weak.upgrade().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(source_dropped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn every_schema_exact_and_opaque_refusal_preserves_original_host_without_tail() {
        let control = Control(AtomicBool::new(false));
        let (types, definitions, invoice) = sources(&control, ValueTypeStorageLifetime::pure());
        let observed = Mutex::new(Vec::new());
        let mut exact = |layout: Layout| {
            observed.lock().unwrap().push((true, layout.size()));
            Ok::<(), ()>(())
        };
        let mut opaque = |bytes: usize| {
            observed.lock().unwrap().push((false, bytes));
            Ok::<(), ()>(())
        };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        prepare_schemas_decode_with_creator_in(
            &definitions,
            &types,
            invoice,
            limits(),
            &mut creation,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        drop(creation);
        let trace = observed.into_inner().unwrap();
        // The shape author only performs the original tuple Vec-to-Box shrink.
        // Root attributes now have their own SourceFacts creator, not a phantom shape map.
        assert_eq!(trace.iter().filter(|(exact, _)| !exact).count(), 1);
        for stop in 0..trace.len() {
            let control = Control(AtomicBool::new(false));
            let failure: Arc<str> = Arc::from("original schema host cause");
            let calls = AtomicUsize::new(0);
            let callback = |_: usize| {
                assert!(!control.0.load(Ordering::SeqCst));
                if calls.fetch_add(1, Ordering::SeqCst) == stop {
                    control.0.store(true, Ordering::SeqCst);
                    Err(failure.clone())
                } else {
                    Ok(())
                }
            };
            let mut exact = |layout: Layout| callback(layout.size());
            let mut opaque = |bytes: usize| callback(bytes);
            let mut creation =
                ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let result = prepare_schemas_decode_with_creator_in(
                &definitions,
                &types,
                invoice,
                limits(),
                &mut creation,
                &mut work,
            );
            let Err(ProjectionFailure::Host(actual)) = result else {
                panic!("schema host cause was reclassified")
            };
            assert!(Arc::ptr_eq(&actual, &failure));
            assert_eq!(calls.load(Ordering::SeqCst), stop + 1);
        }
    }
    #[test]
    fn source_schema_attributes_keep_original_first_host_refusal_after_shape_separation() {
        let control = Control(AtomicBool::new(false));
        let (types, definitions, invoice) = sources(&control, ValueTypeStorageLifetime::pure());
        let mut exact = |_: Layout| Ok::<(), ()>(());
        let mut opaque = |_: usize| Ok::<(), ()>(());
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let schemas = prepare_schemas_decode_with_creator_in(
            &definitions,
            &types,
            invoice,
            limits(),
            &mut creation,
            &mut work,
        )
        .unwrap()
        .emit_observed_in(&mut |_| Ok(()), &mut work)
        .unwrap();
        work.finish().unwrap();
        let source = vec![wire::PublicSourceSchemaFactsDefinition {
            id: Some(u32::MAX),
            source_shape_schema_id: Some(0),
            schema_attributes: vec![ArrowFieldMetadataEntry {
                key: "source.schema.attribute".into(),
                value: "original annotation".into(),
            }],
            field_facts: vec![],
        }];
        let source_invoice = encoded_public_source_schemas_backing_bytes(&source).unwrap()
            + invoice
            + schemas.facts().allocation_request_bytes_upper_bound
            + std::mem::size_of_val(&schemas);
        let trace = Mutex::new(Vec::new());
        let mut exact = |layout: Layout| {
            trace.lock().unwrap().push((true, layout.size()));
            Ok::<(), ()>(())
        };
        let mut opaque = |bytes: usize| {
            trace.lock().unwrap().push((false, bytes));
            Ok::<(), ()>(())
        };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        decode_public_source_schemas_with_creator_in(
            &source,
            &schemas,
            source_invoice,
            limits(),
            &mut creation,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        drop(creation);
        let trace = trace.into_inner().unwrap();
        assert!(
            trace.iter().any(|(exact, _)| !exact),
            "actual source attribute map operation"
        );
        assert!(
            trace.iter().filter(|(exact, _)| *exact).count() >= 2,
            "original key/value string constructors"
        );
        for stop in 0..trace.len() {
            let control = Control(AtomicBool::new(false));
            // A decoded schema lends its original control as well as its fields.
            // Rebuild this real loan on the same caller before installing refusal.
            let (types, definitions, invoice) = sources(&control, ValueTypeStorageLifetime::pure());
            let mut exact = |_: Layout| Ok::<(), ()>(());
            let mut opaque = |_: usize| Ok::<(), ()>(());
            let mut preparation =
                ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
            let mut preparation_work =
                CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let schemas = prepare_schemas_decode_with_creator_in(
                &definitions,
                &types,
                invoice,
                limits(),
                &mut preparation,
                &mut preparation_work,
            )
            .unwrap()
            .emit_observed_in(&mut |_| Ok(()), &mut preparation_work)
            .unwrap();
            preparation_work.finish().unwrap();
            let source_invoice = encoded_public_source_schemas_backing_bytes(&source).unwrap()
                + invoice
                + schemas.facts().allocation_request_bytes_upper_bound
                + std::mem::size_of_val(&schemas);
            let failure: Arc<str> = Arc::from("original source schema host cause");
            let calls = AtomicUsize::new(0);
            let callback = |_: usize| {
                assert!(
                    !control.0.load(Ordering::SeqCst),
                    "callback after original source refusal"
                );
                if calls.fetch_add(1, Ordering::SeqCst) == stop {
                    control.0.store(true, Ordering::SeqCst);
                    Err(failure.clone())
                } else {
                    Ok(())
                }
            };
            let mut exact = |layout: Layout| callback(layout.size());
            let mut opaque = |bytes: usize| callback(bytes);
            let mut creation =
                ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let Err(ProjectionFailure::Host(actual)) = decode_public_source_schemas_with_creator_in(
                &source,
                &schemas,
                source_invoice,
                limits(),
                &mut creation,
                &mut work,
            ) else {
                panic!("original source schema host cause was reclassified")
            };
            assert!(Arc::ptr_eq(&actual, &failure));
            assert_eq!(calls.load(Ordering::SeqCst), stop + 1);
        }
    }
}
