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

fn namespace_limits() -> ConstantNamespaceProjectionLimits {
    ConstantNamespaceProjectionLimits {
        max_records: 4096,
        max_preparation_request_bytes: 8 * 1024 * 1024,
        max_new_allocation_request_bytes: 128 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
        max_cumulative_library_work: 2 * 1024 * 1024 * 1024,
    }
}
fn binding(pool: u32, value_type_id: u32, field_id: u32) -> ConstantRecordTypeIds {
    ConstantRecordTypeIds {
        pool: ConstantPoolId::new(pool),
        value_type_id,
        field_id,
    }
}
fn alias_pools(original: &ConstantPool) -> novarocks_physical_plan::ConstantPools {
    let mut pools = novarocks_physical_plan::ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(u32::MAX), original.clone())
        .unwrap();
    pools
        .insert(ConstantPoolId::new(0), original.clone())
        .unwrap();
    pools
}

#[test]
fn write_namespace_binds_same_type_emission_and_counts_each_alias_output() {
    let original = pool(Arc::new(Float32Array::from(vec![
        Some(f32::from_bits(0x7fc0_1234)),
        None,
        Some(-0.0),
    ])));
    let pools = alias_pools(&original);
    let roots = [(0, original.value_type().clone())];
    let fields = [(u32::MAX, Arc::clone(original.field_ref()))];
    let value_refs = roots
        .iter()
        .map(|(id, value)| (*id, value))
        .collect::<Vec<_>>();
    let field_refs = fields
        .iter()
        .map(|(id, field)| (*id, field))
        .collect::<Vec<_>>();
    let table = encoded_fixture_types(&value_refs, &field_refs);
    let bindings = [binding(0, 0, u32::MAX), binding(u32::MAX, 0, u32::MAX)];
    let control = Control::good(CompilePhase::Encode);
    let prepared = prepare_constant_namespace_write(
        &pools,
        &bindings,
        &table,
        SOURCE,
        write_limits(),
        namespace_limits(),
        &control,
    )
    .unwrap();
    let facts = *prepared.facts();
    assert_eq!(facts.source_retained_bytes, SOURCE);
    assert_eq!(facts.record_count, 2);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + facts.new_allocation_request_bytes_upper_bound
    );
    assert!(facts.binding_work_upper_bound > 0);
    let records = prepared.emit().unwrap();
    assert_eq!(
        records.iter().map(|value| value.id).collect::<Vec<_>>(),
        [0, u32::MAX]
    );
    assert_eq!(records[0].arrow_ipc, records[1].arrow_ipc);
    assert_ne!(records[0].arrow_ipc.as_ptr(), records[1].arrow_ipc.as_ptr());
    let decoded_types = decode_fixture_types(&table, &fields);
    let decoded = decode_constant_namespace(
        &records,
        &decoded_types,
        SOURCE,
        policy(),
        decode_limits(),
        namespace_limits(),
        &verifier(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    assert_ne!(
        decoded.entries()[&ConstantPoolId::new(0)].backing_identity(),
        decoded.entries()[&ConstantPoolId::new(u32::MAX)].backing_identity()
    );
    for result in decoded.entries().values() {
        for ordinal in 0..3 {
            assert!(
                original
                    .value(ordinal)
                    .unwrap()
                    .equals_observed(
                        &result.value(ordinal).unwrap(),
                        CompilePhase::Decode,
                        &Control::good(CompilePhase::Decode)
                    )
                    .unwrap()
            );
        }
    }
}

#[test]
fn write_namespace_mixed_full_fields_and_equal_distinct_arcs_roundtrip() {
    let flat = pool(Arc::new(StringArray::from(vec![Some("exact"), None])));
    let nested = pool(nested_arrays().remove(0));
    let mut pools = novarocks_physical_plan::ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(0), nested.clone())
        .unwrap();
    pools
        .insert(ConstantPoolId::new(u32::MAX), flat.clone())
        .unwrap();
    let roots = [
        (u32::MAX, nested.value_type().clone()),
        (0, flat.value_type().clone()),
    ];
    // An equal distinct Arc is legal. Full facts, rather than pointer identity,
    // bind this authored table source to the original checked pool.
    let fields = [
        (0, Arc::new(nested.field().clone())),
        (u32::MAX, Arc::clone(flat.field_ref())),
    ];
    let value_refs = roots
        .iter()
        .map(|(id, value)| (*id, value))
        .collect::<Vec<_>>();
    let field_refs = fields
        .iter()
        .map(|(id, field)| (*id, field))
        .collect::<Vec<_>>();
    let table = encoded_fixture_types(&value_refs, &field_refs);
    let bindings = [binding(0, u32::MAX, 0), binding(u32::MAX, 0, u32::MAX)];
    let records = encode_constant_namespace(
        &pools,
        &bindings,
        &table,
        SOURCE,
        write_limits(),
        namespace_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let decoded_types = decode_fixture_types(&table, &fields);
    for (record, original) in records.iter().zip([&nested, &flat]) {
        let (_, result) = decode(record, &decoded_types);
        for ordinal in 0..original.array().len() as u32 {
            assert!(
                original
                    .value(ordinal)
                    .unwrap()
                    .equals_observed(
                        &result.value(ordinal).unwrap(),
                        CompilePhase::Decode,
                        &Control::good(CompilePhase::Decode)
                    )
                    .unwrap()
            );
        }
    }
}

#[test]
fn write_namespace_rejects_wrong_full_types_fields_ids_count_and_order() {
    let original = pool(Arc::new(Int64Array::from(vec![7])));
    let pools = alias_pools(&original);
    let roots = [
        (0, original.value_type().clone()),
        (1, FunctionValueType::new(DataType::Int64, false)),
        (2, FunctionValueType::new(DataType::Utf8, true)),
    ];
    let fields = [
        (0, Arc::clone(original.field_ref())),
        (
            1,
            Arc::new(
                original
                    .field()
                    .clone()
                    .with_metadata(HashMap::from([("provider".into(), "different".into())])),
            ),
        ),
    ];
    let value_refs = roots
        .iter()
        .map(|(id, value)| (*id, value))
        .collect::<Vec<_>>();
    let field_refs = fields
        .iter()
        .map(|(id, field)| (*id, field))
        .collect::<Vec<_>>();
    let table = encoded_fixture_types(&value_refs, &field_refs);
    for bindings in [
        vec![],
        vec![binding(0, 0, 0)],
        vec![binding(u32::MAX, 0, 0), binding(0, 0, 0)],
        vec![binding(0, 99, 0), binding(u32::MAX, 0, 0)],
        vec![binding(0, 0, 99), binding(u32::MAX, 0, 0)],
        vec![binding(0, 1, 0), binding(u32::MAX, 0, 0)],
        vec![binding(0, 2, 0), binding(u32::MAX, 0, 0)],
        vec![binding(0, 0, 1), binding(u32::MAX, 0, 0)],
    ] {
        every_prefix(
            CompilePhase::Encode,
            |control| {
                encode_constant_namespace(
                    &pools,
                    &bindings,
                    &table,
                    SOURCE,
                    write_limits(),
                    namespace_limits(),
                    control,
                )
            },
            false,
            false,
        );
    }
}

#[test]
fn write_namespace_original_invoice_cannot_be_padded_with_prepared_storage() {
    let original = pool(Arc::new(Int64Array::from(vec![7i64; 4096])));
    let mut pools = novarocks_physical_plan::ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(0), original.clone())
        .unwrap();
    let roots = [(0, original.value_type().clone())];
    let fields = [(0, Arc::clone(original.field_ref()))];
    let value_refs = roots
        .iter()
        .map(|(id, value)| (*id, value))
        .collect::<Vec<_>>();
    let field_refs = fields
        .iter()
        .map(|(id, field)| (*id, field))
        .collect::<Vec<_>>();
    let table = encoded_fixture_types(&value_refs, &field_refs);
    let bindings = [binding(0, 0, 0)];
    let insufficient = original.resource_facts().retained_buffer_capacity_bytes as usize - 1;
    let result = encode_constant_namespace(
        &pools,
        &bindings,
        &table,
        insufficient,
        write_limits(),
        namespace_limits(),
        &Control::good(CompilePhase::Encode),
    );
    assert!(matches!(
        result,
        Err(PhysicalConstantCodecError::InvalidShape(
            "constant namespace writer source invoice excludes original pool backing"
        ))
    ));
    every_prefix(
        CompilePhase::Encode,
        |control| {
            encode_constant_namespace(
                &pools,
                &bindings,
                &table,
                insufficient,
                write_limits(),
                namespace_limits(),
                control,
            )
        },
        false,
        false,
    );
    assert!(
        encode_constant_namespace(
            &pools,
            &bindings,
            &table,
            SOURCE,
            write_limits(),
            namespace_limits(),
            &Control::good(CompilePhase::Encode)
        )
        .is_ok()
    );
}

#[test]
fn write_namespace_near_over_requests_work_and_all_control_prefixes() {
    let original = pool(Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)])));
    let pools = alias_pools(&original);
    let roots = [(0, original.value_type().clone())];
    let fields = [(0, Arc::clone(original.field_ref()))];
    let value_refs = roots
        .iter()
        .map(|(id, value)| (*id, value))
        .collect::<Vec<_>>();
    let field_refs = fields
        .iter()
        .map(|(id, field)| (*id, field))
        .collect::<Vec<_>>();
    let table = encoded_fixture_types(&value_refs, &field_refs);
    let bindings = [binding(0, 0, 0), binding(u32::MAX, 0, 0)];
    let control = Control::good(CompilePhase::Encode);
    let facts = *prepare_constant_namespace_write(
        &pools,
        &bindings,
        &table,
        SOURCE,
        write_limits(),
        namespace_limits(),
        &control,
    )
    .unwrap()
    .facts();
    for (index, bound) in [
        facts.prepared_storage_request_bytes,
        facts.new_allocation_request_bytes_upper_bound,
        facts.coexisting_source_and_request_bytes_upper_bound,
        facts.cumulative_library_work_upper_bound,
    ]
    .into_iter()
    .enumerate()
    {
        let set = |limits: &mut ConstantNamespaceProjectionLimits, value| match index {
            0 => limits.max_preparation_request_bytes = value,
            1 => limits.max_new_allocation_request_bytes = value,
            2 => limits.max_coexisting_source_and_request_bytes = value,
            3 => limits.max_cumulative_library_work = value,
            _ => unreachable!(),
        };
        let mut limits = namespace_limits();
        set(&mut limits, bound);
        assert!(
            encode_constant_namespace(
                &pools,
                &bindings,
                &table,
                SOURCE,
                write_limits(),
                limits,
                &control
            )
            .is_ok(),
            "exact {index}"
        );
        set(&mut limits, bound - 1);
        assert!(
            encode_constant_namespace(
                &pools,
                &bindings,
                &table,
                SOURCE,
                write_limits(),
                limits,
                &control
            )
            .is_err(),
            "over {index}"
        );
    }
    every_prefix(
        CompilePhase::Encode,
        |control| {
            prepare_constant_namespace_write(
                &pools,
                &bindings,
                &table,
                SOURCE,
                write_limits(),
                namespace_limits(),
                control,
            )
            .map(|value| *value.facts())
        },
        true,
        false,
    );
    every_prefix(
        CompilePhase::Encode,
        |control| {
            encode_constant_namespace(
                &pools,
                &bindings,
                &table,
                SOURCE,
                write_limits(),
                namespace_limits(),
                control,
            )
        },
        true,
        false,
    );
}

#[test]
fn input_derived_namespace_materializes_records_past_old_volume_limits() {
    use novarocks_constant_contract::ConstantValidation;
    use novarocks_type_contract::{
        ValueSchemaCreation, ValueTypeStorageLifetime,
        owned_resources::metadata_materialization::MaterializedFieldNamespace,
    };

    const LARGE_BODY: usize = 17 * 1024 * 1024;
    const FLAT_ROWS: usize = 4097;
    const LIST_ITEMS: i32 = 65537;
    let payload = vec![0x47; LARGE_BODY];
    let mut binary = BinaryBuilder::with_capacity(FLAT_ROWS, LARGE_BODY);
    binary.append_value(&payload);
    for _ in 1..FLAT_ROWS {
        binary.append_null();
    }
    let item_field = Arc::new(Field::new("item", DataType::Int32, true));
    let list = ListArray::new(
        item_field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, LIST_ITEMS, LIST_ITEMS])),
        Arc::new(Int32Array::from_iter_values(0..LIST_ITEMS)),
        Some(NullBuffer::from(vec![true, false])),
    );
    let encode_control = Control::good(CompilePhase::Encode);
    let make_pool = |array: &dyn Array| {
        let field = Arc::new(Field::new("original", array.data_type().clone(), true));
        let value_type = FunctionValueType::new(array.data_type().clone(), true);
        ConstantPool::try_new(
            field,
            value_type,
            array.to_data(),
            ConstantValidation::INPUT_DERIVED,
            CompilePhase::Encode,
            &encode_control,
        )
        .unwrap()
    };
    let flat = make_pool(&binary.finish());
    let recursive = make_pool(&list);
    let fields = [
        (0, Arc::clone(flat.field_ref())),
        (1, Arc::clone(recursive.field_ref())),
    ];
    let fields_ref = [(0, &fields[0].1), (1, &fields[1].1)];
    let values = [(0, flat.value_type()), (1, recursive.value_type())];
    let table = encoded_fixture_types(&values, &fields_ref);
    let mut pools = novarocks_physical_plan::ConstantPools::empty();
    pools.insert(ConstantPoolId::new(0), flat.clone()).unwrap();
    pools
        .insert(ConstantPoolId::new(1), recursive.clone())
        .unwrap();

    let mut work = CompileCheckpoints::try_new(&encode_control, CompilePhase::Encode).unwrap();
    let mut layouts = Vec::new();
    let records = encode_constant_namespace_input_derived_in(
        &pools,
        &[binding(0, 0, 0), binding(1, 1, 1)],
        &table,
        &mut |request| {
            if let ConstantWriteRequest::Layout(layout) = request {
                layouts.push(layout);
            }
            Ok::<_, std::convert::Infallible>(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(records[0].id, 0);
    assert!(records[0].arrow_ipc.len() > LARGE_BODY);
    assert!(
        layouts
            .iter()
            .any(|layout| layout.size() == records[0].arrow_ipc.capacity())
    );

    let decoded_types = decode_fixture_types(&table, &fields);
    let decode_control = Control::good(CompilePhase::Decode);
    let mut work = CompileCheckpoints::try_new(&decode_control, CompilePhase::Decode).unwrap();
    let mut exact = |_: std::alloc::Layout| Ok::<_, std::convert::Infallible>(());
    let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let metadata = MaterializedFieldNamespace::from_original_loans(Arc::from([]));
    let prepared = prepare_constant_namespace_input_derived_with_creator_in(
        &records,
        &decoded_types,
        Some(&metadata),
        &mut creation,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let bodies = prepared.checked_record_body_bytes().collect::<Vec<_>>();
    assert_eq!(bodies[0].0, ConstantPoolId::new(0));
    assert!(bodies[0].1 >= LARGE_BODY && bodies[0].1 < records[0].arrow_ipc.len());
    assert_eq!(bodies[1].0, ConstantPoolId::new(1));
    assert!(bodies[1].1 >= LIST_ITEMS as usize * 4);
    let decoded = prepared
        .materialize_with_creator_in(&mut creation, &mut |_| Ok(()), &mut work)
        .unwrap();
    work.finish().unwrap();

    let flat = &decoded.entries()[&ConstantPoolId::new(0)];
    assert!(Arc::ptr_eq(
        flat.field_ref(),
        decoded_types.source_field(0).unwrap()
    ));
    let binary = flat.array().as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(binary.len(), FLAT_ROWS);
    assert_eq!(binary.null_count(), FLAT_ROWS - 1);
    assert_eq!(binary.value(0), payload.as_slice());
    let recursive = &decoded.entries()[&ConstantPoolId::new(1)];
    let list = recursive
        .array()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_length(0), LIST_ITEMS);
    assert!(list.is_null(1));
    let items = list.values().as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(items.value(0), 0);
    assert_eq!(items.value(LIST_ITEMS as usize - 1), LIST_ITEMS - 1);
}

#[test]
fn input_derived_namespace_writer_preserves_first_stream_layout_refusal() {
    use novarocks_constant_contract::ConstantValidation;

    let control = Control::good(CompilePhase::Encode);
    let array = BinaryArray::from(vec![Some(&vec![5; 1024 * 1024][..])]);
    let field = Arc::new(
        Field::new("data", DataType::Binary, true)
            .with_metadata(HashMap::from([("fixture".into(), "owned source".into())])),
    );
    let value_type = FunctionValueType::new(DataType::Binary, true);
    let original = ConstantPool::try_new(
        Arc::clone(&field),
        value_type.clone(),
        array.to_data(),
        ConstantValidation::INPUT_DERIVED,
        CompilePhase::Encode,
        &control,
    )
    .unwrap();
    let values = [(0, &value_type)];
    let fields = [(0, &field)];
    let table = encoded_fixture_types(&values, &fields);
    let mut pools = novarocks_physical_plan::ConstantPools::empty();
    pools.insert(ConstantPoolId::new(0), original).unwrap();
    let refusal = Arc::new("original allocation refusal");
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let mut refused = false;
    let result = encode_constant_namespace_input_derived_in(
        &pools,
        &[binding(0, 0, 0)],
        &table,
        &mut |request| {
            assert!(!refused, "creator called after its first refusal");
            if matches!(request, ConstantWriteRequest::Layout(layout) if layout.size() > 1024 * 1024)
            {
                refused = true;
                Err(Arc::clone(&refusal))
            } else {
                Ok(())
            }
        },
        &mut work,
    );
    assert!(refused);
    assert!(matches!(result, Err(ProjectionFailure::Host(cause)) if Arc::ptr_eq(&cause, &refusal)));
}

#[test]
fn input_derived_namespace_factory_retains_only_live_array_aliases() {
    use novarocks_type_contract::{
        ValueSchemaCreationFactory, ValueSchemaCreationScope, ValueSchemaScopeBeginFailure,
        ValueTypeStorageHold, ValueTypeStorageLifetime,
        owned_resources::{
            layout::arc_layout, metadata_materialization::MaterializedFieldNamespace,
        },
    };
    use std::{
        alloc::Layout,
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct Hold {
        live: Arc<AtomicUsize>,
        bytes: AtomicUsize,
    }
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.live
                .fetch_sub(self.bytes.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }
    struct Scope(Arc<Hold>);
    impl ValueSchemaCreationScope<Infallible> for Scope {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            ValueTypeStorageLifetime::held(self.0.clone())
        }
        fn request_layout(&mut self, layout: Layout) -> Result<(), Infallible> {
            self.observe_opaque_upper_bound(layout.size())
        }
        fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), Infallible> {
            self.0.bytes.fetch_add(bytes, Ordering::Relaxed);
            self.0.live.fetch_add(bytes, Ordering::Relaxed);
            Ok(())
        }
    }
    struct Factory(Arc<AtomicUsize>);
    impl ValueSchemaCreationFactory<Infallible> for Factory {
        type Scope<'a> = Scope;

        fn begin_scope(&self) -> Result<Scope, Infallible> {
            let bytes = arc_layout(Layout::new::<Hold>()).unwrap().size();
            self.0.fetch_add(bytes, Ordering::Relaxed);
            Ok(Scope(Arc::new(Hold {
                live: self.0.clone(),
                bytes: AtomicUsize::new(bytes),
            })))
        }
        fn begin_scope_observed<O>(
            &self,
            observe: &mut dyn FnMut(Layout) -> Result<(), O>,
        ) -> Result<Scope, ValueSchemaScopeBeginFailure<O, Infallible>> {
            observe(arc_layout(Layout::new::<Hold>()).unwrap())
                .map_err(ValueSchemaScopeBeginFailure::Observer)?;
            self.begin_scope()
                .map_err(ValueSchemaScopeBeginFailure::Creator)
        }
    }

    let array = Int32Array::from(vec![Some(42), None]);
    let field = Arc::new(Field::new("original", DataType::Int32, true));
    let value_type = FunctionValueType::new(DataType::Int32, true);
    let pool = pool_with(Arc::new(array), field.clone(), value_type.clone());
    let values = [(0, &value_type)];
    let fields = [(0, &field)];
    let table = encoded_fixture_types(&values, &fields);
    let types = decode_fixture_types(&table, &[(0, field.clone())]);
    let records = [encode(&pool, 0, 0, 0)];
    let factory = Factory(Arc::new(AtomicUsize::new(0)));
    let control = Control::good(CompilePhase::Decode);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let metadata = MaterializedFieldNamespace::from_original_loans(Arc::from([]));
    let prepared = prepare_constant_namespace_input_derived_with_factory_in(
        &records,
        &types,
        Some(&metadata),
        &factory,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let (pools, _) = prepared
        .materialize_with_factory_in(&factory, &mut |_| Ok(()), &mut work)
        .unwrap();
    work.finish().unwrap();

    let pool = &pools.entries()[&ConstantPoolId::new(0)];
    assert!(Arc::ptr_eq(
        pool.field_ref(),
        types.source_field(0).unwrap()
    ));
    let projection = pool.array_projection();
    let alias = projection.clone();
    let before = factory.0.load(Ordering::Relaxed);
    drop(pools);
    drop(types);
    let remaining = factory.0.load(Ordering::Relaxed);
    assert!(remaining > 0 && remaining < before);
    drop(projection);
    assert_eq!(factory.0.load(Ordering::Relaxed), remaining);
    let array = alias.array().as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(array.value(0), 42);
    assert!(array.is_null(1));
    drop(alias);
    assert_eq!(factory.0.load(Ordering::Relaxed), 0);
}
