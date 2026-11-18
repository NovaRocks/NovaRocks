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

use novarocks_physical_plan::ConstantReferenceError;

fn namespace_limits() -> ConstantNamespaceProjectionLimits {
    ConstantNamespaceProjectionLimits {
        max_records: 4096,
        max_preparation_request_bytes: 8 * 1024 * 1024,
        max_new_allocation_request_bytes: 128 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
        max_cumulative_library_work: 2 * 1024 * 1024 * 1024,
    }
}
fn prepare<'r, 't, 'c>(
    records: &'r [wire::IpcConstantPool],
    types: &'t DecodedTypeTable,
    limits: ConstantNamespaceProjectionLimits,
    control: &'c Control,
) -> Result<PreparedConstantNamespace<'r, 't, 'c>, PhysicalConstantCodecError> {
    prepare_constant_namespace(
        records,
        types,
        SOURCE,
        policy(),
        decode_limits(),
        limits,
        &verifier(),
        control,
    )
}
fn duplicate_values() -> (ConstantPool, Vec<wire::IpcConstantPool>, DecodedTypeTable) {
    let original = pool(Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)])));
    let types = types(
        Arc::clone(original.field_ref()),
        original.value_type().clone(),
        0,
        u32::MAX,
    );
    let records = vec![
        encode(&original, u32::MAX, 0, u32::MAX),
        encode(&original, 0, 0, u32::MAX),
    ];
    (original, records, types)
}

#[test]
fn namespace_preserves_sparse_ids_original_fields_values_and_receiving_backing_counts() {
    let (original, records, types) = duplicate_values();
    let control = Control::good(CompilePhase::Decode);
    let prepared = prepare(&records, &types, namespace_limits(), &control).unwrap();
    let facts = *prepared.facts();
    assert_eq!(facts.record_count, 2);
    assert_eq!(facts.source_retained_bytes, SOURCE);
    assert_eq!(facts.geometry_scratch_request_bytes, 0);
    assert_eq!(
        facts.preparation_request_bytes,
        facts.prepared_storage_request_bytes
    );
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + facts.new_allocation_request_bytes_upper_bound
    );
    assert!(facts.reader_request_bytes_upper_bound > 0);
    let pools = prepared.materialize().unwrap();
    assert_eq!(
        pools
            .entries()
            .keys()
            .map(|id| id.get())
            .collect::<Vec<_>>(),
        vec![0, u32::MAX]
    );
    let zero = &pools.entries()[&ConstantPoolId::new(0)];
    let max = &pools.entries()[&ConstantPoolId::new(u32::MAX)];
    assert!(Arc::ptr_eq(
        zero.field_ref(),
        types.source_field(u32::MAX).unwrap()
    ));
    assert!(Arc::ptr_eq(
        max.field_ref(),
        types.source_field(u32::MAX).unwrap()
    ));
    assert_ne!(zero.backing_identity(), max.backing_identity());
    assert_eq!(zero.value(2).unwrap().try_i64().unwrap(), Some(-9));
    assert!(
        original
            .value(1)
            .unwrap()
            .equals_observed(&max.value(1).unwrap(), CompilePhase::Decode, &control)
            .unwrap()
    );
    // Each record owns its receiving reader. Never deduplicate identical IPC
    // bytes or add the same whole source invoice twice.
    let mut one = records[..1].to_vec();
    one[0].id = 0;
    let single = prepare(&one, &types, namespace_limits(), &control).unwrap();
    assert_eq!(
        facts.reader_request_bytes_upper_bound,
        single.facts().reader_request_bytes_upper_bound * 2
    );
    assert!(
        facts.pool_table_request_bytes_upper_bound
            > single.facts().pool_table_request_bytes_upper_bound
    );
}

#[test]
fn namespace_mixed_roots_retain_geometry_once_and_materialize_same_type_table() {
    let flat = pool(Arc::new(Float32Array::from(vec![
        f32::from_bits(0x7fc0_1234),
        -0.0,
    ])));
    let child = Arc::new(
        Field::new("nested", DataType::Int32, true)
            .with_metadata(HashMap::from([("owner".into(), "exact".into())])),
    );
    let nested = pool(Arc::new(StructArray::new(
        vec![child].into(),
        vec![Arc::new(Int32Array::from(vec![Some(3), None]))],
        None,
    )));
    let types = types_many(
        &[
            (0, flat.value_type().clone()),
            (u32::MAX, nested.value_type().clone()),
        ],
        &[
            (0, Arc::clone(flat.field_ref())),
            (u32::MAX, Arc::clone(nested.field_ref())),
        ],
    );
    let records = [
        encode(&flat, u32::MAX, 0, 0),
        encode(&nested, 0, u32::MAX, u32::MAX),
    ];
    let control = Control::good(CompilePhase::Decode);
    let prepared = prepare(&records, &types, namespace_limits(), &control).unwrap();
    let facts = *prepared.facts();
    assert!(facts.geometry_scratch_request_bytes > 0);
    assert_eq!(
        facts.preparation_request_bytes,
        facts.prepared_storage_request_bytes + facts.geometry_scratch_request_bytes
    );
    let pools = prepared.materialize().unwrap();
    for (id, original, field_id) in [(u32::MAX, &flat, 0), (0, &nested, u32::MAX)] {
        let result = &pools.entries()[&ConstantPoolId::new(id)];
        assert!(Arc::ptr_eq(
            result.field_ref(),
            types.source_field(field_id).unwrap()
        ));
        for ordinal in 0..2 {
            assert!(
                original
                    .value(ordinal)
                    .unwrap()
                    .equals_observed(
                        &result.value(ordinal).unwrap(),
                        CompilePhase::Decode,
                        &control
                    )
                    .unwrap()
            );
        }
    }
    let mut exact = namespace_limits();
    exact.max_preparation_request_bytes = facts.preparation_request_bytes;
    assert!(prepare(&records, &types, exact, &control).is_ok());
    exact.max_preparation_request_bytes -= 1;
    assert!(prepare(&records, &types, exact, &control).is_err());
}

#[test]
fn namespace_aggregate_near_over_caps_do_not_materialize_a_prefix() {
    let (_, records, types) = duplicate_values();
    let control = Control::good(CompilePhase::Decode);
    let facts = *prepare(&records, &types, namespace_limits(), &control)
        .unwrap()
        .facts();
    for (index, bound) in [
        facts.preparation_request_bytes,
        facts.new_allocation_request_bytes_upper_bound,
        facts.coexisting_source_and_request_bytes_upper_bound,
        facts.cumulative_library_work_upper_bound,
    ]
    .into_iter()
    .enumerate()
    {
        let mut limits = namespace_limits();
        let set = |limits: &mut ConstantNamespaceProjectionLimits, value| match index {
            0 => limits.max_preparation_request_bytes = value,
            1 => limits.max_new_allocation_request_bytes = value,
            2 => limits.max_coexisting_source_and_request_bytes = value,
            3 => limits.max_cumulative_library_work = value,
            _ => unreachable!(),
        };
        set(&mut limits, bound);
        assert!(
            prepare(&records, &types, limits, &control).is_ok(),
            "exact {index}"
        );
        set(&mut limits, bound - 1);
        assert!(
            prepare(&records, &types, limits, &control).is_err(),
            "over {index}"
        );
    }
}

#[test]
fn namespace_count_source_capacity_and_overflow_gates_precede_corrupt_ipc() {
    let (_, mut records, types) = duplicate_values();
    records[0].arrow_ipc.clear();
    records[0].arrow_ipc.reserve(4096);
    let control = Control::good(CompilePhase::Decode);
    let limits = ConstantNamespaceProjectionLimits {
        max_records: 1,
        ..namespace_limits()
    };
    assert!(matches!(
        prepare(&records, &types, limits, &control),
        Err(PhysicalConstantCodecError::InvalidShape(
            "constant namespace count envelope exceeded"
        ))
    ));
    assert_eq!(
        control.trace(),
        vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 1)]
    );
    let source_control = Control::good(CompilePhase::Decode);
    let result = prepare_constant_namespace(
        &records,
        &types,
        1,
        policy(),
        decode_limits(),
        namespace_limits(),
        &verifier(),
        &source_control,
    );
    assert!(matches!(
        result,
        Err(PhysicalConstantCodecError::InvalidShape(
            "constant namespace source invoice excludes retained record storage"
        ))
    ));
    assert!(matches!(
        prepare_constant_namespace(
            &records,
            &types,
            usize::MAX,
            policy(),
            decode_limits(),
            namespace_limits(),
            &verifier(),
            &Control::good(CompilePhase::Decode)
        ),
        Err(PhysicalConstantCodecError::InvalidShape(
            "constant namespace resource sum overflow"
        ))
    ));
}

#[test]
fn namespace_duplicate_rejection_uses_original_table_author_and_ordinary_tail() {
    let (_, mut records, types) = duplicate_values();
    records[1].id = records[0].id;
    every_prefix(
        CompilePhase::Decode,
        |control| {
            decode_constant_namespace(
                &records,
                &types,
                SOURCE,
                policy(),
                decode_limits(),
                namespace_limits(),
                &verifier(),
                control,
            )
        },
        false,
        false,
    );
    assert!(
        matches!(decode_constant_namespace(&records, &types, SOURCE, policy(), decode_limits(),
        namespace_limits(), &verifier(), &Control::good(CompilePhase::Decode)),
        Err(PhysicalConstantCodecError::Reference(ConstantReferenceError::Table(novarocks_constant_contract::ConstantTableError::DuplicatePool(id))))
        if id == ConstantPoolId::new(u32::MAX))
    );
}

#[test]
fn namespace_empty_zero_request_and_every_callback_keep_original_control() {
    let (_, records, types) = duplicate_values();
    let empty = ConstantNamespaceProjectionLimits {
        max_records: 0,
        max_preparation_request_bytes: 0,
        max_new_allocation_request_bytes: 0,
        max_coexisting_source_and_request_bytes: 0,
        max_cumulative_library_work: 0,
    };
    let control = Control::good(CompilePhase::Decode);
    let empty_types = types_many(&[], &[]);
    let prepared = prepare_constant_namespace(
        &[],
        &empty_types,
        0,
        policy(),
        decode_limits(),
        empty,
        &verifier(),
        &control,
    )
    .unwrap();
    assert_eq!(*prepared.facts(), ConstantNamespaceResourceFacts::default());
    assert!(prepared.materialize().unwrap().entries().is_empty());
    every_prefix(
        CompilePhase::Decode,
        |control| {
            prepare(&records, &types, namespace_limits(), control).map(|value| *value.facts())
        },
        true,
        false,
    );
    every_prefix(
        CompilePhase::Decode,
        |control| {
            prepare(
                &records,
                &types,
                ConstantNamespaceProjectionLimits {
                    max_new_allocation_request_bytes: 0,
                    ..namespace_limits()
                },
                control,
            )
            .map(|value| *value.facts())
        },
        false,
        false,
    );
    every_prefix(
        CompilePhase::Decode,
        |control| {
            decode_constant_namespace(
                &records,
                &types,
                SOURCE,
                policy(),
                decode_limits(),
                namespace_limits(),
                &verifier(),
                control,
            )
        },
        true,
        false,
    );
}

#[test]
fn namespace_wide_real_quantum_has_no_id_index_allocation_or_control_after_refusal() {
    let original = pool(Arc::new(Int64Array::from(vec![7])));
    let types = types(
        Arc::clone(original.field_ref()),
        original.value_type().clone(),
        0,
        0,
    );
    let record = encode(&original, 0, 0, 0);
    let records: Vec<_> = (0..320)
        .map(|id| {
            let mut value = record.clone();
            value.id = id;
            value
        })
        .collect();
    // Capacity-derived retained DTO invoice plus an explicit 64 KiB margin
    // for this tiny original pool, type table, Fields and metadata owners.
    // Reusing the coarse 1 MiB two-record fixture invoice multiplies its
    // source-derived work upper bound 320 times without corresponding backing.
    let source = std::alloc::Layout::array::<wire::IpcConstantPool>(records.capacity())
        .unwrap()
        .size()
        + records
            .iter()
            .map(|value| value.arrow_ipc.capacity())
            .sum::<usize>()
        + 64 * 1024;
    let good = Control::good(CompilePhase::Decode);
    let prepared = prepare_constant_namespace(
        &records,
        &types,
        source,
        policy(),
        decode_limits(),
        namespace_limits(),
        &verifier(),
        &good,
    )
    .unwrap();
    assert_eq!(prepared.facts().record_count, 320);
    let trace = good.trace();
    let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
    for cause in CAUSES {
        let refusing = Control::refusing(CompilePhase::Decode, quantum, cause.clone());
        assert!(
            matches!(prepare_constant_namespace(&records, &types, source, policy(), decode_limits(), namespace_limits(), &verifier(), &refusing),
            Err(PhysicalConstantCodecError::Control(actual)) if actual == cause)
        );
        assert_eq!(refusing.trace(), trace[..=quantum]);
    }
    assert_eq!(prepared.materialize().unwrap().entries().len(), 320);
}

#[test]
fn namespace_recursive_preparation_and_consumption_preserve_every_control_prefix() {
    let original = pool(Arc::new(ListArray::new(
        Arc::new(Field::new("selected-child", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 2, 3])),
        Arc::new(Int32Array::from(vec![Some(7), None, Some(-9)])),
        None,
    )));
    let types = types(
        Arc::clone(original.field_ref()),
        original.value_type().clone(),
        u32::MAX,
        0,
    );
    let records = [encode(&original, u32::MAX, u32::MAX, 0)];
    every_prefix(
        CompilePhase::Decode,
        |control| {
            decode_constant_namespace(
                &records,
                &types,
                SOURCE,
                policy(),
                decode_limits(),
                namespace_limits(),
                &verifier(),
                control,
            )
        },
        true,
        false,
    );
    let good = Control::good(CompilePhase::Decode);
    let facts = *prepare(&records, &types, namespace_limits(), &good)
        .unwrap()
        .facts();
    let limits = ConstantNamespaceProjectionLimits {
        max_preparation_request_bytes: facts.preparation_request_bytes - 1,
        ..namespace_limits()
    };
    every_prefix(
        CompilePhase::Decode,
        |control| prepare(&records, &types, limits, control).map(|value| *value.facts()),
        false,
        false,
    );
}
