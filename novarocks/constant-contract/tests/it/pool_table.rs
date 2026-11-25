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

//! Sparse table address tests, not package, allocator or provider closure.
use arrow_array::types::Int8Type;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Float32Array, Int8Array, Int64Array, StringArray,
    new_null_array,
};
use arrow_schema::{DataType, Field};
use novarocks_constant_contract::ConstantPolicy;
use novarocks_constant_contract::{
    ConstantError, ConstantPool, ConstantPoolId, ConstantPools, ConstantReference,
    ConstantTableError,
};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, FunctionValueType};
use novarocks_type_contract::{CompilePhase, PureCompileControl, ValueLogicalType};
use std::sync::{Arc, Mutex};

struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self::at(None)
    }
    fn at(refusal: Option<(usize, CompileControlError)>) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            refusal,
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let index = events.len();
        events.push((phase, units));
        if let Some((at, cause)) = self.refusal.clone()
            && index == at
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}

#[test]
fn temporal_constant_content_distinguishes_zone_and_checks_reachable_value() {
    use arrow_array::FixedSizeBinaryArray;
    use novarocks_type_contract::{
        TemporalCarrier, TemporalMicros, TemporalPrecision, TemporalType, TemporalValue,
        TimeZoneKey, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let declaration = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
    let storage = ValueTypeStorageLifetime::pure();
    let ty = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::Temporal(declaration), false, storage.clone()).unwrap(),
        ValueLayout::try_new(&declaration.carrier(), storage).unwrap(),
    )
    .unwrap();
    let field = Arc::new(ty.try_to_field("ts").unwrap());
    let constant = |bytes: [u8; 14]| {
        let array = FixedSizeBinaryArray::try_from_iter([bytes.as_slice()].into_iter()).unwrap();
        ConstantPool::try_new(
            field.clone(),
            ty.clone(),
            array.into_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .and_then(|pool| pool.value(0))
    };
    let payload = |zone| {
        let observed = TemporalValue::TimestampWithTimeZone {
            instant: TemporalMicros::from_picos(-1).unwrap(),
            zone,
        };
        let TemporalCarrier::ZonedTimestamp(bytes) = observed.to_carrier(declaration).unwrap()
        else {
            panic!("unexpected carrier");
        };
        bytes
    };
    let a = constant(payload(TimeZoneKey::UTC)).unwrap();
    let shanghai = payload(TimeZoneKey::parse("Asia/Shanghai").unwrap());
    let b = constant(shanghai).unwrap();
    assert!(
        !a.equals_observed(&b, CompilePhase::Validate, &Control::good())
            .unwrap()
    );
    assert_ne!(
        a.semantic_key_observed(CompilePhase::Validate, &Control::good())
            .unwrap(),
        b.semantic_key_observed(CompilePhase::Validate, &Control::good())
            .unwrap()
    );
    let mut bad = shanghai;
    bad[12..].copy_from_slice(&u16::MAX.to_le_bytes());
    assert!(matches!(constant(bad), Err(ConstantError::Invalid(_))));
}
fn pool(array: ArrayRef, ty: FunctionValueType, field: Arc<Field>) -> ConstantPool {
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap()
}
fn plain(array: ArrayRef, nullable: bool) -> ConstantPool {
    let ty = FunctionValueType::new(array.data_type().clone(), nullable);
    let field = Arc::new(Field::new("literal", array.data_type().clone(), nullable));
    pool(array, ty, field)
}
fn reference(id: u32, ordinal: u32) -> ConstantReference {
    ConstantReference {
        pool: ConstantPoolId::new(id),
        ordinal,
    }
}
// The table never finishes a caller's encompassing scope. This concrete caller
// exposes a result only after its ordinary/successful completed tail.
fn completed<T>(
    control: &Control,
    action: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, ConstantTableError>,
) -> Result<T, ConstantTableError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = action(&mut work);
    if matches!(&result, Err(ConstantTableError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn sparse_zero_and_max_ids_resolve_without_dense_storage_and_duplicate_replacement() {
    let original = plain(Arc::new(Int64Array::from(vec![3, 7])), false);
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(0), original.clone())
        .unwrap();
    table
        .insert(ConstantPoolId::new(u32::MAX), original.clone())
        .unwrap();
    assert_eq!(table.entries().len(), 2);
    assert_eq!(
        table
            .entries()
            .keys()
            .map(|id| id.get())
            .collect::<Vec<_>>(),
        [0, u32::MAX]
    );
    assert_eq!(
        table
            .insert(
                ConstantPoolId::new(0),
                plain(Arc::new(Int64Array::from(vec![99])), false)
            )
            .unwrap_err(),
        ConstantTableError::DuplicatePool(ConstantPoolId::new(0))
    );
    for id in [0, u32::MAX] {
        let value = completed(&Control::good(), |work| {
            table.resolve_observed(reference(id, 1), original.value_type(), work)
        })
        .unwrap();
        assert_eq!(value.try_i64().unwrap(), Some(7));
        assert_eq!(value.ordinal(), 1);
        assert!(Arc::ptr_eq(value.pool().array(), original.array()));
    }
}

#[test]
fn missing_pool_and_bad_ordinal_return_exact_errors_and_observed_ordinary_tails() {
    let backing = plain(Arc::new(Int64Array::from(vec![7])), false);
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(0), backing.clone())
        .unwrap();
    for (reference, expected) in [
        (
            reference(1, 0),
            ConstantTableError::MissingPool(ConstantPoolId::new(1)),
        ),
        (
            reference(0, 1),
            ConstantTableError::Constant(ConstantError::Invalid(
                "constant ordinal is outside its pool",
            )),
        ),
    ] {
        let baseline = Control::good();
        let error = completed(&baseline, |work| {
            table.resolve_observed(reference, backing.value_type(), work)
        })
        .unwrap_err();
        assert_eq!(error, expected);
        use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            error.failure_fact().reason().domain(),
            domain::CONSTANT.get()
        );
        let trace = baseline.trace();
        assert!(trace.len() >= 2);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let control = Control::at(Some((at, cause.clone())));
                assert_eq!(
                    completed(&control, |work| table.resolve_observed(
                        reference,
                        backing.value_type(),
                        work
                    ))
                    .unwrap_err(),
                    ConstantTableError::Control(cause.clone())
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
#[allow(
    deprecated,
    reason = "Dictionary IDs/order are exact frozen public field facts."
)]
fn source_type_requires_exact_nullable_nested_metadata_and_nominal_identity() {
    let integer = plain(Arc::new(Int64Array::from(vec![7])), false);
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(0), integer.clone())
        .unwrap();
    for bad in [
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int32, false),
    ] {
        assert_eq!(
            completed(&Control::good(), |w| table.resolve_observed(
                reference(0, 0),
                &bad,
                w
            ))
            .unwrap_err(),
            ConstantTableError::SourceTypeMismatch(reference(0, 0))
        );
    }
    let child = Arc::new(Field::new("child", DataType::Int64, true).with_metadata(
        std::collections::HashMap::<String, String>::from([("provider.id".into(), "a".into())]),
    ));
    let nested = FunctionValueType::new(DataType::List(child), true);
    let backing = plain(new_null_array(nested.data_type(), 1), true);
    table.insert(ConstantPoolId::new(1), backing).unwrap();
    for (index, bad) in [
        FunctionValueType::new(
            DataType::List(Arc::new(
                Field::new("child", DataType::Int64, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.id".into(),
                        "b".into(),
                    )]),
                ),
            )),
            true,
        ),
        FunctionValueType::new(
            DataType::List(Arc::new(
                Field::new("child", DataType::Int64, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.id".into(),
                        "a".into(),
                    )]),
                ),
            )),
            true,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        if index == 0 {
            completed(&Control::good(), |w| {
                table.resolve_observed(reference(1, 0), &bad, w)
            })
            .unwrap();
            continue;
        }
        assert_eq!(
            completed(&Control::good(), |w| table.resolve_observed(
                reference(1, 0),
                &bad,
                w
            ))
            .unwrap_err(),
            ConstantTableError::SourceTypeMismatch(reference(1, 0))
        );
    }
    let encoded = |id, ordered| {
        FunctionValueType::new(
            DataType::Struct(
                vec![Arc::new(Field::new_dict(
                    "encoded",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                    id,
                    ordered,
                ))]
                .into(),
            ),
            true,
        )
    };
    let exact = encoded(73, true);
    table
        .insert(
            ConstantPoolId::new(3),
            plain(new_null_array(exact.data_type(), 1), true),
        )
        .unwrap();
    for equivalent in [encoded(74, true), encoded(73, false)] {
        completed(&Control::good(), |w| {
            table.resolve_observed(reference(3, 0), &equivalent, w)
        })
        .unwrap();
    }
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    table
        .insert(
            ConstantPoolId::new(2),
            pool(
                Arc::new(arrow_array::LargeBinaryArray::from(vec![
                    b"\x01\x20".as_slice(),
                ])),
                json.clone(),
                Arc::new(json.try_to_field("literal").unwrap()),
            ),
        )
        .unwrap();
    assert_eq!(
        completed(&Control::good(), |w| table.resolve_observed(
            reference(2, 0),
            &FunctionValueType::new(DataType::LargeBinary, false),
            w
        ))
        .unwrap_err(),
        ConstantTableError::SourceTypeMismatch(reference(2, 0))
    );
}

#[test]
fn projection_retains_only_selected_sparse_pools_without_rebuilding_unused_backing() {
    let array: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(320_000), "kept".into()]));
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let field = Arc::new(ty.try_to_field("original").unwrap().with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "provider.fact".into(),
            "unchanged".into(),
        )]),
    ));
    let backing = pool(array, ty.clone(), field.clone());
    let mut table = ConstantPools::empty();
    table
        .insert(
            ConstantPoolId::new(0),
            plain(Arc::new(Int64Array::from(vec![99])), false),
        )
        .unwrap();
    table
        .insert(ConstantPoolId::new(u32::MAX), backing.clone())
        .unwrap();
    let projected = completed(&Control::good(), |w| {
        table.project_observed(
            [(reference(u32::MAX, 1), &ty), (reference(u32::MAX, 1), &ty)],
            w,
        )
    })
    .unwrap();
    assert_eq!(projected.entries().len(), 1);
    assert!(!projected.entries().contains_key(&ConstantPoolId::new(0)));
    let value = completed(&Control::good(), |w| {
        projected.resolve_observed(reference(u32::MAX, 1), &ty, w)
    })
    .unwrap();
    assert_eq!(value.try_utf8().unwrap(), Some("kept"));
    assert_eq!(value.ordinal(), 1);
    assert_eq!(value.pool().array().len(), 2);
    assert!(Arc::ptr_eq(value.pool().field_ref(), &field));
    assert!(Arc::ptr_eq(value.pool().array(), backing.array()));
}

#[test]
fn typed_null_and_dictionary_renumbered_values_retain_two_addressed_backings() {
    let a = plain(
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0]),
                Arc::new(StringArray::from(vec!["kept", "unused"])),
            )
            .unwrap(),
        ),
        false,
    );
    let b = plain(
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![1]),
                Arc::new(StringArray::from(vec!["other-unused", "kept"])),
            )
            .unwrap(),
        ),
        false,
    );
    let mut table = ConstantPools::empty();
    table.insert(ConstantPoolId::new(0), a.clone()).unwrap();
    table
        .insert(ConstantPoolId::new(u32::MAX), b.clone())
        .unwrap();
    let projected = completed(&Control::good(), |w| {
        table.project_observed(
            [
                (reference(0, 0), a.value_type()),
                (reference(u32::MAX, 0), b.value_type()),
            ],
            w,
        )
    })
    .unwrap();
    assert_eq!(
        projected.entries().len(),
        2,
        "value equality cannot collapse sparse addresses"
    );
    let l = completed(&Control::good(), |w| {
        projected.resolve_observed(reference(0, 0), a.value_type(), w)
    })
    .unwrap();
    let r = completed(&Control::good(), |w| {
        projected.resolve_observed(reference(u32::MAX, 0), b.value_type(), w)
    })
    .unwrap();
    assert!(
        l.equals_observed(&r, CompilePhase::Validate, &Control::good())
            .unwrap()
    );
    assert!(Arc::ptr_eq(l.pool().array(), a.array()));
    assert!(Arc::ptr_eq(r.pool().array(), b.array()));
    let null = plain(Arc::new(Int64Array::from(vec![None, Some(4)])), true);
    table.insert(ConstantPoolId::new(7), null.clone()).unwrap();
    let value = completed(&Control::good(), |w| {
        table.resolve_observed(reference(7, 0), null.value_type(), w)
    })
    .unwrap();
    assert!(
        value
            .is_null_observed(CompilePhase::Validate, &Control::good())
            .unwrap()
    );
    assert_eq!(value.value_type(), null.value_type());
}

#[test]
fn float32_raw_nan_payloads_and_signed_zero_survive_sparse_resolution() {
    let bits = [0x7f800001, 0x7fc00002, 0, 0x80000000];
    let backing = plain(
        Arc::new(Float32Array::from(bits.map(f32::from_bits).to_vec())),
        false,
    );
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(u32::MAX), backing.clone())
        .unwrap();
    for (ordinal, bits) in bits.into_iter().enumerate() {
        let value = completed(&Control::good(), |w| {
            table.resolve_observed(reference(u32::MAX, ordinal as u32), backing.value_type(), w)
        })
        .unwrap();
        let array = value
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(array.value(value.ordinal() as usize).to_bits(), bits);
    }
}

#[test]
fn source_metadata_is_retained_without_semantic_projection_copy_and_controls_preserve_prefix() {
    let child = Arc::new(
        Field::new("child", DataType::Int64, true).with_metadata(
            (0..320)
                .map(|i| (format!("provider.{i:04}"), "kept".into()))
                .collect::<std::collections::HashMap<String, String>>(),
        ),
    );
    let source_type = DataType::List(child);
    let ty = FunctionValueType::new(source_type.clone(), true);
    let backing = pool(
        new_null_array(&source_type, 1),
        ty.clone(),
        Arc::new(Field::new("actual.source", source_type, true)),
    );
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(u32::MAX), backing.clone())
        .unwrap();
    let baseline = Control::good();
    let projected = completed(&baseline, |w| {
        table.project_observed(
            [(reference(u32::MAX, 0), &ty), (reference(u32::MAX, 0), &ty)],
            w,
        )
    })
    .unwrap();
    assert_eq!(projected.entries().len(), 1);
    let trace = baseline.trace();
    let DataType::List(original_child) = backing.field().data_type() else {
        unreachable!()
    };
    assert_eq!(original_child.metadata().len(), 320);
    let DataType::List(layout_child) = ty.data_type() else {
        unreachable!()
    };
    assert!(layout_child.metadata().is_empty());
    assert!(trace.len() >= 2);
    assert!(trace.last().is_some_and(|(_, units)| *units > 0));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let control = Control::at(Some((at, cause.clone())));
            assert_eq!(
                completed(&control, |w| table.project_observed(
                    [(reference(u32::MAX, 0), &ty), (reference(u32::MAX, 0), &ty)],
                    w
                ))
                .unwrap_err(),
                ConstantTableError::Control(cause.clone())
            );
            assert_eq!(control.trace(), trace[..=at]);
            assert_eq!(table.entries().len(), 1);
            assert!(Arc::ptr_eq(
                table.entries()[&ConstantPoolId::new(u32::MAX)].array(),
                backing.array()
            ));
        }
    }
}

#[test]
fn optional_constant_projection_observes_nonconstant_prefixes_and_definition_gaps() {
    let original = plain(Arc::new(Int64Array::from(vec![9, 42, -7])), false);
    let ty = original.value_type();
    let mut table = ConstantPools::empty();
    table
        .insert(ConstantPoolId::new(u32::MAX), original.clone())
        .unwrap();
    let mut gap = vec![Some((reference(u32::MAX, 1), ty))];
    gap.extend(std::iter::repeat_n(None, 320));
    gap.push(Some((reference(u32::MAX, 2), ty)));
    for definitions in [vec![None; 320], gap] {
        let baseline = Control::good();
        let result = completed(&baseline, |work| {
            table.project_optional_references_observed(definitions.iter().copied(), work)
        })
        .unwrap();
        assert_eq!(
            result.entries().len(),
            usize::from(definitions[0].is_some())
        );
        if let Some(backing) = result.entries().get(&ConstantPoolId::new(u32::MAX)) {
            assert_eq!(backing.backing_identity(), original.backing_identity());
            assert_eq!(backing.value(1).unwrap().try_i64().unwrap(), Some(42));
            assert_eq!(backing.value(2).unwrap().try_i64().unwrap(), Some(-7));
        }
        let trace = baseline.trace();
        assert!(trace.iter().any(|(_, units)| *units == 256));
        assert!(
            trace
                .iter()
                .map(|(_, units)| u64::from(*units))
                .sum::<u64>()
                >= 320
        );
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let control = Control::at(Some((at, cause.clone())));
                assert!(matches!(completed(&control, |work| {
                    table.project_optional_references_observed(definitions.iter().copied(), work)
                }), Err(ConstantTableError::Control(actual)) if actual == cause));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
fn captured_source_preserves_sparse_address_null_and_original_field_backing() {
    let original = plain(Arc::new(Int64Array::from(vec![Some(7), None])), true);
    let mut table = ConstantPools::empty();
    for id in [0, u32::MAX] {
        table
            .insert(ConstantPoolId::new(id), original.clone())
            .unwrap();
        for ordinal in [0, 1] {
            let c = Control::good();
            let mut calls = 0;
            let value = completed(&c, |work| {
                table.resolve_source_captured_observed(
                    reference(id, ordinal),
                    &mut |value, _| {
                        calls += 1;
                        assert_eq!(value.ordinal(), ordinal);
                        assert!(Arc::ptr_eq(value.pool().field_ref(), original.field_ref()));
                        assert!(std::ptr::eq(value.value_type(), original.value_type()));
                        assert_eq!(c.trace(), [(CompilePhase::Validate, 0)]);
                        Ok::<_, ConstantTableError>(())
                    },
                    work,
                )
            })
            .unwrap();
            assert_eq!(calls, 1);
            assert_eq!(value.ordinal(), ordinal);
            assert!(Arc::ptr_eq(value.pool().field_ref(), original.field_ref()));
            let plain_control = Control::good();
            completed(&plain_control, |work| {
                table.resolve_source_observed(reference(id, ordinal), work)
            })
            .unwrap();
            assert_eq!(c.trace(), plain_control.trace());
        }
    }
}

#[test]
fn captured_source_refusal_precedes_pending_completed_lookup_and_has_no_footer() {
    let mut table = ConstantPools::empty();
    table
        .insert(
            ConstantPoolId::new(0),
            plain(Arc::new(Int64Array::from(vec![8, 9])), false),
        )
        .unwrap();
    for pending in [0, 254, 255] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = Control::at(Some((1, cause)));
            let result = completed(&c, |work| {
                for _ in 0..pending {
                    work.step()?;
                }
                table.resolve_source_captured_observed(
                    reference(0, 1),
                    &mut |value, _| {
                        assert_eq!(value.ordinal(), 1);
                        Err::<(), _>(ConstantTableError::Control(
                            CompileControlError::ResourceExhausted,
                        ))
                    },
                    work,
                )
            });
            assert!(matches!(
                result,
                Err(ConstantTableError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(c.trace(), [(CompilePhase::Validate, 0)]);
        }
    }
}

#[test]
fn captured_source_missing_and_ordinal_errors_keep_plain_observations_and_control_causes() {
    let mut table = ConstantPools::empty();
    table
        .insert(
            ConstantPoolId::new(0),
            plain(Arc::new(Int64Array::from(vec![8, 9])), false),
        )
        .unwrap();
    for address in [reference(0, 1), reference(0, 2), reference(u32::MAX, 0)] {
        let old = Control::good();
        let plain = completed(&old, |work| table.resolve_source_observed(address, work));
        let new = Control::good();
        let mut captures = 0;
        let received = completed(&new, |work| {
            table.resolve_source_captured_observed(
                address,
                &mut |_, _| {
                    captures += 1;
                    Ok::<_, ConstantTableError>(())
                },
                work,
            )
        });
        assert_eq!(plain.is_ok(), received.is_ok());
        if let (Err(left), Err(right)) = (&plain, &received) {
            assert_eq!(left, right);
        }
        assert_eq!(captures, usize::from(plain.is_ok()));
        assert_eq!(old.trace(), new.trace());
        let trace = new.trace();
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let c = Control::at(Some((stop, cause.clone())));
                let result = completed(&c, |work| {
                    table.resolve_source_captured_observed(
                        address,
                        &mut |_, _| Ok::<_, ConstantTableError>(()),
                        work,
                    )
                });
                assert!(
                    matches!(result, Err(ConstantTableError::Control(actual)) if actual == cause)
                );
                assert_eq!(c.trace(), trace[..=stop]);
            }
        }
    }
}
