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

//! Capture receipts preserve original values and instance identity across
//! repeated loans; no test hook or private representation is required.

use super::*;
use arrow::datatypes::Field;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_functions::KernelFailure;
use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
use std::sync::atomic::{AtomicUsize, Ordering};

fn failure<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    match result {
        Err(cause) => cause,
        Ok(_) => panic!("capture refusal must remain visible"),
    }
}
fn capture(
    spec: &KeySpec,
    arrays: &[ArrayRef],
    types: &[FunctionValueType],
    rows: &[usize],
    owner: &dyn InputConstructionOwner,
) -> KeyInput {
    KeyInput::new_for_spec(
        spec,
        arrays,
        types,
        &readable(arrays[0].len()),
        rows,
        &mut ValueConversionContext::default(),
        owner,
        &|| Ok(()),
    )
    .unwrap()
}

#[test]
fn original_component_range_value_comparison_keeps_unknown_false_and_equality_modes() {
    use arrow::datatypes::Int32Type;
    let mut cases: Vec<(FunctionValueType, ArrayRef, ArrayRef)> = vec![
        (
            FunctionValueType::new(DataType::Int64, true),
            Arc::new(Int64Array::from(vec![None, Some(2)])),
            Arc::new(Int64Array::from(vec![Some(2), Some(2), None])),
        ),
        (
            FunctionValueType::new(DataType::Utf8, true),
            Arc::new(StringArray::from(vec![None, Some("two")])),
            Arc::new(StringViewArray::from(vec![Some("two"), Some("two"), None])),
        ),
    ];
    cases.push((
        FunctionValueType::new(DataType::Utf8, true),
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1]),
                Arc::new(StringArray::from(vec![None, Some("two")])),
            )
            .unwrap(),
        ),
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 0, 1]),
                Arc::new(StringArray::from(vec![Some("two"), None])),
            )
            .unwrap(),
        ),
    ));
    cases.push((
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            None,
            Some(vec![Some(2), None]),
        ])),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(2), None]),
            Some(vec![Some(2), None]),
            None,
        ])),
    ));
    for (case, (ty, left_value, right_value)) in cases.into_iter().enumerate() {
        for null_safe in [false, true] {
            let (owner, tracker) = owner_with_tracker();
            let types = [
                FunctionValueType::new(DataType::Int32, false),
                ty.clone(),
                FunctionValueType::new(DataType::Int32, false),
            ];
            let modes = [
                NullPolicy::Equal,
                if null_safe {
                    NullPolicy::Equal
                } else {
                    NullPolicy::Unmatchable
                },
                NullPolicy::Unmatchable,
            ];
            let spec = KeySpec::new(&types, &modes, owner.as_ref(), &|| Ok(())).unwrap();
            let left_arrays: Vec<ArrayRef> = vec![
                Arc::new(Int32Array::from(vec![7, 8])),
                Arc::clone(&left_value),
                Arc::new(Int32Array::from(vec![1, 2])),
            ];
            let right_arrays: Vec<ArrayRef> = vec![
                Arc::new(Int32Array::from(vec![999, 888, 777])),
                Arc::clone(&right_value),
                Arc::new(Int32Array::from(vec![1, 2, 1])),
            ];
            let mut right_types = types.clone();
            if case == 1 {
                // V can compare UTF8 and VIEW, but the incoming VIEW still
                // has its own actual signed carrier declaration.
                right_types[1] = FunctionValueType::new(DataType::Utf8View, true);
            }
            let checked_capture = |arrays: &[ArrayRef],
                                   bindings: &[FunctionValueType],
                                   rows: &[usize],
                                   side| {
                KeyInput::new_for_spec(
                    &spec,
                    arrays,
                    bindings,
                    &readable(arrays[0].len()),
                    rows,
                    &mut ValueConversionContext::default(),
                    owner.as_ref(),
                    &|| Ok(()),
                )
                .unwrap_or_else(|cause| {
                    panic!("value pair fixture {case}/{null_safe}/{side} failed capture: {cause:?}")
                })
            };
            let left = checked_capture(&left_arrays, &types, &[0, 1], "left");
            let right = checked_capture(&right_arrays, &right_types, &[0, 1, 2], "right");
            let a = PreparedKeyBatch::from_captured(
                &spec,
                &left,
                AggregateAllocator::new(tracker.clone()),
                &|| Ok(()),
            )
            .unwrap();
            let b = PreparedKeyBatch::from_captured(
                &spec,
                &right,
                AggregateAllocator::new(tracker),
                &|| Ok(()),
            )
            .unwrap();
            let mut actual = [Some(false); 5];
            a.compare_value_pairs(
                1..3,
                &[0, 0, 1, 1, 0],
                &b,
                &[0, 1, 0, 1, 2],
                &mut actual,
                owner.as_ref(),
                &|| Ok(()),
            )
            .unwrap();
            assert_eq!(
                actual,
                [
                    if null_safe { Some(false) } else { None },
                    Some(false),
                    Some(false),
                    Some(true),
                    if null_safe { Some(true) } else { None }
                ]
            );
            // The scope prefix is excluded explicitly, never guessed by roles.
            a.compare_value_pairs(
                0..3,
                &[0, 0, 1, 1, 0],
                &b,
                &[0, 1, 0, 1, 2],
                &mut actual,
                owner.as_ref(),
                &|| Ok(()),
            )
            .unwrap();
            assert_eq!(actual, [Some(false); 5]);
            a.compare_value_pairs(
                1..1,
                &[0, 0, 1, 1, 0],
                &b,
                &[0, 1, 0, 1, 2],
                &mut actual,
                owner.as_ref(),
                &|| Ok(()),
            )
            .unwrap();
            assert_eq!(actual, [Some(true); 5]);
            let original: ExecutionFailure = KernelFailure::DeadlineExceeded.into();
            let failure = a
                .compare_value_pairs(
                    1..3,
                    &[0, 0, 1, 1, 0],
                    &b,
                    &[0, 1, 0, 1, 2],
                    &mut actual,
                    owner.as_ref(),
                    &|| Err(original.clone()),
                )
                .unwrap_err();
            assert_eq!(failure, original);
            assert_eq!(actual, [Some(true); 5]);
        }
    }
}

#[test]
fn value_comparison_uses_original_native_float_canonical_equality() {
    let (owner, tracker) = owner_with_tracker();
    let types = [FunctionValueType::new(DataType::Float64, true)];
    let spec = KeySpec::new(&types, &[NullPolicy::Unmatchable], owner.as_ref(), &|| {
        Ok(())
    })
    .unwrap();
    let left = capture(
        &spec,
        &[Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            None,
        ]))],
        &types,
        &[0, 1, 2],
        owner.as_ref(),
    );
    let right = capture(
        &spec,
        &[Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0021)),
            None,
        ]))],
        &types,
        &[0, 1, 2],
        owner.as_ref(),
    );
    let a = PreparedKeyBatch::from_captured(
        &spec,
        &left,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    let b = PreparedKeyBatch::from_captured(
        &spec,
        &right,
        AggregateAllocator::new(tracker),
        &|| Ok(()),
    )
    .unwrap();
    let mut output = [Some(false); 3];
    a.compare_value_pairs(
        0..1,
        &[0, 1, 2],
        &b,
        &[0, 1, 2],
        &mut output,
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(output, [Some(true), Some(true), None]);
}
fn captured_hash(
    spec: &KeySpec,
    input: &KeyInput,
    policy: &LocalHashPolicy,
    tracker: &Arc<MemTracker>,
    owner: &dyn InputConstructionOwner,
) -> LocalHashBatch {
    let loan = PreparedKeyBatch::from_captured(
        spec,
        input,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    hash_batch(&loan, policy, &mut HashScratch::new(), owner, &|| Ok(())).unwrap()
}
fn mixed(large: bool) -> (Vec<ArrayRef>, Vec<FunctionValueType>) {
    let bytes = [
        Some(b"\xff".as_slice()),
        None,
        Some(b"".as_slice()),
        Some(b"\xff".as_slice()),
        None,
    ];
    let children: ArrayRef = if large {
        Arc::new(LargeBinaryArray::from(bytes.to_vec()))
    } else {
        Arc::new(BinaryArray::from(bytes.to_vec()))
    };
    let field = Arc::new(Field::new("item", children.data_type().clone(), true));
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            field,
            OffsetBuffer::new(vec![0, 2, 2, 3, 5].into()),
            children,
            Some(NullBuffer::from(vec![true, false, true, true])),
        )
        .unwrap(),
    );
    let plain: ArrayRef = if large {
        Arc::new(StringArray::from(vec![
            Some("alpha external value"),
            None,
            Some("beta external value"),
            Some("alpha external value"),
        ]))
    } else {
        Arc::new(StringViewArray::from(vec![
            Some("alpha external value"),
            None,
            Some("beta external value"),
            Some("alpha external value"),
        ]))
    };
    let types = vec![
        FunctionValueType::new(plain.data_type().clone(), true),
        FunctionValueType::new(list.data_type().clone(), true),
    ];
    (vec![plain, list], types)
}
fn projected_types(nullable: bool) -> [FunctionValueType; 2] {
    [
        FunctionValueType::new(DataType::Utf8, nullable),
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
            nullable,
        ),
    ]
}

#[test]
fn captured_occurrences_cached_hashes_and_selected_index_keep_values_order_and_nulls() {
    let (owner, tracker) = owner_with_tracker();
    let types = [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8View, true),
    ];
    let spec = KeySpec::new(
        &types,
        &[NullPolicy::Equal, NullPolicy::Unmatchable],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index = KeyIndex::new(
        spec.clone(),
        IndexPurpose::KeyIds,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let input = capture(
        &spec,
        &[
            Arc::new(Int32Array::from(vec![7, 8, 7, 9])),
            Arc::new(StringViewArray::from(vec![
                Some("a"),
                None,
                Some("a"),
                Some("b"),
            ])),
        ],
        &types,
        &[2, 0, 3, 2, 1],
        owner.as_ref(),
    );
    assert_eq!(
        (0..input.len())
            .map(|i| input.row(i).unwrap())
            .collect::<Vec<_>>(),
        vec![2, 0, 3, 2, 1]
    );
    let hashes = captured_hash(&spec, &input, index.policy(), &tracker, owner.as_ref());
    assert_eq!(hashes.hash(0).unwrap(), hashes.hash(1).unwrap());
    assert_eq!(hashes.hash(0).unwrap(), hashes.hash(3).unwrap());
    assert!(!hashes.participates(4).unwrap());
    let hashes = hashes.force_hash(0);
    let loan = PreparedKeyBatch::from_captured(
        &spec,
        &input,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    let before = super::super::local_hash::BYTE_HASH_INITS.with(Cell::get);
    let output = index
        .intern_selected(
            &loan,
            &hashes,
            &[3, 1, 3, 4, 2],
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(
        output.matches(),
        &[
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Unmatchable,
            KeyMatch::Found(KeyId(1))
        ]
    );
    assert_eq!(
        output.new_keys(),
        &[
            NewKey {
                occurrence: 3,
                row: 2,
                id: KeyId(0)
            },
            NewKey {
                occurrence: 2,
                row: 3,
                id: KeyId(1)
            },
        ]
    );
    assert_eq!(
        index
            .probe_selected(&loan, &hashes, &[2, 0, 4, 1], owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &[
            KeyMatch::Found(KeyId(1)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Unmatchable,
            KeyMatch::Found(KeyId(0))
        ]
    );
    assert_eq!(
        super::super::local_hash::BYTE_HASH_INITS.with(Cell::get),
        before
    );
    let exported = index
        .export_keys(
            &[KeyId(1), KeyId(0), KeyId(1)],
            &types,
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![9, 7, 9])
    );
    assert_eq!(
        exported.arrays()[1]
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("b"), Some("a"), Some("b")]
    );
}

#[test]
fn captured_mixed_layout_projection_preserves_source_nulls_and_is_shared_by_4096_loans() {
    let (owner, tracker) = owner_with_tracker();
    let types = projected_types(false);
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    let (arrays, actual) = mixed(false);
    let count = CountOwner {
        original: owner.as_ref(),
        calls: AtomicUsize::new(0),
        stop: usize::MAX,
        cause: ExecutionFailure::from(KernelFailure::ResourceExhausted),
    };
    let input = capture(&spec, &arrays, &actual, &[3, 1, 0, 2, 3], &count);
    let loan = PreparedKeyBatch::from_captured(
        &spec,
        &input,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(loan.component_binding(0).unwrap(), &actual[0]);
    assert_eq!(
        loan.component_binding(1).unwrap().data_type(),
        types[1].data_type()
    );
    assert!(loan.component_binding(1).unwrap().nullable());
    let normalized = loan.tuples().physical_component(1).unwrap().0.clone();
    assert_eq!(normalized.data_type(), types[1].data_type());
    let requested = count.calls.load(Ordering::SeqCst);
    let checkpoints = Cell::new(0);
    for _ in 0..4_096 {
        let loan = PreparedKeyBatch::from_captured(
            &spec,
            &input,
            AggregateAllocator::new(tracker.clone()),
            &|| {
                checkpoints.set(checkpoints.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            &normalized,
            loan.tuples().physical_component(1).unwrap().0
        ));
        assert_eq!(loan.bytes(0).unwrap().unwrap().len(), 4);
    }
    assert_eq!(checkpoints.get(), 4_096);
    assert_eq!(count.calls.load(Ordering::SeqCst), requested);

    let (other_arrays, other_types) = mixed(true);
    let other = capture(
        &spec,
        &other_arrays,
        &other_types,
        &[0, 1, 2, 3],
        owner.as_ref(),
    );
    let other_loan = PreparedKeyBatch::from_captured(
        &spec,
        &other,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    let mut equal = [0; 6];
    super::super::pairs::compare_prepared_pairs(
        &loan,
        &[0, 1, 2, 3, 0, 2],
        &other_loan,
        &[3, 1, 2, 0, 2, 0],
        &mut equal,
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(equal, [1, 1, 1, 1, 0, 0]);
    let mut index = KeyIndex::new(
        spec.clone(),
        IndexPurpose::KeyIds,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let hashes = captured_hash(&spec, &input, index.policy(), &tracker, owner.as_ref());
    let other_hashes = captured_hash(&spec, &other, index.policy(), &tracker, owner.as_ref());
    for (occurrence, other_occurrence) in [(0, 3), (1, 1), (2, 0), (3, 2)] {
        assert_eq!(
            hashes.hash(occurrence).unwrap(),
            other_hashes.hash(other_occurrence).unwrap()
        );
    }
    assert_eq!(
        index
            .intern_batch(&loan, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &[
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(1)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(2)),
            KeyMatch::Found(KeyId(0))
        ]
    );
    let targets = projected_types(true);
    let exported = index
        .export_keys(
            &[KeyId(2), KeyId(1), KeyId(0), KeyId(2)],
            &targets,
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(exported.bindings(), &targets);
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("beta external value"),
            None,
            Some("alpha external value"),
            Some("beta external value")
        ]
    );
    let lists = exported.arrays()[1]
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(lists.data_type(), targets[1].data_type());
    assert_eq!(lists.value_offsets(), &[0, 1, 1, 3, 4]);
    assert_eq!(
        (0..4).map(|row| lists.is_null(row)).collect::<Vec<_>>(),
        vec![false, true, false, false]
    );
    assert_eq!(
        lists
            .values()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some(b"".as_slice()),
            Some(b"\xff".as_slice()),
            None,
            Some(b"".as_slice())
        ]
    );
    for array in exported.arrays() {
        array.to_data().validate_full().unwrap();
    }
}

#[test]
fn captured_loan_rejects_missing_or_foreign_receipts_and_new_subset_cannot_expand_domain() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
        Some("selected"),
        Some("not captured"),
    ]))];
    let plain = input(&arrays, std::slice::from_ref(&ty), &[0], owner.as_ref());
    let input = capture(
        &spec,
        &arrays,
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    let foreign = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    for (requested, page) in [(&spec, &plain), (&foreign, &input)] {
        let calls = Cell::new(0);
        assert_value_failure(
            &failure(PreparedKeyBatch::from_captured(
                requested,
                page,
                AggregateAllocator::new(tracker.clone()),
                &|| {
                    calls.set(calls.get() + 1);
                    Ok(())
                },
            )),
            ValueViolation::Layout,
        );
        assert_eq!(calls.get(), 1);
    }
    let loan = PreparedKeyBatch::from_captured(
        &spec,
        &input,
        AggregateAllocator::new(tracker),
        &|| Ok(()),
    )
    .unwrap();
    let mut expanded = SelectionSet::try_new(2).unwrap();
    expanded.insert(1).unwrap();
    assert_value_failure(
        &failure(loan.byte_subset(0, &expanded, &|| Ok(()))),
        ValueViolation::UndefinedRow,
    );
    expanded = SelectionSet::try_new(2).unwrap();
    expanded.insert(0).unwrap();
    assert_eq!(loan.byte_subset(0, &expanded, &|| Ok(())).unwrap().len(), 1);
    assert_eq!(loan.bytes(0).unwrap().unwrap().len(), 1);
}

struct Tag(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Tag {}
impl Drop for Tag {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn tagged<'a>(
    owner: &'a dyn InputConstructionOwner,
    drops: &Arc<AtomicUsize>,
) -> TaggedCreator<'a> {
    TaggedCreator {
        original: owner,
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    }
}

#[test]
fn captured_input_and_saved_hash_alias_keep_actual_source_and_preparation_creators_after_move() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8View, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut source_index = KeyIndex::new(
        spec.clone(),
        IndexPurpose::KeyIds,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let source = input(
        &[Arc::new(StringViewArray::from(vec![
            "source backing kept after caller exit",
        ]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    let source_hashes = index_hash(&source, &source_index, &tracker, owner.as_ref());
    let source_loan = PreparedKeyBatch::new(
        &spec,
        &source,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    source_index
        .intern_batch(&source_loan, &source_hashes, owner.as_ref(), &CopyControl)
        .unwrap();
    let source_drops = Arc::new(AtomicUsize::new(0));
    let preparation_drops = Arc::new(AtomicUsize::new(0));
    let source_owner = tagged(owner.as_ref(), &source_drops);
    let exported = source_index
        .export_keys(
            &[KeyId(0)],
            std::slice::from_ref(&ty),
            &source_owner,
            &CopyControl,
        )
        .unwrap();
    let preparation_owner = tagged(owner.as_ref(), &preparation_drops);
    let mut context = ValueConversionContext::default();
    let captured = KeyInput::new_for_spec(
        &spec,
        exported.arrays(),
        exported.bindings(),
        &readable(1),
        &[0],
        &mut context,
        &preparation_owner,
        &|| Ok(()),
    )
    .unwrap();
    context.clear();
    let captured_hashes = captured_hash(
        &spec,
        &captured,
        source_index.policy(),
        &tracker,
        owner.as_ref(),
    );
    drop(context);
    drop(exported);
    drop(source_owner);
    drop(preparation_owner);
    drop(source_loan);
    drop(source_hashes);
    drop(source);
    drop(source_index);
    drop(owner);
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    assert_eq!(preparation_drops.load(Ordering::SeqCst), 0);
    let (captured, hashes) = std::thread::spawn(move || {
        let loan = PreparedKeyBatch::from_captured(
            &spec,
            &captured,
            AggregateAllocator::new(tracker),
            &|| Ok(()),
        )
        .unwrap();
        assert_eq!(loan.len(), 1);
        assert_eq!(loan.bytes(0).unwrap().unwrap().len(), 1);
        drop(loan);
        (captured, captured_hashes)
    })
    .join()
    .unwrap();
    drop(captured);
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    assert_eq!(preparation_drops.load(Ordering::SeqCst), 0);
    assert!(hashes.participates(0).unwrap());
    drop(hashes);
    assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    assert_eq!(preparation_drops.load(Ordering::SeqCst), 1);
}

struct CountOwner<'a> {
    original: &'a dyn InputConstructionOwner,
    calls: AtomicUsize,
    stop: usize,
    cause: ExecutionFailure,
}
impl InputConstructionOwner for CountOwner<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.original.storage_lifetime()
    }
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.stop {
            Err(ConstructionFailure::from(self.cause.clone()))
        } else {
            self.original.request(layout)
        }
    }
    fn observe_opaque_upper_bound(&self, n: usize) -> Result<(), ConstructionFailure> {
        self.original.observe_opaque_upper_bound(n)
    }
    fn observe(
        &self,
        n: usize,
        path: &[crate::exec::chunk::ArrayPathStep],
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.original.observe(n, path, step)
    }
}

#[test]
fn captured_input_actual_growth_refusals_preserve_the_original_cause_without_publishing() {
    let (owner, _) = owner_with_tracker();
    let types = projected_types(false);
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    let (arrays, actual) = mixed(false);
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let baseline = CountOwner {
        original: owner.as_ref(),
        calls: AtomicUsize::new(0),
        stop: usize::MAX,
        cause: cause.clone(),
    };
    drop(capture(&spec, &arrays, &actual, &[3, 0, 1, 2], &baseline));
    let requests = baseline.calls.load(Ordering::SeqCst);
    assert!(requests > 3);
    for stop in [1, requests / 2, requests] {
        let refusing = CountOwner {
            original: owner.as_ref(),
            calls: AtomicUsize::new(0),
            stop,
            cause: cause.clone(),
        };
        let result = KeyInput::new_for_spec(
            &spec,
            &arrays,
            &actual,
            &readable(4),
            &[3, 0, 1, 2],
            &mut ValueConversionContext::default(),
            &refusing,
            &|| Ok(()),
        );
        assert_eq!(failure(result), cause);
        assert_eq!(refusing.calls.load(Ordering::SeqCst), stop);
    }
}

#[test]
fn captured_input_mid_capture_and_loan_entry_cancellation_preserve_original_occurrence() {
    let (owner, tracker) = owner_with_tracker();
    let types = projected_types(false);
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    let (arrays, actual) = mixed(false);
    let calls = Cell::new(0);
    let captured = KeyInput::new_for_spec(
        &spec,
        &arrays,
        &actual,
        &readable(4),
        &[3, 0, 1, 2],
        &mut ValueConversionContext::default(),
        owner.as_ref(),
        &|| {
            calls.set(calls.get() + 1);
            Ok(())
        },
    )
    .unwrap();
    assert!(calls.get() > 3);
    let cause = ExecutionFailure::from(KernelFailure::Cancelled);
    let stopped = Cell::new(0);
    let stop = calls.get() / 2;
    let result = KeyInput::new_for_spec(
        &spec,
        &arrays,
        &actual,
        &readable(4),
        &[3, 0, 1, 2],
        &mut ValueConversionContext::default(),
        owner.as_ref(),
        &|| {
            stopped.set(stopped.get() + 1);
            if stopped.get() == stop {
                Err(cause.clone())
            } else {
                Ok(())
            }
        },
    );
    assert!(failure(result).same_occurrence(&cause));
    assert_eq!(stopped.get(), stop);
    assert!(
        failure(PreparedKeyBatch::from_captured(
            &spec,
            &captured,
            AggregateAllocator::new(tracker.clone()),
            &|| Err(cause.clone())
        ))
        .same_occurrence(&cause)
    );
    assert_eq!(
        PreparedKeyBatch::from_captured(
            &spec,
            &captured,
            AggregateAllocator::new(tracker),
            &|| Ok(())
        )
        .unwrap()
        .len(),
        4
    );
}

// These metadata tests protect the B3 scope/value split at the original K
// author. Expected NULLs come from explicit source rows, not tuple hash policy.
#[test]
fn component_root_null_batch_separates_scope_value_and_original_occurrence_order() {
    let (owner, _) = owner_with_tracker();
    let actual: [_; 2] = std::array::from_fn(|_| FunctionValueType::new(DataType::Int32, true));
    let declared: [_; 2] = std::array::from_fn(|_| FunctionValueType::new(DataType::Int32, false));
    let spec = KeySpec::new(&declared, &[NullPolicy::Equal; 2], owner.as_ref(), &|| {
        Ok(())
    })
    .unwrap();
    let input = capture(
        &spec,
        &[
            Arc::new(Int32Array::from(vec![None, Some(1), Some(2), None])),
            Arc::new(Int32Array::from(vec![Some(10), None, Some(20), None])),
        ],
        &actual,
        &[2, 0, 1, 0, 3],
        owner.as_ref(),
    );
    let mut scope = [false; 5];
    let mut value = [false; 5];
    let mut tuple = [false; 5];
    input
        .component_range_root_nulls_observed(0..1, 0..5, &mut scope, &|| Ok(()))
        .unwrap();
    input
        .component_range_root_nulls_observed(1..2, 0..5, &mut value, &|| Ok(()))
        .unwrap();
    input
        .component_range_root_nulls_observed(0..2, 0..5, &mut tuple, &|| Ok(()))
        .unwrap();
    assert_eq!(scope, [false, true, false, true, true]);
    assert_eq!(value, [false, false, true, false, true]);
    assert_eq!(tuple, [false, true, true, true, true]);
    for (occurrence, expected) in tuple.into_iter().enumerate() {
        assert_eq!(
            input.row_has_null_observed(occurrence, &|| Ok(())).unwrap(),
            expected
        );
    }
    let mut middle = [false; 3];
    input
        .component_range_root_nulls_observed(1..2, 1..4, &mut middle, &|| Ok(()))
        .unwrap();
    assert_eq!(middle, [false, true, false]);
}

#[test]
fn component_root_null_batch_uses_sliced_dictionary_bottom_null_without_hashing() {
    let (owner, _) = owner_with_tracker();
    let types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Utf8, true),
    ];
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![
                Some(2),
                Some(1),
                None,
                Some(0),
                Some(2),
                Some(3),
                Some(0),
            ]),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c"), None])),
        )
        .unwrap(),
    );
    let input = capture(
        &spec,
        &[
            Arc::new(Int32Array::from(vec![
                None,
                Some(1),
                Some(1),
                None,
                Some(1),
            ])),
            dictionary.slice(1, 5),
        ],
        &types,
        &[4, 2, 0, 4, 3, 1],
        owner.as_ref(),
    );
    let hash_inits = super::super::local_hash::BYTE_HASH_INITS.with(Cell::get);
    let mut values = [false; 6];
    input
        .component_range_root_nulls_observed(1..2, 0..6, &mut values, &|| Ok(()))
        .unwrap();
    assert_eq!(values, [true, false, true, true, false, true]);
    assert_eq!(
        super::super::local_hash::BYTE_HASH_INITS.with(Cell::get),
        hash_inits
    );
    let mut selected = [true; 4];
    input
        .component_range_root_nulls_observed(1..2, 1..5, &mut selected, &|| Ok(()))
        .unwrap();
    assert_eq!(selected, [false, true, true, false]);
}

#[test]
fn component_root_null_batch_keeps_normalized_complex_and_logical_null_roots() {
    let (owner, _) = owner_with_tracker();
    let mut declared = projected_types(false).to_vec();
    declared.push(FunctionValueType::new(DataType::Null, true));
    let spec = KeySpec::new(&declared, &[NullPolicy::Equal; 3], owner.as_ref(), &|| {
        Ok(())
    })
    .unwrap();
    let (mut arrays, mut actual) = mixed(true);
    arrays.push(Arc::new(NullArray::new(4)));
    actual.push(FunctionValueType::new(DataType::Null, true));
    let input = capture(&spec, &arrays, &actual, &[3, 1, 0, 2, 1], owner.as_ref());
    let mut complex = [false; 5];
    input
        .component_range_root_nulls_observed(1..2, 0..5, &mut complex, &|| Ok(()))
        .unwrap();
    assert_eq!(complex, [false, true, false, false, true]);
    let mut logical = [false; 5];
    input
        .component_range_root_nulls_observed(2..3, 0..5, &mut logical, &|| Ok(()))
        .unwrap();
    assert_eq!(logical, [true; 5]);
}

#[test]
fn component_root_null_batch_checks_exact_ranges_before_writing_and_accepts_empty_ranges() {
    let (owner, _) = owner_with_tracker();
    let types: [_; 2] = std::array::from_fn(|_| FunctionValueType::new(DataType::Int32, true));
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    let input = capture(
        &spec,
        &[
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(Int32Array::from(vec![None, Some(2)])),
        ],
        &types,
        &[1, 0, 1],
        owner.as_ref(),
    );
    for (component_start, component_end, start, end, output_len) in [
        (2, 1, 0, 1, 1),
        (0, 3, 0, 1, 1),
        (3, 3, 0, 1, 1),
        (0, 1, 2, 1, 1),
        (0, 1, 0, 4, 4),
        (0, 1, 4, 4, 0),
        (0, 1, 0, 2, 1),
        (0, 1, 0, 1, 2),
    ] {
        let mut output = vec![true; output_len];
        let cause = failure(input.component_range_root_nulls_observed(
            component_start..component_end,
            start..end,
            &mut output,
            &|| Ok(()),
        ));
        assert_value_failure(&cause, novarocks_value_arrays::ValueViolation::Bounds);
        assert!(output.iter().all(|&value| value));
    }
    let mut empty_components = [true; 3];
    input
        .component_range_root_nulls_observed(2..2, 0..3, &mut empty_components, &|| Ok(()))
        .unwrap();
    assert_eq!(empty_components, [false; 3]);
    input
        .component_range_root_nulls_observed(0..2, 3..3, &mut [], &|| Ok(()))
        .unwrap();
}

#[test]
fn component_root_null_batch_preserves_original_entry_and_mid_batch_control_cause() {
    let (owner, _) = owner_with_tracker();
    let types: [_; 2] = std::array::from_fn(|_| FunctionValueType::new(DataType::Int32, false));
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    let arrays: [ArrayRef; 2] = [
        Arc::new(Int32Array::from(vec![1; 700])),
        Arc::new(Int32Array::from(vec![2; 700])),
    ];
    let input = capture(
        &spec,
        &arrays,
        &types,
        &(0..700).collect::<Vec<_>>(),
        owner.as_ref(),
    );
    let mut output = vec![true; 700];
    let cause = ExecutionFailure::from(KernelFailure::Cancelled);
    assert!(
        failure(
            input.component_range_root_nulls_observed(0..2, 0..700, &mut output, &|| Err(
                cause.clone()
            ),)
        )
        .same_occurrence(&cause)
    );
    assert!(output.iter().all(|&value| value));
    let calls = Cell::new(0);
    input
        .component_range_root_nulls_observed(0..2, 0..700, &mut output, &|| {
            calls.set(calls.get() + 1);
            Ok(())
        })
        .unwrap();
    assert_eq!(output, vec![false; 700]);
    assert!(calls.get() > 3);
    let stop = calls.get() / 2;
    calls.set(0);
    assert!(
        failure(
            input.component_range_root_nulls_observed(0..2, 0..700, &mut output, &|| {
                calls.set(calls.get() + 1);
                if calls.get() == stop {
                    Err(cause.clone())
                } else {
                    Ok(())
                }
            })
        )
        .same_occurrence(&cause)
    );
    assert_eq!(calls.get(), stop);
    input
        .component_range_root_nulls_observed(0..2, 0..700, &mut output, &|| Ok(()))
        .unwrap();
    assert_eq!(output, vec![false; 700]);
}
