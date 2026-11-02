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

fn insert(
    index: &mut KeyIndex,
    keys: &KeyInput,
    tracker: &Arc<MemTracker>,
    owner: &dyn InputConstructionOwner,
) {
    let spec = index.spec().clone();
    let prepared = PreparedKeyBatch::new(
        &spec,
        keys,
        AggregateAllocator::new(tracker.clone()),
        owner,
        &|| Ok(()),
    )
    .unwrap();
    let hashes = index_hash(keys, index, tracker, owner).force_hash(0);
    index
        .intern_batch(&prepared, &hashes, owner, &CopyControl)
        .unwrap();
}

#[test]
fn compact_preserves_collision_classes_requested_values_and_future_interns() {
    let (owner, tracker) = owner_with_tracker();
    let types = [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::new(
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            true,
        ),
    ];
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 3], owner.as_ref(), &|| Ok(())).unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    for batch in 0..3 {
        let mut lists = ListBuilder::new(Int32Builder::new());
        lists.values().append_value(batch);
        lists.values().append_null();
        lists.append(true);
        let keys = input(
            &[
                Arc::new(Int32Array::from(vec![batch])),
                Arc::new(StringArray::from(vec![if batch == 1 {
                    None
                } else {
                    Some("same long prefix with payload")
                }])),
                Arc::new(lists.finish()),
            ],
            &types,
            &[0],
            owner.as_ref(),
        );
        insert(&mut index, &keys, &tracker, owner.as_ref());
    }
    let ids = [KeyId(2), KeyId(0), KeyId(2), KeyId(1)];
    let before = index.read_keys(&ids, owner.as_ref(), &CopyControl).unwrap();
    let old_page_count = before.columns()[1].pages().len();
    let old_frames: Vec<Vec<u8>> = before.columns()[2]
        .pages()
        .iter()
        .flat_map(|page| {
            let KeyReadValues::Canonical { encoded, .. } = page.values() else {
                panic!("encoded column")
            };
            page.rows()
                .iter()
                .map(|row| encoded.value(row.row).to_vec())
                .collect::<Vec<_>>()
        })
        .collect();
    drop(before);
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    let after = index.read_keys(&ids, owner.as_ref(), &CopyControl).unwrap();
    assert!(after.columns()[1].pages().len() < old_page_count);
    let mut new_frames = vec![Vec::new(); ids.len()];
    for page in after.columns()[2].pages() {
        let KeyReadValues::Canonical { encoded, .. } = page.values() else {
            panic!("encoded column")
        };
        for row in page.rows() {
            new_frames[row.ordinal] = encoded.value(row.row).to_vec();
        }
    }
    let mut sorted_before = old_frames;
    sorted_before.sort();
    let mut sorted_after = new_frames.clone();
    sorted_after.sort();
    assert_eq!(sorted_after, sorted_before);
    assert_eq!(new_frames[0], new_frames[2]);
    drop(after);
    let exported = index
        .export_keys(&ids, &types, owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![2, 0, 2, 1])
    );
    assert_eq!(
        exported.arrays()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec![
            Some("same long prefix with payload"),
            Some("same long prefix with payload"),
            Some("same long prefix with payload"),
            None
        ])
    );
    let list = exported.arrays()[2]
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    for (ordinal, value) in [2, 0, 2, 1].into_iter().enumerate() {
        assert_eq!(
            list.value(ordinal)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![Some(value), None])
        );
    }
    // Continue through the same frozen hash identity and private collision chain.
    let mut lists = ListBuilder::new(Int32Builder::new());
    for value in [2, 3, 0] {
        lists.values().append_value(value);
        lists.values().append_null();
        lists.append(true);
    }
    let keys = input(
        &[
            Arc::new(Int32Array::from(vec![2, 3, 0])),
            Arc::new(StringArray::from(vec!["same long prefix with payload"; 3])),
            Arc::new(lists.finish()),
        ],
        &types,
        &[0, 1, 2, 1],
        owner.as_ref(),
    );
    insert(&mut index, &keys, &tracker, owner.as_ref());
    assert_eq!(index.len(), 4);
    let hashes = index_hash(&keys, &index, &tracker, owner.as_ref()).force_hash(0);
    let prepared = PreparedKeyBatch::new(
        index.spec(),
        &keys,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(
        index
            .probe_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &[
            KeyMatch::Found(KeyId(2)),
            KeyMatch::Found(KeyId(3)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(3))
        ]
    );
}

#[test]
fn compact_hash_presence_keeps_membership_without_exposing_ids_and_accepts_empty() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index = KeyIndex::new(
        spec,
        IndexPurpose::PresenceOnly,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    for values in [vec![Some("alpha"), None], vec![Some("beta"), Some("alpha")]] {
        let keys = input(
            &[Arc::new(StringArray::from(values))],
            std::slice::from_ref(&ty),
            &[0, 1],
            owner.as_ref(),
        );
        let spec = index.spec().clone();
        let prepared = PreparedKeyBatch::new(
            &spec,
            &keys,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let hashes = index_hash(&keys, &index, &tracker, owner.as_ref()).force_hash(0);
        index
            .include_presence(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap();
    }
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    let query = input(
        &[Arc::new(StringArray::from(vec![
            Some("alpha"),
            Some("gamma"),
            None,
            Some("beta"),
        ]))],
        std::slice::from_ref(&ty),
        &[0, 1, 2, 3],
        owner.as_ref(),
    );
    let hashes = index_hash(&query, &index, &tracker, owner.as_ref()).force_hash(0);
    let prepared = PreparedKeyBatch::new(
        index.spec(),
        &query,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(
        index
            .contains_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .present(),
        &[1, 0, 1, 1]
    );
    assert!(index.read_keys(&[], owner.as_ref(), &CopyControl).is_err());
    assert!(
        index
            .export_keys(&[], &[ty], owner.as_ref(), &CopyControl)
            .is_err()
    );
}

#[test]
fn compact_refusal_during_long_payload_growth_latches_original_cause() {
    struct DenyLarge<'a>(&'a dyn InputConstructionOwner, ExecutionFailure);
    impl InputConstructionOwner for DenyLarge<'_> {
        fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
            self.0.storage_lifetime()
        }
        fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
            if layout.size() > 100_000 {
                Err(ConstructionFailure::from(self.1.clone()))
            } else {
                self.0.request(layout)
            }
        }
        fn observe_opaque_upper_bound(&self, n: usize) -> Result<(), ConstructionFailure> {
            self.0.observe_opaque_upper_bound(n)
        }
        fn observe(
            &self,
            n: usize,
            path: &[crate::exec::chunk::ArrayPathStep],
            step: crate::exec::chunk::ArrayNormalizationStep<'_>,
        ) -> Result<(), ConstructionFailure> {
            self.0.observe(n, path, step)
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    let payload = "x".repeat(200_000);
    let keys = input(
        &[Arc::new(StringArray::from(vec![payload.as_str()]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    insert(&mut index, &keys, &tracker, owner.as_ref());
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusal = DenyLarge(owner.as_ref(), cause.clone());
    let error = index.compact(&refusal, &CopyControl).unwrap_err();
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error),
        novarocks_failure_contract::FailureProjection::failure_fact(&cause)
    );
    let again = index.compact(owner.as_ref(), &CopyControl).unwrap_err();
    assert_eq!(error, again);
    assert_eq!(
        index
            .export_keys(&[KeyId(0)], &[ty], owner.as_ref(), &CopyControl)
            .err()
            .unwrap(),
        error
    );
}

#[test]
fn compact_releases_previous_creator_only_after_last_escaped_buffer_alias() {
    use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Tag(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Tag {}
    impl Drop for Tag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    let keys = input(
        &[Arc::new(StringArray::from(vec![
            "old creator retained by escaped immutable key buffer",
        ]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let tagged = TaggedCreator {
        original: owner.as_ref(),
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    };
    insert(&mut index, &keys, &tracker, &tagged);
    let read = index
        .read_keys(&[KeyId(0)], owner.as_ref(), &CopyControl)
        .unwrap();
    let KeyReadValues::Bytes { array, .. } = read.columns()[0].pages()[0].values() else {
        panic!("byte key")
    };
    let alias = Arc::clone(array);
    let buffer = alias.to_data().buffers()[0].clone();
    drop(read);
    drop(tagged);
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        alias
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0),
        "old creator retained by escaped immutable key buffer"
    );
    drop(alias);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(buffer);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let exported = index
        .export_keys(&[KeyId(0)], &[ty], owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "old creator retained by escaped immutable key buffer"
    );
}

#[test]
fn compact_final_control_refusal_never_exposes_a_committed_prefix_as_success() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Control {
        calls: AtomicUsize,
        stop: usize,
    }
    impl novarocks_functions::KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.stop {
                Err(novarocks_functions::KernelFailure::DeadlineExceeded)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            panic!("compaction never waits")
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Int64, false);
    let keys = input(
        &[Arc::new(Int64Array::from_iter_values(0..513))],
        std::slice::from_ref(&ty),
        &(0..513).collect::<Vec<_>>(),
        owner.as_ref(),
    );
    let make = || {
        let spec = KeySpec::new(
            std::slice::from_ref(&ty),
            &[NullPolicy::Equal],
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let mut index =
            KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
        insert(&mut index, &keys, &tracker, owner.as_ref());
        index
    };
    let mut baseline = make();
    let count = Control {
        calls: AtomicUsize::new(0),
        stop: usize::MAX,
    };
    baseline.compact(owner.as_ref(), &count).unwrap();
    let calls = count.calls.load(Ordering::SeqCst);
    assert!(calls > 1);
    let mut index = make();
    let final_stop = Control {
        calls: AtomicUsize::new(0),
        stop: calls,
    };
    let expected = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
    assert_eq!(
        index.compact(owner.as_ref(), &final_stop).unwrap_err(),
        expected
    );
    assert_eq!(final_stop.calls.load(Ordering::SeqCst), calls);
    assert_eq!(
        index.compact(owner.as_ref(), &CopyControl).unwrap_err(),
        expected
    );
    assert_eq!(
        index
            .read_keys(&[KeyId(0)], owner.as_ref(), &CopyControl)
            .err()
            .unwrap(),
        expected
    );
}

#[test]
fn compact_preserves_actual_nullable_page_declarations_when_spec_root_is_required() {
    let (owner, tracker) = owner_with_tracker();
    let mut lists = ListBuilder::new(Int32Builder::new());
    lists.append(false);
    lists.values().append_value(7);
    lists.append(true);
    let sources: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![None, Some("kept")])),
        Arc::new(Int32Array::from(vec![None, Some(7)])),
        Arc::new(lists.finish()),
    ];
    for source in sources {
        let declared = FunctionValueType::new(source.data_type().clone(), false);
        let actual = FunctionValueType::new(source.data_type().clone(), true);
        let spec = KeySpec::new(
            std::slice::from_ref(&declared),
            &[NullPolicy::Equal],
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let mut index =
            KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
        let keys = input(
            &[source],
            std::slice::from_ref(&actual),
            &[0, 1],
            owner.as_ref(),
        );
        insert(&mut index, &keys, &tracker, owner.as_ref());
        index.compact(owner.as_ref(), &CopyControl).unwrap();
        let read = index
            .read_keys(&[KeyId(0), KeyId(1)], owner.as_ref(), &CopyControl)
            .unwrap();
        for page in read.columns()[0].pages() {
            let binding = match page.values() {
                KeyReadValues::Fixed { binding, .. } | KeyReadValues::Bytes { binding, .. } => {
                    *binding
                }
                KeyReadValues::NominalBytes {
                    logical_binding, ..
                }
                | KeyReadValues::Canonical {
                    logical_binding, ..
                } => *logical_binding,
            };
            assert!(binding.nullable());
        }
        drop(read);
        let nullable = index
            .export_keys(&[KeyId(0)], &[actual], owner.as_ref(), &CopyControl)
            .unwrap();
        assert_eq!(nullable.arrays()[0].null_count(), 1);
        let error = index
            .export_keys(&[KeyId(0)], &[declared], owner.as_ref(), &CopyControl)
            .err()
            .unwrap();
        assert_value_failure(&error, novarocks_value_arrays::ValueViolation::RequiredNull);
    }
}

fn location_texts(read: &OwnedKeyReadBatch<'_>) -> Vec<Option<String>> {
    let mut ordered = vec![None; read.len()];
    for page in read.columns()[0].pages() {
        let KeyReadValues::Bytes { array, .. } = page.values() else {
            panic!("expected the original typed byte page")
        };
        let values = array.as_any().downcast_ref::<StringViewArray>().unwrap();
        for selected in page.rows() {
            let value =
                (!values.is_null(selected.row)).then(|| values.value(selected.row).to_owned());
            assert!(ordered[selected.ordinal].replace(value).is_none());
        }
    }
    ordered
        .into_iter()
        .map(|v| v.expect("every requested ordinal is present"))
        .collect()
}

#[test]
fn class_location_compact_tail_and_recompact_keep_collision_ids_and_ordered_values() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
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
    for values in [
        vec![Some("gamma"), None],
        vec![Some("alpha"), Some("gamma")],
    ] {
        let keys = input(
            &[Arc::new(StringArray::from(values))],
            std::slice::from_ref(&ty),
            &[0, 1],
            owner.as_ref(),
        );
        insert(&mut index, &keys, &tracker, owner.as_ref());
    }
    assert_eq!(index.len(), 3);
    let before = super::super::local_hash::BYTE_HASH_INITS.with(Cell::get);
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    assert_eq!(
        super::super::local_hash::BYTE_HASH_INITS.with(Cell::get),
        before
    );
    let ids = [KeyId(2), KeyId(0), KeyId(2), KeyId(1)];
    let read = index.read_keys(&ids, owner.as_ref(), &CopyControl).unwrap();
    assert_eq!(
        location_texts(&read),
        vec![
            Some("alpha".into()),
            Some("gamma".into()),
            Some("alpha".into()),
            None
        ]
    );
    drop(read);

    let keys = input(
        &[Arc::new(StringArray::from(vec![
            Some("delta"),
            Some("alpha"),
            Some("epsilon"),
            Some("delta"),
            None,
        ]))],
        std::slice::from_ref(&ty),
        &[2, 0, 1, 4, 3, 0],
        owner.as_ref(),
    );
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let hashes = index_hash(&keys, &index, &tracker, owner.as_ref()).force_hash(0);
    let before = super::super::local_hash::BYTE_HASH_INITS.with(Cell::get);
    let inserted = index
        .intern_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
        .unwrap();
    let expected = [
        KeyMatch::Found(KeyId(3)),
        KeyMatch::Found(KeyId(4)),
        KeyMatch::Found(KeyId(2)),
        KeyMatch::Found(KeyId(1)),
        KeyMatch::Found(KeyId(4)),
        KeyMatch::Found(KeyId(4)),
    ];
    assert_eq!(inserted.matches(), &expected);
    assert_eq!(
        inserted.new_keys(),
        &[
            NewKey {
                occurrence: 0,
                row: 2,
                id: KeyId(3)
            },
            NewKey {
                occurrence: 1,
                row: 0,
                id: KeyId(4)
            },
        ]
    );
    assert_eq!(index.len(), 5);
    assert_eq!(
        index
            .probe_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &expected
    );
    let prefix_and_tail = index
        .read_class_range(1..4, owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(
        location_texts(&prefix_and_tail),
        vec![None, Some("alpha".into()), Some("epsilon".into())]
    );
    drop(prefix_and_tail);
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    assert_eq!(
        index
            .probe_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &expected
    );
    let range = index
        .read_class_range(1..4, owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(
        location_texts(&range),
        vec![None, Some("alpha".into()), Some("epsilon".into())]
    );
    drop(range);
    let ids = [KeyId(4), KeyId(2), KeyId(0), KeyId(3), KeyId(1), KeyId(4)];
    let read = index.read_keys(&ids, owner.as_ref(), &CopyControl).unwrap();
    let expected_values = vec![
        Some("delta".to_owned()),
        Some("alpha".to_owned()),
        Some("gamma".to_owned()),
        Some("epsilon".to_owned()),
        None,
        Some("delta".to_owned()),
    ];
    assert_eq!(location_texts(&read), expected_values);
    drop(read);
    let exported = index
        .export_keys(&ids, &[ty], owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_owned))
            .collect::<Vec<_>>(),
        expected_values
    );
    assert_eq!(
        super::super::local_hash::BYTE_HASH_INITS.with(Cell::get),
        before
    );
}

#[test]
fn class_range_fixed_reads_keep_local_ordinals_and_refuse_only_invalid_coordinates() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    for value in [40, 10, 30, 20] {
        let keys = input(
            &[Arc::new(Int32Array::from(vec![value]))],
            std::slice::from_ref(&ty),
            &[0],
            owner.as_ref(),
        );
        insert(&mut index, &keys, &tracker, owner.as_ref());
    }
    for compacted in [false, true] {
        if compacted {
            index.compact(owner.as_ref(), &CopyControl).unwrap();
        }
        let range = index
            .read_class_range(1..3, owner.as_ref(), &CopyControl)
            .unwrap();
        let mut values = vec![None; range.len()];
        for page in range.columns()[0].pages() {
            let KeyReadValues::Fixed { array, .. } = page.values() else {
                panic!("expected a typed fixed loan")
            };
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for selected in page.rows() {
                assert!(
                    values[selected.ordinal]
                        .replace(array.value(selected.row))
                        .is_none()
                );
            }
        }
        assert_eq!(values, vec![Some(10), Some(30)]);
        drop(range);
        assert_eq!(
            index
                .read_class_range(4..4, owner.as_ref(), &CopyControl)
                .unwrap()
                .len(),
            0
        );
        for invalid in [std::ops::Range { start: 3, end: 2 }, 0..5] {
            let error = index
                .read_class_range(invalid, owner.as_ref(), &CopyControl)
                .err()
                .unwrap();
            assert_value_failure(&error, ValueViolation::Bounds);
        }
    }
}

fn chunk_error<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    match result {
        Err(cause) => cause,
        Ok(_) => panic!("invalid chunk publication cannot succeed"),
    }
}
fn chunk_index(
    spec: &KeySpec,
    values: &[Option<&str>],
    tracker: &Arc<MemTracker>,
    owner: &dyn InputConstructionOwner,
) -> KeyIndex {
    let mut index =
        KeyIndex::new(spec.clone(), IndexPurpose::KeyIds, tracker.clone(), owner).unwrap();
    for &value in values {
        let keys = input(
            &[Arc::new(StringArray::from(vec![value]))],
            spec.types(),
            &[0],
            owner,
        );
        insert(&mut index, &keys, tracker, owner);
    }
    index
}
fn drain_chunk_retirement(mut retired: super::super::table::RetiredClassBatches) {
    let original = retired.remaining_batches();
    for remaining in (0..original).rev() {
        assert!(retired.retire_one());
        assert_eq!(retired.remaining_batches(), remaining);
    }
    assert!(!retired.retire_one());
}

#[test]
fn class_chunk_scoped_workers_reverse_results_keep_dense_ids_and_append_after_retirement() {
    use super::super::table::ClassSourcePlan;
    use std::{num::NonZeroUsize, sync::Barrier};
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let texts: Vec<_> = (0..32).map(|n| format!("key{n:02}")).collect();
    let mut values: Vec<_> = texts.iter().map(|s| Some(s.as_str())).collect();
    values.push(None);
    let mut index = chunk_index(&spec, &values, &tracker, owner.as_ref());
    // Hundreds of tiny original batches repeat a small fixed value domain.
    for i in 0..256 {
        let keys = input(
            &[Arc::new(StringArray::from(vec![values[i % values.len()]]))],
            std::slice::from_ref(&ty),
            &[0],
            owner.as_ref(),
        );
        insert(&mut index, &keys, &tracker, owner.as_ref());
    }
    assert_eq!(index.len(), 33);
    let ids: Vec<_> = (0..index.len()).map(KeyId).collect();
    let old_pages = index
        .read_keys(&ids, owner.as_ref(), &CopyControl)
        .unwrap()
        .columns()[0]
        .pages()
        .len();
    let source = ClassSourcePlan::new(
        index,
        NonZeroUsize::new(17).unwrap(),
        owner.as_ref(),
        &CopyControl,
    )
    .unwrap();
    assert_eq!(source.chunk_count(), 2);
    let barrier = Barrier::new(2);
    let chunks = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            barrier.wait();
            let result = source
                .materialize_chunk(0, owner.as_ref(), &CopyControl)
                .unwrap();
            assert_eq!(super::super::local_hash::BYTE_HASH_INITS.with(Cell::get), 0);
            result
        });
        let second = scope.spawn(|| {
            barrier.wait();
            let result = source
                .materialize_chunk(1, owner.as_ref(), &CopyControl)
                .unwrap();
            assert_eq!(super::super::local_hash::BYTE_HASH_INITS.with(Cell::get), 0);
            result
        });
        vec![second.join().unwrap(), first.join().unwrap()]
    });
    assert_eq!(
        Arc::strong_count(&source),
        1,
        "new receipts do not own the old source"
    );
    let (mut index, retired) =
        ClassSourcePlan::publish(source, chunks, owner.as_ref(), &CopyControl).unwrap();
    assert!(retired.remaining_batches() > 2);
    let read = index.read_keys(&ids, owner.as_ref(), &CopyControl).unwrap();
    assert!(read.columns()[0].pages().len() < old_pages);
    assert_eq!(
        location_texts(&read),
        values
            .iter()
            .map(|v| v.map(str::to_owned))
            .collect::<Vec<_>>()
    );
    drop(read);
    drain_chunk_retirement(retired);
    let keys = input(
        &[Arc::new(StringArray::from(vec![
            Some("key31"),
            Some("new B"),
            Some("key00"),
            Some("new A"),
            Some("new B"),
            None,
        ]))],
        std::slice::from_ref(&ty),
        &[3, 1, 4, 0, 5, 2],
        owner.as_ref(),
    );
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let hashes = index_hash(&keys, &index, &tracker, owner.as_ref()).force_hash(0);
    let expected = [
        KeyMatch::Found(KeyId(33)),
        KeyMatch::Found(KeyId(34)),
        KeyMatch::Found(KeyId(34)),
        KeyMatch::Found(KeyId(31)),
        KeyMatch::Found(KeyId(32)),
        KeyMatch::Found(KeyId(0)),
    ];
    assert_eq!(
        index
            .intern_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &expected
    );
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    assert_eq!(
        index
            .probe_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &expected
    );
    let ids = [
        KeyId(34),
        KeyId(17),
        KeyId(32),
        KeyId(0),
        KeyId(33),
        KeyId(34),
    ];
    let exported = index
        .export_keys(&ids, &[ty], owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("new B"),
            Some("key17"),
            None,
            Some("key00"),
            Some("new A"),
            Some("new B")
        ]
    );
}

#[test]
fn class_chunk_publication_rejects_missing_foreign_repeated_and_still_shared_sources() {
    use super::super::table::ClassSourcePlan;
    use std::num::NonZeroUsize;
    enum Refusal {
        Missing,
        Foreign,
        Repeated,
    }
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    for refusal in [Refusal::Missing, Refusal::Foreign, Refusal::Repeated] {
        let source = ClassSourcePlan::new(
            chunk_index(
                &spec,
                &[Some("alpha"), Some("beta"), Some("gamma")],
                &tracker,
                owner.as_ref(),
            ),
            NonZeroUsize::new(2).unwrap(),
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
        let first = source
            .materialize_chunk(0, owner.as_ref(), &CopyControl)
            .unwrap();
        let chunks = match refusal {
            Refusal::Missing => vec![first],
            Refusal::Repeated => vec![
                first,
                source
                    .materialize_chunk(0, owner.as_ref(), &CopyControl)
                    .unwrap(),
            ],
            Refusal::Foreign => {
                let other = ClassSourcePlan::new(
                    chunk_index(
                        &spec,
                        &[Some("alpha"), Some("beta"), Some("gamma")],
                        &tracker,
                        owner.as_ref(),
                    ),
                    NonZeroUsize::new(2).unwrap(),
                    owner.as_ref(),
                    &CopyControl,
                )
                .unwrap();
                let foreign = other
                    .materialize_chunk(1, owner.as_ref(), &CopyControl)
                    .unwrap();
                assert_eq!(Arc::strong_count(&other), 1);
                drop(other);
                vec![first, foreign]
            }
        };
        assert_value_failure(
            &chunk_error(ClassSourcePlan::publish(
                source,
                chunks,
                owner.as_ref(),
                &CopyControl,
            )),
            ValueViolation::Layout,
        );
    }
    let source = ClassSourcePlan::new(
        chunk_index(&spec, &[Some("alpha")], &tracker, owner.as_ref()),
        NonZeroUsize::new(2).unwrap(),
        owner.as_ref(),
        &CopyControl,
    )
    .unwrap();
    let chunk = source
        .materialize_chunk(0, owner.as_ref(), &CopyControl)
        .unwrap();
    let outstanding_source = Arc::clone(&source);
    assert_value_failure(
        &chunk_error(ClassSourcePlan::publish(
            source,
            vec![chunk],
            owner.as_ref(),
            &CopyControl,
        )),
        ValueViolation::Layout,
    );
    assert_eq!(outstanding_source.chunk_count(), 1);
    drop(outstanding_source);
}

#[test]
fn class_chunk_retirement_and_last_escaped_buffer_keep_separate_old_and_new_creators() {
    use super::super::table::ClassSourcePlan;
    use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
    };
    struct Tag(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Tag {}
    impl Drop for Tag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    let keys = input(
        &[Arc::new(StringArray::from(vec![
            "old and new escaped buffers keep their real creators",
        ]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    let old_drops = Arc::new(AtomicUsize::new(0));
    let new_drops = Arc::new(AtomicUsize::new(0));
    let old_owner = TaggedCreator {
        original: owner.as_ref(),
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(old_drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    };
    insert(&mut index, &keys, &tracker, &old_owner);
    let read = index
        .read_keys(&[KeyId(0)], owner.as_ref(), &CopyControl)
        .unwrap();
    let KeyReadValues::Bytes { array, .. } = read.columns()[0].pages()[0].values() else {
        panic!("old byte loan")
    };
    let old_alias = Arc::clone(array);
    let old_buffer = old_alias.to_data().buffers()[0].clone();
    drop(read);
    drop(old_owner);
    let new_owner = TaggedCreator {
        original: owner.as_ref(),
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(new_drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    };
    let source = ClassSourcePlan::new(
        index,
        NonZeroUsize::new(2).unwrap(),
        &new_owner,
        &CopyControl,
    )
    .unwrap();
    let chunk = source
        .materialize_chunk(0, &new_owner, &CopyControl)
        .unwrap();
    assert_eq!(Arc::strong_count(&source), 1);
    let (index, retired) =
        ClassSourcePlan::publish(source, vec![chunk], &new_owner, &CopyControl).unwrap();
    assert_eq!(old_drops.load(Ordering::SeqCst), 0);
    drain_chunk_retirement(retired);
    assert_eq!(old_drops.load(Ordering::SeqCst), 0);
    drop(old_alias);
    assert_eq!(old_drops.load(Ordering::SeqCst), 0);
    drop(old_buffer);
    assert_eq!(old_drops.load(Ordering::SeqCst), 1);
    let read = index
        .read_keys(&[KeyId(0)], owner.as_ref(), &CopyControl)
        .unwrap();
    let KeyReadValues::Bytes { array, .. } = read.columns()[0].pages()[0].values() else {
        panic!("new byte loan")
    };
    let new_alias = Arc::clone(array);
    let new_buffer = new_alias.to_data().buffers()[0].clone();
    drop(read);
    drop(new_owner);
    drop(index);
    assert_eq!(new_drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        new_alias
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0),
        "old and new escaped buffers keep their real creators"
    );
    drop(new_alias);
    assert_eq!(new_drops.load(Ordering::SeqCst), 0);
    drop(new_buffer);
    assert_eq!(new_drops.load(Ordering::SeqCst), 1);
}

// HLL states are created by the original sketch implementation. K treats their
// serialized bytes as nominal opaque values; it does not parse or recode them.
fn nominal_hll_values() -> (Vec<u8>, Vec<u8>) {
    use novarocks_functions::datasketches_hll::{HllHandle, HllTargetType};
    let mut sketch = HllHandle::new_unreserved(12, HllTargetType::Hll8).unwrap();
    sketch.update_hash_unreserved(17).unwrap();
    let first = sketch.serialize().unwrap();
    sketch.update_hash_unreserved(19).unwrap();
    (first, sketch.serialize().unwrap())
}
fn nominal_byte_fixture(
    dictionary: bool,
    first: &[u8],
    second: &[u8],
) -> (Vec<ArrayRef>, Vec<FunctionValueType>, Vec<usize>) {
    use novarocks_type_contract::ValueLogicalType;
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let hll = FunctionValueType::try_with_logical_type(
        if dictionary {
            DataType::LargeBinary
        } else {
            DataType::Binary
        },
        true,
        ValueLogicalType::Hll,
    )
    .unwrap();
    if dictionary {
        let codes = Int32Array::from(vec![2, 0, 1]);
        (
            vec![
                Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        codes,
                        Arc::new(StringArray::from(vec![Some("[1]"), None, Some("[2]")])),
                    )
                    .unwrap(),
                ),
                // The frozen dictionary vocabulary is string-only. HLL uses
                // its legal wider plain carrier with the same logical rows.
                Arc::new(LargeBinaryArray::from(vec![
                    Some(second),
                    Some(first),
                    None,
                ])),
                Arc::new(StringViewArray::from(vec![
                    Some("beta"),
                    Some("alpha"),
                    None,
                ])),
                Arc::new(BinaryViewArray::from(vec![
                    Some(b"B".as_slice()),
                    Some(b"A".as_slice()),
                    None,
                ])),
            ],
            vec![
                json,
                hll,
                FunctionValueType::new(DataType::Utf8View, true),
                FunctionValueType::new(DataType::BinaryView, true),
            ],
            vec![1, 2, 0, 1],
        )
    } else {
        (
            vec![
                Arc::new(StringArray::from(vec![Some("[1]"), None, Some("[2]")])),
                Arc::new(BinaryArray::from(vec![Some(first), None, Some(second)])),
                Arc::new(StringArray::from(vec![Some("alpha"), None, Some("beta")])),
                Arc::new(BinaryArray::from(vec![
                    Some(b"A".as_slice()),
                    None,
                    Some(b"B".as_slice()),
                ])),
            ],
            vec![
                json,
                hll,
                FunctionValueType::new(DataType::Utf8, true),
                FunctionValueType::new(DataType::Binary, true),
            ],
            vec![0, 1, 2, 0],
        )
    }
}
fn assert_nominal_byte_export(exported: &ExportedKeys, first: &[u8], second: &[u8], null: bool) {
    let mut strings = vec![Some("[2]"), Some("[1]"), Some("[1]")];
    let mut hll = vec![Some(second), Some(first), Some(first)];
    let mut physical_text = vec![Some("beta"), Some("alpha"), Some("alpha")];
    let mut physical_bytes = vec![
        Some(b"B".as_slice()),
        Some(b"A".as_slice()),
        Some(b"A".as_slice()),
    ];
    if null {
        strings.push(None);
        hll.push(None);
        physical_text.push(None);
        physical_bytes.push(None);
    }
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        strings
    );
    let actual_hll = match exported.arrays()[1].data_type() {
        DataType::Binary => exported.arrays()[1]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        DataType::LargeBinary => exported.arrays()[1]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        other => panic!("nominal HLL output must retain its signed byte layout: {other:?}"),
    };
    assert_eq!(actual_hll, hll);
    assert_eq!(
        exported.arrays()[2]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        physical_text
    );
    assert_eq!(
        exported.arrays()[3]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        physical_bytes
    );
}
#[test]
fn nominal_raw_bytes_preserve_legal_domains_cross_carriers_dictionaries_and_both_null_policies() {
    for null_policy in [NullPolicy::Equal, NullPolicy::Unmatchable] {
        let (owner, tracker) = owner_with_tracker();
        let (first, second) = nominal_hll_values();
        let (arrays, types, rows) = nominal_byte_fixture(false, &first, &second);
        let spec = KeySpec::new(&types, &[null_policy; 4], owner.as_ref(), &|| Ok(())).unwrap();
        let mut index = KeyIndex::new(
            spec.clone(),
            IndexPurpose::KeyIds,
            tracker.clone(),
            owner.as_ref(),
        )
        .unwrap();
        let keys = input(&arrays, &types, &rows, owner.as_ref());
        let prepared = PreparedKeyBatch::new(
            &spec,
            &keys,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let hashes = index_hash(&keys, &index, &tracker, owner.as_ref()).force_hash(0);
        let null_equal = null_policy == NullPolicy::Equal;
        let expected = [
            KeyMatch::Found(KeyId(0)),
            if null_equal {
                KeyMatch::Found(KeyId(1))
            } else {
                KeyMatch::Unmatchable
            },
            KeyMatch::Found(KeyId(if null_equal { 2 } else { 1 })),
            KeyMatch::Found(KeyId(0)),
        ];
        assert_eq!(
            index
                .intern_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
                .unwrap()
                .matches(),
            &expected
        );
        let (other_arrays, other_types, other_rows) = nominal_byte_fixture(true, &first, &second);
        let other = input(&other_arrays, &other_types, &other_rows, owner.as_ref());
        let other_prepared = PreparedKeyBatch::new(
            &spec,
            &other,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let other_hashes = index_hash(&other, &index, &tracker, owner.as_ref()).force_hash(0);
        let mut ids = vec![KeyId(if null_equal { 2 } else { 1 }), KeyId(0), KeyId(0)];
        if null_equal {
            ids.push(KeyId(1));
        }
        for after_compact in [false, true] {
            if after_compact {
                index.compact(owner.as_ref(), &CopyControl).unwrap();
            }
            assert_eq!(
                index
                    .probe_batch(&other_prepared, &other_hashes, owner.as_ref(), &CopyControl)
                    .unwrap()
                    .matches(),
                &expected
            );
            assert_eq!(
                index
                    .intern_batch(&other_prepared, &other_hashes, owner.as_ref(), &CopyControl)
                    .unwrap()
                    .matches(),
                &expected
            );
            let read = index.read_keys(&ids, owner.as_ref(), &CopyControl).unwrap();
            for (column, source) in read.columns().iter().enumerate() {
                for page in source.pages() {
                    if column < 2 {
                        let KeyReadValues::NominalBytes {
                            raw,
                            logical_binding,
                            page: admitted,
                        } = page.values()
                        else {
                            panic!("nominal bytes require an explicit raw-storage loan")
                        };
                        assert_eq!(*logical_binding, &types[column]);
                        assert_eq!(
                            raw.data_type(),
                            if column == 0 {
                                &DataType::Utf8View
                            } else {
                                &DataType::BinaryView
                            }
                        );
                        assert_eq!(admitted.is_utf8(), column == 0);
                    } else {
                        let KeyReadValues::Bytes { array, binding, .. } = page.values() else {
                            panic!("ordinary byte values retain their original view binding")
                        };
                        assert_eq!(array.data_type(), binding.data_type());
                    }
                }
            }
            drop(read);
            for large in [false, true] {
                let mut targets = types.clone();
                targets[1] = FunctionValueType::try_with_logical_type(
                    if large {
                        DataType::LargeBinary
                    } else {
                        DataType::Binary
                    },
                    true,
                    novarocks_type_contract::ValueLogicalType::Hll,
                )
                .unwrap();
                let exported = index
                    .export_keys(&ids, &targets, owner.as_ref(), &CopyControl)
                    .unwrap();
                assert_eq!(exported.bindings(), &targets);
                assert_nominal_byte_export(&exported, &first, &second, null_equal);
            }
            if null_equal {
                let mut required = types.clone();
                required[0] = required[0].with_nullable(false).unwrap();
                assert_value_failure(
                    &index
                        .export_keys(&[KeyId(1)], &required, owner.as_ref(), &CopyControl)
                        .err()
                        .unwrap(),
                    ValueViolation::RequiredNull,
                );
                // A failed readonly export cannot poison the published index.
                assert_eq!(
                    index
                        .probe_batch(&other_prepared, &other_hashes, owner.as_ref(), &CopyControl)
                        .unwrap()
                        .matches(),
                    &expected
                );
            }
        }
    }
}

#[test]
fn nominal_raw_bytes_follow_the_real_bulk_hash_graph_without_reframing() {
    use std::num::NonZeroUsize;
    for null_policy in [NullPolicy::Equal, NullPolicy::Unmatchable] {
        let (owner, tracker) = owner_with_tracker();
        let (first, second) = nominal_hll_values();
        let (_, types, _) = nominal_byte_fixture(false, &first, &second);
        let spec = KeySpec::new(&types, &[null_policy; 4], owner.as_ref(), &|| Ok(())).unwrap();
        let domain = BulkDomain::new(&spec, owner.as_ref(), &CopyControl).unwrap();
        let host: Arc<dyn novarocks_functions::AggregateStateAllocator> =
            crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone());
        let capture = CaptureWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(tracker.clone()),
            host: Arc::clone(&host),
            control: &CopyControl,
        };
        let mut captures = Vec::new();
        for dictionary in [false, true] {
            let mut writer = domain.begin_capture(&capture).unwrap();
            let (arrays, bindings, occurrences) = nominal_byte_fixture(dictionary, &first, &second);
            writer
                .push_evaluated(
                    EvaluatedPage {
                        arrays: &arrays,
                        bindings: &bindings,
                        readable: &readable(3),
                        occurrences: &occurrences,
                    },
                    &mut ValueConversionContext::default(),
                    &capture,
                )
                .unwrap();
            captures.push(writer.seal(&capture).unwrap());
        }
        let complete = CompleteInput::from_captures(
            BulkInput::new(&domain, captures, &capture).unwrap(),
            IndexPurpose::KeyIds,
            &capture,
        )
        .unwrap();
        let ports = BulkWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(tracker.clone()),
            host: Arc::clone(&host),
            control: &CopyControl,
        };
        let graph = BulkTaskGraph::begin(
            complete,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            &ports,
        )
        .unwrap();
        let mut pending = Vec::new();
        loop {
            // A retained route cursor can wait for frontier space. Continue
            // claiming eager merge work before advancing each retained task
            // once; do not try to finish one route before claiming its merge.
            while let BulkClaim::Task(task) = graph.claim(&ports).unwrap() {
                pending.push(task);
            }
            if pending.is_empty() {
                break;
            }
            let mut next = Vec::new();
            for mut task in pending {
                if task.advance(1, &ports).unwrap() {
                    let mut complete = task.finish().unwrap();
                    graph.complete(&mut complete, &ports).unwrap();
                } else {
                    next.push(task);
                }
            }
            pending = next;
        }
        let ready = graph.take_ready(&ports).unwrap().unwrap();
        let (arrays, bindings, rows) = nominal_byte_fixture(true, &first, &second);
        let keys = input(&arrays, &bindings, &rows, owner.as_ref());
        let prepared = PreparedKeyBatch::new(
            ready.spec(),
            &keys,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let found = ready
            .probe(&prepared, None, owner.as_ref(), &CopyControl)
            .unwrap();
        let BulkMatch::Found(first_id) = found.matches()[0] else {
            panic!("first nominal key")
        };
        let BulkMatch::Found(second_id) = found.matches()[2] else {
            panic!("second nominal key")
        };
        assert_eq!(found.matches()[3], BulkMatch::Found(first_id));
        assert_ne!(first_id, second_id);
        let mut ids = vec![second_id, first_id, first_id];
        if null_policy == NullPolicy::Equal {
            let BulkMatch::Found(null_id) = found.matches()[1] else {
                panic!("NULL equality class")
            };
            assert_ne!(null_id, first_id);
            assert_ne!(null_id, second_id);
            ids.push(null_id);
        } else {
            assert_eq!(found.matches()[1], BulkMatch::Unmatchable);
        }
        let read = ready.read_keys(&ids, owner.as_ref(), &CopyControl).unwrap();
        assert!(
            read.columns()[0]
                .pages()
                .iter()
                .all(|page| matches!(page.values(), KeyReadValues::NominalBytes { .. }))
        );
        drop(read);
        let exported = ready
            .export_keys(
                &ids,
                &types,
                owner.as_ref(),
                Arc::clone(&host),
                &CopyControl,
            )
            .unwrap();
        assert_nominal_byte_export(&exported, &first, &second, null_policy == NullPolicy::Equal);
    }
}

#[test]
fn nominal_raw_byte_compaction_retains_the_actual_old_creator_until_the_last_buffer_alias() {
    use novarocks_type_contract::{
        ValueLogicalType, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Tag(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Tag {}
    impl Drop for Tag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ty =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    let keys = input(
        &[Arc::new(StringArray::from(vec![
            "{\"message\":\"the private raw JSON view retains its original creator\"}",
        ]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let tagged = TaggedCreator {
        original: owner.as_ref(),
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(Arc::clone(&drops)))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    };
    insert(&mut index, &keys, &tracker, &tagged);
    let read = index
        .read_keys(&[KeyId(0)], owner.as_ref(), &CopyControl)
        .unwrap();
    let KeyReadValues::NominalBytes { raw, .. } = read.columns()[0].pages()[0].values() else {
        panic!("explicit nominal raw storage")
    };
    let alias = Arc::clone(raw);
    let buffer = alias.to_data().buffers()[0].clone();
    drop(read);
    drop(tagged);
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(alias);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(buffer);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let exported = index
        .export_keys(&[KeyId(0)], &[ty], owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(exported.arrays()[0].data_type(), &DataType::Utf8);
}
