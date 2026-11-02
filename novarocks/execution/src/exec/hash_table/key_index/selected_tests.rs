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

//! Routed morsels retain original occurrence coordinates and saved hash
//! provenance. Independent values/IDs remain the oracle, not another index.
use super::*;

#[test]
fn selected_intern_and_probe_reuse_one_preparation_and_preserve_source_coordinates() {
    let (owner, tracker) = owner_with_tracker();
    let types = [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8, true),
    ];
    let spec = KeySpec::new(
        &types,
        &[NullPolicy::Equal, NullPolicy::Unmatchable],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    let keys = input(
        &[
            Arc::new(Int32Array::from(vec![7, 8, 9, 10])),
            Arc::new(StringArray::from(vec![
                Some("alpha"),
                None,
                Some("beta"),
                Some("alpha"),
            ])),
        ],
        &types,
        &[2, 0, 3, 2, 1],
        owner.as_ref(),
    );
    let hashes = index_hash(&keys, &index, &tracker, owner.as_ref()).force_hash(0);
    let spec = index.spec().clone();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let before = super::super::local_hash::BYTE_HASH_INITS.with(Cell::get);
    let output = index
        .intern_selected(
            &prepared,
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
            KeyMatch::Found(KeyId(1)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Unmatchable,
            KeyMatch::Found(KeyId(2))
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
                occurrence: 1,
                row: 0,
                id: KeyId(1)
            },
            NewKey {
                occurrence: 2,
                row: 3,
                id: KeyId(2)
            }
        ]
    );
    assert_eq!(
        index
            .probe_selected(
                &prepared,
                &hashes,
                &[2, 0, 4, 1, 0],
                owner.as_ref(),
                &CopyControl
            )
            .unwrap()
            .matches(),
        &[
            KeyMatch::Found(KeyId(2)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Unmatchable,
            KeyMatch::Found(KeyId(1)),
            KeyMatch::Found(KeyId(0))
        ]
    );
    assert!(
        index
            .intern_selected(&prepared, &hashes, &[0, 1], owner.as_ref(), &CopyControl)
            .unwrap()
            .new_keys()
            .is_empty()
    );
    assert_eq!(
        super::super::local_hash::BYTE_HASH_INITS.with(Cell::get),
        before
    );
    assert_eq!(index.len(), 3);
    let exported = index
        .export_keys(
            &[KeyId(0), KeyId(1), KeyId(2)],
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
        &Int32Array::from(vec![9, 7, 10])
    );
    assert_eq!(
        exported.arrays()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec!["beta", "alpha", "alpha"])
    );
}

#[test]
fn selected_morsels_cross_work_boundaries_without_resigning_first_occurrence_ids() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Int64, false);
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
        &[Arc::new(Int64Array::from_iter_values(0..900))],
        std::slice::from_ref(&ty),
        &(0..900).collect::<Vec<_>>(),
        owner.as_ref(),
    );
    let hashes = index_hash(&keys, &index, &tracker, owner.as_ref());
    let spec = index.spec().clone();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let selected = (100..800).rev().collect::<Vec<_>>();
    let output = index
        .intern_selected(&prepared, &hashes, &selected, owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(output.matches().len(), 700);
    assert_eq!(output.new_keys().len(), 700);
    for (position, new) in output.new_keys().iter().enumerate() {
        assert_eq!(new.id, KeyId(position));
        assert_eq!((new.occurrence, new.row), (799 - position, 799 - position));
    }
    let output = index
        .export_keys(
            &[KeyId(0), KeyId(256), KeyId(699)],
            &[ty],
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(
        output.arrays()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![799, 543, 100])
    );
    assert_eq!(
        index
            .probe_selected(
                &prepared,
                &hashes,
                &[99, 100, 799, 800],
                owner.as_ref(),
                &CopyControl
            )
            .unwrap()
            .matches(),
        &[
            KeyMatch::Miss,
            KeyMatch::Found(KeyId(699)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Miss
        ]
    );
}

#[test]
fn selected_hash_presence_returns_only_membership_and_original_participation() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Unmatchable],
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
    let keys = input(
        &[Arc::new(StringArray::from(vec![
            Some("alpha"),
            Some("beta"),
            None,
            Some("gamma"),
        ]))],
        &[ty],
        &[3, 1, 0, 1, 2],
        owner.as_ref(),
    );
    let hashes = index_hash(&keys, &index, &tracker, owner.as_ref()).force_hash(0);
    let spec = index.spec().clone();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(
        index
            .include_selected_presence(&prepared, &hashes, &[2, 4, 2], owner.as_ref(), &CopyControl)
            .unwrap(),
        1
    );
    let found = index
        .contains_selected(
            &prepared,
            &hashes,
            &[3, 2, 4, 0],
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(found.present(), &[0, 1, 0, 0]);
    assert_eq!(found.participates(), &[1, 1, 0, 1]);
    assert_eq!(
        index
            .include_selected_presence(&prepared, &hashes, &[1, 0], owner.as_ref(), &CopyControl)
            .unwrap(),
        2
    );
    assert!(
        index
            .probe_selected(&prepared, &hashes, &[], owner.as_ref(), &CopyControl)
            .is_err()
    );
    assert_eq!(
        index
            .contains_selected(&prepared, &hashes, &[0, 1, 2], owner.as_ref(), &CopyControl)
            .unwrap()
            .present(),
        &[1, 1, 1]
    );
}

#[test]
fn selected_bounds_refusal_is_readonly_until_mutation_latches_the_original_cause() {
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
    let keys = input(
        &[Arc::new(Int32Array::from(vec![1, 2]))],
        &[ty],
        &[0, 1],
        owner.as_ref(),
    );
    let hashes = index_hash(&keys, &index, &tracker, owner.as_ref());
    let spec = index.spec().clone();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert!(
        index
            .intern_selected(&prepared, &hashes, &[], owner.as_ref(), &CopyControl)
            .unwrap()
            .matches()
            .is_empty()
    );
    let error = index
        .probe_selected(&prepared, &hashes, &[2], owner.as_ref(), &CopyControl)
        .err()
        .unwrap();
    assert_value_failure(&error, novarocks_value_arrays::ValueViolation::Bounds);
    assert_eq!(
        index
            .probe_selected(&prepared, &hashes, &[0], owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &[KeyMatch::Miss]
    );
    let error = index
        .intern_selected(&prepared, &hashes, &[0, 2], owner.as_ref(), &CopyControl)
        .err()
        .unwrap();
    assert_value_failure(&error, novarocks_value_arrays::ValueViolation::Bounds);
    assert!(
        index
            .probe_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .err()
            .unwrap()
            .same_occurrence(&error)
    );
    assert!(
        index
            .intern_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .err()
            .unwrap()
            .same_occurrence(&error)
    );
}
