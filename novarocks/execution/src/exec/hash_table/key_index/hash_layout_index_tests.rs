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

use super::super::hash_layout::HashLayout;
use super::*;

#[test]
fn closed_bulk_hash_index_routes_out_of_table_hash_and_rehashes_only_cached_values() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Int64, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let policy = LocalHashPolicy::new(&spec, owner.as_ref()).unwrap();
    let layout = HashLayout::for_bulk(4, 1025);
    assert!(layout.partitions() > 1);
    let route_basis = (0..64)
        .map(|bit| 1u64 << bit)
        .find(|&value| layout.route(value) != 0)
        .unwrap();
    assert_eq!(layout.table_hash(route_basis), 0);
    let mut index = KeyIndex::new_for_bulk_hash(
        spec.clone(),
        IndexPurpose::KeyIds,
        policy.clone(),
        layout,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1025));
    for rows in [
        (0..513).collect::<Vec<_>>(),
        (513..1025).collect::<Vec<_>>(),
    ] {
        let keys = input(
            std::slice::from_ref(&values),
            std::slice::from_ref(&ty),
            &rows,
            owner.as_ref(),
        );
        let cached: Vec<_> = rows.iter().map(|&row| route_basis | (row as u64)).collect();
        assert!(
            cached
                .iter()
                .all(|&full| layout.route(full) == layout.route(route_basis))
        );
        let hashes = hash(&keys, &spec, &policy, owner.as_ref()).with_hashes_for_test(&cached);
        let prepared = PreparedKeyBatch::new(
            &spec,
            &keys,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let result = index
            .intern_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap();
        for (ordinal, matched) in result.matches().iter().enumerate() {
            assert_eq!(*matched, KeyMatch::Found(KeyId(rows[ordinal])));
        }
    }
    assert_eq!(index.len(), 1025);
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    let rows = [1024, 0, 512, 1024];
    let keys = input(&[values], std::slice::from_ref(&ty), &rows, owner.as_ref());
    let cached: Vec<_> = rows.iter().map(|&row| route_basis | (row as u64)).collect();
    let hashes = hash(&keys, &spec, &policy, owner.as_ref()).with_hashes_for_test(&cached);
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
            .probe_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &[
            KeyMatch::Found(KeyId(1024)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(512)),
            KeyMatch::Found(KeyId(1024))
        ]
    );
    let output = index
        .export_keys(
            &[KeyId(1024), KeyId(0), KeyId(512)],
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
        &Int64Array::from(vec![1024, 0, 512])
    );
}
