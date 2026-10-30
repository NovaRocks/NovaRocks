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

//! Public HashTable integration properties. Synthetic basis bits exercise
//! routing and retained entropy independently of any key hash algorithm.
use super::hash_layout::HashLayout;
use hashbrown::HashTable;

#[test]
fn underestimated_hints_grow_with_cached_table_hashes_under_maximally_skewed_routing() {
    const ELIGIBLE: usize = 1025;
    let layout = HashLayout::for_bulk(4, ELIGIBLE);
    // All hashes have route 0, so there is no eligible/P assumption.
    for initial_hint in [0, 1, 7, 17, 100] {
        let mut table = HashTable::<(u64, u64)>::new();
        assert!(layout.admits_reservation(0, initial_hint));
        table.try_reserve(initial_hint, |entry| entry.1).unwrap();
        for row in 0..ELIGIBLE {
            let full = row as u64;
            assert_eq!(layout.route(full), 0);
            let table_hash = layout.table_hash(full);
            assert!(layout.admits_reservation(table.len(), 1));
            table.try_reserve(1, |entry| entry.1).unwrap();
            table.insert_unique(table_hash, (full, table_hash), |entry| entry.1);
            assert!(table.capacity() >= table.len());
            // For ELIGIBLE=1025, the locked constructor's rounded upper bound
            // is 2048 buckets / 1792 entries; this is a container layout fact,
            // not a hash golden or production threshold.
            assert!(table.capacity() <= 1792);
        }
        for full in 0..ELIGIBLE as u64 {
            let found = table
                .find(layout.table_hash(full), |entry| entry.0 == full)
                .unwrap();
            assert_eq!(found.0, full);
            assert_eq!(found.1, layout.table_hash(full));
        }
    }
}

#[test]
fn changing_only_route_bits_uses_the_same_table_hash_in_separate_public_tables() {
    let layout = HashLayout::for_bulk(3, 112);
    let mut tables: Vec<HashTable<(u64, u64)>> =
        (0..layout.partitions()).map(|_| HashTable::new()).collect();
    // Find a basis bit that changes route and not table hash without inspecting
    // private field values or relying on a specific chosen middle window.
    let route_bits: Vec<u64> = (0..64)
        .map(|bit| 1u64 << bit)
        .filter(|&bit| layout.route(bit) != 0)
        .collect();
    for pattern in 0..layout.partitions() {
        let route = route_bits
            .iter()
            .enumerate()
            .fold(0u64, |hash, (bit, value)| {
                hash | if pattern & (1usize << bit) != 0 {
                    *value
                } else {
                    0
                }
            });
        for bucket in 0..64u64 {
            let full = route | bucket;
            let table_hash = layout.table_hash(full);
            assert_eq!(table_hash, bucket);
            let table = &mut tables[layout.route(full)];
            table.try_reserve(1, |entry| entry.1).unwrap();
            table.insert_unique(table_hash, (full, table_hash), |entry| entry.1);
        }
    }
    for (partition, table) in tables.iter().enumerate() {
        assert_eq!(table.len(), 64);
        for &(full, hash) in table.iter() {
            assert_eq!(layout.route(full), partition);
            assert!(table.find(hash, |entry| entry.0 == full).is_some());
        }
    }
}
