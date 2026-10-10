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
use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
};

#[test]
fn fresh_string_table_has_independent_small_and_load_factor_layout_goldens() {
    assert_eq!(Layout::new::<(String, String)>().size(), 48);
    assert_eq!(Layout::new::<(String, String)>().align(), 8);
    let width = group_layout().unwrap().size();
    for (count, buckets) in [
        (1, 4),
        (3, 4),
        (7, 8),
        (14, 16),
        (15, 32),
        (28, 32),
        (29, 64),
    ] {
        let f = fresh_table_layout::<String, String>(count).unwrap();
        let aligned_pairs = (48 * buckets + width.max(8) - 1) & !(width.max(8) - 1);
        let golden =
            Layout::from_size_align(aligned_pairs + buckets + width, width.max(8)).unwrap();
        assert_eq!(f.buckets, buckets);
        assert_eq!(f.layout, Some(golden));
        assert_eq!(f.allocation_requests_upper_bound, 1);
        assert_eq!(f.request_bytes_upper_bound, golden.size());
    }
    let small = fresh_table_layout::<String, String>(3)
        .unwrap()
        .layout
        .unwrap();
    // The actual raw request can have an unpadded size.
    assert_eq!(small.size(), 192 + 4 + width);
    assert_ne!(small.size(), small.pad_to_align().size());
}

#[test]
fn fresh_generic_pairs_use_actual_size_alignment_and_small_type_thresholds() {
    let width = group_layout().unwrap().size();
    let tiny_buckets = if width == 16 { 16 } else { 8 };
    let zero = fresh_table_layout::<(), ()>(3).unwrap();
    let byte = fresh_table_layout::<u8, ()>(3).unwrap();
    assert_eq!(zero.buckets, tiny_buckets);
    assert_eq!(byte.buckets, tiny_buckets);
    assert_eq!(zero.layout.unwrap().size(), tiny_buckets + width);
    assert_eq!(byte.layout.unwrap().size(), 2 * tiny_buckets + width);
    let two = fresh_table_layout::<u8, u8>(3).unwrap();
    assert_eq!(two.buckets, if width == 16 { 8 } else { 4 });
    let aligned = fresh_table_layout::<[u128; 2], ()>(3).unwrap();
    let pair = Layout::new::<([u128; 2], ())>();
    let align = pair.align().max(width);
    let size = ((pair.size() * 4 + align - 1) & !(align - 1)) + 4 + width;
    assert_eq!(aligned.buckets, 4);
    assert_eq!(
        aligned.layout,
        Some(Layout::from_size_align(size, align).unwrap())
    );
}

#[test]
fn insertion_only_requests_include_every_original_growth_extent() {
    let width = group_layout().unwrap().size();
    for (calls, buckets) in [
        (0, &[][..]),
        (1, &[4][..]),
        (3, &[4][..]),
        (4, &[4, 8][..]),
        (7, &[4, 8][..]),
        (8, &[4, 8, 16][..]),
        (14, &[4, 8, 16][..]),
        (15, &[4, 8, 16, 32][..]),
        (29, &[4, 8, 16, 32, 64][..]),
    ] {
        let mut actual = Vec::new();
        let facts = original_fresh_insertion_allocation_requests_observed::<
            String,
            String,
            HashMapResourceError,
        >(calls, &mut |layout| {
            actual.push(layout);
            Ok(())
        })
        .unwrap();
        // Independent locked String/String pair size and explicit growth
        // sequence, including the unpadded four-bucket request.
        let expected: Vec<_> = buckets
            .iter()
            .map(|buckets| {
                let align = width.max(8);
                let pairs = (48 * buckets + align - 1) & !(align - 1);
                Layout::from_size_align(pairs + buckets + width, align).unwrap()
            })
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(facts.insertion_calls, calls);
        assert_eq!(
            facts.final_bucket_upper_bound,
            buckets.last().copied().unwrap_or(0)
        );
        assert_eq!(facts.allocation_requests_upper_bound, expected.len());
        assert_eq!(
            facts.request_bytes_upper_bound,
            expected.iter().map(Layout::size).sum::<usize>()
        );
    }
}

#[test]
fn insertion_call_bound_keeps_original_small_pair_profile_and_duplicate_growth() {
    let width = group_layout().unwrap().size();
    let tiny =
        original_fresh_insertion_allocation_requests_observed::<(), (), HashMapResourceError>(
            15,
            &mut |_| Ok(()),
        )
        .unwrap();
    assert_eq!(tiny.final_bucket_upper_bound, 32);
    assert_eq!(
        tiny.allocation_requests_upper_bound,
        if width == 16 { 2 } else { 3 }
    );

    let mut actual = HashMap::<u64, u64>::new();
    for key in 0..3 {
        actual.insert(key, key);
    }
    let before_duplicate = actual.capacity();
    actual.insert(0, 99);
    // The pinned insert reserves before lookup, even for this duplicate. A
    // distinct-key-only bound would miss this real growth occurrence.
    assert_eq!(before_duplicate, 3);
    assert_eq!(actual.capacity(), 7);
    let bound = original_fresh_insertion_allocation_requests_observed::<
        u64,
        u64,
        HashMapResourceError,
    >(4, &mut |_| Ok(()))
    .unwrap();
    assert_eq!(bound.final_bucket_upper_bound, 8);
    assert_eq!(bound.allocation_requests_upper_bound, 2);
}

#[test]
fn insertion_request_loan_refusal_and_overflow_do_not_visit_later_extents() {
    let mut calls = 0;
    let refusal = original_fresh_insertion_allocation_requests_observed::<
        String,
        String,
        HashMapResourceError,
    >(29, &mut |_| {
        calls += 1;
        Err(Arithmetic("original allocation loan refused"))
    });
    assert_eq!(refusal, Err(Arithmetic("original allocation loan refused")));
    assert_eq!(calls, 1);
    calls = 0;
    let overflow = original_fresh_insertion_allocation_requests_observed::<
        String,
        String,
        HashMapResourceError,
    >(usize::MAX, &mut |_| {
        calls += 1;
        Ok(())
    });
    assert!(matches!(overflow, Err(Arithmetic(_))));
    assert_eq!(calls, 0);
}

#[test]
fn zero_table_has_no_request_and_target_uses_actual_public_group_carrier() {
    let f = fresh_table_layout::<String, String>(0).unwrap();
    assert_eq!(
        f,
        FreshTableFacts {
            layout: None,
            buckets: 0,
            allocation_requests_upper_bound: 0,
            request_bytes_upper_bound: 0
        }
    );
    let mut map = HashMap::<String, String>::new();
    map.try_reserve(0).unwrap();
    assert_eq!(map.capacity(), 0);
    assert_eq!(fresh_string_table_work_upper_bound(0, 0, 0).unwrap(), 0);
    assert_eq!(
        Layout::new::<usize>(),
        Layout::new::<Option<std::ptr::NonNull<()>>>()
    );
    #[cfg(all(
        not(miri),
        target_arch = "aarch64",
        target_feature = "neon",
        target_endian = "little"
    ))]
    assert_eq!(
        group_layout().unwrap(),
        Layout::new::<core::arch::aarch64::uint8x8_t>()
    );
    #[cfg(all(not(miri), target_arch = "x86_64", target_feature = "sse2"))]
    assert_eq!(
        group_layout().unwrap(),
        Layout::new::<core::arch::x86_64::__m128i>()
    );
}

#[test]
fn counts_layouts_and_opaque_work_overflows_remain_typed_arithmetic() {
    for count in [usize::MAX, usize::MAX / 8 + 1] {
        assert!(matches!(
            fresh_table_layout::<String, String>(count),
            Err(Arithmetic(_))
        ));
    }
    // Bucket arithmetic succeeds here, but the real pair allocation cannot fit.
    assert!(matches!(
        fresh_table_layout::<[u8; 1024], ()>(isize::MAX as usize / 1024),
        Err(Arithmetic(_))
    ));
    for result in [
        string_operations_work_upper_bound(usize::MAX, 1, 0, 0),
        string_operations_work_upper_bound(4, usize::MAX, 0, 0),
        string_operations_work_upper_bound(4, 1, usize::MAX, 0),
        string_operations_work_upper_bound(4, 1, 0, usize::MAX),
        source_iterator_work_upper_bound(usize::MAX, 0),
        source_iterator_work_upper_bound(0, usize::MAX),
    ] {
        assert!(matches!(result, Err(Arithmetic(_))));
    }
}

#[derive(Default)]
struct CollisionHasher;
impl Hasher for CollisionHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}
#[test]
fn one_reservation_unique_inserts_keep_table_capacity_even_under_total_collisions() {
    for count in [3, 14, 15, 29] {
        let mut ordinary = HashMap::<String, String>::new();
        let mut collisions =
            HashMap::<String, String, BuildHasherDefault<CollisionHasher>>::default();
        ordinary.try_reserve(count).unwrap();
        collisions.try_reserve(count).unwrap();
        let capacity = ordinary.capacity();
        let collision_capacity = collisions.capacity();
        let f = fresh_table_layout::<String, String>(count).unwrap();
        assert_eq!(
            capacity,
            if f.buckets < 8 {
                f.buckets - 1
            } else {
                f.buckets / 8 * 7
            }
        );
        assert_eq!(collision_capacity, capacity);
        for at in 0..count {
            let key = format!("same-prefix-雪-{at:03}");
            let value = format!("independent-{at}");
            assert!(ordinary.insert(key.clone(), value.clone()).is_none());
            assert!(collisions.insert(key, value).is_none());
            assert_eq!(ordinary.capacity(), capacity);
            assert_eq!(collisions.capacity(), collision_capacity);
        }
        for at in 0..count {
            let key = format!("same-prefix-雪-{at:03}");
            assert_eq!(ordinary.get(&key), Some(&format!("independent-{at}")));
            assert_eq!(collisions.get(&key), ordinary.get(&key));
        }
        // The adversarial test hasher proves table behavior only; its custom
        // code is deliberately outside the String/RandomState work contract.
        let longest = "same-prefix-雪-000".len();
        let work = fresh_string_table_work_upper_bound(count, count * longest, longest).unwrap();
        assert!(work >= count * (count - 1) / 2 * longest);
    }
}

#[test]
fn string_work_has_independent_collision_oracle_and_deleted_source_invoice_bound() {
    let group = group_layout().unwrap().size();
    let work = string_operations_work_upper_bound(4, 3, 9, 3).unwrap();
    assert_eq!(
        work,
        256 * 3 + 64 * (9 + 3) + 3 * (4 + group) * (32 + 2 * 3)
    );
    assert_eq!(
        fresh_string_table_work_upper_bound(3, 9, 3).unwrap(),
        4 + group + 3 * 48 + work
    );
    assert!(string_operations_work_upper_bound(8, 3, 9, 3).unwrap() > work);
    assert!(string_operations_work_upper_bound(4, 4, 9, 3).unwrap() > work);
    assert!(string_operations_work_upper_bound(4, 3, 10, 3).unwrap() > work);
    assert!(string_operations_work_upper_bound(4, 3, 9, 4).unwrap() > work);
    let mut map = HashMap::<String, String>::new();
    map.try_reserve(4096).unwrap();
    for i in 0..4096 {
        map.insert(format!("key-{i}"), String::new());
    }
    // Locked one-reserve allocation invoice remains retained across deletions.
    let invoice = fresh_table_layout::<String, String>(4096)
        .unwrap()
        .request_bytes_upper_bound;
    let before = source_iterator_work_upper_bound(invoice, 4096).unwrap();
    map.retain(|key, _| key == "never-inserted");
    assert!(map.is_empty());
    let after = source_iterator_work_upper_bound(invoice, 0).unwrap();
    assert_eq!(after, 32 * (invoice + group) + 16);
    assert!(after >= invoice);
    assert!(before > after);
    let mut iter = map.iter();
    assert!(iter.next().is_none());
    // Public capacity is deliberately not an input; neither iterator output
    // length nor an empty table licenses omitting retained bucket scan work.
}

#[derive(Debug, Eq, PartialEq)]
enum HashEvent {
    Length(usize),
    Bytes(Vec<u8>),
}
#[derive(Default)]
struct RecordingHasher {
    events: Vec<HashEvent>,
}
impl Hasher for RecordingHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.events.push(HashEvent::Bytes(bytes.to_vec()));
    }
    fn write_usize(&mut self, value: usize) {
        self.events.push(HashEvent::Length(value));
    }
}
#[test]
fn fixed_array_hash_uses_actual_length_32_and_bytes_without_string_sentinel() {
    use std::hash::{BuildHasher, Hash};
    let key: [u8; 32] = std::array::from_fn(|i| i as u8);
    let mut hasher = RecordingHasher::default();
    key.hash(&mut hasher);
    assert_eq!(
        hasher.events,
        [HashEvent::Length(32), HashEvent::Bytes((0..32).collect())]
    );
    // Actual RandomState delegates the same array and slice Hash bodies. This
    // is a value oracle, not observation inside SipHasher or a seed grant.
    let state = std::collections::hash_map::RandomState::new();
    assert_eq!(state.hash_one(key), state.hash_one(key.as_slice()));
    let mut other = key;
    other[31] = 255;
    assert_ne!(key, other);
    assert_eq!(key, std::array::from_fn(|i| i as u8));
}

#[test]
fn fixed_array_geometry_and_total_collision_unique_inserts_have_independent_goldens() {
    let group = group_layout().unwrap().size();
    assert_eq!(Layout::new::<([u8; 32], usize)>().size(), 40);
    assert_eq!(Layout::new::<([u8; 32], usize)>().align(), 8);
    for (count, buckets) in [(3, 4), (14, 16), (15, 32), (29, 64)] {
        let table = fresh_table_layout::<[u8; 32], usize>(count).unwrap();
        let align = group.max(8);
        let pairs = (40 * buckets + align - 1) & !(align - 1);
        let golden = Layout::from_size_align(pairs + buckets + group, align).unwrap();
        assert_eq!(table.buckets, buckets);
        assert_eq!(table.layout, Some(golden));
        assert_eq!(table.request_bytes_upper_bound, golden.size());
        assert_eq!(table.allocation_requests_upper_bound, 1);
        let mut ordinary = HashMap::<[u8; 32], usize>::new();
        let mut collisions =
            HashMap::<[u8; 32], usize, BuildHasherDefault<CollisionHasher>>::default();
        ordinary.try_reserve(count).unwrap();
        collisions.try_reserve(count).unwrap();
        let capacity = ordinary.capacity();
        assert_eq!(collisions.capacity(), capacity);
        for i in 0..count {
            let mut key = [0; 32];
            key[31] = i as u8;
            assert_eq!(ordinary.insert(key, i), None);
            assert_eq!(collisions.insert(key, i), None);
            assert_eq!(ordinary.capacity(), capacity);
            assert_eq!(collisions.capacity(), capacity);
        }
        for i in 0..count {
            let mut key = [0; 32];
            key[31] = i as u8;
            assert_eq!(ordinary.get(&key), Some(&i));
            assert_eq!(collisions.get(&key), Some(&i));
        }
        let hash = count * (256 + 64 * (8 + 32));
        let comparisons = count * (buckets + group) * (32 + 2 * 32);
        assert_eq!(
            fresh_byte_array32_table_work_upper_bound::<usize>(count).unwrap(),
            buckets + group + count * 40 + hash + comparisons
        );
        assert!(comparisons >= count * (count - 1) / 2 * 32);
        // The custom hasher witnesses collisions/table growth only; none of
        // its user code is admitted by the RandomState operation work author.
    }
    assert_eq!(
        fresh_table_layout::<[u8; 32], usize>(0).unwrap(),
        FreshTableFacts {
            layout: None,
            buckets: 0,
            allocation_requests_upper_bound: 0,
            request_bytes_upper_bound: 0,
        }
    );
    assert_eq!(
        fresh_byte_array32_table_work_upper_bound::<usize>(0).unwrap(),
        0
    );
}

#[test]
fn fixed_array_work_exact_extent_one_over_monotonicity_and_overflows_are_pure_arithmetic() {
    let group = group_layout().unwrap().size();
    let golden = 3 * (256 + 64 * 40) + 3 * (4 + group) * 96;
    assert_eq!(
        byte_array32_operations_work_upper_bound(4, 3).unwrap(),
        golden
    );
    assert_eq!(byte_array32_operations_work_upper_bound(4, 0).unwrap(), 0);
    assert!(byte_array32_operations_work_upper_bound(5, 3).unwrap() > golden);
    assert!(byte_array32_operations_work_upper_bound(4, 4).unwrap() > golden);
    // No huge table is allocated: this is the checked numerical extent of the
    // actual one-operation formula, including all potential collision probes.
    let hash = 256 + 64 * 40;
    let exact_buckets = (usize::MAX - hash) / 96 - group;
    assert_eq!(
        byte_array32_operations_work_upper_bound(exact_buckets, 1).unwrap(),
        hash + (exact_buckets + group) * 96
    );
    for result in [
        byte_array32_operations_work_upper_bound(exact_buckets + 1, 1),
        byte_array32_operations_work_upper_bound(usize::MAX, 1),
        byte_array32_operations_work_upper_bound(4, usize::MAX),
        fresh_byte_array32_table_work_upper_bound::<usize>(usize::MAX),
        fresh_byte_array32_table_work_upper_bound::<[u8; 1024]>(isize::MAX as usize / 1024),
    ] {
        assert!(matches!(result, Err(Arithmetic(_))));
    }
}
