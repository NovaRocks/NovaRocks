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

use super::*;

fn state(writers: u32) -> WriteSpreadState {
    WriteSpreadState::new(NonZeroU32::new(writers).unwrap())
}

fn slots(state: &WriteSpreadState, key: i64) -> u32 {
    state.buckets[bucket_index(key)]
        .slots
        .load(Ordering::Relaxed)
}

#[test]
fn one_hot_bucket_adds_one_slot_per_page_up_to_the_writer_count() {
    let threshold = MIN_BYTES_PER_SLOT;
    let shared = state(4);
    let mut driver = WriteSpreadDriver::new();
    let key = [-42, -42];
    for (page, bytes, expected) in [
        (0, threshold - 1, 1),
        (1, 1, 2),
        (2, threshold, 3),
        (3, threshold, 4),
        (4, threshold, 4),
    ] {
        let expected_key = mix(key[0], page % expected);
        assert_eq!(
            driver.layout_keys(&shared, &key, bytes).collect::<Vec<_>>(),
            [expected_key, expected_key]
        );
        assert_eq!(slots(&shared, key[0]), expected);
    }
    let mut separate = WriteSpreadDriver::new();
    let independent = state(4);
    let first: Vec<_> = separate
        .layout_keys(&independent, &key, threshold)
        .collect();
    assert_eq!(first, key);
    let second: Vec<_> = separate.layout_keys(&independent, &key, 1).collect();
    assert_eq!(second, [mix(key[0], 1); 2]);
    assert_ne!(first, second);
}

#[test]
fn stable_equal_shares_respect_the_average_and_do_not_consume_extra_slots() {
    let shared = state(12);
    let mut driver = WriteSpreadDriver::new();
    for _ in 0..10 {
        driver
            .layout_keys(&shared, &[1, 2, 3], MIN_BYTES_PER_SLOT * 3)
            .for_each(drop);
    }
    let allocated: u32 = [1, 2, 3].into_iter().map(|key| slots(&shared, key)).sum();
    assert_eq!(allocated, 12);
    assert!(allocated < 3 + shared.writers.get());

    let shared = state(12);
    let mut driver = WriteSpreadDriver::new();
    let equal: Vec<_> = (0..32).collect();
    let actual: Vec<_> = driver
        .layout_keys(&shared, &equal, MIN_BYTES_PER_SLOT * 32)
        .collect();
    assert_eq!(actual, equal);
    assert!(equal.iter().all(|&key| slots(&shared, key) == 1));
}

#[test]
fn existing_slots_survive_a_shift_in_the_hot_bucket() {
    let shared = state(4);
    let mut driver = WriteSpreadDriver::new();
    for _ in 0..3 {
        driver
            .layout_keys(&shared, &[7], MIN_BYTES_PER_SLOT)
            .for_each(drop);
    }
    assert_eq!(slots(&shared, 7), 4);
    for _ in 0..5 {
        driver
            .layout_keys(&shared, &[19], MIN_BYTES_PER_SLOT * 10)
            .for_each(drop);
    }
    assert_eq!(slots(&shared, 7), 4);
    assert_eq!(slots(&shared, 19), 4);
}

#[test]
fn divides_complete_page_bytes_by_row_share() {
    let shared = state(1);
    let mut driver = WriteSpreadDriver::new();
    let keys = [7, 19, 7];
    assert_eq!(
        driver.layout_keys(&shared, &keys, 5).collect::<Vec<_>>(),
        keys
    );
    assert_eq!(shared.total_bytes.load(Ordering::Relaxed), 5);
    assert_eq!(shared.buckets[7].bytes.load(Ordering::Relaxed), 3);
    assert_eq!(shared.buckets[19].bytes.load(Ordering::Relaxed), 2);
    assert_eq!(driver.layout_keys(&shared, &[], 80).count(), 0);
    assert_eq!(shared.total_bytes.load(Ordering::Relaxed), 5);
}
