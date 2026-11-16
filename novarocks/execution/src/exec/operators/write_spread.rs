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

//! Task-local scaling of the partition-affine write exchange.

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use xxhash_rust::xxh3::xxh3_64_with_seed;

const BUCKET_COUNT: usize = 4096;
const MIN_BYTES_PER_SLOT: u64 = 120 * 1024 * 1024;

struct Bucket {
    bytes: AtomicU64,
    slots: AtomicU32,
}

pub(super) struct WriteSpreadState {
    buckets: Box<[Bucket]>,
    total_bytes: AtomicU64,
    writers: NonZeroU32,
}

impl WriteSpreadState {
    pub(super) fn new(writers: NonZeroU32) -> Self {
        let buckets = (0..BUCKET_COUNT)
            .map(|_| Bucket {
                bytes: AtomicU64::new(0),
                slots: AtomicU32::new(1),
            })
            .collect();
        Self {
            buckets,
            total_bytes: AtomicU64::new(0),
            writers,
        }
    }

    fn record_total(&self, bytes: u64) {
        add_bytes(&self.total_bytes, bytes);
    }

    fn record_bucket(&self, bucket: usize, bytes: u64) {
        add_bytes(&self.buckets[bucket].bytes, bytes);
    }

    fn slots_after_page(&self, bucket: usize) -> u32 {
        let bucket = &self.buckets[bucket];
        let slots = bucket.slots.load(Ordering::Relaxed);
        let writers = self.writers.get();
        let bytes = bucket.bytes.load(Ordering::Relaxed);
        let total = self.total_bytes.load(Ordering::Relaxed);
        if slots == writers
            || bytes < MIN_BYTES_PER_SLOT * u64::from(slots)
            || u128::from(bytes) * u128::from(writers) <= u128::from(total) * u128::from(slots)
        {
            return slots;
        }
        match bucket
            .slots
            .compare_exchange(slots, slots + 1, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => slots + 1,
            Err(current) => current,
        }
    }
}

fn add_bytes(counter: &AtomicU64, bytes: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
        Some(live.saturating_add(bytes))
    });
}

#[derive(Clone, Copy, Default)]
struct DriverBucket {
    page_rows: usize,
    page_sequence: u64,
    selected_slot: u32,
}

pub(super) struct WriteSpreadDriver {
    buckets: Box<[DriverBucket]>,
    touched: Vec<usize>,
}

impl WriteSpreadDriver {
    pub(super) fn new() -> Self {
        Self {
            buckets: vec![DriverBucket::default(); BUCKET_COUNT].into_boxed_slice(),
            touched: Vec::new(),
        }
    }

    pub(super) fn layout_keys(
        &mut self,
        state: &WriteSpreadState,
        partition_keys: &[i64],
        page_bytes: u64,
    ) -> impl Iterator<Item = i64> {
        for &key in partition_keys {
            let bucket = bucket_index(key);
            let entry = &mut self.buckets[bucket];
            if entry.page_rows == 0 {
                self.touched.push(bucket);
            }
            entry.page_rows += 1;
        }

        if !partition_keys.is_empty() {
            state.record_total(page_bytes);
            let mut rows_seen = 0;
            let mut bytes_apportioned = 0;
            for &bucket in &self.touched {
                rows_seen += self.buckets[bucket].page_rows;
                // Divide cumulative counts to attribute the whole page, including
                // bytes left over by integer division.
                let through_bucket = (u128::from(page_bytes) * rows_seen as u128
                    / partition_keys.len() as u128) as u64;
                state.record_bucket(bucket, through_bucket - bytes_apportioned);
                bytes_apportioned = through_bucket;
            }
            for bucket in self.touched.drain(..) {
                let entry = &mut self.buckets[bucket];
                let slots = state.slots_after_page(bucket);
                entry.selected_slot = (entry.page_sequence % u64::from(slots)) as u32;
                entry.page_sequence = entry.page_sequence.wrapping_add(1);
                entry.page_rows = 0;
            }
        }

        partition_keys.iter().copied().map(|key| {
            let slot = self.buckets[bucket_index(key)].selected_slot;
            mix(key, slot)
        })
    }
}

fn bucket_index(key: i64) -> usize {
    (key as usize) & (BUCKET_COUNT - 1)
}

fn mix(key: i64, slot: u32) -> i64 {
    if slot == 0 {
        key
    } else {
        xxh3_64_with_seed(&key.to_le_bytes(), u64::from(slot)) as i64
    }
}

#[cfg(test)]
mod tests;
