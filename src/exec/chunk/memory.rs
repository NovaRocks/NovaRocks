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
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayData, RecordBatch};
use arrow::buffer::Buffer;

use crate::runtime::mem_tracker::MemTracker;

/// Estimate RecordBatch size by summing unique buffers inside the batch.
///
/// NOTE: Scheme S (per-batch accounting).
/// We de-duplicate buffers only within a single RecordBatch.
/// Shared buffers across batches (e.g., slices/dictionaries) will be double-counted.
/// TODO: Upgrade to Scheme R with global buffer refcount to avoid cross-batch double counting.
pub fn record_batch_bytes(batch: &RecordBatch) -> usize {
    let mut seen = HashSet::new();
    let mut total = 0usize;
    for column in batch.columns() {
        total = total.saturating_add(array_data_bytes(&column.to_data(), &mut seen));
    }
    total
}

#[derive(Debug)]
pub(super) struct ChunkAccounting {
    bytes: i64,
    tracker: Mutex<Arc<MemTracker>>,
}

impl ChunkAccounting {
    pub(super) fn new(bytes: i64, tracker: &Arc<MemTracker>) -> Self {
        tracker.consume(bytes);
        Self {
            bytes,
            tracker: Mutex::new(Arc::clone(tracker)),
        }
    }

    pub(super) fn transfer_to(&self, tracker: &Arc<MemTracker>) {
        let mut guard = self.tracker.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(&guard, tracker) {
            return;
        }
        guard.release(self.bytes);
        tracker.consume(self.bytes);
        *guard = Arc::clone(tracker);
    }

    pub(super) fn tracker(&self) -> Arc<MemTracker> {
        let guard = self.tracker.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(&guard)
    }
}

impl Drop for ChunkAccounting {
    fn drop(&mut self) {
        let guard = self.tracker.lock().unwrap_or_else(|e| e.into_inner());
        guard.release(self.bytes);
    }
}

pub(super) fn chunk_bytes_i64(batch: &RecordBatch) -> i64 {
    i64::try_from(record_batch_bytes(batch)).unwrap_or(i64::MAX)
}

fn array_data_bytes(data: &ArrayData, seen: &mut HashSet<usize>) -> usize {
    let mut total = 0usize;
    for buffer in data.buffers() {
        total = total.saturating_add(buffer_bytes(buffer, seen));
    }
    if let Some(nulls) = data.nulls() {
        total = total.saturating_add(buffer_bytes(nulls.buffer(), seen));
    }
    for child in data.child_data() {
        total = total.saturating_add(array_data_bytes(child, seen));
    }
    total
}

fn buffer_bytes(buffer: &Buffer, seen: &mut HashSet<usize>) -> usize {
    let ptr = buffer.data_ptr().as_ptr() as usize;
    if !seen.insert(ptr) {
        return 0;
    }
    buffer.capacity().max(buffer.len())
}
