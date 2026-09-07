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

/// Returns bytes retained by `batch` whose Arrow buffers are not already
/// retained by `owner`. This is used when a zero-copy projection transfers the
/// owner's accounting lease together with the projected batch.
pub(crate) fn record_batch_additional_bytes(batch: &RecordBatch, owner: &RecordBatch) -> usize {
    let mut seen = HashSet::new();
    for column in owner.columns() {
        collect_array_buffers(&column.to_data(), &mut seen);
    }
    let mut total = 0usize;
    for column in batch.columns() {
        total = total.saturating_add(array_data_bytes(&column.to_data(), &mut seen));
    }
    total
}

#[derive(Clone, Debug)]
pub(crate) struct ChunkMemoryLease {
    pub(super) accounting: Arc<ChunkAccounting>,
}

impl ChunkMemoryLease {
    /// Splits an exact byte charge out of an exclusively owned source chunk.
    /// The returned guard becomes the queue's accounting owner; dropping this
    /// lease releases only the unprojected remainder.
    pub(crate) fn try_split_to(
        &self,
        tracker: &Arc<MemTracker>,
        bytes: usize,
    ) -> Result<Option<TransferredChunkBytes>, String> {
        if Arc::strong_count(&self.accounting) != 1 {
            return Ok(None);
        }
        let bytes = i64::try_from(bytes)
            .map_err(|_| "chunk accounting split exceeds i64 range".to_string())?;
        if bytes == 0 {
            return Ok(Some(TransferredChunkBytes::empty(Arc::clone(tracker))));
        }
        let mut state = self
            .accounting
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if bytes > state.bytes {
            return Err(format!(
                "chunk accounting split of {bytes} bytes exceeds source charge of {} bytes",
                state.bytes
            ));
        }
        MemTracker::try_transfer_charge(&state.tracker, tracker, bytes)?;
        state.bytes -= bytes;
        Ok(Some(TransferredChunkBytes {
            bytes,
            tracker: Arc::clone(tracker),
        }))
    }

    pub(crate) fn tracker(&self) -> Arc<MemTracker> {
        self.accounting.tracker()
    }
}

#[derive(Debug)]
pub(crate) struct TransferredChunkBytes {
    bytes: i64,
    tracker: Arc<MemTracker>,
}

impl TransferredChunkBytes {
    fn empty(tracker: Arc<MemTracker>) -> Self {
        Self { bytes: 0, tracker }
    }
}

impl Drop for TransferredChunkBytes {
    fn drop(&mut self) {
        self.tracker.release(self.bytes);
    }
}

#[derive(Debug)]
pub(super) struct ChunkAccounting {
    state: Mutex<ChunkAccountingState>,
}

#[derive(Debug)]
struct ChunkAccountingState {
    bytes: i64,
    tracker: Arc<MemTracker>,
}

impl ChunkAccounting {
    pub(super) fn new(bytes: i64, tracker: &Arc<MemTracker>) -> Self {
        tracker.consume(bytes);
        Self::from_charged(bytes, tracker)
    }

    pub(super) fn from_charged(bytes: i64, tracker: &Arc<MemTracker>) -> Self {
        Self {
            state: Mutex::new(ChunkAccountingState {
                bytes,
                tracker: Arc::clone(tracker),
            }),
        }
    }

    pub(super) fn transfer_to(&self, tracker: &Arc<MemTracker>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(&state.tracker, tracker) {
            return;
        }
        state.tracker.release(state.bytes);
        tracker.consume(state.bytes);
        state.tracker = Arc::clone(tracker);
    }

    pub(super) fn try_transfer_to(&self, tracker: &Arc<MemTracker>) -> Result<(), String> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(&state.tracker, tracker) {
            return Ok(());
        }
        MemTracker::try_transfer_charge(&state.tracker, tracker, state.bytes)?;
        state.tracker = Arc::clone(tracker);
        Ok(())
    }

    pub(super) fn tracker(&self) -> Arc<MemTracker> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(&state.tracker)
    }
}

impl Drop for ChunkAccounting {
    fn drop(&mut self) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.tracker.release(state.bytes);
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

fn collect_array_buffers(data: &ArrayData, seen: &mut HashSet<usize>) {
    for buffer in data.buffers() {
        seen.insert(buffer.data_ptr().as_ptr() as usize);
    }
    if let Some(nulls) = data.nulls() {
        seen.insert(nulls.buffer().data_ptr().as_ptr() as usize);
    }
    for child in data.child_data() {
        collect_array_buffers(child, seen);
    }
}

fn buffer_bytes(buffer: &Buffer, seen: &mut HashSet<usize>) -> usize {
    let ptr = buffer.data_ptr().as_ptr() as usize;
    if !seen.insert(ptr) {
        return 0;
    }
    buffer.capacity().max(buffer.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    #[test]
    fn additional_bytes_excludes_zero_copy_projection_buffers() {
        let left = Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let right = Arc::new(Int64Array::from(vec![4, 5, 6])) as Arc<dyn Array>;
        let owner =
            RecordBatch::try_from_iter(vec![("left", left.clone()), ("right", right)]).unwrap();
        let projection = RecordBatch::try_from_iter(vec![("left", left)]).unwrap();

        assert_eq!(record_batch_additional_bytes(&projection, &owner), 0);
    }

    #[test]
    fn additional_bytes_charges_new_projection_buffers() {
        let owner_values = Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let owner = RecordBatch::try_from_iter(vec![("value", owner_values)]).unwrap();
        let projected_values = Arc::new(Int64Array::from(vec![2, 4, 6])) as Arc<dyn Array>;
        let projection = RecordBatch::try_from_iter(vec![("value", projected_values)]).unwrap();

        assert_eq!(
            record_batch_additional_bytes(&projection, &owner),
            record_batch_bytes(&projection)
        );
    }
}
