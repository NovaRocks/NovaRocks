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
use novarocks_spi::connector::ConnectorOutputMemoryToken;

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

/// Returns the unique Arrow buffer capacity in `owner` that remains reachable
/// from `batch`.
///
/// Provider pages use a conservative whole-array estimate while they own the
/// source. At the writer boundary, ownership changes to the queue's Scheme S
/// buffer accounting. Intersect allocations rather than columns because a
/// cast can retain only a null or child buffer while replacing its values.
pub(crate) fn record_batch_shared_owner_bytes(batch: &RecordBatch, owner: &RecordBatch) -> usize {
    let mut retained_buffers = HashSet::new();
    for column in batch.columns() {
        collect_array_buffers(&column.to_data(), &mut retained_buffers);
    }
    let mut counted = HashSet::new();
    owner.columns().iter().fold(0usize, |total, column| {
        total.saturating_add(array_data_shared_bytes(
            &column.to_data(),
            &retained_buffers,
            &mut counted,
        ))
    })
}

#[derive(Clone, Debug)]
pub(crate) struct ChunkMemoryLease {
    owner: ChunkMemoryLeaseOwner,
}

#[derive(Clone, Debug)]
enum ChunkMemoryLeaseOwner {
    Native(Arc<ChunkAccounting>),
    Connector(Arc<Mutex<ConnectorOutputMemoryToken>>),
}

impl ChunkMemoryLease {
    pub(super) fn native(accounting: Arc<ChunkAccounting>) -> Self {
        Self {
            owner: ChunkMemoryLeaseOwner::Native(accounting),
        }
    }

    pub(super) fn connector(output_memory: Arc<Mutex<ConnectorOutputMemoryToken>>) -> Self {
        Self {
            owner: ChunkMemoryLeaseOwner::Connector(output_memory),
        }
    }

    /// Splits an exact byte charge out of an exclusively owned source chunk.
    /// The returned guard becomes the queue's accounting owner; dropping this
    /// lease releases only the unprojected remainder.
    pub(crate) fn try_split_to(
        &self,
        tracker: Option<&Arc<MemTracker>>,
        bytes: usize,
        connector_retained_bytes: usize,
    ) -> Result<Option<TransferredChunkBytes>, String> {
        match &self.owner {
            ChunkMemoryLeaseOwner::Native(accounting) => {
                let Some(tracker) = tracker else {
                    return Ok(None);
                };
                if Arc::strong_count(accounting) != 1 {
                    return Ok(None);
                }
                let bytes = i64::try_from(bytes)
                    .map_err(|_| "chunk accounting split exceeds i64 range".to_string())?;
                if bytes == 0 {
                    return Ok(Some(TransferredChunkBytes::empty(Arc::clone(tracker))));
                }
                let mut state = accounting
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
                Ok(Some(TransferredChunkBytes::native(
                    bytes,
                    Arc::clone(tracker),
                )))
            }
            ChunkMemoryLeaseOwner::Connector(output_memory) => {
                // A connector reservation may move only when this chunk is its
                // sole owner. Shared clones keep the reservation where it is
                // and let the writer use the existing per-batch fallback.
                if Arc::strong_count(output_memory) != 1 {
                    return Ok(None);
                }
                let shared_bytes = u64::try_from(connector_retained_bytes).map_err(|_| {
                    "connector output accounting split exceeds u64 range".to_string()
                })?;
                let reserved_bytes = output_memory
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .bytes();
                if shared_bytes > reserved_bytes {
                    return Err(format!(
                        "connector output accounting transfer of {shared_bytes} bytes exceeds source reservation of {reserved_bytes} bytes"
                    ));
                }
                // Keep the complete reservation: Arrow projections can share
                // buffers whose allocation exceeds the projected logical
                // prefix. Moving the one owner is exact and avoids both a
                // release/recharge gap and a second query-hierarchy charge.
                Ok(Some(TransferredChunkBytes::connector(
                    Arc::clone(output_memory),
                    shared_bytes,
                )))
            }
        }
    }

    pub(crate) fn tracker(&self) -> Option<Arc<MemTracker>> {
        match &self.owner {
            ChunkMemoryLeaseOwner::Native(accounting) => Some(accounting.tracker()),
            ChunkMemoryLeaseOwner::Connector(_) => None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct TransferredChunkBytes {
    owner: TransferredChunkBytesOwner,
}

#[derive(Debug)]
enum TransferredChunkBytesOwner {
    Native {
        bytes: i64,
        tracker: Arc<MemTracker>,
    },
    Connector {
        _output_memory: Arc<Mutex<ConnectorOutputMemoryToken>>,
        retained_bytes: u64,
    },
}

impl TransferredChunkBytes {
    fn native(bytes: i64, tracker: Arc<MemTracker>) -> Self {
        Self {
            owner: TransferredChunkBytesOwner::Native { bytes, tracker },
        }
    }

    fn connector(
        output_memory: Arc<Mutex<ConnectorOutputMemoryToken>>,
        retained_bytes: u64,
    ) -> Self {
        Self {
            owner: TransferredChunkBytesOwner::Connector {
                _output_memory: output_memory,
                retained_bytes,
            },
        }
    }

    fn empty(tracker: Arc<MemTracker>) -> Self {
        Self::native(0, tracker)
    }

    /// Finalize a connector projection after the source `Chunk` has been
    /// dropped and any newly allocated buffers have been charged.
    pub(crate) fn release_unshared_connector_bytes(&mut self) -> Result<(), String> {
        let TransferredChunkBytesOwner::Connector {
            _output_memory,
            retained_bytes,
        } = &self.owner
        else {
            return Ok(());
        };
        _output_memory
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .shrink_to(*retained_bytes)
            .map_err(|error| {
                format!("shrink connector output accounting after projection: {error}")
            })
    }
}

impl Drop for TransferredChunkBytes {
    fn drop(&mut self) {
        if let TransferredChunkBytesOwner::Native { bytes, tracker } = &self.owner {
            tracker.release(*bytes);
        }
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

fn array_data_shared_bytes(
    data: &ArrayData,
    retained: &HashSet<usize>,
    counted: &mut HashSet<usize>,
) -> usize {
    let mut total = 0usize;
    for buffer in data.buffers() {
        if retained.contains(&(buffer.data_ptr().as_ptr() as usize)) {
            total = total.saturating_add(buffer_bytes(buffer, counted));
        }
    }
    if let Some(nulls) = data.nulls()
        && retained.contains(&(nulls.buffer().data_ptr().as_ptr() as usize))
    {
        total = total.saturating_add(buffer_bytes(nulls.buffer(), counted));
    }
    for child in data.child_data() {
        total = total.saturating_add(array_data_shared_bytes(child, retained, counted));
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};

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

    #[test]
    fn shared_owner_bytes_keep_only_source_columns_reachable_from_projection() {
        let left = Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let right = Arc::new(Int64Array::from(vec![4, 5, 6])) as Arc<dyn Array>;
        let owner =
            RecordBatch::try_from_iter(vec![("left", left.clone()), ("right", right)]).unwrap();
        let projection = RecordBatch::try_from_iter(vec![("left", left.clone())]).unwrap();
        let materialized = RecordBatch::try_from_iter(vec![(
            "left",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        )])
        .unwrap();

        assert_eq!(
            record_batch_shared_owner_bytes(&projection, &owner),
            record_batch_bytes(&projection)
        );
        assert_eq!(record_batch_shared_owner_bytes(&materialized, &owner), 0);
    }

    #[test]
    fn shared_owner_bytes_keep_only_a_reused_null_bitmap_after_cast() {
        let source_values = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let owner =
            RecordBatch::try_from_iter(vec![("value", source_values.clone() as Arc<dyn Array>)])
                .unwrap();
        let cast_values = Arc::new(Int64Array::new(
            vec![1_i64, 0, 3].into(),
            source_values.nulls().cloned(),
        )) as Arc<dyn Array>;
        let projection = RecordBatch::try_from_iter(vec![("value", cast_values)]).unwrap();
        let additional = record_batch_additional_bytes(&projection, &owner);
        let shared = record_batch_shared_owner_bytes(&projection, &owner);

        assert!(shared > 0, "the null bitmap must remain shared");
        assert_eq!(shared + additional, record_batch_bytes(&projection));
        assert!(shared < record_batch_bytes(&owner));
    }

    #[test]
    fn offset_slice_uses_the_same_underlying_buffer_allocation_identity() {
        let source_values = Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as Arc<dyn Array>;
        let owner =
            RecordBatch::try_from_iter(vec![("value", Arc::clone(&source_values))]).unwrap();
        let sliced = source_values.slice(1, 2);
        let projection = RecordBatch::try_from_iter(vec![("value", sliced)]).unwrap();

        assert_eq!(record_batch_additional_bytes(&projection, &owner), 0);
        assert_eq!(
            record_batch_shared_owner_bytes(&projection, &owner),
            record_batch_bytes(&projection)
        );
    }
}
