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

use crate::runtime::mem_tracker::MemoryAccountingFailure;
use crate::runtime::task::{ExecutionFailure, ExecutionReason, ExecutionResult};

#[derive(Debug)]
enum ChunkChargeFailure {
    NativeSplit { requested: i64, charged: i64 },
    ConnectorSplit { requested: u64, reserved: u64 },
}
impl std::fmt::Display for ChunkChargeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NativeSplit { requested, charged } => write!(
                f,
                "chunk accounting split of {requested} bytes exceeds source charge of {charged} bytes"
            ),
            Self::ConnectorSplit {
                requested,
                reserved,
            } => write!(
                f,
                "connector output accounting transfer of {requested} bytes exceeds source reservation of {reserved} bytes"
            ),
        }
    }
}
impl std::error::Error for ChunkChargeFailure {}
impl novarocks_failure_contract::FailureProjection for ChunkChargeFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        let reason = match self {
            Self::NativeSplit { .. } => ExecutionReason::ChunkSplitExceedsCharge,
            Self::ConnectorSplit { .. } => ExecutionReason::ChunkSplitExceedsReservation,
        };
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            reason.reason(),
        )
    }
}

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
    infallible(additional_bytes_with(
        batch,
        owner,
        || Ok(()),
        |total, bytes| Ok(total.saturating_add(bytes)),
    ))
}

/// Observe the original buffer-identity traversal under the caller's public
/// operation lease. Identity measures actual new stock; it grants no source
/// authority. The controlled caller refuses overflow instead of undercharging.
pub(crate) fn record_batch_additional_bytes_observed(
    batch: &RecordBatch,
    owner: &RecordBatch,
    observe: impl FnMut() -> Result<(), novarocks_functions::KernelFailure>,
) -> Result<usize, novarocks_functions::KernelFailure> {
    additional_bytes_with(batch, owner, observe, |total, bytes| {
        total
            .checked_add(bytes)
            .ok_or(novarocks_functions::KernelFailure::ResourceExhausted)
    })
}

fn additional_bytes_with<E>(
    batch: &RecordBatch,
    owner: &RecordBatch,
    mut observe: impl FnMut() -> Result<(), E>,
    mut add: impl FnMut(usize, usize) -> Result<usize, E>,
) -> Result<usize, E> {
    let mut seen = HashSet::new();
    for column in owner.columns() {
        observe()?;
        visit_array_buffers(&column.to_data(), &mut observe, &mut |buffer| {
            seen.insert(buffer.data_ptr().as_ptr() as usize);
            Ok(())
        })?;
    }
    let mut total = 0usize;
    for column in batch.columns() {
        observe()?;
        visit_array_buffers(&column.to_data(), &mut observe, &mut |buffer| {
            total = add(total, buffer_bytes(buffer, &mut seen))?;
            Ok(())
        })?;
    }
    Ok(total)
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
pub(crate) struct ChunkMemoryCharge {
    owner: ChunkMemoryChargeOwner,
}

#[derive(Clone, Debug)]
enum ChunkMemoryChargeOwner {
    Native(Arc<ChunkAccounting>),
    Connector(Arc<Mutex<ConnectorOutputMemoryToken>>),
}

impl ChunkMemoryCharge {
    pub(super) fn native(accounting: Arc<ChunkAccounting>) -> Self {
        Self {
            owner: ChunkMemoryChargeOwner::Native(accounting),
        }
    }

    pub(super) fn connector(output_memory: Arc<Mutex<ConnectorOutputMemoryToken>>) -> Self {
        Self {
            owner: ChunkMemoryChargeOwner::Connector(output_memory),
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
    ) -> ExecutionResult<Option<TransferredChunkBytes>> {
        match &self.owner {
            ChunkMemoryChargeOwner::Native(accounting) => {
                let Some(tracker) = tracker else {
                    return Ok(None);
                };
                if Arc::strong_count(accounting) != 1 {
                    return Ok(None);
                }
                let bytes = i64::try_from(bytes)
                    .map_err(|_| MemoryAccountingFailure::BytesOutOfRange { requested: bytes })?;
                if bytes == 0 {
                    return Ok(Some(TransferredChunkBytes::empty(Arc::clone(tracker))));
                }
                let mut state = accounting
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if bytes > state.bytes {
                    return Err(ExecutionFailure::from_domain(
                        ChunkChargeFailure::NativeSplit {
                            requested: bytes,
                            charged: state.bytes,
                        },
                    ));
                }
                MemTracker::try_transfer_charge_typed(&state.tracker, tracker, bytes)
                    .map_err(MemoryAccountingFailure::Limit)?;
                state.bytes -= bytes;
                Ok(Some(TransferredChunkBytes::native(
                    bytes,
                    Arc::clone(tracker),
                )))
            }
            ChunkMemoryChargeOwner::Connector(output_memory) => {
                // A connector reservation may move only when this chunk is its
                // sole owner. Shared clones keep the reservation where it is
                // and let the writer use the existing per-batch fallback.
                if Arc::strong_count(output_memory) != 1 {
                    return Ok(None);
                }
                const {
                    assert!(std::mem::size_of::<usize>() <= std::mem::size_of::<u64>());
                }
                let shared_bytes = connector_retained_bytes as u64;
                let reserved_bytes = output_memory
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .bytes();
                if shared_bytes > reserved_bytes {
                    return Err(ExecutionFailure::from_domain(
                        ChunkChargeFailure::ConnectorSplit {
                            requested: shared_bytes,
                            reserved: reserved_bytes,
                        },
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
            ChunkMemoryChargeOwner::Native(accounting) => Some(accounting.tracker()),
            ChunkMemoryChargeOwner::Connector(_) => None,
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
    pub(crate) fn release_unshared_connector_bytes(&mut self) -> ExecutionResult<()> {
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
            .map_err(|cause| crate::runtime::task::connector_failure::ConnectorCallFailure::new(
                crate::runtime::task::connector_failure::ConnectorOperation::ShrinkOutputReservation, cause).into())
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

    pub(super) fn try_transfer_to_typed(
        &self,
        tracker: &Arc<MemTracker>,
    ) -> Result<(), crate::runtime::mem_tracker::MemTrackerLimitExceeded> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(&state.tracker, tracker) {
            return Ok(());
        }
        MemTracker::try_transfer_charge_typed(&state.tracker, tracker, state.bytes)?;
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

fn infallible<T>(result: Result<T, std::convert::Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

/// One structural visitor for original inventory, sharing and controlled delta
/// callers. The observer runs before traversing each array, buffer and child.
fn visit_array_buffers<E>(
    data: &ArrayData,
    observe: &mut impl FnMut() -> Result<(), E>,
    visit: &mut impl FnMut(&Buffer) -> Result<(), E>,
) -> Result<(), E> {
    observe()?;
    for buffer in data.buffers() {
        observe()?;
        visit(buffer)?;
    }
    if let Some(nulls) = data.nulls() {
        observe()?;
        visit(nulls.buffer())?;
    }
    for child in data.child_data() {
        observe()?;
        visit_array_buffers(child, observe, visit)?;
    }
    Ok(())
}

fn array_data_bytes(data: &ArrayData, seen: &mut HashSet<usize>) -> usize {
    let mut total = 0usize;
    infallible(visit_array_buffers(data, &mut || Ok(()), &mut |buffer| {
        total = total.saturating_add(buffer_bytes(buffer, seen));
        Ok(())
    }));
    total
}

fn collect_array_buffers(data: &ArrayData, seen: &mut HashSet<usize>) {
    infallible(visit_array_buffers(data, &mut || Ok(()), &mut |buffer| {
        seen.insert(buffer.data_ptr().as_ptr() as usize);
        Ok(())
    }));
}

fn array_data_shared_bytes(
    data: &ArrayData,
    retained: &HashSet<usize>,
    counted: &mut HashSet<usize>,
) -> usize {
    let mut total = 0usize;
    infallible(visit_array_buffers(data, &mut || Ok(()), &mut |buffer| {
        if retained.contains(&(buffer.data_ptr().as_ptr() as usize)) {
            total = total.saturating_add(buffer_bytes(buffer, counted));
        }
        Ok(())
    }));
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
    fn observed_delta_uses_original_allocation_identity_and_deduplicates_new_aliases() {
        let original = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as Arc<dyn Array>;
        let owner = RecordBatch::try_from_iter(vec![("old", original.clone())]).unwrap();
        let shared = RecordBatch::try_from_iter(vec![("slice", original.slice(1, 2))]).unwrap();
        assert_eq!(
            record_batch_additional_bytes_observed(&shared, &owner, || Ok(())).unwrap(),
            0
        );

        let new = Arc::new(Int32Array::from(vec![7, 8])) as Arc<dyn Array>;
        let data = new.to_data();
        let values = &data.buffers()[0];
        let expected = values.capacity().max(values.len());
        let batch = RecordBatch::try_from_iter(vec![
            ("slice", original.slice(1, 2)),
            ("new", new.clone()),
            ("alias", new),
        ])
        .unwrap();
        assert_eq!(
            record_batch_additional_bytes_observed(&batch, &owner, || Ok(())).unwrap(),
            expected
        );
        assert_eq!(record_batch_additional_bytes(&batch, &owner), expected);
    }

    #[test]
    fn observed_delta_stops_at_the_original_callback_first_cause() {
        use novarocks_functions::KernelFailure;
        let owner = RecordBatch::try_from_iter(vec![(
            "old",
            Arc::new(Int64Array::from(vec![Some(1), None])) as Arc<dyn Array>,
        )])
        .unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "new",
            Arc::new(Int64Array::from(vec![Some(3), None])) as Arc<dyn Array>,
        )])
        .unwrap();
        let mut total = 0usize;
        record_batch_additional_bytes_observed(&batch, &owner, || {
            total += 1;
            Ok(())
        })
        .unwrap();
        assert!(total > 1);
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for stop in [1, total] {
                let mut calls = 0usize;
                let error = record_batch_additional_bytes_observed(&batch, &owner, || {
                    calls += 1;
                    if calls == stop {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
                assert_eq!(error, cause);
                assert_eq!(
                    calls, stop,
                    "no traversal continues after the first refusal"
                );
            }
        }
    }

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
