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

//! Claiming a batch's backings at a handover point.
//!
//! # Why this does not call Arrow's own `claim`
//!
//! `RecordBatch::claim` and `ArrayData::claim` walk the array tree and claim
//! every buffer they meet. Sharing is normal in that tree — a dictionary's
//! values, a shared null bitmap, two columns sliced from one allocation — so a
//! plain walk claims the same backing several times. Each claim replaces the
//! previous reservation, creating the new charge before dropping the old one,
//! which reports the same bytes twice for an instant and leaves a peak that
//! never happened.
//!
//! So this module walks the tree itself, reduces it to distinct backings by
//! allocation identity, and claims each exactly once.
//!
//! # Identity and capacity
//!
//! `Buffer::data_ptr` is the base of the allocation and does not move when a
//! buffer is sliced, so it identifies the backing rather than the view.
//! `Buffer::capacity` is the whole allocation for the same reason. Together
//! they are what makes the third-party case correct: an owner holding 64 MiB
//! and exposing a 1 KiB slice is charged for the 64 MiB it actually retains,
//! for as long as any alias keeps it alive.

use arrow::array::ArrayData;
use arrow::array::{Array, RecordBatch};
use arrow_buffer::Buffer;

use crate::pool::FulfilmentPool;
use crate::receipt::ClaimReceipt;

/// Collects the distinct backings reachable from an array's data.
///
/// Distinct means distinct allocation: two buffers that are views of one
/// allocation appear once. The returned buffers are in a stable traversal
/// order, values first and null bitmaps after, so a claim is reproducible.
pub fn unique_backings(data: &ArrayData) -> Vec<Buffer> {
    let mut seen = Vec::new();
    let mut collected = Vec::new();
    collect_backings(data, &mut seen, &mut collected);
    collected
}

fn collect_backings(data: &ArrayData, seen: &mut Vec<usize>, collected: &mut Vec<Buffer>) {
    for buffer in data.buffers() {
        push_unique(buffer, seen, collected);
    }
    if let Some(nulls) = data.nulls() {
        push_unique(nulls.inner().inner(), seen, collected);
    }
    for child in data.child_data() {
        collect_backings(child, seen, collected);
    }
}

fn push_unique(buffer: &Buffer, seen: &mut Vec<usize>, collected: &mut Vec<Buffer>) {
    let identity = buffer.data_ptr().as_ptr() as usize;
    // A linear scan is the right shape here: an array tree has a handful of
    // buffers, and a hash set would allocate on every claim.
    if seen.contains(&identity) {
        return;
    }
    seen.push(identity);
    collected.push(buffer.clone());
}

/// Charges one immutable buffer's full backing capacity.
///
/// The buffer must be at a handover point: the caller is taking ownership of
/// backing that already exists, and from here on the charge follows that
/// backing through slices, clones and exported arrays until the last alias
/// goes away.
pub fn claim_buffer(buffer: &Buffer, pool: &FulfilmentPool) -> ClaimReceipt {
    let session = pool.begin_session();
    buffer.claim(&session);
    session.into_receipt()
}

/// Charges the distinct backings of several immutable buffers in one claim.
///
/// One session covers the whole handover, so the receipt names every charge
/// and a later move takes them together. Buffers that share an allocation are
/// charged once across the whole set, not once per buffer.
pub fn claim_buffers(buffers: &[Buffer], pool: &FulfilmentPool) -> ClaimReceipt {
    let session = pool.begin_session();
    let mut seen = Vec::new();
    let mut collected = Vec::new();
    for buffer in buffers {
        push_unique(buffer, &mut seen, &mut collected);
    }
    for buffer in &collected {
        buffer.claim(&session);
    }
    session.into_receipt()
}

/// Charges the distinct backings of one array's data.
pub fn claim_array_data(data: &ArrayData, pool: &FulfilmentPool) -> ClaimReceipt {
    let session = pool.begin_session();
    for buffer in unique_backings(data) {
        buffer.claim(&session);
    }
    session.into_receipt()
}

/// Charges the distinct backings of a whole record batch.
///
/// Columns that share an allocation are charged once. The receipt names one
/// charge per distinct backing, which is what a later handover moves.
pub fn claim_batch(batch: &RecordBatch, pool: &FulfilmentPool) -> ClaimReceipt {
    let session = pool.begin_session();
    let mut seen = Vec::new();
    let mut collected = Vec::new();
    for column in batch.columns() {
        collect_backings(&column.to_data(), &mut seen, &mut collected);
    }
    for buffer in &collected {
        buffer.claim(&session);
    }
    session.into_receipt()
}
