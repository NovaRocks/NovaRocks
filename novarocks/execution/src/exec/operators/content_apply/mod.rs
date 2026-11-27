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

//! Task-local content weights, live-position claims, and tail verification.

use std::{
    hash::{BuildHasher, RandomState},
    num::NonZeroUsize,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow::array::Int8Array;
use hashbrown::{HashMap, HashSet, HashTable};
use novarocks_functions::{KernelDiagnostic, KernelFailure};
use novarocks_result_contract::DeleteVerificationCounts;
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::{
    ValueFailure, decode::ValueConversionContext, reachability::ReachableRows,
};

use crate::{
    exec::{
        hash_table::{
            allocation::{AggregateAllocator, AggregateVec},
            key_index::{
                BulkAssignments, BulkKey, BulkMatch, BulkReady, BulkWork, CapturedPageId,
                ExportedKeys, IndexView, KeyEquality, KeyInput, KeySpec, PreparedKeyBatch,
            },
        },
        pipeline::driver_step::TurnWork,
        shared_stage::arithmetic,
    },
    runtime::task::{ExecutionFailure, ExecutionResult},
};

fn contract(detail: &'static str) -> ExecutionFailure {
    KernelFailure::Internal(KernelDiagnostic::new(detail)).into()
}

pub(crate) fn content_spec(
    types: &[FunctionValueType],
    work: &BulkWork<'_>,
) -> ExecutionResult<KeySpec> {
    let checkpoint = || work.control.checkpoint(0).map_err(ExecutionFailure::from);
    let mut equalities = AggregateVec::new_in(work.allocator.clone());
    equalities
        .try_reserve_exact(types.len())
        .map_err(|source| {
            work.allocator
                .allocation_error("reserve content key equalities", source)
        })?;
    for _ in types {
        checkpoint()?;
        equalities.push(KeyEquality::ResultContent);
    }
    KeySpec::with_equalities(types, &equalities, work.owner, &checkpoint)
}

/// Dense weights are bound to the original page identity returned by K.
pub(crate) struct CapturedWeights {
    run: Option<usize>,
    pages: AggregateVec<AggregateVec<i8>>,
    rows: usize,
}

impl CapturedWeights {
    pub(crate) fn new(allocator: AggregateAllocator) -> Self {
        Self {
            run: None,
            pages: AggregateVec::new_in(allocator),
            rows: 0,
        }
    }

    pub(crate) fn record(
        &mut self,
        page: CapturedPageId,
        weights: &Int8Array,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<()> {
        let run = page.run_index();
        if self.run.is_some_and(|captured| captured != run) || self.pages.len() != page.page_index()
        {
            return Err(contract(
                "content weight page differs from its original capture order",
            ));
        }
        let rows = self
            .rows
            .checked_add(weights.len())
            .ok_or_else(arithmetic)?;
        let mut owned = AggregateVec::new_in(work.allocator.clone());
        owned.try_reserve_exact(weights.len()).map_err(|source| {
            work.allocator
                .allocation_error("reserve captured content weights", source)
        })?;
        for weight in weights {
            work.control.checkpoint(0)?;
            match weight {
                Some(weight @ (-1 | 1)) => owned.push(weight),
                _ => {
                    return Err(contract(
                        "content event weight must be non-null plus or minus one",
                    ));
                }
            }
        }
        self.pages.try_reserve(1).map_err(|source| {
            work.allocator
                .allocation_error("reserve captured content page", source)
        })?;
        self.pages.push(owned);
        self.run = Some(run);
        self.rows = rows;
        Ok(())
    }
}

struct SealedWeights {
    runs: HashMap<usize, CapturedWeights, RandomState, AggregateAllocator>,
    rows: usize,
}

impl SealedWeights {
    fn new(
        captures: impl IntoIterator<Item = CapturedWeights>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Self> {
        let mut runs = HashMap::with_hasher_in(RandomState::new(), work.allocator.clone());
        let mut rows = 0usize;
        for capture in captures {
            work.control.checkpoint(0)?;
            let Some(id) = capture.run else { continue };
            rows = rows.checked_add(capture.rows).ok_or_else(arithmetic)?;
            runs.try_reserve(1).map_err(|source| {
                work.allocator
                    .allocation_error("reserve sealed content capture", source)
            })?;
            if runs.insert(id, capture).is_some() {
                return Err(contract("duplicate original content weight capture"));
            }
        }
        Ok(Self { runs, rows })
    }

    fn weight(&self, page: CapturedPageId, offset: usize) -> ExecutionResult<i8> {
        self.runs
            .get(&page.run_index())
            .and_then(|capture| capture.pages.get(page.page_index()))
            .and_then(|weights| weights.get(offset))
            .copied()
            .ok_or_else(|| contract("content assignment has no captured weight"))
    }
}

struct ContentWeight {
    net: i64,
    selected: AtomicU64,
}

struct KeyWeights {
    view: Arc<IndexView>,
    partition_ends: AggregateVec<usize>,
    values: AggregateVec<ContentWeight>,
}

impl KeyWeights {
    fn new(view: Arc<IndexView>, work: &BulkWork<'_>) -> ExecutionResult<Self> {
        let mut partition_ends = AggregateVec::new_in(work.allocator.clone());
        partition_ends
            .try_reserve_exact(view.partition_count())
            .map_err(|source| {
                work.allocator
                    .allocation_error("reserve content key partitions", source)
            })?;
        let mut count = 0usize;
        for partition in 0..view.partition_count() {
            work.control.checkpoint(0)?;
            count = count
                .checked_add(view.key_count(partition)?)
                .ok_or_else(arithmetic)?;
            partition_ends.push(count);
        }
        let mut values = AggregateVec::new_in(work.allocator.clone());
        values.try_reserve_exact(count).map_err(|source| {
            work.allocator
                .allocation_error("reserve net content weights", source)
        })?;
        Ok(Self {
            view,
            partition_ends,
            values,
        })
    }

    fn len(&self) -> usize {
        self.partition_ends.last().copied().unwrap_or(0)
    }

    fn partition_start(&self, partition: usize) -> usize {
        partition
            .checked_sub(1)
            .map_or(0, |prior| self.partition_ends[prior])
    }

    fn ordinal(&self, key: BulkKey) -> ExecutionResult<usize> {
        let partition = key.partition();
        let end = *self
            .partition_ends
            .get(partition)
            .ok_or_else(|| contract("content key has no original partition"))?;
        let start = self.partition_start(partition);
        let id = key.id().index();
        if id >= end - start {
            return Err(contract("content key is outside its original partition"));
        }
        Ok(start + id)
    }

    fn key_at(&self, ordinal: usize) -> ExecutionResult<BulkKey> {
        let partition = self.partition_ends.partition_point(|end| *end <= ordinal);
        if partition >= self.partition_ends.len() {
            return Err(contract("content tail ordinal is outside the key space"));
        }
        self.view
            .key_at(partition, ordinal - self.partition_start(partition))
    }

    fn weight(&self, key: BulkKey) -> ExecutionResult<&ContentWeight> {
        self.values
            .get(self.ordinal(key)?)
            .ok_or_else(|| contract("content key is missing its finalized weight"))
    }
}

/// Assignment order comes from the original capture. No output is visible until
/// every published range has consumed its own weights and the source was sealed.
pub(crate) struct WeightFinalizer {
    keys: KeyWeights,
    assignments: BulkAssignments,
    weights: SealedWeights,
    range: usize,
    offset: usize,
    consumed: usize,
}

impl WeightFinalizer {
    pub(crate) fn new(
        ready: BulkReady,
        weights: impl IntoIterator<Item = CapturedWeights>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Self> {
        let (view, assignments) = ready.into_parts();
        Ok(Self {
            keys: KeyWeights::new(view, work)?,
            assignments,
            weights: SealedWeights::new(weights, work)?,
            range: 0,
            offset: 0,
            consumed: 0,
        })
    }

    /// Returns true at the end of the original assignment directory.
    pub(crate) fn advance(
        &mut self,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<bool> {
        while self.keys.values.len() != self.keys.len() {
            if !turn.try_consume(1) {
                return Ok(false);
            }
            work.control.checkpoint(0)?;
            self.keys.values.push(ContentWeight {
                net: 0,
                selected: AtomicU64::new(0),
            });
        }
        while self.range != self.assignments.range_count() {
            let assignment = self.assignments.range(self.range)?;
            while let Some(found) = assignment.matches().get(self.offset) {
                if !turn.try_consume(1) {
                    return Ok(false);
                }
                work.control.checkpoint(0)?;
                let BulkMatch::Found(key) = found else {
                    return Err(contract("content equality failed to admit a captured row"));
                };
                let offset = assignment
                    .start()
                    .checked_add(self.offset)
                    .ok_or_else(arithmetic)?;
                let weight = self.weights.weight(assignment.page(), offset)?;
                let ordinal = self.keys.ordinal(*key)?;
                let net = &mut self.keys.values[ordinal].net;
                *net = add_weight(*net, weight)?;
                self.offset += 1;
                self.consumed = self.consumed.checked_add(1).ok_or_else(arithmetic)?;
            }
            self.range += 1;
            self.offset = 0;
        }
        if self.consumed != self.weights.rows {
            return Err(contract(
                "content assignments and captured weights differ in coverage",
            ));
        }
        Ok(true)
    }

    pub(crate) fn finish(self, work: &BulkWork<'_>) -> ExecutionResult<ContentApplyIndex> {
        work.control.checkpoint(0)?;
        if self.keys.values.len() != self.keys.len()
            || self.range != self.assignments.range_count()
            || self.consumed != self.weights.rows
        {
            return Err(contract(
                "content weight finalization has incomplete inputs",
            ));
        }
        Ok(ContentApplyIndex {
            keys: self.keys,
            positions: SelectedPositions::new(work.allocator.clone()),
        })
    }
}

fn add_weight(net: i64, weight: i8) -> ExecutionResult<i64> {
    net.checked_add(i64::from(weight)).ok_or_else(arithmetic)
}

struct SelectedFile {
    hash: u64,
    file: AggregateVec<u8>,
    positions: HashSet<i64, RandomState, AggregateAllocator>,
}

struct SelectedPositions {
    table: Mutex<HashTable<SelectedFile, AggregateAllocator>>,
    hasher: RandomState,
    allocator: AggregateAllocator,
}

impl SelectedPositions {
    fn new(allocator: AggregateAllocator) -> Self {
        Self {
            table: Mutex::new(HashTable::new_in(allocator.clone())),
            hasher: RandomState::new(),
            allocator,
        }
    }

    fn select(&self, weight: &ContentWeight, file: &str, position: i64) -> ExecutionResult<bool> {
        let quota = weight.net.unsigned_abs();
        if weight.net >= 0 || weight.selected.load(Ordering::Relaxed) == quota {
            return Ok(false);
        }
        if position < 0 {
            return Err(contract(
                "content target location has a negative file position",
            ));
        }
        let hash = self.hasher.hash_one(file);
        let mut table = self
            .table
            .lock()
            .map_err(|_| contract("content target location state was poisoned"))?;
        let selected = weight.selected.load(Ordering::Relaxed);
        if selected == quota {
            return Ok(false);
        }

        if let Some(entry) = table.find_mut(hash, |entry| entry.file.as_slice() == file.as_bytes())
        {
            if entry.positions.contains(&position) {
                return Ok(false);
            }
            entry.positions.try_reserve(1).map_err(|source| {
                self.allocator
                    .allocation_error("reserve content target position", source)
            })?;
            entry.positions.insert(position);
        } else {
            let mut owned = AggregateVec::new_in(self.allocator.clone());
            owned.try_reserve_exact(file.len()).map_err(|source| {
                self.allocator
                    .allocation_error("reserve content target file identity", source)
            })?;
            owned.extend_from_slice(file.as_bytes());
            let mut positions = HashSet::with_hasher_in(RandomState::new(), self.allocator.clone());
            positions.try_reserve(1).map_err(|source| {
                self.allocator
                    .allocation_error("reserve content target position", source)
            })?;
            positions.insert(position);
            table.try_reserve(1, |entry| entry.hash).map_err(|source| {
                self.allocator
                    .allocation_error("reserve content target files", source)
            })?;
            table.insert_unique(
                hash,
                SelectedFile {
                    hash,
                    file: owned,
                    positions,
                },
                |entry| entry.hash,
            );
        }
        weight.selected.store(selected + 1, Ordering::Release);
        Ok(true)
    }
}

pub(crate) struct ContentApplyIndex {
    keys: KeyWeights,
    positions: SelectedPositions,
}

impl ContentApplyIndex {
    pub(crate) fn spec(&self) -> &KeySpec {
        self.keys.view.spec()
    }

    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn probe(
        &self,
        input: &PreparedKeyBatch<'_>,
        rows: Range<usize>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<crate::exec::hash_table::key_index::BulkProbeBatch> {
        self.keys
            .view
            .probe_range(input, None, rows, work.owner, work.control)
    }

    pub(crate) fn select_position(
        &self,
        key: BulkKey,
        file: &str,
        position: i64,
    ) -> ExecutionResult<bool> {
        self.positions
            .select(self.keys.weight(key)?, file, position)
    }

    pub(crate) fn key_at(&self, ordinal: usize) -> ExecutionResult<BulkKey> {
        self.keys.key_at(ordinal)
    }

    pub(crate) fn insertion_weight(&self, key: BulkKey) -> ExecutionResult<u64> {
        Ok(self.keys.weight(key)?.net.max(0) as u64)
    }

    /// The caller owns a cooperative key-space range. Only final negative
    /// weights contribute to the target scan's demand.
    pub(crate) fn export_demand(
        &self,
        range: Range<usize>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<ExportedKeys> {
        let mut negative = AggregateVec::new_in(work.allocator.clone());
        for ordinal in range {
            work.control.checkpoint(0)?;
            let key = self.key_at(ordinal)?;
            if self.keys.weight(key)?.net < 0 {
                negative.try_reserve(1).map_err(|source| {
                    work.allocator
                        .allocation_error("reserve content demand keys", source)
                })?;
                negative.push(key);
            }
        }
        self.export(&negative, work)
    }

    /// The caller repeats a key in a bounded output page to expand insertions.
    pub(crate) fn export(
        &self,
        keys: &[BulkKey],
        work: &BulkWork<'_>,
    ) -> ExecutionResult<ExportedKeys> {
        self.keys.view.export_keys(
            keys,
            self.spec().types(),
            work.owner,
            work.host.clone(),
            work.control,
        )
    }

    /// This reader is used after every target probe has ended normally.
    pub(crate) fn verify_key(
        &self,
        key: BulkKey,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<DeleteVerificationCounts> {
        let checkpoint = || work.control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        let weight = self.keys.weight(key)?;
        if weight.net >= 0 {
            return Ok(DeleteVerificationCounts::ZERO);
        }
        let requested = weight.net.unsigned_abs();
        let matched = weight.selected.load(Ordering::Acquire);
        let insufficient = matched < requested;
        let fingerprint = if insufficient {
            let exported = self.export(&[key], work)?;
            let readable = ReachableRows::all(1)
                .materialize_observed(
                    &mut |layout| work.owner.request(layout).map_err(ExecutionFailure::from),
                    &mut |_| checkpoint(),
                )
                .map_err(|error| match error {
                    ValueFailure::Value(value) => ExecutionFailure::from_domain(value),
                    ValueFailure::Caller(cause) => cause,
                })?;
            let input = KeyInput::new_for_spec(
                self.spec(),
                exported.arrays(),
                self.spec().types(),
                &readable,
                &[0],
                conversions,
                work.owner,
                &checkpoint,
            )?;
            let prepared = PreparedKeyBatch::from_captured(
                self.spec(),
                &input,
                work.allocator.clone(),
                &checkpoint,
            )?;
            prepared.content_fingerprint(0, &checkpoint)?
        } else {
            0
        };
        DeleteVerificationCounts::try_new(requested, matched, u64::from(insufficient), fingerprint)
            .map_err(|_| arithmetic())
    }
}

/// A tail unit owns one disjoint range. It advances its output position only
/// after the original export succeeds.
pub(crate) struct ContentApplyTail {
    next: usize,
    end: usize,
    copies: u64,
    counts: DeleteVerificationCounts,
}

impl ContentApplyTail {
    pub(crate) fn new(range: Range<usize>, index: &ContentApplyIndex) -> ExecutionResult<Self> {
        if range.start > range.end || range.end > index.len() {
            return Err(contract("content tail received an invalid key range"));
        }
        Ok(Self {
            next: range.start,
            end: range.end,
            copies: 0,
            counts: DeleteVerificationCounts::ZERO,
        })
    }

    pub(crate) fn next_page(
        &mut self,
        index: &ContentApplyIndex,
        page_rows: NonZeroUsize,
        turn: &mut TurnWork,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Option<ExportedKeys>> {
        let mut next = self.next;
        let mut copies = self.copies;
        let mut counts = self.counts;
        let mut keys = AggregateVec::new_in(work.allocator.clone());
        while next < self.end && keys.len() < page_rows.get() && turn.try_consume(1) {
            work.control.checkpoint(0)?;
            let key = index.key_at(next)?;
            if copies == 0 {
                copies = index.insertion_weight(key)?;
                if copies == 0 {
                    let verified = index.verify_key(key, conversions, work)?;
                    counts = counts.checked_add(verified).map_err(|_| arithmetic())?;
                    next += 1;
                    continue;
                }
            }
            keys.try_reserve(1).map_err(|source| {
                work.allocator
                    .allocation_error("reserve content insertion output", source)
            })?;
            keys.push(key);
            copies -= 1;
            if copies == 0 {
                next += 1;
            }
        }
        let output = if keys.is_empty() {
            None
        } else {
            Some(index.export(&keys, work)?)
        };
        self.next = next;
        self.copies = copies;
        self.counts = counts;
        Ok(output)
    }

    pub(crate) fn counts(&self) -> Option<DeleteVerificationCounts> {
        (self.next == self.end).then_some(self.counts)
    }
}

#[cfg(test)]
mod tests;
