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

//! Instance-local equality classes. Full-hash directory callbacks inspect only
//! cached integers; fallible equality runs in bounded candidate batches outside
//! hashbrown. This is not a join row chain or a distributed hash identity.

use super::hash_layout::HashLayout;
use super::owned_batch::{CompareScratch, OwnedKeyBatch};
use super::{
    IndexPurpose, KeyId, KeySpec, LocalHashBatch, LocalHashPolicy, PreparedKeyBatch, checkpoint_at,
    invariant, shared, vector,
};
use crate::{
    exec::{
        hash_table::allocation::{AggregateAllocator, AggregateVec},
        pipeline::operator::InputConstructionOwner,
    },
    runtime::{
        fragment::{ExecutionFailure, ExecutionResult},
        mem_tracker::MemTracker,
        tracked_state_allocator::expression_allocation_host,
    },
};
use hashbrown::HashTable;
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl};
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_value_arrays::ValueViolation;
use std::{num::NonZeroUsize, ops::Range, sync::Arc};

// Work segmentation only. All further occurrences and collision candidates
// continue in subsequent turns; no input or collision limit follows from it.
const WORK_ROWS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyMatch {
    Unmatchable,
    Miss,
    Found(KeyId),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NewKey {
    pub(crate) occurrence: usize,
    pub(crate) row: usize,
    pub(crate) id: KeyId,
}
pub(crate) struct InternBatch {
    matches: Vec<KeyMatch>,
    new_keys: Vec<NewKey>,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) struct PresenceBatch {
    present: Vec<u8>,
    participates: Vec<u8>,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) struct ProbeBatch {
    matches: Vec<KeyMatch>,
    _storage: ValueTypeStorageLifetime,
}
impl InternBatch {
    pub(crate) fn matches(&self) -> &[KeyMatch] {
        &self.matches
    }
    pub(crate) fn new_keys(&self) -> &[NewKey] {
        &self.new_keys
    }
}
impl PresenceBatch {
    pub(crate) fn present(&self) -> &[u8] {
        &self.present
    }
    pub(crate) fn participates(&self) -> &[u8] {
        &self.participates
    }
}
impl ProbeBatch {
    pub(crate) fn matches(&self) -> &[KeyMatch] {
        &self.matches
    }
}
#[derive(Clone, Copy)]
struct Directory {
    full: u64,
    table: u64,
    head: usize,
}
#[derive(Clone, Copy)]
struct StoredKey {
    batch: usize,
    row: usize,
}
struct Class {
    key: StoredKey,
    next: Option<usize>,
}
/// Immutable coordinates for the already compacted class prefix. Chunk rows
/// are work/storage geometry, never a limit on legal keys or input rows.
#[derive(Clone, Copy)]
struct CompactedPrefix {
    classes: usize,
    chunk_rows: NonZeroUsize,
}
// A soft work/storage preference. Every remaining class continues in another
// chunk; neither this value nor a page target bounds legal input or key size.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Chunk target belongs to the frozen incremental compaction API; bulk uses ClassSourcePlan."
    )
)]
const COMPACTION_CLASS_ROWS: usize = 4096;

struct ClassSourceIdentity {
    spec: KeySpec,
    classes: usize,
    chunk_rows: NonZeroUsize,
    _storage: ValueTypeStorageLifetime,
}
impl ClassSourceIdentity {
    fn chunk_count(&self) -> usize {
        self.classes.div_ceil(self.chunk_rows.get())
    }
    fn range(&self, ordinal: usize) -> ExecutionResult<Range<usize>> {
        if ordinal >= self.chunk_count() {
            return Err(invariant(ValueViolation::Bounds));
        }
        let start = ordinal
            .checked_mul(self.chunk_rows.get())
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        let end = start
            .checked_add((self.classes - start).min(self.chunk_rows.get()))
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        Ok(start..end)
    }
}

/// A moved immutable source, funded through the original shared author. The
/// common task graph owns exclusive claims and actual Body-exit evidence.
/// An Arc count is an ownership check, never a replacement for that evidence.
pub(super) struct ClassSourcePlan {
    index: KeyIndex,
    identity: Arc<ClassSourceIdentity>,
    _storage: ValueTypeStorageLifetime,
}
/// An already-validated new representative chunk. This non-Clone receipt owns
/// no whole old source/index; its small paid identity cannot retain old pages.
pub(super) struct MaterializedClassChunk {
    batch: Arc<OwnedKeyBatch>,
    identity: Arc<ClassSourceIdentity>,
    ordinal: usize,
    classes: Range<usize>,
}
impl ClassSourcePlan {
    pub(super) fn new(
        index: KeyIndex,
        chunk_rows: NonZeroUsize,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Arc<Self>> {
        index.check()?;
        control.checkpoint(0)?;
        let identity = index.class_source_identity(chunk_rows, owner, control)?;
        shared(
            owner,
            Self {
                index,
                identity,
                _storage: owner.storage_lifetime(),
            },
        )
    }
    pub(super) fn chunk_count(&self) -> usize {
        self.identity.chunk_count()
    }
    pub(super) fn materialize_chunk(
        &self,
        ordinal: usize,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<MaterializedClassChunk> {
        self.index
            .materialize_class_chunk(&self.identity, ordinal, owner, control)
    }
    /// The common graph must first establish every actual Body exit. No
    /// duplicate table, source clone, retry/fallback or wait is introduced here.
    pub(super) fn publish(
        source: Arc<Self>,
        completed: Vec<MaterializedClassChunk>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<(KeyIndex, RetiredClassBatches)> {
        control.checkpoint(0)?;
        let mut source = Arc::try_unwrap(source).map_err(|_| invariant(ValueViolation::Layout))?;
        let retired =
            source
                .index
                .install_class_chunks(&source.identity, completed, owner, control)?;
        Ok((source.index, retired))
    }
}

/// Owns the real former batch directory. Returning it prevents publication
/// from synchronously destroying all old fragmented representatives. The
/// common graph lends this holder to its original retirement data-work Body.
/// Escaping Arrow aliases can independently retain old backing after a pop.
pub(super) struct RetiredClassBatches {
    batches: AggregateVec<Arc<OwnedKeyBatch>>,
}
impl RetiredClassBatches {
    pub(super) fn remaining_batches(&self) -> usize {
        self.batches.len()
    }
    /// One prior batch header is one retirement work unit. The caller owns
    /// original control/CPU allowance and actual exit; no observation, worker,
    /// completion claim or new resource authority is minted by this method.
    pub(super) fn retire_one(&mut self) -> bool {
        self.batches.pop().is_some()
    }
}

struct Pending {
    row: usize,
    occurrence: usize,
    hash: u64,
    table: u64,
    next: Option<usize>,
}
#[derive(Clone, Copy, PartialEq)]
enum Match {
    Unmatchable,
    Miss,
    Hit(usize),
    Temporary(usize),
}
struct Chunk {
    occurrences: Vec<usize>,
    rows: Vec<usize>,
    hashes: Vec<u64>,
    table_hashes: Vec<u64>,
    valid: Vec<u8>,
}
#[derive(Clone, Copy)]
struct Node {
    handle: usize,
    batch: usize,
    row: usize,
    next: Option<usize>,
}
#[derive(Clone, Copy)]
struct Candidate {
    probe: usize,
    node: Node,
}
struct PairBuffers {
    left: Vec<usize>,
    right: Vec<usize>,
    owned: CompareScratch,
}
impl PairBuffers {
    fn new(count: usize, owner: &dyn InputConstructionOwner) -> ExecutionResult<Self> {
        Ok(Self {
            left: vector(owner, count)?,
            right: vector(owner, count)?,
            owned: CompareScratch::new(),
        })
    }
    fn rows(
        &mut self,
        chunk: &Chunk,
        candidates: &[Candidate],
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        self.left.clear();
        self.right.clear();
        for (i, candidate) in candidates.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            self.left.push(chunk.rows[candidate.probe]);
            self.right.push(candidate.node.row);
        }
        Ok(())
    }
}

pub(crate) struct KeyIndex {
    spec: KeySpec,
    purpose: IndexPurpose,
    policy: LocalHashPolicy,
    layout: HashLayout,
    directory: HashTable<Directory, AggregateAllocator>,
    classes: AggregateVec<Class>,
    batches: AggregateVec<Arc<OwnedKeyBatch>>,
    compacted: Option<CompactedPrefix>,
    allocator: AggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
    failed: Option<ExecutionFailure>,
    _storage: ValueTypeStorageLifetime,
}
impl KeyIndex {
    pub(crate) fn new(
        spec: KeySpec,
        purpose: IndexPurpose,
        tracker: Arc<MemTracker>,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        let policy = LocalHashPolicy::new(&spec, owner)?;
        Self::new_with_layout(
            spec,
            purpose,
            policy,
            HashLayout::incremental(),
            tracker,
            owner,
        )
    }
    /// Private mutable construction for one closed bulk input. The task graph
    /// retains this value until it publishes an immutable Ready index.
    #[cfg(test)]
    pub(super) fn new_for_bulk_hash(
        spec: KeySpec,
        purpose: IndexPurpose,
        policy: LocalHashPolicy,
        layout: HashLayout,
        tracker: Arc<MemTracker>,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        if layout.closed_input_bound().is_none() {
            return Err(invariant(ValueViolation::Layout));
        }
        Self::new_with_layout(spec, purpose, policy, layout, tracker, owner)
    }
    fn new_with_layout(
        spec: KeySpec,
        purpose: IndexPurpose,
        policy: LocalHashPolicy,
        layout: HashLayout,
        tracker: Arc<MemTracker>,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        Self::new_with_resources(
            spec,
            purpose,
            policy,
            layout,
            AggregateAllocator::new(tracker.clone()),
            expression_allocation_host(tracker),
            owner,
        )
    }
    pub(super) fn new_for_bulk_hash_with_resources(
        spec: KeySpec,
        purpose: IndexPurpose,
        policy: LocalHashPolicy,
        layout: HashLayout,
        allocator: AggregateAllocator,
        host: Arc<dyn AggregateStateAllocator>,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        if layout.closed_input_bound().is_none() {
            return Err(invariant(ValueViolation::Layout));
        }
        Self::new_with_resources(spec, purpose, policy, layout, allocator, host, owner)
    }
    fn new_with_resources(
        spec: KeySpec,
        purpose: IndexPurpose,
        policy: LocalHashPolicy,
        layout: HashLayout,
        allocator: AggregateAllocator,
        host: Arc<dyn AggregateStateAllocator>,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        if !policy.accepts(&spec) {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(Self {
            policy,
            layout,
            spec,
            purpose,
            directory: HashTable::new_in(allocator.clone()),
            classes: AggregateVec::new_in(allocator.clone()),
            batches: AggregateVec::new_in(allocator.clone()),
            compacted: None,
            allocator,
            host,
            failed: None,
            _storage: owner.storage_lifetime(),
        })
    }
    pub(crate) fn spec(&self) -> &KeySpec {
        &self.spec
    }
    pub(crate) fn policy(&self) -> &LocalHashPolicy {
        &self.policy
    }
    pub(crate) fn len(&self) -> usize {
        self.classes.len()
    }
    /// Translate a caller's dense state/output position without exposing an
    /// unchecked ID constructor or requiring a second O(D) identity vector.
    pub(crate) fn key_id_at(&self, index: usize) -> ExecutionResult<KeyId> {
        self.check()?;
        if self.purpose != IndexPurpose::KeyIds {
            return Err(invariant(ValueViolation::Layout));
        }
        if index >= self.classes.len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        Ok(KeyId(index))
    }
    /// Header validation for the private read-only bulk view. The parent graph
    /// alone certifies all partition coverage and immutable Ready publication.
    pub(super) fn validate_bulk_view_input(
        &self,
        spec: &KeySpec,
        policy: &LocalHashPolicy,
        purpose: IndexPurpose,
        layout: HashLayout,
    ) -> ExecutionResult<()> {
        self.check()?;
        if !self.spec.same_instance(spec)
            || !self.policy.same_instance(policy)
            || self.purpose != purpose
            || layout.closed_input_bound().is_none()
            || !self.layout.same_layout(layout)
        {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(())
    }
    /// HashInit's actual empty construction state, never an estimate of D.
    pub(super) fn validate_bulk_build_input(
        &self,
        spec: &KeySpec,
        policy: &LocalHashPolicy,
        purpose: IndexPurpose,
    ) -> ExecutionResult<()> {
        self.check()?;
        if !self.spec.same_instance(spec)
            || !self.policy.same_instance(policy)
            || self.purpose != purpose
            || !self.classes.is_empty()
            || self.layout.closed_input_bound().is_none()
        {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(())
    }
    /// The common graph supplies a qualified partition estimate plus margin.
    /// This only preallocates metadata through its existing real allocator;
    /// exact new classes still grow normally and retain the saved hashes/IDs.
    pub(super) fn reserve_bulk_distinct(
        &mut self,
        initial: usize,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        let result = (|| {
            self.check()?;
            let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
            checkpoint()?;
            if !self.classes.is_empty()
                || self.layout.closed_input_bound().is_none()
                || !self.layout.admits_reservation(0, initial)
            {
                return Err(invariant(ValueViolation::Layout));
            }
            reserve_directory(&mut self.directory, initial, &checkpoint)?;
            reserve_state(&mut self.classes, initial, &checkpoint)?;
            checkpoint()
        })();
        if let Err(cause) = &result
            && self.failed.is_none()
        {
            self.failed = Some(cause.clone());
        }
        result
    }
    /// The sole owner of class-to-representative coordinates. Old Class.key
    /// values in a compacted prefix are only dormant numeric coordinates;
    /// later incremental classes keep their original append-tail locations.
    fn key_location(&self, handle: usize) -> ExecutionResult<StoredKey> {
        let class = self
            .classes
            .get(handle)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if let Some(prefix) = self.compacted
            && handle < prefix.classes
        {
            return Ok(StoredKey {
                batch: handle / prefix.chunk_rows.get(),
                row: handle % prefix.chunk_rows.get(),
            });
        }
        Ok(class.key)
    }
    fn check(&self) -> ExecutionResult<()> {
        match &self.failed {
            Some(cause) => Err(cause.clone()),
            None => Ok(()),
        }
    }
    /// Borrow actual published pages, grouping each column at page boundaries.
    /// The metadata preserves every requested occurrence in caller order.
    pub(crate) fn read_keys(
        &self,
        ids: &[KeyId],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::read::OwnedKeyReadBatch<'_>> {
        self.check()?;
        if self.purpose != IndexPurpose::KeyIds {
            return Err(invariant(ValueViolation::Layout));
        }
        self.read_representatives(ids, owner, control)
    }
    // Private class coordinates also serve hash-presence compaction. They are
    // never exported as public IDs or caller row assignments for that purpose.
    fn read_representatives(
        &self,
        ids: &[KeyId],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::read::OwnedKeyReadBatch<'_>> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        let mut requests = vector(owner, ids.len())?;
        for (ordinal, id) in ids.iter().enumerate() {
            checkpoint_at(ordinal, &checkpoint)?;
            let location = self.key_location(id.index())?;
            requests.push(super::read::ReadRequest {
                ordinal,
                batch: location.batch,
                row: location.row,
            });
        }
        self.read_requests(requests, ids.len(), owner, control)
    }
    /// Borrow one contiguous class interval in local output-ordinal order.
    /// This avoids an intermediate KeyId vector; materialization remains with
    /// the original compact author and no task/Ready protocol is created here.
    pub(super) fn read_class_range(
        &self,
        classes: Range<usize>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::read::OwnedKeyReadBatch<'_>> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        if classes.start > classes.end || classes.end > self.classes.len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        let len = classes.end - classes.start;
        let mut requests = vector(owner, len)?;
        for (ordinal, handle) in classes.enumerate() {
            checkpoint_at(ordinal, &checkpoint)?;
            let location = self.key_location(handle)?;
            requests.push(super::read::ReadRequest {
                ordinal,
                batch: location.batch,
                row: location.row,
            });
        }
        self.read_requests(requests, len, owner, control)
    }
    fn read_requests(
        &self,
        mut requests: Vec<super::read::ReadRequest>,
        len: usize,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::read::OwnedKeyReadBatch<'_>> {
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        super::prepared::sort_by_key(&mut requests, owner, &checkpoint, |r| (r.batch, r.ordinal))?;
        let mut columns = vector(owner, self.spec.types().len())?;
        for column in 0..self.spec.types().len() {
            checkpoint()?;
            let mut pages = Vec::new();
            let mut begin = 0;
            while begin < requests.len() {
                checkpoint()?;
                let mut end = begin + 1;
                while end < requests.len() && requests[end].batch == requests[begin].batch {
                    checkpoint_at(end - begin, &checkpoint)?;
                    end += 1;
                }
                self.batches
                    .get(requests[begin].batch)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?
                    .append_reads(
                        column,
                        &requests[begin..end],
                        &mut pages,
                        owner,
                        &checkpoint,
                    )?;
                begin = end;
            }
            columns.push(super::read::KeyReadColumn { pages });
        }
        checkpoint()?;
        Ok(super::read::OwnedKeyReadBatch {
            columns,
            len,
            _storage: owner.storage_lifetime(),
        })
    }
    fn validate(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
    ) -> ExecutionResult<()> {
        self.check()?;
        if !self.spec.same_instance(&input.spec) || hashes.len() != input.len() {
            return Err(invariant(ValueViolation::Layout));
        }
        hashes.validate(input.input, &self.policy)
    }
    /// Construct complete typed output in the caller's requested ID order.
    /// Refusal does not poison this immutable index.
    /// Export an actual dense output interval through the same typed read and
    /// output authors, without a second O(D) vector of synthetic key IDs.
    pub(crate) fn export_key_range(
        &self,
        classes: Range<usize>,
        targets: &[novarocks_type_contract::FunctionValueType],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::ExportedKeys> {
        self.check()?;
        if self.purpose != IndexPurpose::KeyIds {
            return Err(invariant(ValueViolation::Layout));
        }
        super::export::validate_targets(&self.spec, targets, control)?;
        let read = self.read_class_range(classes, owner, control)?;
        super::export::construct(
            &read,
            &self.spec,
            targets,
            owner,
            Arc::clone(&self.host),
            control,
        )
    }
    pub(crate) fn export_keys(
        &self,
        ids: &[KeyId],
        targets: &[novarocks_type_contract::FunctionValueType],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::ExportedKeys> {
        self.check()?;
        super::export::validate_targets(&self.spec, targets, control)?;
        let read = self.read_keys(ids, owner, control)?;
        super::export::construct(
            &read,
            &self.spec,
            targets,
            owner,
            Arc::clone(&self.host),
            control,
        )
    }
    /// Rebuild immutable representative pages without rehashing or resigning
    /// classes. Escaped Arrow aliases continue to own their previous creators.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen incremental compaction preserves KeyIds and escaped old Buffer aliases."
        )
    )]
    pub(crate) fn compact(
        &mut self,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        self.check()?;
        let result = self.compact_pages(owner, control);
        if let Err(cause) = &result
            && self.failed.is_none()
        {
            self.failed = Some(cause.clone());
        }
        result
    }
    fn class_source_identity(
        &self,
        chunk_rows: NonZeroUsize,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Arc<ClassSourceIdentity>> {
        self.check()?;
        control.checkpoint(0)?;
        shared(
            owner,
            ClassSourceIdentity {
                spec: self.spec.clone(),
                classes: self.classes.len(),
                chunk_rows,
                _storage: owner.storage_lifetime(),
            },
        )
    }
    /// One finite class chunk is one materialization work unit. Original V
    /// gather/copy calls retain their cooperative/opaque operation boundaries;
    /// this delegate does not claim a row-exact resumable quota.
    fn materialize_class_chunk(
        &self,
        identity: &Arc<ClassSourceIdentity>,
        ordinal: usize,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<MaterializedClassChunk> {
        self.check()?;
        control.checkpoint(0)?;
        if !self.spec.same_instance(&identity.spec) || self.classes.len() != identity.classes {
            return Err(invariant(ValueViolation::Layout));
        }
        let classes = identity.range(ordinal)?;
        let read = self.read_class_range(classes.clone(), owner, control)?;
        let batch =
            super::compact::gather(&read, &self.spec, owner, Arc::clone(&self.host), control)?;
        if batch.len() != classes.len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        let batch = shared(owner, batch)?;
        control.checkpoint(0)?;
        Ok(MaterializedClassChunk {
            batch,
            identity: Arc::clone(identity),
            ordinal,
            classes,
        })
    }
    /// Only O(chunks) headers/directories are inspected here. Each private
    /// receipt was value-validated by its worker, not by this publisher.
    fn install_class_chunks(
        &mut self,
        identity: &Arc<ClassSourceIdentity>,
        completed: Vec<MaterializedClassChunk>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<RetiredClassBatches> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        if !self.spec.same_instance(&identity.spec)
            || self.classes.len() != identity.classes
            || completed.len() != identity.chunk_count()
        {
            return Err(invariant(ValueViolation::Layout));
        }
        let mut ordered = vector::<Option<Arc<OwnedKeyBatch>>>(owner, completed.len())?;
        for ordinal in 0..completed.len() {
            checkpoint_at(ordinal, &checkpoint)?;
            ordered.push(None);
        }
        for (position, chunk) in completed.into_iter().enumerate() {
            checkpoint_at(position, &checkpoint)?;
            if !Arc::ptr_eq(identity, &chunk.identity)
                || chunk.classes != identity.range(chunk.ordinal)?
                || chunk.batch.len() != chunk.classes.len()
            {
                return Err(invariant(ValueViolation::Layout));
            }
            let slot = ordered
                .get_mut(chunk.ordinal)
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            if slot.is_some() {
                return Err(invariant(ValueViolation::Layout));
            }
            *slot = Some(chunk.batch);
        }
        let mut replacement = AggregateVec::new_in(self.allocator.clone());
        reserve_state(&mut replacement, ordered.len(), &checkpoint)?;
        for (ordinal, chunk) in ordered.into_iter().enumerate() {
            checkpoint_at(ordinal, &checkpoint)?;
            replacement.push(chunk.ok_or_else(|| invariant(ValueViolation::Layout))?);
        }
        // All fallible preparation/observation precedes this constant-time
        // commit. The old directory remains an explicit owned output, so the
        // graph can retire its actual Arc headers as separate data work.
        checkpoint()?;
        let retired = std::mem::replace(&mut self.batches, replacement);
        self.compacted = Some(CompactedPrefix {
            classes: identity.classes,
            chunk_rows: identity.chunk_rows,
        });
        Ok(RetiredClassBatches { batches: retired })
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen incremental compaction preserves KeyIds and escaped old Buffer aliases."
        )
    )]
    fn compact_pages(
        &mut self,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        self.check()?;
        control.checkpoint(0)?;
        if self.classes.is_empty() {
            return control.checkpoint(0).map_err(ExecutionFailure::from);
        }
        let rows = NonZeroUsize::new(COMPACTION_CLASS_ROWS)
            .expect("the soft class chunk preference is positive");
        let identity = self.class_source_identity(rows, owner, control)?;
        let mut completed = vector(owner, identity.chunk_count())?;
        for ordinal in 0..identity.chunk_count() {
            completed.push(self.materialize_class_chunk(&identity, ordinal, owner, control)?);
        }
        let mut retired = self.install_class_chunks(&identity, completed, owner, control)?;
        // The incremental wrapper is synchronous, including real retirement.
        // Bulk uses the same holder under its separate original data-work Body.
        while retired.remaining_batches() != 0 {
            control.checkpoint(0)?;
            retired.retire_one();
        }
        drop(retired);
        control.checkpoint(0).map_err(ExecutionFailure::from)
    }
    /// Mutation publishes a complete result or permanently latches its first
    /// failure. Partially committed internals can never be retried as success.
    pub(crate) fn intern_batch(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<InternBatch> {
        self.intern_ids(input, hashes, 0..input.len(), owner, control)
    }
    /// Read only these original occurrence coordinates from one already
    /// prepared input and its saved hashes. Repetition and caller order remain.
    pub(super) fn intern_selected(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        occurrences: &[usize],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<InternBatch> {
        self.intern_ids(input, hashes, occurrences.iter().copied(), owner, control)
    }
    fn intern_ids<I: ExactSizeIterator<Item = usize>>(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        occurrences: I,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<InternBatch> {
        let result = self.intern(input, hashes, occurrences, true, owner, control);
        match result {
            Ok(Some(output)) => Ok(output),
            Ok(None) => unreachable!("KeyIds intern returns its assignment"),
            Err(cause) => {
                if self.failed.is_none() {
                    self.failed = Some(cause.clone());
                }
                Err(cause)
            }
        }
    }
    /// Presence construction returns only cardinality change. No public KeyId,
    /// per-input build assignment or duplicate physical row list is produced.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen whole-batch K membership API; current production consumers use selected occurrence ports."
        )
    )]
    pub(crate) fn include_presence(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<usize> {
        self.include_occurrences(input, hashes, 0..input.len(), owner, control)
    }
    pub(super) fn include_selected_presence(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        occurrences: &[usize],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<usize> {
        self.include_occurrences(input, hashes, occurrences.iter().copied(), owner, control)
    }
    fn include_occurrences<I: ExactSizeIterator<Item = usize>>(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        occurrences: I,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<usize> {
        let before = self.len();
        match self.intern(input, hashes, occurrences, false, owner, control) {
            Ok(_) => Ok(self.len() - before),
            Err(cause) => {
                if self.failed.is_none() {
                    self.failed = Some(cause.clone());
                }
                Err(cause)
            }
        }
    }
    pub(crate) fn probe_batch(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<ProbeBatch> {
        self.probe_occurrences(input, hashes, 0..input.len(), owner, control)
    }
    pub(super) fn probe_selected(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        occurrences: &[usize],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<ProbeBatch> {
        self.probe_occurrences(input, hashes, occurrences.iter().copied(), owner, control)
    }
    fn probe_occurrences<I: ExactSizeIterator<Item = usize>>(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        mut occurrences: I,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<ProbeBatch> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        self.validate(input, hashes)?;
        if self.purpose != IndexPurpose::KeyIds {
            return Err(invariant(ValueViolation::Layout));
        }
        let mut output = vector(owner, occurrences.len())?;
        while occurrences.len() != 0 {
            checkpoint()?;
            let chunk = Chunk::load(
                input,
                hashes,
                &mut occurrences,
                self.layout,
                owner,
                &checkpoint,
            )?;
            let mut found = chunk.matches(owner, &checkpoint)?;
            self.probe_existing(input, &chunk, &mut found, owner, &checkpoint)?;
            for (i, found) in found.into_iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                output.push(public_match(found)?);
            }
        }
        checkpoint()?;
        Ok(ProbeBatch {
            matches: output,
            _storage: owner.storage_lifetime(),
        })
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen whole-batch K membership API; current production consumers use selected occurrence ports."
        )
    )]
    pub(crate) fn contains_batch(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<PresenceBatch> {
        self.contains_occurrences(input, hashes, 0..input.len(), owner, control)
    }
    pub(super) fn contains_selected(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        occurrences: &[usize],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<PresenceBatch> {
        self.contains_occurrences(input, hashes, occurrences.iter().copied(), owner, control)
    }
    fn contains_occurrences<I: ExactSizeIterator<Item = usize>>(
        &self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        mut occurrences: I,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<PresenceBatch> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        self.validate(input, hashes)?;
        let mut present = vector(owner, occurrences.len())?;
        let mut participates = vector(owner, occurrences.len())?;
        while occurrences.len() != 0 {
            checkpoint()?;
            let chunk = Chunk::load(
                input,
                hashes,
                &mut occurrences,
                self.layout,
                owner,
                &checkpoint,
            )?;
            let mut found = chunk.matches(owner, &checkpoint)?;
            self.probe_existing(input, &chunk, &mut found, owner, &checkpoint)?;
            for (i, found) in found.into_iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                present.push(u8::from(matches!(found, Match::Hit(_))));
                participates.push(u8::from(found != Match::Unmatchable));
            }
        }
        checkpoint()?;
        Ok(PresenceBatch {
            present,
            participates,
            _storage: owner.storage_lifetime(),
        })
    }
    fn probe_existing(
        &self,
        input: &PreparedKeyBatch<'_>,
        chunk: &Chunk,
        output: &mut [Match],
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        let mut buffers = PairBuffers::new(chunk.rows.len(), owner)?;
        probe_chains(
            chunk,
            output,
            &self.directory,
            |handle| {
                let class = self
                    .classes
                    .get(handle)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                let location = self.key_location(handle)?;
                Ok(Node {
                    handle,
                    batch: location.batch,
                    row: location.row,
                    next: class.next,
                })
            },
            |candidates, truth| {
                buffers.rows(chunk, candidates, checkpoint)?;
                let batch = candidates[0].node.batch;
                self.batches
                    .get(batch)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?
                    .compare_pairs(
                        input,
                        &buffers.left,
                        &buffers.right,
                        truth,
                        &mut buffers.owned,
                        owner,
                        checkpoint,
                    )
            },
            owner,
            checkpoint,
        )
    }
    fn intern<I: ExactSizeIterator<Item = usize>>(
        &mut self,
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        mut occurrences: I,
        ids: bool,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Option<InternBatch>> {
        self.check()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        self.validate(input, hashes)?;
        if ids != (self.purpose == IndexPurpose::KeyIds) {
            return Err(invariant(ValueViolation::Layout));
        }
        checkpoint()?;
        let base = self.classes.len();
        let mut pending = AggregateVec::<Pending>::new_in(self.allocator.clone());
        let mut directory =
            HashTable::<Directory, AggregateAllocator>::new_in(self.allocator.clone());
        let mut output = if ids {
            Some(vector(owner, occurrences.len())?)
        } else {
            None
        };
        while occurrences.len() != 0 {
            checkpoint()?;
            let chunk = Chunk::load(
                input,
                hashes,
                &mut occurrences,
                self.layout,
                owner,
                &checkpoint,
            )?;
            let mut found = chunk.matches(owner, &checkpoint)?;
            self.probe_existing(input, &chunk, &mut found, owner, &checkpoint)?;
            let mut buffers = PairBuffers::new(chunk.rows.len(), owner)?;
            probe_chains(
                &chunk,
                &mut found,
                &directory,
                |handle| {
                    let value = pending
                        .get(handle)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                    Ok(Node {
                        handle: base
                            .checked_add(handle)
                            .ok_or_else(|| invariant(ValueViolation::Bounds))?,
                        batch: 0,
                        row: value.row,
                        next: value.next,
                    })
                },
                |candidates, truth| {
                    buffers.rows(&chunk, candidates, &checkpoint)?;
                    super::pairs::compare_prepared_pairs(
                        input,
                        &buffers.left,
                        input,
                        &buffers.right,
                        truth,
                        owner,
                        &checkpoint,
                    )
                },
                owner,
                &checkpoint,
            )?;
            deduplicate_chunk(
                input,
                &chunk,
                &mut found,
                &mut pending,
                &mut directory,
                base,
                self.layout,
                owner,
                &checkpoint,
            )?;
            if let Some(output) = &mut output {
                for (i, found) in found.into_iter().enumerate() {
                    checkpoint_at(i, &checkpoint)?;
                    output.push(public_match(found)?);
                }
            }
        }
        let mut new_keys = if ids {
            Some(vector(owner, pending.len())?)
        } else {
            None
        };
        if !pending.is_empty() {
            let mut rows = vector(owner, pending.len())?;
            for (i, value) in pending.iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                rows.push(value.row);
            }
            let batch = shared(
                owner,
                OwnedKeyBatch::copy_selected(input, &rows, owner, Arc::clone(&self.host), control)?,
            )?;
            if batch.len() != pending.len() {
                return Err(invariant(ValueViolation::Bounds));
            }
            let mut additional = 0;
            for (i, entry) in directory.iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                if self
                    .directory
                    .find(entry.table, |old| old.full == entry.full)
                    .is_none()
                {
                    additional += 1;
                }
            }
            reserve_directory(&mut self.directory, additional, &checkpoint)?;
            reserve_state(&mut self.classes, pending.len(), &checkpoint)?;
            reserve_state(&mut self.batches, 1, &checkpoint)?;
            let batch_id = self.batches.len();
            self.batches.push(batch);
            for (row, value) in pending.iter().enumerate() {
                checkpoint_at(row, &checkpoint)?;
                let handle = self.classes.len();
                let next = self
                    .directory
                    .find(value.table, |old| old.full == value.hash)
                    .map(|old| old.head);
                self.classes.push(Class {
                    key: StoredKey {
                        batch: batch_id,
                        row,
                    },
                    next,
                });
                if let Some(entry) = self
                    .directory
                    .find_mut(value.table, |old| old.full == value.hash)
                {
                    entry.head = handle;
                } else {
                    self.directory.insert_unique(
                        value.table,
                        Directory {
                            full: value.hash,
                            table: value.table,
                            head: handle,
                        },
                        |entry| entry.table,
                    );
                }
                if let Some(new_keys) = &mut new_keys {
                    new_keys.push(NewKey {
                        occurrence: value.occurrence,
                        row: value.row,
                        id: KeyId(handle),
                    });
                }
            }
        }
        checkpoint()?;
        Ok(output.map(|matches| InternBatch {
            matches,
            new_keys: new_keys.expect("KeyIds new selection"),
            _storage: owner.storage_lifetime(),
        }))
    }
}
impl Chunk {
    fn load<I: ExactSizeIterator<Item = usize>>(
        input: &PreparedKeyBatch<'_>,
        hashes: &LocalHashBatch,
        input_occurrences: &mut I,
        layout: HashLayout,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        let count = input_occurrences.len().min(WORK_ROWS);
        let mut occurrences = vector(owner, count)?;
        let mut rows = vector(owner, count)?;
        let mut full = vector(owner, count)?;
        let mut table_hashes = vector(owner, count)?;
        let mut valid = vector(owner, count)?;
        for offset in 0..count {
            checkpoint_at(offset, checkpoint)?;
            let occurrence = input_occurrences
                .next()
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            occurrences.push(occurrence);
            rows.push(input.input.row(occurrence)?);
            let hash = hashes.hash(occurrence)?;
            full.push(hash);
            table_hashes.push(layout.table_hash(hash));
            valid.push(u8::from(hashes.participates(occurrence)?));
        }
        Ok(Self {
            occurrences,
            rows,
            hashes: full,
            table_hashes,
            valid,
        })
    }
    fn matches(
        &self,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Vec<Match>> {
        let mut output = vector(owner, self.rows.len())?;
        for (i, &valid) in self.valid.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            output.push(if valid != 0 {
                Match::Miss
            } else {
                Match::Unmatchable
            });
        }
        Ok(output)
    }
}
fn public_match(value: Match) -> ExecutionResult<KeyMatch> {
    match value {
        Match::Unmatchable => Ok(KeyMatch::Unmatchable),
        Match::Miss => Ok(KeyMatch::Miss),
        Match::Hit(id) => Ok(KeyMatch::Found(KeyId(id))),
        Match::Temporary(_) => Err(invariant(ValueViolation::Layout)),
    }
}
#[allow(clippy::too_many_arguments)]
fn probe_chains(
    chunk: &Chunk,
    output: &mut [Match],
    directory: &HashTable<Directory, AggregateAllocator>,
    resolve: impl Fn(usize) -> ExecutionResult<Node>,
    mut compare: impl FnMut(&[Candidate], &mut [u8]) -> ExecutionResult<()>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    let mut active = vector(owner, chunk.rows.len())?;
    for (probe, value) in output.iter().enumerate() {
        checkpoint_at(probe, checkpoint)?;
        if *value == Match::Miss
            && let Some(entry) = directory.find(chunk.table_hashes[probe], |entry| {
                entry.full == chunk.hashes[probe]
            })
        {
            active.push(Candidate {
                probe,
                node: resolve(entry.head)?,
            });
        }
    }
    let mut truth = vector(owner, active.len())?;
    while !active.is_empty() {
        checkpoint()?;
        super::prepared::sort_by_key(&mut active, owner, checkpoint, |candidate| {
            (candidate.node.batch, candidate.probe)
        })?;
        truth.clear();
        for i in 0..active.len() {
            checkpoint_at(i, checkpoint)?;
            truth.push(0);
        }
        let mut start = 0;
        while start < active.len() {
            checkpoint()?;
            let mut end = start + 1;
            while end < active.len() && active[end].node.batch == active[start].node.batch {
                checkpoint_at(end, checkpoint)?;
                end += 1;
            }
            compare(&active[start..end], &mut truth[start..end])?;
            start = end;
        }
        let mut kept = 0;
        for i in 0..active.len() {
            checkpoint_at(i, checkpoint)?;
            let candidate = active[i];
            if truth[i] != 0 {
                output[candidate.probe] = Match::Hit(candidate.node.handle);
            } else if let Some(next) = candidate.node.next {
                active[kept] = Candidate {
                    probe: candidate.probe,
                    node: resolve(next)?,
                };
                kept += 1;
            }
        }
        active.truncate(kept);
    }
    checkpoint()
}
#[allow(clippy::too_many_arguments)]
fn deduplicate_chunk(
    input: &PreparedKeyBatch<'_>,
    chunk: &Chunk,
    output: &mut [Match],
    pending: &mut AggregateVec<Pending>,
    directory: &mut HashTable<Directory, AggregateAllocator>,
    base: usize,
    layout: HashLayout,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    let mut remaining = vector(owner, output.len())?;
    for (i, value) in output.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        if *value == Match::Miss {
            remaining.push(i);
        }
    }
    super::prepared::sort_by_key(&mut remaining, owner, checkpoint, |i| (chunk.hashes[i], i))?;
    let mut representatives = vector(owner, remaining.len())?;
    let mut next = vector(owner, remaining.len())?;
    let mut probes = vector(owner, remaining.len())?;
    let mut rep_ids = vector(owner, remaining.len())?;
    let mut left = vector(owner, remaining.len())?;
    let mut right = vector(owner, remaining.len())?;
    let mut truth = vector(owner, remaining.len())?;
    while !remaining.is_empty() {
        checkpoint()?;
        next.clear();
        probes.clear();
        rep_ids.clear();
        left.clear();
        right.clear();
        truth.clear();
        let mut start = 0;
        while start < remaining.len() {
            checkpoint()?;
            let first = remaining[start];
            let temp = representatives.len();
            representatives.push(first);
            output[first] = Match::Temporary(temp);
            let mut end = start + 1;
            while end < remaining.len() && chunk.hashes[remaining[end]] == chunk.hashes[first] {
                checkpoint_at(end, checkpoint)?;
                probes.push(remaining[end]);
                rep_ids.push(temp);
                left.push(chunk.rows[remaining[end]]);
                right.push(chunk.rows[first]);
                truth.push(0);
                end += 1;
            }
            start = end;
        }
        super::pairs::compare_prepared_pairs(
            input, &left, input, &right, &mut truth, owner, checkpoint,
        )?;
        for i in 0..probes.len() {
            checkpoint_at(i, checkpoint)?;
            if truth[i] != 0 {
                output[probes[i]] = Match::Temporary(rep_ids[i]);
            } else {
                next.push(probes[i]);
            }
        }
        std::mem::swap(&mut remaining, &mut next);
    }
    let mut order = vector(owner, representatives.len())?;
    let mut assigned = vector(owner, representatives.len())?;
    for i in 0..representatives.len() {
        checkpoint_at(i, checkpoint)?;
        order.push(i);
        assigned.push(0usize);
    }
    super::prepared::sort_by_key(&mut order, owner, checkpoint, |i| representatives[i])?;
    for (i, &temp) in order.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        let probe = representatives[temp];
        let hash = chunk.hashes[probe];
        let table = chunk.table_hashes[probe];
        let handle = pending.len();
        let additional = handle
            .checked_add(1)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if !layout.admits_reservation(base, additional) {
            // A closed graph cannot produce more classes than its actual
            // eligible input occurrences. This is a bookkeeping defect,
            // never a user quota or a resource fallback condition.
            return Err(invariant(ValueViolation::Layout));
        }
        reserve_state(pending, 1, checkpoint)?;
        if directory.find(table, |entry| entry.full == hash).is_none() {
            reserve_directory(directory, 1, checkpoint)?;
        }
        let previous = directory
            .find(table, |entry| entry.full == hash)
            .map(|entry| entry.head);
        pending.push(Pending {
            row: chunk.rows[probe],
            occurrence: chunk.occurrences[probe],
            hash,
            table,
            next: previous,
        });
        if let Some(entry) = directory.find_mut(table, |entry| entry.full == hash) {
            entry.head = handle;
        } else {
            directory.insert_unique(
                table,
                Directory {
                    full: hash,
                    table,
                    head: handle,
                },
                |entry| entry.table,
            );
        }
        assigned[temp] = base
            .checked_add(handle)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
    }
    for (i, value) in output.iter_mut().enumerate() {
        checkpoint_at(i, checkpoint)?;
        if let Match::Temporary(temp) = *value {
            *value = Match::Hit(assigned[temp]);
        }
    }
    checkpoint()
}
fn reserve_directory(
    directory: &mut HashTable<Directory, AggregateAllocator>,
    additional: usize,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let allocator = directory.allocator().clone();
    directory
        .try_reserve(additional, |entry| entry.table)
        .map_err(|source| {
            ExecutionFailure::from(
                allocator.allocation_error("reserve local key directory", source),
            )
        })?;
    checkpoint()
}
fn reserve_state<T>(
    values: &mut AggregateVec<T>,
    additional: usize,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let allocator = values.allocator().clone();
    values.try_reserve(additional).map_err(|source| {
        ExecutionFailure::from(allocator.allocation_error("reserve local key state", source))
    })?;
    checkpoint()
}

#[derive(Clone, Copy, Debug)]
pub(super) struct IntegerHashCosts {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains original purpose/eligible/cost caveat facts; NDV estimates never replace exact eligible counts."
        )
    )]
    pub(super) purpose: super::IndexPurpose,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains original purpose/eligible/cost caveat facts; NDV estimates never replace exact eligible counts."
        )
    )]
    pub(super) exact_eligible: usize,
    pub(super) reserve_classes_estimate: usize,
    pub(super) index_inline_bytes: u128,
    pub(super) directory: super::hash_layout::HashTableCost,
    pub(super) class_live_bytes_estimate: u128,
    pub(super) representative_payload_estimate: u128,
    /// Includes the actual OwnedKeyBatch Arc, one Column Vec element and one
    /// actual batches-vector Arc handle. Column includes OwnedFixedColumn;
    /// do not add that same inline body again.
    pub(super) representative_batch_structure_bytes: u128,
    /// Original Functions node author; not an authority to allocate that node.
    pub(super) representative_node_metadata_upper: u128,
    pub(super) representative_validity_may_exist: bool,
    /// Vec spare capacity, private hashbrown group/alignment, creator/custody
    /// chains and source retention are not represented as known zero bytes.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains original purpose/eligible/cost caveat facts; NDV estimates never replace exact eligible counts."
        )
    )]
    pub(super) has_unmodeled_custody_and_capacity: bool,
}
impl IntegerHashCosts {
    /// Caller chooses a concrete batch-fragmentation estimate within this
    /// range. Class cardinality itself remains only the complete NDV estimate.
    /// This does not claim actual representatives will occupy that many batches.
    pub(super) fn batch_range(self) -> std::ops::RangeInclusive<usize> {
        if self.reserve_classes_estimate == 0 {
            0..=0
        } else {
            1..=self.reserve_classes_estimate
        }
    }

    /// A comparable modeled subtotal, not an invoice, physical upper bound,
    /// feasibility proof or a resource-failure retry/fallback decision.
    pub(super) fn modeled_bytes(self, batches_estimate: usize) -> Option<u128> {
        if !self.batch_range().contains(&batches_estimate) {
            return None;
        }
        let per_batch = self
            .representative_batch_structure_bytes
            .checked_add(self.representative_node_metadata_upper)?;
        let metadata = (batches_estimate as u128).checked_mul(per_batch)?;
        let validity = if self.representative_validity_may_exist && batches_estimate != 0 {
            // Sum ceil(rows_i/8) <= ceil(total_rows/8) + batches - 1.
            (self.reserve_classes_estimate as u128)
                .div_ceil(8)
                .checked_add((batches_estimate - 1) as u128)?
        } else {
            0
        };
        self.index_inline_bytes
            .checked_add(self.directory.entry_slot_bytes)?
            .checked_add(self.directory.control_byte_minimum)?
            .checked_add(self.class_live_bytes_estimate)?
            .checked_add(self.representative_payload_estimate)?
            .checked_add(metadata)?
            .checked_add(validity)
    }
}

/// Costs for the single signed integer path being compared with Direct.
/// None means this model cannot represent its own geometry; it never refuses
/// legal input or permits retry after an actual allocation refusal.
pub(super) fn integer_hash_costs(
    stats: &super::bulk::CompleteStats,
) -> ExecutionResult<Option<IntegerHashCosts>> {
    stats.check()?;
    let spec = stats.exact().spec();
    if stats.exact().integer().is_none() || spec.types().len() != 1 {
        return Ok(None);
    }
    let Some(width) = spec.types()[0].data_type().primitive_width() else {
        return Ok(None);
    };
    let reserve = stats.hint().initial_reservation();
    let nullable = stats
        .exact()
        .integer_root_nullable_union()
        .unwrap_or(spec.types()[0].nullable());
    let estimate = (|| {
        let directory = HashLayout::table_cost::<Directory>(reserve)?;
        let batch_arc =
            novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
                OwnedKeyBatch,
            >())
            .ok()?
            .size() as u128;
        let batch_structure = batch_arc
            .checked_add(std::mem::size_of::<super::owned_batch::Column>() as u128)?
            .checked_add(std::mem::size_of::<Arc<OwnedKeyBatch>>() as u128)?;
        let node_metadata = fixed_node_cost(spec.types()[0].data_type(), nullable)?;
        Some(IntegerHashCosts {
            purpose: stats.purpose(),
            exact_eligible: stats.exact().indexable_rows(),
            reserve_classes_estimate: reserve,
            index_inline_bytes: std::mem::size_of::<KeyIndex>() as u128,
            directory,
            class_live_bytes_estimate: (reserve as u128)
                .checked_mul(std::mem::size_of::<Class>() as u128)?,
            representative_payload_estimate: (reserve as u128).checked_mul(width as u128)?,
            representative_batch_structure_bytes: batch_structure,
            representative_node_metadata_upper: node_metadata,
            representative_validity_may_exist: nullable,
            has_unmodeled_custody_and_capacity: true,
        })
    })();
    Ok(estimate)
}

fn fixed_node_cost(ty: &arrow::datatypes::DataType, has_nulls: bool) -> Option<u128> {
    use std::convert::Infallible;
    let mut exact = Some(0u128);
    let mut opaque = Some(0u128);
    let mut request = |layout: std::alloc::Layout| {
        exact = exact.and_then(|n| n.checked_add(layout.size() as u128));
        Ok::<_, Infallible>(())
    };
    let mut bound = |bytes| {
        opaque = opaque.and_then(|n| n.checked_add(bytes as u128));
        Ok::<_, Infallible>(())
    };
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut bound,
    );
    novarocks_functions::selected_copy::preflight_arrow_node_structure_construction(
        ty,
        has_nulls,
        &mut creation,
        &mut |_| Ok::<_, Infallible>(()),
    )
    .ok()?;
    exact?.checked_add(opaque?)
}
