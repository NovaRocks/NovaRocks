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

//! Read-only operations over already-constructed local index data.
//! This holder is not a Ready or complete-coverage proof. Only the original
//! graph may place it behind its final immutable publication boundary.

use super::{
    ExportedKeys, IndexPurpose, KeyId, KeyIndex, KeyMatch, KeySpec, LocalHashBatch,
    LocalHashPolicy, NullPolicy, OwnedKeyReadBatch, PreparedKeyBatch, checkpoint_at,
    direct_int::Storage,
    export, hash_batch,
    hash_layout::HashLayout,
    invariant,
    prepared::sort_by_key,
    read::{self, KeyReadColumn},
    statistics::visit_integer_occurrences,
    vector,
};
use crate::{
    exec::pipeline::operator::InputConstructionOwner,
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::{ValueViolation, batch::hash::HashScratch};
use std::{ops::Range, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct BulkKey {
    pub(super) partition: usize,
    pub(super) id: KeyId,
}
impl BulkKey {
    pub(crate) fn partition(self) -> usize {
        self.partition
    }
    pub(crate) fn id(self) -> KeyId {
        self.id
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BulkMatch {
    Unmatchable,
    Miss,
    Found(BulkKey),
}
pub(crate) struct BulkProbeBatch {
    matches: Vec<BulkMatch>,
    _storage: ValueTypeStorageLifetime,
}
impl BulkProbeBatch {
    pub(crate) fn matches(&self) -> &[BulkMatch] {
        &self.matches
    }
}
pub(crate) struct BulkPresenceBatch {
    present: Vec<u8>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen K presence contract retains participation separately from match or miss."
        )
    )]
    participates: Vec<u8>,
    _storage: ValueTypeStorageLifetime,
}
impl BulkPresenceBatch {
    pub(crate) fn present(&self) -> &[u8] {
        &self.present
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen K presence contract retains participation separately from match or miss."
        )
    )]
    pub(crate) fn participates(&self) -> &[u8] {
        &self.participates
    }
}
enum Backend {
    Empty {
        policy: LocalHashPolicy,
    },
    Direct(Arc<Storage>),
    Hash {
        partitions: Vec<KeyIndex>,
        policy: LocalHashPolicy,
        layout: HashLayout,
    },
}
pub(crate) struct IndexView {
    backend: Backend,
    spec: KeySpec,
    purpose: IndexPurpose,
    _storage: ValueTypeStorageLifetime,
}
impl IndexView {
    /// Only the parent graph can certify exact empty construction coverage.
    /// Later queries may contain new participating values and correctly miss.
    pub(super) fn empty(
        spec: KeySpec,
        policy: LocalHashPolicy,
        purpose: IndexPurpose,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        check(control)?;
        if !policy.accepts(&spec) {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(Self {
            backend: Backend::Empty { policy },
            spec,
            purpose,
            _storage: owner.storage_lifetime(),
        })
    }
    /// The parent must already own all phase barriers. In particular, presence
    /// has no constructed ID data from which to infer a global Mark barrier.
    pub(super) fn direct(
        store: Arc<Storage>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        store.check_failure()?;
        let checkpoint = || check(control);
        checkpoint()?;
        let purpose = store.geometry().purpose();
        if purpose == IndexPurpose::KeyIds {
            store.id_count_after_fill()?;
        }
        Ok(Self {
            spec: store.spec().clone(),
            purpose,
            backend: Backend::Direct(store),
            _storage: owner.storage_lifetime(),
        })
    }
    /// Re-home only the small partition directory under the actual constructor
    /// owner. Each moved index retains its own original data/creator. The caller
    /// keeps the incoming Vec's creator alive until this call actually exits.
    pub(super) fn hash(
        indexes: Vec<KeyIndex>,
        spec: KeySpec,
        policy: LocalHashPolicy,
        layout: HashLayout,
        purpose: IndexPurpose,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        let checkpoint = || check(control);
        checkpoint()?;
        if layout.closed_input_bound().is_none() || indexes.len() != layout.partitions() {
            return Err(invariant(ValueViolation::Layout));
        }
        let mut partitions = vector(owner, indexes.len())?;
        for (i, index) in indexes.into_iter().enumerate() {
            checkpoint_at(i, &checkpoint)?;
            index.validate_bulk_view_input(&spec, &policy, purpose, layout)?;
            partitions.push(index);
        }
        checkpoint()?;
        Ok(Self {
            backend: Backend::Hash {
                partitions,
                policy,
                layout,
            },
            spec,
            purpose,
            _storage: owner.storage_lifetime(),
        })
    }
    /// Observe the real selected production layout from owner correctness
    /// tests without exposing geometry or a mutable strategy choice.
    #[cfg(test)]
    pub(crate) fn is_direct_for_test(&self) -> bool {
        matches!(self.backend, Backend::Direct(_))
    }
    pub(crate) fn spec(&self) -> &KeySpec {
        &self.spec
    }
    #[expect(
        dead_code,
        reason = "Frozen K view purpose accessor remains available to handoff consumers."
    )]
    pub(crate) fn purpose(&self) -> IndexPurpose {
        self.purpose
    }
    fn validate(&self, input: &PreparedKeyBatch<'_>) -> ExecutionResult<()> {
        if !self.spec.same_instance(&input.spec) {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(())
    }
    fn ids(&self) -> ExecutionResult<()> {
        if self.purpose != IndexPurpose::KeyIds {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(())
    }
    /// Caller order and duplicate occurrences are retained. Direct does not
    /// inspect saved hashes and never computes an otherwise-unused hash batch.
    pub(crate) fn probe(
        &self,
        input: &PreparedKeyBatch<'_>,
        saved: Option<&LocalHashBatch>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<BulkProbeBatch> {
        self.probe_range(input, saved, 0..input.len(), owner, control)
    }
    /// Matches are aligned with this original occurrence range, not physical
    /// rows. The caller keeps its original page/range coordinates for assignment.
    pub(crate) fn probe_range(
        &self,
        input: &PreparedKeyBatch<'_>,
        saved: Option<&LocalHashBatch>,
        range: Range<usize>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<BulkProbeBatch> {
        self.ids()?;
        self.validate(input)?;
        validate_range(input, &range)?;
        let start = range.start;
        let checkpoint = || check(control);
        checkpoint()?;
        let mut matches = vector(owner, range.len())?;
        for i in 0..range.len() {
            checkpoint_at(i, &checkpoint)?;
            matches.push(BulkMatch::Unmatchable);
        }
        match &self.backend {
            Backend::Empty { policy } => {
                with_hashes(input, saved, policy, owner, control, |hashes| {
                    for (ordinal, occurrence) in range.clone().enumerate() {
                        checkpoint_at(ordinal, &checkpoint)?;
                        if hashes.participates(occurrence)? {
                            matches[ordinal] = BulkMatch::Miss;
                        }
                    }
                    Ok(())
                })?;
            }
            Backend::Direct(store) => {
                visit_integer_occurrences(
                    input,
                    range.clone(),
                    &checkpoint,
                    &mut |occurrence, value| {
                        matches[occurrence - start] =
                            public_match(0, store.lookup_value_after_fill(value)?);
                        Ok(())
                    },
                )?;
            }
            Backend::Hash {
                partitions,
                policy,
                layout,
            } => {
                with_hashes(input, saved, policy, owner, control, |hashes| {
                    let routed =
                        route_occurrences(hashes, *layout, range.clone(), owner, &checkpoint)?;
                    each_partition(
                        &routed,
                        owner,
                        &checkpoint,
                        &mut |partition, occurrences| {
                            let index = partitions
                                .get(partition)
                                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                            let found =
                                index.probe_selected(input, hashes, occurrences, owner, control)?;
                            if found.matches().len() != occurrences.len() {
                                return Err(invariant(ValueViolation::Bounds));
                            }
                            for (i, (&occurrence, &value)) in
                                occurrences.iter().zip(found.matches()).enumerate()
                            {
                                checkpoint_at(i, &checkpoint)?;
                                matches[occurrence - start] = public_match(partition, value);
                            }
                            Ok(())
                        },
                    )
                })?;
            }
        }
        checkpoint()?;
        Ok(BulkProbeBatch {
            matches,
            _storage: owner.storage_lifetime(),
        })
    }
    /// Logical participation is preserved independently of membership; NULL
    /// cannot be inferred from an absent key or an unwritten hash value.
    pub(crate) fn contains(
        &self,
        input: &PreparedKeyBatch<'_>,
        saved: Option<&LocalHashBatch>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<BulkPresenceBatch> {
        self.contains_range(input, saved, 0..input.len(), owner, control)
    }
    pub(crate) fn contains_range(
        &self,
        input: &PreparedKeyBatch<'_>,
        saved: Option<&LocalHashBatch>,
        range: Range<usize>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<BulkPresenceBatch> {
        self.validate(input)?;
        validate_range(input, &range)?;
        let start = range.start;
        let checkpoint = || check(control);
        checkpoint()?;
        let mut present = vector(owner, range.len())?;
        let mut participates = vector(owner, range.len())?;
        for i in 0..range.len() {
            checkpoint_at(i, &checkpoint)?;
            present.push(0);
            participates.push(0);
        }
        match &self.backend {
            Backend::Empty { policy } => {
                with_hashes(input, saved, policy, owner, control, |hashes| {
                    for (ordinal, occurrence) in range.clone().enumerate() {
                        checkpoint_at(ordinal, &checkpoint)?;
                        participates[ordinal] = u8::from(hashes.participates(occurrence)?);
                    }
                    Ok(())
                })?;
            }
            Backend::Direct(store) => {
                visit_integer_occurrences(
                    input,
                    range.clone(),
                    &checkpoint,
                    &mut |occurrence, value| {
                        participates[occurrence - start] =
                            u8::from(value.is_some() || self.spec.nulls()[0] == NullPolicy::Equal);
                        present[occurrence - start] =
                            u8::from(store.contains_value_after_mark(value)?);
                        Ok(())
                    },
                )?;
            }
            Backend::Hash {
                partitions,
                policy,
                layout,
            } => {
                with_hashes(input, saved, policy, owner, control, |hashes| {
                    let routed =
                        route_occurrences(hashes, *layout, range.clone(), owner, &checkpoint)?;
                    each_partition(
                        &routed,
                        owner,
                        &checkpoint,
                        &mut |partition, occurrences| {
                            let index = partitions
                                .get(partition)
                                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                            let found = index.contains_selected(
                                input,
                                hashes,
                                occurrences,
                                owner,
                                control,
                            )?;
                            if found.present().len() != occurrences.len()
                                || found.participates().len() != occurrences.len()
                            {
                                return Err(invariant(ValueViolation::Bounds));
                            }
                            for (i, &occurrence) in occurrences.iter().enumerate() {
                                checkpoint_at(i, &checkpoint)?;
                                present[occurrence - start] = found.present()[i];
                                participates[occurrence - start] = found.participates()[i];
                            }
                            Ok(())
                        },
                    )
                })?;
            }
        }
        checkpoint()?;
        Ok(BulkPresenceBatch {
            present,
            participates,
            _storage: owner.storage_lifetime(),
        })
    }
    /// Only requested partition/ID metadata is grouped. Borrowed typed pages
    /// are moved by the original read author, with no copied key payload.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen K typed read/export API; current join consumes lookup and assignments."
        )
    )]
    pub(crate) fn read_keys(
        &self,
        keys: &[BulkKey],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<OwnedKeyReadBatch<'_>> {
        self.ids()?;
        let checkpoint = || check(control);
        checkpoint()?;
        match &self.backend {
            Backend::Empty { .. } => {
                if !keys.is_empty() {
                    return Err(invariant(ValueViolation::Bounds));
                }
                let mut columns = vector(owner, self.spec.types().len())?;
                for column in 0..self.spec.types().len() {
                    checkpoint_at(column, &checkpoint)?;
                    columns.push(KeyReadColumn { pages: Vec::new() });
                }
                checkpoint()?;
                Ok(OwnedKeyReadBatch {
                    columns,
                    len: 0,
                    _storage: owner.storage_lifetime(),
                })
            }
            Backend::Direct(store) => {
                let mut ids = vector(owner, keys.len())?;
                for (i, key) in keys.iter().enumerate() {
                    checkpoint_at(i, &checkpoint)?;
                    if key.partition != 0 {
                        return Err(invariant(ValueViolation::Bounds));
                    }
                    ids.push(key.id);
                }
                store.read_keys(&ids, owner, control)
            }
            Backend::Hash { partitions, .. } => {
                let mut requests = vector(owner, keys.len())?;
                for (ordinal, &key) in keys.iter().enumerate() {
                    checkpoint_at(ordinal, &checkpoint)?;
                    if key.partition >= partitions.len() {
                        return Err(invariant(ValueViolation::Bounds));
                    }
                    requests.push(Request { key, ordinal });
                }
                sort_by_key(&mut requests, owner, &checkpoint, |r| {
                    (r.key.partition, r.ordinal)
                })?;
                let mut columns = vector(owner, self.spec.types().len())?;
                for column in 0..self.spec.types().len() {
                    checkpoint_at(column, &checkpoint)?;
                    columns.push(KeyReadColumn { pages: Vec::new() });
                }
                let mut output = OwnedKeyReadBatch {
                    columns,
                    len: keys.len(),
                    _storage: owner.storage_lifetime(),
                };
                let mut start = 0;
                while start < requests.len() {
                    checkpoint()?;
                    let mut end = start + 1;
                    while end < requests.len()
                        && requests[end].key.partition == requests[start].key.partition
                    {
                        checkpoint_at(end - start, &checkpoint)?;
                        end += 1;
                    }
                    let mut ids = vector(owner, end - start)?;
                    let mut ordinals = vector(owner, end - start)?;
                    for (i, request) in requests[start..end].iter().enumerate() {
                        checkpoint_at(i, &checkpoint)?;
                        ids.push(request.key.id);
                        ordinals.push(request.ordinal);
                    }
                    let loan = partitions[requests[start].key.partition]
                        .read_keys(&ids, owner, control)?;
                    read::append_reindexed(loan, &ordinals, &mut output, owner, &checkpoint)?;
                    start = end;
                }
                checkpoint()?;
                Ok(output)
            }
        }
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen K typed read/export API; current join consumes lookup and assignments."
        )
    )]
    pub(crate) fn export_keys(
        &self,
        keys: &[BulkKey],
        targets: &[FunctionValueType],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<ExportedKeys> {
        self.ids()?;
        export::validate_targets(&self.spec, targets, control)?;
        let read = self.read_keys(keys, owner, control)?;
        export::construct(&read, &self.spec, targets, owner, host, control)
    }
}
#[derive(Clone, Copy)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Partition grouping metadata belongs to the frozen K typed read/export path."
    )
)]
struct Request {
    key: BulkKey,
    ordinal: usize,
}
#[derive(Clone, Copy)]
struct Routed {
    partition: usize,
    occurrence: usize,
}
fn public_match(partition: usize, value: KeyMatch) -> BulkMatch {
    match value {
        KeyMatch::Unmatchable => BulkMatch::Unmatchable,
        KeyMatch::Miss => BulkMatch::Miss,
        KeyMatch::Found(id) => BulkMatch::Found(BulkKey { partition, id }),
    }
}
fn check(control: &dyn KernelEvaluationControl) -> ExecutionResult<()> {
    control.checkpoint(0).map_err(ExecutionFailure::from)
}
fn validate_range(input: &PreparedKeyBatch<'_>, range: &Range<usize>) -> ExecutionResult<()> {
    if range.start > range.end || range.end > input.len() {
        return Err(invariant(ValueViolation::Bounds));
    }
    Ok(())
}
fn with_hashes<T>(
    input: &PreparedKeyBatch<'_>,
    saved: Option<&LocalHashBatch>,
    policy: &LocalHashPolicy,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
    visit: impl FnOnce(&LocalHashBatch) -> ExecutionResult<T>,
) -> ExecutionResult<T> {
    if let Some(hashes) = saved {
        hashes.validate(input.input, policy)?;
        visit(hashes)
    } else {
        let hashes = hash_batch(input, policy, &mut HashScratch::new(), owner, &|| {
            check(control)
        })?;
        visit(&hashes)
    }
}
fn route_occurrences(
    hashes: &LocalHashBatch,
    layout: HashLayout,
    range: Range<usize>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Vec<Routed>> {
    let mut routed = vector(owner, range.len())?;
    for (i, occurrence) in range.enumerate() {
        checkpoint_at(i, checkpoint)?;
        if hashes.participates(occurrence)? {
            routed.push(Routed {
                partition: layout.route(hashes.hash(occurrence)?),
                occurrence,
            });
        }
    }
    sort_by_key(&mut routed, owner, checkpoint, |r| {
        (r.partition, r.occurrence)
    })?;
    Ok(routed)
}
fn each_partition(
    routed: &[Routed],
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
    visit: &mut impl FnMut(usize, &[usize]) -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    let mut start = 0;
    while start < routed.len() {
        checkpoint()?;
        let mut end = start + 1;
        while end < routed.len() && routed[end].partition == routed[start].partition {
            checkpoint_at(end - start, checkpoint)?;
            end += 1;
        }
        let mut occurrences = vector(owner, end - start)?;
        for (i, row) in routed[start..end].iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            occurrences.push(row.occurrence);
        }
        visit(routed[start].partition, &occurrences)?;
        start = end;
    }
    checkpoint()
}

#[cfg(test)]
#[path = "bulk_read_tests.rs"]
mod tests;
