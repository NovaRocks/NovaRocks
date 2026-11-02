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

//! Exclusive construction of one routed hash partition from saved input facts.
//! Complete coverage, qualified sizing, reservation and publication belong to
//! the parent task graph. This cursor neither evaluates nor hashes a key.

use super::{
    IndexPurpose, KeyIndex, PreparedKeyBatch, invariant,
    route::{MergedPartition, MergedReferencePages, ReferencePage},
    sizing::SizingDomainKind,
};
use crate::{
    exec::{
        hash_table::allocation::AggregateAllocator, pipeline::operator::InputConstructionOwner,
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_value_arrays::ValueViolation;

/// Original operation ports; the index already owns its actual allocator/host
/// and retained creators. No work/factory is stored in the cursor or result.
pub(super) struct BuildWork<'a> {
    pub(super) owner: &'a dyn InputConstructionOwner,
    pub(super) allocator: AggregateAllocator,
    pub(super) control: &'a dyn KernelEvaluationControl,
}

struct Building {
    index: KeyIndex,
    current: Option<ReferencePage>,
    input: MergedReferencePages,
    purpose: IndexPurpose,
    eligible: usize,
    offset: usize,
    consumed: usize,
}
impl Building {
    fn complete(&self) -> bool {
        self.current.is_none()
            && self.input.remaining() == 0
            && self.offset == 0
            && self.consumed == self.eligible
    }
}

/// A single owner resumes this value. Original reference pages retain their
/// own source directories/creators; the newly built index owns copied keys.
/// Dropping failed state destroys those data holders under their real owners.
pub(super) struct PartitionBuildCursor {
    state: Option<Building>,
    failed: Option<ExecutionFailure>,
}
impl PartitionBuildCursor {
    pub(super) fn new(
        index: KeyIndex,
        input: MergedPartition,
        purpose: IndexPurpose,
        work: &BuildWork<'_>,
    ) -> ExecutionResult<Self> {
        // This proposed checked port belongs to table.rs, which alone owns
        // index lifecycle, the actual hash layout, purpose and first failure.
        index.validate_bulk_build_input(input.domain().spec(), input.domain().policy(), purpose)?;
        checkpoint(work)?;
        if input.domain().kind() != SizingDomainKind::Partition(input.partition())
            || input.pages().is_empty() != (input.eligible() == 0)
        {
            return Err(invariant(ValueViolation::Layout));
        }
        let eligible = input.eligible();
        Ok(Self {
            state: Some(Building {
                index,
                current: None,
                input: input.into_reference_pages(),
                purpose,
                eligible,
                offset: 0,
                consumed: 0,
            }),
            failed: None,
        })
    }

    fn check(&self) -> ExecutionResult<()> {
        match &self.failed {
            Some(cause) => Err(cause.clone()),
            None => Ok(()),
        }
    }
    fn latch(&mut self, cause: ExecutionFailure) {
        if self.failed.is_none() {
            self.failed = Some(cause);
        }
        // No retry can observe or publish the partially mutated index. Each
        // actual data allocation retains its original finite creator on drop.
        self.state = None;
    }

    /// Counts only references actually committed by this cursor, not complete
    /// route coverage or the number of distinct classes in the partition.
    #[cfg(test)]
    pub(super) fn consumed(&self) -> ExecutionResult<usize> {
        self.check()?;
        self.state
            .as_ref()
            .map(|state| state.consumed)
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))
    }

    /// Work segmentation only: one original reference page and at most quota
    /// original occurrences per call. A large quota is not a distinct estimate
    /// or reservation. Even quota=0 observes the original control.
    pub(super) fn advance(&mut self, quota: usize, work: &BuildWork<'_>) -> ExecutionResult<bool> {
        let result: ExecutionResult<_> = (|| {
            self.check()?;
            checkpoint(work)?;
            let state = self
                .state
                .as_mut()
                .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
            if state.complete() {
                return Ok(true);
            }
            if quota == 0 {
                return Ok(false);
            }
            if state.current.is_none() {
                state.current = state.input.next_page();
            }
            let references = state
                .current
                .as_ref()
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            if references.len() == 0 || state.offset >= references.len() {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            let end = state.offset.saturating_add(quota).min(references.len());
            let page_finished = end == references.len();
            let occurrences = &references.occurrences()[state.offset..end];
            let consumed = state
                .consumed
                .checked_add(occurrences.len())
                .filter(|&count| count <= state.eligible)
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            let source = references.source_page()?;
            let prepared = PreparedKeyBatch::from_captured(
                state.index.spec(),
                source.input(),
                work.allocator.clone(),
                &|| checkpoint(work),
            )?;
            // Both selected authors validate the saved batch against this
            // original input/policy and own all equality/store operations.
            match state.purpose {
                IndexPurpose::KeyIds => {
                    drop(state.index.intern_selected(
                        &prepared,
                        source.hashes(),
                        occurrences,
                        work.owner,
                        work.control,
                    )?);
                }
                IndexPurpose::PresenceOnly => {
                    state.index.include_selected_presence(
                        &prepared,
                        source.hashes(),
                        occurrences,
                        work.owner,
                        work.control,
                    )?;
                }
            }
            drop(prepared);
            state.consumed = consumed;
            if page_finished {
                state.offset = 0;
                // Return this page's actual references/source custody during
                // the turn which consumed it, before moving to another page.
                drop(state.current.take());
            } else {
                state.offset = end;
            }
            if state.current.is_none() && state.input.remaining() == 0 && !state.complete() {
                return Err(invariant(ValueViolation::Bounds));
            }
            checkpoint(work)?;
            Ok(state.complete())
        })();
        if let Err(cause) = &result {
            self.latch(cause.clone());
        }
        result
    }

    /// Moves the real index only after every supplied reference was consumed.
    /// The parent still owes route completion, materialization, assignments,
    /// phase barriers and the immutable Ready publication contract.
    pub(super) fn finish(mut self, work: &BuildWork<'_>) -> ExecutionResult<KeyIndex> {
        self.check()?;
        checkpoint(work)?;
        if !self.state.as_ref().is_some_and(Building::complete) {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        let Building { index, input, .. } = self
            .state
            .take()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        // Every page was retired by its consuming advance. Only the empty
        // original iterator/backing and its directory creator remain here.
        drop(input);
        Ok(index)
    }
}
fn checkpoint(work: &BuildWork<'_>) -> ExecutionResult<()> {
    work.control.checkpoint(0).map_err(ExecutionFailure::from)
}

#[cfg(test)]
#[path = "hash_build_tests.rs"]
mod tests;
