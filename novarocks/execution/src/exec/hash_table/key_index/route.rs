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

//! Cached-hash routing and exclusive partition accumulation. The parent graph
//! owns worker/frontier limits, merge priority, coverage and Ready publication.
//! There is no task registry, queue, driver, observable or payload copy here.

#[cfg(test)]
use super::capture::BulkOccurrence;
use super::{
    bulk::{CapturedRuns, CompleteInput},
    capture::{BulkDomain, CapturedPage, CapturedPageId},
    checkpoint_at,
    hash_layout::HashLayout,
    invariant, reserve_push, shared,
    sizing::{SizingDomain, SizingDomainKind, SizingPayload, SizingSketch},
    vector,
};
use crate::{
    exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl};
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_value_arrays::ValueViolation;
use std::{num::NonZeroUsize, ops::Range, sync::Arc};

// Soft reference-page aggregation only. Larger inputs emit more partials.
const REFERENCE_ROWS: usize = 1024;
pub(super) struct RouteWork<'a> {
    pub(super) owner: &'a dyn InputConstructionOwner,
    pub(super) host: Arc<dyn AggregateStateAllocator>,
    pub(super) control: &'a dyn KernelEvaluationControl,
}
pub(super) struct RoutePlan {
    sources: CapturedRuns,
    domain: BulkDomain,
    layout: HashLayout,
    partitions: Vec<SizingDomain>,
    _storage: ValueTypeStorageLifetime,
}
impl RoutePlan {
    /// Parent-created partition domains are moved, never recreated by name.
    /// This checks O(P) immutable metadata; no input page or row is flattened.
    pub(super) fn new(
        input: &CompleteInput,
        layout: HashLayout,
        partitions: Vec<SizingDomain>,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<Arc<Self>> {
        let domain = input.stats().domain().clone();
        let result: ExecutionResult<_> = (|| {
            input.stats().check()?;
            checkpoint(work)?;
            if layout.closed_input_bound() != Some(input.stats().exact().indexable_rows())
                || partitions.len() != layout.partitions()
            {
                return Err(invariant(ValueViolation::Layout));
            }
            let mut domains = vector(work.owner, partitions.len())?;
            for (index, partition) in partitions.into_iter().enumerate() {
                checkpoint_at(index, &|| checkpoint(work))?;
                if partition.kind() != SizingDomainKind::Partition(index)
                    || !partition.spec().same_instance(domain.spec())
                    || !partition.policy().same_instance(domain.policy())
                {
                    return Err(invariant(ValueViolation::Layout));
                }
                domains.push(partition);
            }
            let plan = shared(
                work.owner,
                Self {
                    sources: input.runs().clone(),
                    domain: domain.clone(),
                    layout,
                    partitions: domains,
                    _storage: work.owner.storage_lifetime(),
                },
            )?;
            checkpoint(work)?;
            Ok(plan)
        })();
        if let Err(cause) = &result {
            domain.latch(cause.clone());
        }
        result
    }
    pub(super) fn partitions(&self) -> usize {
        self.partitions.len()
    }
    pub(super) fn sources(&self) -> &CapturedRuns {
        &self.sources
    }
    fn check(&self) -> ExecutionResult<()> {
        self.domain.check_failure()
    }
    fn latch(&self, cause: ExecutionFailure) {
        self.domain.latch(cause)
    }
    fn partition(&self, partition: usize) -> ExecutionResult<&SizingDomain> {
        self.partitions
            .get(partition)
            .ok_or_else(|| invariant(ValueViolation::Bounds))
    }
}
struct OpenPartial {
    partition: usize,
    occurrences: Vec<usize>,
    sketch: SizingSketch,
    // The exact final reallocating owner of this reference Vec, not the HLL
    // payload's possibly different allocator/charge and not the whole factory.
    storage: ValueTypeStorageLifetime,
}
impl OpenPartial {
    fn new(partition: usize, plan: &RoutePlan, work: &RouteWork<'_>) -> ExecutionResult<Self> {
        let sketch = SizingSketch::new(
            plan.partition(partition)?,
            Arc::clone(&work.host),
            work.control,
        )?;
        Ok(Self {
            partition,
            occurrences: Vec::new(),
            sketch,
            storage: ValueTypeStorageLifetime::pure(),
        })
    }
}
/// One exclusive workspace per real worker. Move it into a range cursor and
/// recover it only after that cursor drains all partials. It is not Clone.
pub(super) struct RouteWorkspace {
    plan: Arc<RoutePlan>,
    slots: Vec<Option<OpenPartial>>,
    evict: usize,
    page_rows: usize,
    failed: Option<ExecutionFailure>,
    storage: ValueTypeStorageLifetime,
}
impl RouteWorkspace {
    pub(super) fn new(
        plan: Arc<RoutePlan>,
        slots: NonZeroUsize,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<Self> {
        Self::with_target(plan, slots, REFERENCE_ROWS, work)
    }
    fn with_target(
        plan: Arc<RoutePlan>,
        slots: NonZeroUsize,
        page_rows: usize,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<Self> {
        let result: ExecutionResult<_> = (|| {
            plan.check()?;
            checkpoint(work)?;
            if page_rows == 0 {
                return Err(invariant(ValueViolation::Bounds));
            }
            let count = slots.get().min(plan.partitions());
            let mut entries = vector(work.owner, count)?;
            for index in 0..count {
                checkpoint_at(index, &|| checkpoint(work))?;
                entries.push(None);
            }
            Ok(Self {
                plan: Arc::clone(&plan),
                slots: entries,
                evict: 0,
                page_rows,
                failed: None,
                storage: work.owner.storage_lifetime(),
            })
        })();
        if let Err(cause) = &result {
            plan.latch(cause.clone());
        }
        result
    }
    fn check(&self) -> ExecutionResult<()> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        self.plan.check()
    }
    fn latch(&mut self, cause: ExecutionFailure) {
        if self.failed.is_none() {
            self.failed = Some(cause.clone());
        }
        self.plan.latch(cause);
        // Destroy handles/reference backing before releasing this actual
        // workspace directory hold. A failed workspace is never reusable.
        self.slots = Vec::new();
        self.storage = ValueTypeStorageLifetime::pure();
    }
    pub(super) fn start(
        self,
        page: CapturedPageId,
        range: Range<usize>,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<RouteCursor> {
        let plan = Arc::clone(&self.plan);
        let result: ExecutionResult<_> = (|| {
            self.check()?;
            checkpoint(work)?;
            if self.slots.iter().any(Option::is_some) {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            let source = plan.sources.page(page)?;
            if range.start > range.end || range.end > source.len() {
                return Err(invariant(ValueViolation::Bounds));
            }
            source
                .hashes()
                .validate(source.input(), plan.domain.policy())?;
            let start = range.start;
            let end = range.end;
            Ok(RouteCursor {
                workspace: self,
                page,
                start,
                position: start,
                end,
                eligible: 0,
                completed: false,
            })
        })();
        if let Err(cause) = &result {
            plan.latch(cause.clone());
        }
        result
    }
    #[cfg(test)]
    fn with_page_target_for_test(
        plan: Arc<RoutePlan>,
        slots: NonZeroUsize,
        page_rows: usize,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<Self> {
        Self::with_target(plan, slots, page_rows, work)
    }
}
/// Every row in a reference page belongs to this one captured source page.
/// Its original directory/creator stays alive without copying key payload.
pub(super) struct ReferencePage {
    sources: CapturedRuns,
    page: CapturedPageId,
    occurrences: Vec<usize>,
    _storage: ValueTypeStorageLifetime,
}
impl ReferencePage {
    #[cfg(test)]
    pub(super) fn page_id(&self) -> CapturedPageId {
        self.page
    }
    pub(super) fn source_page(&self) -> ExecutionResult<&CapturedPage> {
        self.sources.page(self.page)
    }
    pub(super) fn occurrences(&self) -> &[usize] {
        &self.occurrences
    }
    pub(super) fn len(&self) -> usize {
        self.occurrences.len()
    }
    #[cfg(test)]
    pub(super) fn reference(&self, index: usize) -> ExecutionResult<BulkOccurrence> {
        let occurrence = *self
            .occurrences
            .get(index)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        Ok(BulkOccurrence {
            page: self.page,
            occurrence,
        })
    }
}
pub(super) struct RoutePartial {
    plan: Arc<RoutePlan>,
    partition: usize,
    references: ReferencePage,
    payload: SizingPayload,
}
impl RoutePartial {
    pub(super) fn partition(&self) -> usize {
        self.partition
    }
    #[cfg(test)]
    pub(super) fn references(&self) -> &ReferencePage {
        &self.references
    }
}
pub(super) enum RouteAdvance {
    Progress,
    Yielded(RoutePartial),
    Completed,
}
pub(super) struct RoutedRange {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the completed route source/range fact beside exact eligible counts."
        )
    )]
    page: CapturedPageId,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the completed route source/range fact beside exact eligible counts."
        )
    )]
    range: Range<usize>,
    eligible: usize,
}
impl RoutedRange {
    #[cfg(test)]
    pub(super) fn page(&self) -> CapturedPageId {
        self.page
    }
    #[cfg(test)]
    pub(super) fn range(&self) -> Range<usize> {
        self.range.clone()
    }
    pub(super) fn eligible(&self) -> usize {
        self.eligible
    }
}
pub(super) struct RouteCursor {
    workspace: RouteWorkspace,
    page: CapturedPageId,
    start: usize,
    position: usize,
    end: usize,
    eligible: usize,
    completed: bool,
}
impl RouteCursor {
    #[cfg(test)]
    pub(super) fn position(&self) -> usize {
        self.position
    }
    #[cfg(test)]
    pub(super) fn active_partials(&self) -> usize {
        self.workspace.slots.iter().filter(|s| s.is_some()).count()
    }
    pub(super) fn advance(
        &mut self,
        quota: usize,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<RouteAdvance> {
        let result: ExecutionResult<_> = self.advance_inner(quota, work);
        if let Err(cause) = &result {
            self.workspace.latch(cause.clone());
        }
        result
    }
    fn advance_inner(
        &mut self,
        mut quota: usize,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<RouteAdvance> {
        self.workspace.check()?;
        checkpoint(work)?;
        if self.completed {
            return Ok(RouteAdvance::Completed);
        }
        let plan = Arc::clone(&self.workspace.plan);
        let page = plan.sources.page(self.page)?;
        while quota > 0 && self.position < self.end {
            checkpoint(work)?;
            let occurrence = self.position;
            if !page.hashes().participates(occurrence)? {
                self.position += 1;
                quota -= 1;
                continue;
            }
            let partition = plan.layout.route(page.hashes().hash(occurrence)?);
            let existing = self
                .workspace
                .slots
                .iter()
                .position(|s| s.as_ref().is_some_and(|s| s.partition == partition));
            let index = if let Some(index) = existing {
                index
            } else if let Some(index) = self.workspace.slots.iter().position(Option::is_none) {
                self.workspace.slots[index] = Some(OpenPartial::new(partition, &plan, work)?);
                index
            } else {
                let index = self.workspace.evict;
                self.workspace.evict = if index + 1 == self.workspace.slots.len() {
                    0
                } else {
                    index + 1
                };
                // The current input occurrence is intentionally not consumed.
                // Parent merges this partial before admitting more frontier.
                return self.emit(index, work).map(RouteAdvance::Yielded);
            };
            let slot = self.workspace.slots[index]
                .as_mut()
                .expect("selected occupied slot");
            let old_capacity = slot.occurrences.capacity();
            reserve_push(&mut slot.occurrences, work.owner, &|| checkpoint(work))?;
            if slot.occurrences.capacity() != old_capacity {
                slot.storage = work.owner.storage_lifetime();
            }
            // Exactly this admitted occurrence's HLL work is charged now.
            // Flush never loops over an old reference page to defer that work.
            slot.sketch.update_occurrences(
                page.hashes(),
                std::iter::once(occurrence),
                work.control,
            )?;
            slot.occurrences.push(occurrence);
            self.position += 1;
            self.eligible = self.eligible.checked_add(1).ok_or_else(arithmetic)?;
            quota -= 1;
            if slot.occurrences.len() >= self.workspace.page_rows {
                return self.emit(index, work).map(RouteAdvance::Yielded);
            }
        }
        if self.position == self.end {
            if let Some(index) = self.workspace.slots.iter().position(Option::is_some) {
                if quota == 0 {
                    return Ok(RouteAdvance::Progress);
                }
                return self.emit(index, work).map(RouteAdvance::Yielded);
            }
            self.completed = true;
            self.workspace.check()?;
            checkpoint(work)?;
            return Ok(RouteAdvance::Completed);
        }
        self.workspace.check()?;
        checkpoint(work)?;
        Ok(RouteAdvance::Progress)
    }
    fn emit(&mut self, index: usize, work: &RouteWork<'_>) -> ExecutionResult<RoutePartial> {
        self.workspace.check()?;
        checkpoint(work)?;
        let slot = self
            .workspace
            .slots
            .get_mut(index)
            .and_then(Option::take)
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        if slot.occurrences.is_empty() {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        let payload = slot.sketch.seal(work.control)?;
        self.workspace.check()?;
        checkpoint(work)?;
        Ok(RoutePartial {
            plan: Arc::clone(&self.workspace.plan),
            partition: slot.partition,
            references: ReferencePage {
                sources: self.workspace.plan.sources.clone(),
                page: self.page,
                occurrences: slot.occurrences,
                _storage: slot.storage,
            },
            payload,
        })
    }
    pub(super) fn finish(
        self,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<(RouteWorkspace, RoutedRange)> {
        let plan = Arc::clone(&self.workspace.plan);
        let result: ExecutionResult<_> = (|| {
            self.workspace.check()?;
            checkpoint(work)?;
            if !self.completed
                || self.position != self.end
                || self.workspace.slots.iter().any(Option::is_some)
            {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            Ok((
                self.workspace,
                RoutedRange {
                    page: self.page,
                    range: self.start..self.end,
                    eligible: self.eligible,
                },
            ))
        })();
        if let Err(cause) = &result {
            plan.latch(cause.clone());
        }
        result
    }
}

/// This describes the refs actually merged so far, not completed routing
/// coverage or exact distinct classes. Only the parent graph can seal coverage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RoutedNdv {
    ExactEmptyMergedSet,
    ObservedHll(u64),
}
pub(super) struct MergeAccumulator {
    plan: Arc<RoutePlan>,
    partition: usize,
    sketch: Option<SizingSketch>,
    pages: Vec<ReferencePage>,
    eligible: usize,
    directory_storage: ValueTypeStorageLifetime,
    failed: Option<ExecutionFailure>,
}
impl MergeAccumulator {
    /// No P-fold sketch initialization in a coordinator. The first real merge
    /// creates this partition's one accumulator through its actual worker host.
    pub(super) fn new(
        plan: Arc<RoutePlan>,
        partition: usize,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<Self> {
        let result: ExecutionResult<_> = (|| {
            plan.check()?;
            checkpoint(work)?;
            plan.partition(partition)?;
            Ok(Self {
                plan: Arc::clone(&plan),
                partition,
                sketch: None,
                pages: Vec::new(),
                eligible: 0,
                directory_storage: ValueTypeStorageLifetime::pure(),
                failed: None,
            })
        })();
        if let Err(cause) = &result {
            plan.latch(cause.clone());
        }
        result
    }
    fn check(&self) -> ExecutionResult<()> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        self.plan.check()
    }
    fn latch(&mut self, cause: ExecutionFailure) {
        if self.failed.is_none() {
            self.failed = Some(cause.clone());
        }
        self.plan.latch(cause);
        self.sketch = None;
        self.pages = Vec::new();
        self.directory_storage = ValueTypeStorageLifetime::pure();
    }
    pub(super) fn merge(
        &mut self,
        partial: RoutePartial,
        work: &RouteWork<'_>,
    ) -> ExecutionResult<()> {
        let result: ExecutionResult<_> = (|| {
            self.check()?;
            checkpoint(work)?;
            if !Arc::ptr_eq(&self.plan, &partial.plan)
                || partial.partition != self.partition
                || !partial
                    .payload
                    .domain()
                    .same_instance(self.plan.partition(self.partition)?)
                || partial.references.len() == 0
            {
                return Err(invariant(ValueViolation::Layout));
            }
            let eligible = self
                .eligible
                .checked_add(partial.references.len())
                .ok_or_else(arithmetic)?;
            let old_capacity = self.pages.capacity();
            reserve_push(&mut self.pages, work.owner, &|| checkpoint(work))?;
            if self.pages.capacity() != old_capacity {
                self.directory_storage = work.owner.storage_lifetime();
            }
            if self.sketch.is_none() {
                self.sketch = Some(SizingSketch::new(
                    self.plan.partition(self.partition)?,
                    Arc::clone(&work.host),
                    work.control,
                )?);
            }
            self.sketch
                .as_mut()
                .expect("participating partial created sketch")
                .merge_payload(partial.payload, work.control)?;
            self.check()?;
            checkpoint(work)?;
            self.pages.push(partial.references);
            self.eligible = eligible;
            Ok(())
        })();
        if let Err(cause) = &result {
            self.latch(cause.clone());
        }
        result
    }
    pub(super) fn eligible(&self) -> ExecutionResult<usize> {
        self.check()?;
        Ok(self.eligible)
    }
    pub(super) fn ndv_so_far(&self, work: &RouteWork<'_>) -> ExecutionResult<RoutedNdv> {
        self.check()?;
        match &self.sketch {
            Some(sketch) => sketch
                .estimate_observed(work.control)
                .map(RoutedNdv::ObservedHll),
            None => Ok(RoutedNdv::ExactEmptyMergedSet),
        }
    }
    /// Consumes one private accumulator. This output is not a graph completion
    /// or Ready proof; main must still certify all route ranges/partials merged.
    pub(super) fn finish(self, work: &RouteWork<'_>) -> ExecutionResult<MergedPartition> {
        let plan = Arc::clone(&self.plan);
        let result: ExecutionResult<_> = (|| {
            self.check()?;
            checkpoint(work)?;
            let ndv = self.ndv_so_far(work)?;
            Ok(MergedPartition {
                partition: self.partition,
                domain: self.plan.partition(self.partition)?.clone(),
                pages: self.pages,
                eligible: self.eligible,
                ndv,
                _storage: self.directory_storage,
            })
        })();
        if let Err(cause) = &result {
            plan.latch(cause.clone());
        }
        result
    }
}
pub(super) struct MergedPartition {
    partition: usize,
    domain: SizingDomain,
    pages: Vec<ReferencePage>,
    eligible: usize,
    ndv: RoutedNdv,
    _storage: ValueTypeStorageLifetime,
}
impl MergedPartition {
    pub(super) fn partition(&self) -> usize {
        self.partition
    }
    pub(super) fn domain(&self) -> &SizingDomain {
        &self.domain
    }
    pub(super) fn pages(&self) -> &[ReferencePage] {
        &self.pages
    }
    pub(super) fn eligible(&self) -> usize {
        self.eligible
    }
    pub(super) fn ndv(&self) -> RoutedNdv {
        self.ndv
    }
    /// Move the original directory without allocation or a second page list.
    /// Its original creator outlives both unread pages and the Vec backing.
    pub(super) fn into_reference_pages(self) -> MergedReferencePages {
        MergedReferencePages {
            pages: self.pages.into_iter(),
            _storage: self._storage,
        }
    }
}
/// An owning view of the existing merged reference directory, not a new
/// routing/completion protocol. Unread pages drop before their directory owner.
/// Consumed pages move out once; their original source/creator goes with them.
pub(super) struct MergedReferencePages {
    pages: std::vec::IntoIter<ReferencePage>,
    _storage: ValueTypeStorageLifetime,
}
impl MergedReferencePages {
    pub(super) fn next_page(&mut self) -> Option<ReferencePage> {
        self.pages.next()
    }
    pub(super) fn remaining(&self) -> usize {
        self.pages.len()
    }
}
fn checkpoint(work: &RouteWork<'_>) -> ExecutionResult<()> {
    work.control.checkpoint(0).map_err(ExecutionFailure::from)
}
fn arithmetic() -> ExecutionFailure {
    ConstructionFailure::from(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
    .into()
}

#[cfg(test)]
#[path = "route_tests.rs"]
mod tests;
