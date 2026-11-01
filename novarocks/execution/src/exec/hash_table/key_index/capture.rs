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

//! Actual per-producer capture of immutable evaluated key pages. The private
//! writer certifies its completed appends; small summaries do not certify an
//! arbitrary caller's declared input. No payload store or task graph lives here.

use super::{
    KeyInput, KeySpec, LocalHashBatch, LocalHashPolicy, PreparedKeyBatch, checkpoint_at,
    hash_batch, invariant, reserve_push, shared,
    sizing::{SizingDomain, SizingDomainKind, SizingPayload, SizingSketch},
    statistics::BuildRangeStats,
    vector,
};
use crate::{
    exec::{
        hash_table::allocation::AggregateAllocator,
        pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::array::ArrayRef;
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ValueViolation, batch::hash::HashScratch, decode::ValueConversionContext,
    reachability::SelectionSet,
};
use std::sync::{Arc, Mutex, MutexGuard};

/// Borrow only the original caller's existing construction and allocation ports.
/// Nothing here is retained in a sealed page or grants resource capacity.
pub(crate) struct CaptureWork<'a> {
    pub(crate) owner: &'a dyn InputConstructionOwner,
    pub(crate) allocator: AggregateAllocator,
    pub(crate) host: Arc<dyn AggregateStateAllocator>,
    pub(crate) control: &'a dyn KernelEvaluationControl,
}
pub(crate) struct EvaluatedPage<'a> {
    pub(crate) arrays: &'a [ArrayRef],
    pub(crate) bindings: &'a [FunctionValueType],
    pub(crate) readable: &'a SelectionSet,
    pub(crate) occurrences: &'a [usize],
}

struct CaptureGate {
    issued: usize,
    closed: bool,
    failed: Option<ExecutionFailure>,
}
struct Domain {
    spec: KeySpec,
    policy: LocalHashPolicy,
    whole: SizingDomain,
    gate: Mutex<CaptureGate>,
    #[cfg(test)]
    forced_hash: Option<u64>,
    _storage: ValueTypeStorageLifetime,
}
/// All producers share this actual spec/policy/whole-sketch instance. Equal
/// seeds, root declarations or numeric run labels do not recreate this domain.
#[derive(Clone)]
pub(crate) struct BulkDomain(Arc<Domain>);
impl BulkDomain {
    pub(crate) fn new(
        spec: &KeySpec,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        control.checkpoint(0)?;
        let policy = LocalHashPolicy::new(spec, owner)?;
        let whole = SizingDomain::new(spec, &policy, SizingDomainKind::WholeInput, owner)?;
        let result = Self(shared(
            owner,
            Domain {
                spec: spec.clone(),
                policy,
                whole,
                gate: Mutex::new(CaptureGate {
                    issued: 0,
                    closed: false,
                    failed: None,
                }),
                #[cfg(test)]
                forced_hash: None,
                _storage: owner.storage_lifetime(),
            },
        )?);
        control.checkpoint(0)?;
        Ok(result)
    }
    /// A build-wide test policy, installed before any capture claim exists.
    /// Counts and HLL observe the same saved hashes as routing and indexing.
    #[cfg(test)]
    pub(super) fn new_forced_hash_for_test(
        spec: &KeySpec,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
        hash: u64,
    ) -> ExecutionResult<Self> {
        let mut domain = Self::new(spec, owner, control)?;
        Arc::get_mut(&mut domain.0)
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?
            .forced_hash = Some(hash);
        Ok(domain)
    }
    pub(crate) fn spec(&self) -> &KeySpec {
        &self.0.spec
    }
    pub(crate) fn policy(&self) -> &LocalHashPolicy {
        &self.0.policy
    }
    pub(super) fn sizing_domain(&self) -> &SizingDomain {
        &self.0.whole
    }
    fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    fn gate(&self) -> ExecutionResult<MutexGuard<'_, CaptureGate>> {
        self.0
            .gate
            .lock()
            .map_err(|_| invariant(ValueViolation::RowTransaction))
    }
    fn check(&self) -> ExecutionResult<()> {
        let gate = self.gate()?;
        if let Some(cause) = &gate.failed {
            return Err(cause.clone());
        }
        if gate.closed {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        Ok(())
    }
    /// The later graph checks this original capture failure before publishing
    /// Ready. A normal closed capture set is allowed; this creates no rollback
    /// or mutation capability for an already published immutable index.
    pub(super) fn check_failure(&self) -> ExecutionResult<()> {
        let gate = self.gate()?;
        match &gate.failed {
            Some(cause) => Err(cause.clone()),
            None => Ok(()),
        }
    }
    pub(super) fn latch(&self, cause: ExecutionFailure) {
        if let Ok(mut gate) = self.gate()
            && gate.failed.is_none()
        {
            gate.failed = Some(cause);
        }
    }
    fn claim(&self) -> ExecutionResult<RunClaim> {
        let mut gate = self.gate()?;
        if let Some(cause) = &gate.failed {
            return Err(cause.clone());
        }
        if gate.closed {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        let ordinal = gate.issued;
        gate.issued = gate.issued.checked_add(1).ok_or_else(arithmetic)?;
        Ok(RunClaim {
            domain: self.clone(),
            run: CaptureRunId(ordinal),
        })
    }
    /// The claim is private and non-Clone. Reusing a caller usize cannot create
    /// another writer for the same run, or manufacture a completed run receipt.
    pub(crate) fn begin_capture(&self, work: &CaptureWork<'_>) -> ExecutionResult<CaptureBuilder> {
        let result = (|| {
            let claim = self.claim()?;
            CaptureBuilder::from_claim(claim, work)
        })();
        if let Err(cause) = &result {
            self.latch(cause.clone());
        }
        result
    }
    fn close_claims(&self) -> ExecutionResult<usize> {
        let mut gate = self.gate()?;
        if let Some(cause) = &gate.failed {
            return Err(cause.clone());
        }
        if gate.closed {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        gate.closed = true;
        Ok(gate.issued)
    }
}
struct RunClaim {
    domain: BulkDomain,
    run: CaptureRunId,
}
/// Build-local coordinate only. It grants neither coverage nor storage access;
/// assembly separately checks the actual domain and privately minted claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(super) struct CaptureRunId(usize);
impl CaptureRunId {
    pub(super) fn index(self) -> usize {
        self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CapturedPageId {
    pub(super) run: CaptureRunId,
    pub(super) page: usize,
}
impl CapturedPageId {
    /// Original build-local directory coordinates, not a fresh identity or
    /// permission to construct, replay or settle a captured page.
    pub(crate) fn run_index(self) -> usize {
        self.run.index()
    }
    pub(crate) fn page_index(self) -> usize {
        self.page
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(super) struct BulkOccurrence {
    pub(super) page: CapturedPageId,
    pub(super) occurrence: usize,
}
pub(super) struct CapturedPage {
    id: CapturedPageId,
    input: KeyInput,
    hashes: LocalHashBatch,
    occurrences: usize,
}
impl CapturedPage {
    pub(super) fn id(&self) -> CapturedPageId {
        self.id
    }
    pub(super) fn input(&self) -> &KeyInput {
        &self.input
    }
    pub(super) fn hashes(&self) -> &LocalHashBatch {
        &self.hashes
    }
    pub(super) fn len(&self) -> usize {
        self.occurrences
    }
    #[cfg(test)]
    pub(super) fn occurrence(&self, occurrence: usize) -> ExecutionResult<BulkOccurrence> {
        if occurrence >= self.occurrences {
            return Err(invariant(ValueViolation::Bounds));
        }
        Ok(BulkOccurrence {
            page: self.id,
            occurrence,
        })
    }
}
struct OpenCapture {
    pages: Vec<CapturedPage>,
    counts: BuildRangeStats,
    sketch: SizingSketch,
    occurrences: usize,
    // Directory backing drops before the finite hold of its actual last grower.
    directory_storage: ValueTypeStorageLifetime,
}
pub(crate) struct CaptureBuilder {
    domain: BulkDomain,
    run: CaptureRunId,
    open: Option<OpenCapture>,
    failed: Option<ExecutionFailure>,
}
impl CaptureBuilder {
    fn from_claim(claim: RunClaim, work: &CaptureWork<'_>) -> ExecutionResult<Self> {
        work.control.checkpoint(0)?;
        claim.domain.check()?;
        let sketch = SizingSketch::new(
            claim.domain.sizing_domain(),
            Arc::clone(&work.host),
            work.control,
        )?;
        Ok(Self {
            run: claim.run,
            open: Some(OpenCapture {
                pages: Vec::new(),
                counts: BuildRangeStats::empty_for_spec(claim.domain.spec()),
                sketch,
                occurrences: 0,
                directory_storage: ValueTypeStorageLifetime::pure(),
            }),
            domain: claim.domain,
            failed: None,
        })
    }
    #[cfg(test)]
    pub(super) fn run_id(&self) -> CaptureRunId {
        self.run
    }
    fn check(&self) -> ExecutionResult<()> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        if self.open.is_none() {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        self.domain.check()
    }
    fn latch(&mut self, cause: ExecutionFailure) {
        if self.failed.is_none() {
            self.failed = Some(cause.clone());
        }
        self.domain.latch(cause);
        // Failed capture cannot publish partial success. Actual inputs and HLL
        // backing exit under their existing creator holds, before any refund.
        self.open = None;
    }
    /// Borrow this still-open producer's stored page by build-local directory
    /// coordinates. The builder is the access owner; a numeric page ID is not
    /// an independent proof of identity, coverage, completion or capacity.
    pub(crate) fn captured_page(
        &self,
        id: CapturedPageId,
    ) -> ExecutionResult<(&KeyInput, &LocalHashBatch)> {
        self.check()?;
        if id.run != self.run {
            return Err(invariant(ValueViolation::Layout));
        }
        let page = self
            .open
            .as_ref()
            .and_then(|open| open.pages.get(id.page))
            .filter(|page| page.id == id)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        Ok((&page.input, &page.hashes))
    }
    pub(crate) fn push_evaluated(
        &mut self,
        page: EvaluatedPage<'_>,
        conversions: &mut ValueConversionContext,
        work: &CaptureWork<'_>,
    ) -> ExecutionResult<CapturedPageId> {
        let result = (|| {
            self.check()?;
            self.push(page, conversions, work)
        })();
        if let Err(cause) = &result {
            self.latch(cause.clone());
        }
        result
    }
    fn push(
        &mut self,
        page: EvaluatedPage<'_>,
        conversions: &mut ValueConversionContext,
        work: &CaptureWork<'_>,
    ) -> ExecutionResult<CapturedPageId> {
        let checkpoint = || work.control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        let input = KeyInput::new_for_spec(
            self.domain.spec(),
            page.arrays,
            page.bindings,
            page.readable,
            page.occurrences,
            conversions,
            work.owner,
            &checkpoint,
        )?;
        // Scope this original scratch to this actual creator. Cross-page
        // capacity reuse would need its own real last-allocation custody facts.
        let (hashes, counts) = {
            let prepared = PreparedKeyBatch::from_captured(
                self.domain.spec(),
                &input,
                work.allocator.clone(),
                &checkpoint,
            )?;
            let hashes = hash_batch(
                &prepared,
                self.domain.policy(),
                &mut HashScratch::new(),
                work.owner,
                &checkpoint,
            )?;
            #[cfg(test)]
            let hashes = if let Some(value) = self.domain.0.forced_hash {
                hashes.force_hash(value)
            } else {
                hashes
            };
            let counts = BuildRangeStats::collect_hashed(
                &prepared,
                &hashes,
                self.domain.policy(),
                0..input.len(),
                &checkpoint,
            )?;
            (hashes, counts)
        };
        let open = self
            .open
            .as_mut()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        let occurrences = open
            .occurrences
            .checked_add(input.len())
            .ok_or_else(arithmetic)?;
        let id = CapturedPageId {
            run: self.run,
            page: open.pages.len(),
        };
        let old_capacity = open.pages.capacity();
        reserve_push(&mut open.pages, work.owner, &checkpoint)?;
        if open.pages.capacity() != old_capacity {
            open.directory_storage = work.owner.storage_lifetime();
        }
        open.sketch
            .update_occurrences(&hashes, 0..input.len(), work.control)?;
        open.counts.merge_completed_range(counts, &checkpoint)?;
        checkpoint()?;
        open.pages.push(CapturedPage {
            id,
            occurrences: input.len(),
            input,
            hashes,
        });
        open.occurrences = occurrences;
        Ok(id)
    }
    /// Exactly one successful seal. All page work is already complete; only
    /// fixed-parameter HLL serialization and ownership moves happen here.
    pub(crate) fn seal(&mut self, work: &CaptureWork<'_>) -> ExecutionResult<SealedCapture> {
        let result = (|| {
            self.check()?;
            self.seal_inner(work)
        })();
        if let Err(cause) = &result {
            self.latch(cause.clone());
        }
        result
    }
    fn seal_inner(&mut self, work: &CaptureWork<'_>) -> ExecutionResult<SealedCapture> {
        work.control.checkpoint(0)?;
        let open = self
            .open
            .take()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        if open.occurrences != open.counts.input_rows() {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        let sizing = open.sketch.seal(work.control)?;
        work.control.checkpoint(0)?;
        Ok(SealedCapture {
            directory: SealedPages {
                pages: open.pages,
                domain: self.domain.clone(),
                run: self.run,
                occurrences: open.occurrences,
                _storage: open.directory_storage,
            },
            counts: open.counts,
            sizing,
        })
    }
}
pub(super) struct SealedPages {
    pages: Vec<CapturedPage>,
    domain: BulkDomain,
    run: CaptureRunId,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the sealed producer occurrence subtotal alongside its actual page directory."
        )
    )]
    occurrences: usize,
    _storage: ValueTypeStorageLifetime,
}
impl SealedPages {
    pub(super) fn run_id(&self) -> CaptureRunId {
        self.run
    }
    pub(super) fn len(&self) -> usize {
        self.pages.len()
    }
    #[cfg(test)]
    pub(super) fn occurrences(&self) -> usize {
        self.occurrences
    }
    pub(super) fn page(&self, page: usize) -> ExecutionResult<&CapturedPage> {
        self.pages
            .get(page)
            .ok_or_else(|| invariant(ValueViolation::Bounds))
    }
}
pub(crate) struct SealedCapture {
    directory: SealedPages,
    counts: BuildRangeStats,
    sizing: SizingPayload,
}
impl SealedCapture {
    #[cfg(test)]
    pub(super) fn pages(&self) -> &SealedPages {
        &self.directory
    }
    #[cfg(test)]
    pub(super) fn counts(&self) -> &BuildRangeStats {
        &self.counts
    }
    pub(super) fn into_parts(self) -> (SealedPages, BuildRangeStats, SizingPayload) {
        (self.directory, self.counts, self.sizing)
    }
}
/// E sealed directories, never a flattened B-page or B-row coordinator array.
/// The private domain closes its issued run set exactly once at assembly.
pub(crate) struct BulkInput {
    captures: Vec<Option<SealedCapture>>,
    sealed_runs: usize,
    domain: BulkDomain,
    _storage: ValueTypeStorageLifetime,
}
impl BulkInput {
    pub(crate) fn new(
        domain: &BulkDomain,
        captures: Vec<SealedCapture>,
        work: &CaptureWork<'_>,
    ) -> ExecutionResult<Self> {
        let result = (|| {
            let issued = domain.close_claims()?;
            Self::assemble(domain, captures, issued, work)
        })();
        if let Err(cause) = &result {
            domain.latch(cause.clone());
        }
        result
    }
    fn assemble(
        domain: &BulkDomain,
        captures: Vec<SealedCapture>,
        issued: usize,
        work: &CaptureWork<'_>,
    ) -> ExecutionResult<Self> {
        let checkpoint = || work.control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        if captures.len() != issued {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        let mut output = vector(work.owner, issued)?;
        for run in 0..issued {
            checkpoint_at(run, &checkpoint)?;
            output.push(None);
        }
        for (index, capture) in captures.into_iter().enumerate() {
            checkpoint_at(index, &checkpoint)?;
            if !domain.same_instance(&capture.directory.domain)
                || !domain.spec().same_instance(capture.counts.spec())
                || !domain
                    .sizing_domain()
                    .same_instance(capture.sizing.domain())
            {
                return Err(invariant(ValueViolation::Layout));
            }
            let run = capture.directory.run;
            if run.0 >= issued {
                return Err(invariant(ValueViolation::Bounds));
            }
            let slot = output
                .get_mut(run.0)
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            if slot.is_some() {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            // Only E privately minted run coordinates are indexed here. No
            // page descriptor, row, key or saved hash is visited at assembly.
            *slot = Some(capture);
        }
        checkpoint()?;
        Ok(Self {
            captures: output,
            sealed_runs: issued,
            domain: domain.clone(),
            _storage: work.owner.storage_lifetime(),
        })
    }
    pub(super) fn domain(&self) -> &BulkDomain {
        &self.domain
    }
    pub(super) fn len(&self) -> usize {
        self.captures.len()
    }
    /// Frozen original set size. Draining some receipts does not certify that
    /// the remaining input still covers this set.
    pub(super) fn sealed_runs(&self) -> usize {
        self.sealed_runs
    }
    #[cfg(test)]
    pub(super) fn capture(&self, run_ordinal: usize) -> ExecutionResult<&SealedCapture> {
        self.captures
            .get(run_ordinal)
            .and_then(Option::as_ref)
            .ok_or_else(|| invariant(ValueViolation::Bounds))
    }
    /// Move one whole E receipt without discarding the top directory's finite
    /// allocation hold. The caller may later place its page directory in a graph.
    pub(super) fn pop_capture(&mut self) -> Option<SealedCapture> {
        self.captures.pop().flatten()
    }
}
fn arithmetic() -> ExecutionFailure {
    ConstructionFailure::from(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
    .into()
}

#[cfg(test)]
#[path = "capture_tests.rs"]
mod tests;
