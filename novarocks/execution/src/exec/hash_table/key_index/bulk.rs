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

//! Merge sealed producer facts without visiting their input pages. Complete
//! capture coverage is distinct from completion of the later index task graph.

#[cfg(test)]
use super::capture::BulkOccurrence;
use super::{
    IndexPurpose,
    capture::{BulkDomain, BulkInput, CaptureWork, CapturedPage, CapturedPageId, SealedPages},
    checkpoint_at, invariant, shared,
    sizing::{SizingDomain, SizingSketch},
    statistics::BuildRangeStats,
    vector,
};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_value_arrays::ValueViolation;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NdvMethod {
    /// Exact absence of participating occurrences, independently of HLL.
    EmptyIndexableDomain,
    /// The existing fixed-parameter sizing author, over all sealed runs.
    WholeInputHll,
}

/// Only the complete sealed-run merger can construct this fact. The original
/// sizing domain carries the exact spec, NULL/hash policy and sketch settings;
/// counts bind coverage to this one complete captured input.
pub(super) struct NdvHint {
    domain: SizingDomain,
    input_rows: usize,
    indexable_rows: usize,
    estimate: u64,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the original selector cost/provenance fact; it grants no allocation authority."
        )
    )]
    method: NdvMethod,
}
impl NdvHint {
    pub(super) fn estimate(&self) -> u64 {
        self.estimate
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the complete-input NDV provenance accessor; tests distinguish exact-empty facts from HLL estimates."
        )
    )]
    pub(super) fn method(&self) -> NdvMethod {
        self.method
    }
    pub(super) fn validate(
        &self,
        domain: &BulkDomain,
        exact: &BuildRangeStats,
    ) -> ExecutionResult<()> {
        if !self.domain.same_instance(domain.sizing_domain())
            || !exact.spec().same_instance(domain.spec())
            || self.input_rows != exact.input_rows()
            || self.indexable_rows != exact.indexable_rows()
        {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(())
    }
    /// Checked 25% sizing margin, bounded only by exact eligible occurrences.
    /// Retain the original estimate separately; this is no distinct-count fact
    /// and creates no capacity grant. A later hash table may grow normally.
    pub(super) fn initial_reservation(&self) -> usize {
        let observed = u128::from(self.estimate);
        let with_margin = observed + observed.div_ceil(4);
        with_margin.min(self.indexable_rows as u128) as usize
    }
}

struct RunDirectory {
    runs: Vec<SealedPages>,
    _storage: ValueTypeStorageLifetime,
}
/// Moving or cloning this holder keeps the actual captured pages and directory
/// creator alive. A task borrows one page; it never flattens the full input.
#[derive(Clone)]
pub(super) struct CapturedRuns(Arc<RunDirectory>);
impl CapturedRuns {
    pub(super) fn len(&self) -> usize {
        self.0.runs.len()
    }
    pub(super) fn run(&self, ordinal: usize) -> ExecutionResult<&SealedPages> {
        self.0
            .runs
            .get(ordinal)
            .ok_or_else(|| invariant(ValueViolation::Bounds))
    }
    pub(super) fn page(&self, id: CapturedPageId) -> ExecutionResult<&CapturedPage> {
        let page = self.run(id.run.index())?.page(id.page)?;
        if page.id() != id {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        Ok(page)
    }
    #[cfg(test)]
    pub(super) fn row(&self, reference: BulkOccurrence) -> ExecutionResult<usize> {
        self.page(reference.page)?.input().row(reference.occurrence)
    }
}

pub(super) struct CompleteStats {
    exact: BuildRangeStats,
    hint: NdvHint,
    purpose: IndexPurpose,
    domain: BulkDomain,
}
impl CompleteStats {
    pub(super) fn exact(&self) -> &BuildRangeStats {
        &self.exact
    }
    pub(super) fn hint(&self) -> &NdvHint {
        &self.hint
    }
    pub(super) fn purpose(&self) -> IndexPurpose {
        self.purpose
    }
    pub(super) fn domain(&self) -> &BulkDomain {
        &self.domain
    }
    pub(super) fn check(&self) -> ExecutionResult<()> {
        self.domain.check_failure()?;
        self.hint.validate(&self.domain, &self.exact)
    }
}

/// This proves complete capture facts only. The later graph must still build,
/// materialize and assign every required range before it can publish Ready.
pub(crate) struct CompleteInput {
    runs: CapturedRuns,
    stats: CompleteStats,
}
impl CompleteInput {
    pub(crate) fn from_captures(
        input: BulkInput,
        purpose: IndexPurpose,
        work: &CaptureWork<'_>,
    ) -> ExecutionResult<Self> {
        let domain = input.domain().clone();
        let result = Self::merge(input, purpose, work);
        if let Err(cause) = &result {
            domain.latch(cause.clone());
        }
        result
    }
    fn merge(
        mut input: BulkInput,
        purpose: IndexPurpose,
        work: &CaptureWork<'_>,
    ) -> ExecutionResult<Self> {
        let checkpoint = || work.control.checkpoint(0).map_err(ExecutionFailure::from);
        let domain = input.domain().clone();
        domain.check_failure()?;
        checkpoint()?;
        let count = input.sealed_runs();
        if input.len() != count {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        let mut runs = vector(work.owner, count)?;
        let mut exact = BuildRangeStats::empty_for_spec(domain.spec());
        let mut sketch: Option<SizingSketch> = None;
        for expected in (0..count).rev() {
            checkpoint_at(expected, &checkpoint)?;
            let capture = input
                .pop_capture()
                .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
            let (pages, counts, payload) = capture.into_parts();
            if pages.run_id().index() != expected {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            // The private capture writer updates a sketch only for eligible
            // occurrences. An exact empty participating domain needs neither
            // a destination sketch nor a decoder for that empty partial.
            if counts.indexable_rows() != 0 {
                if sketch.is_none() {
                    sketch = Some(SizingSketch::new(
                        domain.sizing_domain(),
                        Arc::clone(&work.host),
                        work.control,
                    )?);
                }
                sketch
                    .as_mut()
                    .expect("the participating run created a sketch")
                    .merge_payload(payload, work.control)?;
            }
            exact.merge_completed_range(counts, &checkpoint)?;
            runs.push(pages);
        }
        // Reverse only E directory headers. Payload/page/occurrence storage is
        // moved intact and remains indexed by its privately minted run id.
        for ordinal in 0..count / 2 {
            checkpoint_at(ordinal, &checkpoint)?;
            runs.swap(ordinal, count - 1 - ordinal);
        }
        let (estimate, method) = match sketch {
            Some(sketch) => (
                sketch.estimate_observed(work.control)?,
                NdvMethod::WholeInputHll,
            ),
            None => (0, NdvMethod::EmptyIndexableDomain),
        };
        let hint = NdvHint {
            domain: domain.sizing_domain().clone(),
            input_rows: exact.input_rows(),
            indexable_rows: exact.indexable_rows(),
            estimate,
            method,
        };
        let runs = CapturedRuns(shared(
            work.owner,
            RunDirectory {
                runs,
                _storage: work.owner.storage_lifetime(),
            },
        )?);
        let stats = CompleteStats {
            exact,
            hint,
            purpose,
            domain,
        };
        stats.check()?;
        checkpoint()?;
        Ok(Self { runs, stats })
    }
    pub(super) fn stats(&self) -> &CompleteStats {
        &self.stats
    }
    pub(super) fn runs(&self) -> &CapturedRuns {
        &self.runs
    }
    pub(super) fn into_parts(self) -> (CapturedRuns, CompleteStats) {
        (self.runs, self.stats)
    }
}
