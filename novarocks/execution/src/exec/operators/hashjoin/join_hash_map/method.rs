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
//! The compiled join adapter to K. Only physical row topology stays here.

use super::super::join_hash_table::{JoinHashTable, allocated};
use super::search::{JoinSelection, ProbeMask, SearchStats};
use crate::{
    exec::hash_table::{
        allocation::AggregateVec,
        key_index::{
            BulkClaim, BulkDomain, BulkInput, BulkTask, BulkTaskGraph, BulkWork, CaptureBuilder,
            CaptureWork, CompleteInput, EvaluatedPage, IndexPurpose, IndexView, KeyInput, KeySpec,
            NullPolicy, PreparedKeyBatch,
        },
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::array::ArrayRef;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ValueFailure, decode::ValueConversionContext, reachability::ReachableRows,
};
use std::{num::NonZeroUsize, sync::Arc};

#[derive(Debug)]
struct JoinMapContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for JoinMapContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for JoinMapContract {}
impl novarocks_failure_contract::FailureProjection for JoinMapContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::JoinMapContract.reason(),
        )
    }
}
pub(crate) fn map_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(JoinMapContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
pub(crate) fn arithmetic() -> ExecutionFailure {
    crate::exec::pipeline::operator::ConstructionFailure::from(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
    .into()
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinHashMapBuildPurpose {
    RowMatches,
    PresenceOnly,
}
fn check(work: &BulkWork<'_>) -> ExecutionResult<()> {
    work.control.checkpoint(0).map_err(Into::into)
}
fn original<E: Into<ExecutionFailure>>(failure: ValueFailure<E>) -> ExecutionFailure {
    match failure {
        ValueFailure::Value(error) => ExecutionFailure::from_domain(error),
        ValueFailure::Caller(error) => error.into(),
    }
}
fn selection(
    work: &BulkWork<'_>,
    rows: usize,
) -> ExecutionResult<(
    novarocks_value_arrays::reachability::SelectionSet,
    AggregateVec<usize>,
)> {
    let readable = ReachableRows::all(rows)
        .materialize_observed(
            &mut |layout| work.owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| check(work),
        )
        .map_err(original)?;
    let mut occurrences = allocated(&work.allocator, rows)?;
    for row in 0..rows {
        check(work)?;
        occurrences.push(row);
    }
    Ok((readable, occurrences))
}
fn capture_work<'a>(work: &'a BulkWork<'_>) -> CaptureWork<'a> {
    CaptureWork {
        owner: work.owner,
        allocator: work.allocator.clone(),
        host: work.host.clone(),
        control: work.control,
    }
}

/// The current one-driver build owns one capture run. It evaluates no keys.
/// Its caller seals only on the original pipeline's successful upstream EOS.
pub(crate) struct JoinKeyBuild {
    domain: BulkDomain,
    capture: CaptureBuilder,
    purpose: IndexPurpose,
    page_bases: AggregateVec<usize>,
    rows: usize,
    failed: Option<ExecutionFailure>,
    _storage: ValueTypeStorageLifetime,
}
impl JoinKeyBuild {
    pub(crate) fn new(
        bindings: &[FunctionValueType],
        null_safe_eq: &[bool],
        purpose: JoinHashMapBuildPurpose,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Self> {
        let mut nulls = allocated(&work.allocator, null_safe_eq.len())?;
        for &safe in null_safe_eq {
            check(work)?;
            nulls.push(if safe {
                NullPolicy::Equal
            } else {
                NullPolicy::Unmatchable
            });
        }
        let spec = KeySpec::new(bindings, &nulls, work.owner, &|| check(work))?;
        let domain = BulkDomain::new(&spec, work.owner, work.control)?;
        let capture = domain.begin_capture(&capture_work(work))?;
        Ok(Self {
            domain,
            capture,
            purpose: match purpose {
                JoinHashMapBuildPurpose::RowMatches => IndexPurpose::KeyIds,
                JoinHashMapBuildPurpose::PresenceOnly => IndexPurpose::PresenceOnly,
            },
            page_bases: allocated(&work.allocator, 0)?,
            rows: 0,
            _storage: work.owner.storage_lifetime(),
            failed: None,
        })
    }
    #[cfg(test)]
    pub(crate) fn push(
        &mut self,
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        rows: usize,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<()> {
        self.push_observed(arrays, bindings, rows, conversions, work, &mut |_, _, _| {
            Ok(())
        })
    }
    pub(crate) fn push_observed(
        &mut self,
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        rows: usize,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
        observe: &mut dyn FnMut(usize, bool, bool) -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        let result = self.push_inner(arrays, bindings, rows, conversions, work, observe);
        if let Err(cause) = &result {
            self.failed = Some(cause.clone());
        }
        result
    }
    fn push_inner(
        &mut self,
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        rows: usize,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
        observe: &mut dyn FnMut(usize, bool, bool) -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        check(work)?;
        if arrays.len() != bindings.len() || arrays.iter().any(|array| array.len() != rows) {
            return Err(map_contract("join evaluated key page shape mismatch"));
        }
        let (readable, occurrences) = selection(work, rows)?;
        let id = self.capture.push_evaluated(
            EvaluatedPage {
                arrays,
                bindings,
                readable: &readable,
                occurrences: &occurrences,
            },
            conversions,
            &capture_work(work),
        )?;
        if id.run_index() != 0 || id.page_index() != self.page_bases.len() {
            return Err(map_contract("join capture page coordinate mismatch"));
        }
        let (input, hashes) = self.capture.captured_page(id)?;
        for occurrence in 0..input.len() {
            check(work)?;
            observe(
                input.row(occurrence)?,
                input.row_has_null_observed(occurrence, &|| check(work))?,
                hashes.participates(occurrence)?,
            )?;
        }
        self.page_bases.try_reserve(1).map_err(|source| {
            work.allocator
                .allocation_error("join original page coordinates", source)
        })?;
        self.page_bases.push(self.rows);
        self.rows = self.rows.checked_add(rows).ok_or_else(arithmetic)?;
        check(work)
    }
    pub(crate) fn finish(mut self, work: &BulkWork<'_>) -> ExecutionResult<JoinHashMap> {
        if let Some(cause) = self.failed.take() {
            return Err(cause);
        }
        let sealed = self.capture.seal(&capture_work(work))?;
        // Only one header: this is the current real single build driver, not
        // a cluster/backend count or a bound on the original input.
        let layout =
            std::alloc::Layout::array::<crate::exec::hash_table::key_index::SealedCapture>(1)
                .map_err(|_| arithmetic())?;
        work.owner.request(layout)?;
        let mut captures = Vec::new();
        captures.try_reserve_exact(1).map_err(|source| {
            work.allocator
                .allocation_error("join capture receipt directory", source)
        })?;
        captures.push(sealed);
        let complete = CompleteInput::from_captures(
            BulkInput::new(&self.domain, captures, &capture_work(work))?,
            self.purpose,
            &capture_work(work),
        )?;
        let graph = BulkTaskGraph::begin(
            complete,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            work,
        )?;
        let mut tasks: AggregateVec<BulkTask> = allocated(&work.allocator, 0)?;
        loop {
            check(work)?;
            // A paused route cursor retains data, not a CPU permit. Eager
            // merges must be claimed before advancing that cursor again.
            match graph.claim(work)? {
                BulkClaim::Task(task) => {
                    tasks.try_reserve(1).map_err(|source| {
                        work.allocator
                            .allocation_error("join local bulk task custody", source)
                    })?;
                    tasks.push(task);
                }
                BulkClaim::Pending => {}
                BulkClaim::Finished => break,
            }
            let mut index = 0;
            while index < tasks.len() {
                check(work)?;
                if tasks[index].advance(
                    novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize,
                    work,
                )? {
                    let task = tasks.swap_remove(index);
                    let mut completion = task.finish()?;
                    graph.complete(&mut completion, work)?;
                } else {
                    index += 1;
                }
            }
        }
        if !tasks.is_empty() {
            return Err(map_contract("join bulk graph finished with active tasks"));
        }
        let ready = graph
            .take_ready(work)?
            .ok_or_else(|| map_contract("join bulk graph did not publish a ready index"))?;
        let (view, assignments) = ready.into_parts();
        let groups = if self.purpose == IndexPurpose::KeyIds {
            Some(JoinHashTable::from_assignments(
                &assignments,
                &self.page_bases,
                self.rows,
                work,
            )?)
        } else {
            None
        };
        drop(assignments);
        check(work)?;
        Ok(JoinHashMap {
            view,
            groups,
            row_count: self.rows,
        })
    }
}

/// K owns the immutable key index; this wrapper owns only physical row topology.
pub(crate) struct JoinHashMap {
    view: Arc<IndexView>,
    groups: Option<JoinHashTable>,
    row_count: usize,
}
impl JoinHashMap {
    pub(crate) fn is_empty(&self) -> bool {
        self.row_count == 0
    }
    pub(crate) fn input(
        &self,
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        rows: usize,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<KeyInput> {
        if arrays.len() != bindings.len() || arrays.iter().any(|array| array.len() != rows) {
            return Err(map_contract("join probe key page shape mismatch"));
        }
        let (readable, occurrences) = selection(work, rows)?;
        KeyInput::new_for_spec(
            self.view.spec(),
            arrays,
            bindings,
            &readable,
            &occurrences,
            conversions,
            work.owner,
            &|| check(work),
        )
    }
    #[cfg(test)]
    pub(crate) fn search_membership_arrays(
        &self,
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        rows: usize,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<(ProbeMask, SearchStats)> {
        let input = self.input(arrays, bindings, rows, conversions, work)?;
        let prepared = PreparedKeyBatch::from_captured(
            self.view.spec(),
            &input,
            work.allocator.clone(),
            &|| check(work),
        )?;
        self.membership(&prepared, work)
    }
    pub(crate) fn membership(
        &self,
        prepared: &PreparedKeyBatch<'_>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<(ProbeMask, SearchStats)> {
        let rows = prepared.len();
        let result = self
            .view
            .contains(prepared, None, work.owner, work.control)?;
        work.owner
            .request(std::alloc::Layout::array::<bool>(rows).map_err(|_| arithmetic())?)?;
        let mut mask = ProbeMask::new(rows, false);
        let mut stats = SearchStats {
            lookup_hit_rows: 0,
            lookup_miss_rows: 0,
        };
        for (row, &present) in result.present().iter().enumerate() {
            check(work)?;
            mask.set(row, present != 0)?;
            if present != 0 {
                stats.lookup_hit_rows += 1;
            } else {
                stats.lookup_miss_rows += 1;
            }
        }
        check(work)?;
        Ok((mask, stats))
    }
    #[cfg(test)]
    pub(crate) fn lookup_group_ids_arrays(
        &self,
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        rows: usize,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Vec<Option<usize>>> {
        let input = self.input(arrays, bindings, rows, conversions, work)?;
        let prepared = PreparedKeyBatch::from_captured(
            self.view.spec(),
            &input,
            work.allocator.clone(),
            &|| check(work),
        )?;
        self.lookup_groups(&prepared, work)
    }
    pub(crate) fn spec(&self) -> &KeySpec {
        self.view.spec()
    }
    pub(crate) fn lookup_groups(
        &self,
        prepared: &PreparedKeyBatch<'_>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Vec<Option<usize>>> {
        let groups = self
            .groups
            .as_ref()
            .ok_or_else(|| ExecutionFailure::from_domain(JoinCursorFailure::PresenceOnly))?;
        let rows = prepared.len();
        let result = self.view.probe(prepared, None, work.owner, work.control)?;
        work.owner
            .request(std::alloc::Layout::array::<Option<usize>>(rows).map_err(|_| arithmetic())?)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(rows)
            .map_err(|source| work.allocator.allocation_error("join probe groups", source))?;
        for &found in result.matches() {
            check(work)?;
            output.push(match found {
                crate::exec::hash_table::key_index::BulkMatch::Found(key) => {
                    Some(groups.group(key)?)
                }
                _ => None,
            });
        }
        check(work)?;
        Ok(output)
    }
    #[cfg(test)]
    pub(crate) fn new_chained(
        types: Vec<arrow::datatypes::DataType>,
        null_safe_eq: Vec<bool>,
    ) -> ExecutionResult<Self> {
        // Legacy artifact tests need only a legitimate empty lookup component.
        // This adapter has no alternate algorithm or production type inference.
        let (owner, tracker) = tests::fixture();
        let control = tests::Control::default();
        let work = tests::ports(owner.as_ref(), &tracker, &control);
        let bindings: Vec<_> = types
            .into_iter()
            .map(|ty| FunctionValueType::new(ty, true))
            .collect();
        JoinKeyBuild::new(
            &bindings,
            &null_safe_eq,
            JoinHashMapBuildPurpose::RowMatches,
            &work,
        )?
        .finish(&work)
    }
}

/// A cursor retains only per-probe group coordinates and one finite page.
pub(crate) struct JoinPairCursor {
    groups: Vec<Option<usize>>,
    probe_row: usize,
    offset: usize,
    _storage: ValueTypeStorageLifetime,
}
#[derive(Debug)]
pub(crate) enum JoinCursorFailure {
    GroupUnavailable { group: usize },
    PresenceOnly,
    RowIdExtent,
}
impl std::fmt::Display for JoinCursorFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GroupUnavailable { group } => {
                write!(f, "join cursor group {group} is unavailable")
            }
            Self::PresenceOnly => f.write_str("presence-only join storage cannot enumerate rows"),
            Self::RowIdExtent => f.write_str("join row exceeds its row-id representation"),
        }
    }
}
impl std::error::Error for JoinCursorFailure {}
impl novarocks_failure_contract::FailureProjection for JoinCursorFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, reason) = match self {
            Self::GroupUnavailable { .. } => (
                FailureClass::Invariant,
                ExecutionReason::JoinGroupUnavailable,
            ),
            Self::PresenceOnly => (FailureClass::Invariant, ExecutionReason::JoinPresenceOnly),
            Self::RowIdExtent => (
                FailureClass::Representation,
                ExecutionReason::JoinRowIdExtent,
            ),
        };
        FailureFact::new(class, reason.reason())
    }
}
impl JoinPairCursor {
    pub(crate) fn new(groups: Vec<Option<usize>>, storage: ValueTypeStorageLifetime) -> Self {
        Self {
            groups,
            probe_row: 0,
            offset: 0,
            _storage: storage,
        }
    }
    pub(crate) fn is_complete(&self) -> bool {
        self.probe_row == self.groups.len()
    }
    pub(crate) fn next_page(
        &mut self,
        map: &JoinHashMap,
        limit: NonZeroUsize,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> ExecutionResult<JoinSelection> {
        let groups = map
            .groups
            .as_ref()
            .ok_or_else(|| ExecutionFailure::from_domain(JoinCursorFailure::PresenceOnly))?;
        let mut output = JoinSelection::new();
        while !self.is_complete() && output.len() < limit.get() {
            control.checkpoint(0)?;
            let Some(group) = self.groups[self.probe_row] else {
                self.probe_row += 1;
                self.offset = 0;
                continue;
            };
            let probe = u32::try_from(self.probe_row)
                .map_err(|_| ExecutionFailure::from_domain(JoinCursorFailure::RowIdExtent))?;
            let rows = groups
                .group_build_rows(group)
                .map_err(ExecutionFailure::from_domain)?;
            let end = self
                .offset
                .saturating_add(limit.get() - output.len())
                .min(rows.len());
            for &row in &rows[self.offset..end] {
                control.checkpoint(0)?;
                output.push(probe, row);
            }
            self.offset = end;
            if end == rows.len() {
                self.probe_row += 1;
                self.offset = 0;
            }
        }
        Ok(output)
    }
}

#[cfg(test)]
#[path = "key_index_tests.rs"]
mod tests;
