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

//! Immutable per-E page directories. Coordinates identify data within the
//! original artifact; they grant neither resource nor task authority.

use super::{
    build_collection::CollectedInputs,
    build_state::{ProjectionDomains, build_contract},
    join_completion::{arithmetic, funded_shared, funded_vec},
};
use crate::{
    exec::{
        chunk::{ChunkBatchProjection, ChunkSlotSchema},
        hash_table::key_index::{
            BulkDomain, BulkInput, BulkWork, CaptureWork, CapturedPageId, CompleteInput,
            IndexPurpose, KeyInput, KeySpec, PreparedKeyBatch, SealedCapture,
        },
        pipeline::operator::ConstructionFailure,
    },
    runtime::task::{ExecutionFailure, ExecutionResult},
};
use arrow::array::ArrayRef;
#[cfg(test)]
use novarocks_type_contract::FunctionValueType;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{ops::Range, sync::Arc};

#[cfg(test)]
use crate::exec::pipeline::operator::InputConstructionOwner;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct B3RunId(usize);
impl B3RunId {
    /// Only the actual builder supplies its installed E-slot count.
    pub(super) fn from_input_slot(slot: usize, inputs: usize) -> ExecutionResult<Self> {
        if slot >= inputs {
            return Err(build_contract("join run has no installed input slot"));
        }
        Ok(Self(slot))
    }
    pub(crate) fn index(self) -> usize {
        self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BuildRowId(usize);
impl BuildRowId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BuildPageAddress {
    run: B3RunId,
    page: usize,
    row: usize,
}
impl BuildPageAddress {
    pub(crate) fn run(self) -> B3RunId {
        self.run
    }
    pub(crate) fn page(self) -> usize {
        self.page
    }
    pub(crate) fn row_in_page(self) -> usize {
        self.row
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionKind {
    Ordinary,
    Group,
    Full,
}
#[derive(Clone, Copy)]
pub(super) enum PageProjectionIds {
    PayloadOnly,
    Ordinary(CapturedPageId),
    Naa {
        group: Option<CapturedPageId>,
        full: CapturedPageId,
    },
}
impl PageProjectionIds {
    fn get(self, kind: ProjectionKind) -> Option<CapturedPageId> {
        match (self, kind) {
            (Self::Ordinary(id), ProjectionKind::Ordinary) => Some(id),
            (Self::Naa { group, .. }, ProjectionKind::Group) => group,
            (Self::Naa { full, .. }, ProjectionKind::Full) => Some(full),
            _ => None,
        }
    }
}
#[derive(Clone, Copy)]
pub(super) enum ProjectionRunSummary {
    PayloadOnly,
    Ordinary(Option<usize>),
    Naa {
        group: Option<usize>,
        full: Option<usize>,
    },
}
pub(super) enum InputCaptures {
    PayloadOnly,
    Ordinary(SealedCapture),
    Naa {
        group: Option<SealedCapture>,
        full: SealedCapture,
    },
}
pub(crate) struct BuildInputReceipt {
    pub(super) run: B3RunId,
    pub(super) captures: InputCaptures,
    pub(super) pages: Arc<PayloadRunPages>,
}
impl BuildInputReceipt {
    #[cfg(test)]
    pub(crate) fn run(&self) -> B3RunId {
        self.run
    }
    #[cfg(test)]
    pub(crate) fn rows(&self) -> usize {
        self.pages.rows
    }
    #[cfg(test)]
    pub(crate) fn page_count(&self) -> usize {
        self.pages.pages.len()
    }
}

pub(super) struct RetainedNaaValues {
    pub(super) input: KeyInput,
    pub(super) spec: KeySpec,
    pub(super) values: Range<usize>,
    // This is the actual neutral source holder also used by K capture.
    pub(super) _source: ValueTypeStorageLifetime,
}
#[cfg(test)]
pub(crate) struct ValueNullRow {
    pub(super) row: usize,
    pub(super) ordinary: bool,
}
#[cfg(test)]
impl ValueNullRow {
    pub(crate) fn row_in_page(&self) -> usize {
        self.row
    }
    pub(crate) fn has_ordinary_null(&self) -> bool {
        self.ordinary
    }
}
pub(crate) struct ValueNullFacts {
    #[cfg(test)]
    pub(super) rows: Vec<ValueNullRow>,
    pub(super) ordinary_rows: Vec<usize>,
    pub(super) _storage: ValueTypeStorageLifetime,
}
impl ValueNullFacts {
    #[cfg(test)]
    pub(crate) fn rows(&self) -> &[ValueNullRow] {
        &self.rows
    }
    #[cfg(test)]
    pub(crate) fn ordinary_rows(&self) -> impl Iterator<Item = usize> + '_ {
        self.ordinary_rows.iter().copied()
    }
    pub(crate) fn ordinary_row_indices(&self) -> &[usize] {
        &self.ordinary_rows
    }
    #[cfg(test)]
    pub(crate) fn nullsafe_only_rows(&self) -> impl Iterator<Item = usize> + '_ {
        self.rows
            .iter()
            .filter(|row| !row.ordinary)
            .map(|row| row.row)
    }
}
pub(super) struct BuildPageData {
    pub(super) start: usize,
    pub(super) rows: usize,
    pub(super) ids: PageProjectionIds,
    pub(super) payload: Option<ChunkBatchProjection>,
    pub(super) values: Option<RetainedNaaValues>,
    pub(super) nulls: Option<ValueNullFacts>,
    pub(super) _storage: ValueTypeStorageLifetime,
}
pub(crate) struct PayloadRunPages {
    pub(super) run: B3RunId,
    pub(super) pages: Vec<Arc<BuildPageData>>,
    pub(super) rows: usize,
    pub(super) ordinary_nulls: Option<usize>,
    pub(super) ordinary_null_pages: Vec<usize>,
    pub(super) summary: ProjectionRunSummary,
    // Page-vector backing and this Arc header may have different creators.
    pub(super) _table_storage: ValueTypeStorageLifetime,
    pub(super) _header_storage: ValueTypeStorageLifetime,
}
impl PayloadRunPages {
    pub(crate) fn ordinary_null_pages(&self) -> ExecutionResult<&[usize]> {
        self.ordinary_nulls
            .ok_or_else(|| build_contract("join run has no original NAA NULL directory"))?;
        Ok(&self.ordinary_null_pages)
    }
    #[cfg(test)]
    pub(crate) fn page_count(&self) -> usize {
        self.pages.len()
    }
    pub(crate) fn page(&self, page: usize) -> ExecutionResult<BuildPageProjection> {
        self.pages
            .get(page)
            .cloned()
            .map(BuildPageProjection)
            .ok_or_else(|| build_contract("join page is outside its original run"))
    }
}
#[derive(Clone)]
pub(crate) struct BuildPageProjection(pub(super) Arc<BuildPageData>);
impl BuildPageProjection {
    pub(crate) fn rows(&self) -> usize {
        self.0.rows
    }
    pub(crate) fn payload(&self) -> Option<PayloadPageLoan<'_>> {
        self.0.payload.as_ref().map(PayloadPageLoan)
    }
    pub(crate) fn values(&self) -> Option<NaaValuePageLoan<'_>> {
        self.0.values.as_ref().map(NaaValuePageLoan)
    }
    pub(crate) fn value_nulls(&self) -> Option<&ValueNullFacts> {
        self.0.nulls.as_ref()
    }
    pub(crate) fn projection_id(&self, kind: ProjectionKind) -> Option<CapturedPageId> {
        self.0.ids.get(kind)
    }
}
pub(crate) struct PayloadPageLoan<'a>(&'a ChunkBatchProjection);
pub(crate) struct TypedPayloadColumnLoan<'a> {
    pub(crate) array: &'a ArrayRef,
    pub(crate) slot: &'a ChunkSlotSchema,
}
impl PayloadPageLoan<'_> {
    /// Continue the original finite holders without retaining this page's
    /// RecordBatch or the entire build artifact in an output alias.
    pub(crate) fn source_storage_lifetime_observed<
        E: From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
    >(
        &self,
        storage: ValueTypeStorageLifetime,
        request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
    ) -> Result<ValueTypeStorageLifetime, E> {
        self.0
            .clone()
            .into_chunk()
            .source_storage_lifetime_observed(storage, request)
    }
    pub(crate) fn column(&self, column: usize) -> ExecutionResult<TypedPayloadColumnLoan<'_>> {
        let array = self
            .0
            .columns()
            .get(column)
            .ok_or_else(|| build_contract("join payload column is outside its exact page"))?;
        let slot = self
            .0
            .chunk_schema()
            .slots()
            .get(column)
            .ok_or_else(|| build_contract("join payload has no exact column declaration"))?;
        Ok(TypedPayloadColumnLoan { array, slot })
    }
}
pub(crate) struct NaaValuePageLoan<'a>(&'a RetainedNaaValues);
impl NaaValuePageLoan<'_> {
    pub(crate) fn components(&self) -> Range<usize> {
        self.0.values.clone()
    }
    pub(crate) fn source_row(&self, occurrence: usize) -> ExecutionResult<usize> {
        self.0.input.row(occurrence)
    }
    #[cfg(test)]
    pub(crate) fn column(&self, value: usize) -> ExecutionResult<(&ArrayRef, &FunctionValueType)> {
        let column = self
            .0
            .values
            .start
            .checked_add(value)
            .ok_or_else(arithmetic)?;
        if column >= self.0.values.end {
            return Err(build_contract(
                "join value column is outside its original role",
            ));
        }
        Ok((
            &self.0.input.arrays()[column],
            &self.0.input.bindings()[column],
        ))
    }
    pub(crate) fn prepared(&self, work: &BulkWork<'_>) -> ExecutionResult<PreparedKeyBatch<'_>> {
        PreparedKeyBatch::from_captured(
            &self.0.spec,
            &self.0.input,
            work.allocator.clone(),
            &|| check(work),
        )
    }
}

struct RunEntry {
    run: Arc<PayloadRunPages>,
    start: usize,
    end: usize,
}
struct ProjectionRunMap {
    by_k_run: Vec<Option<B3RunId>>,
    _storage: ValueTypeStorageLifetime,
}
impl ProjectionRunMap {
    fn new(inputs: usize, work: &BulkWork<'_>) -> ExecutionResult<Self> {
        let mut by_k_run = funded_vec(work.owner, inputs)?;
        for i in 0..inputs {
            checkpoint(i, work)?;
            by_k_run.push(None);
        }
        Ok(Self {
            by_k_run,
            _storage: work.owner.storage_lifetime(),
        })
    }
    fn insert(&mut self, observed: Option<usize>, run: B3RunId) -> ExecutionResult<()> {
        if let Some(observed) = observed {
            let slot = self.by_k_run.get_mut(observed).ok_or_else(|| {
                build_contract("join observed K run is outside the complete E set")
            })?;
            if slot.replace(run).is_some() {
                return Err(build_contract("join K run was mapped twice"));
            }
        }
        Ok(())
    }
}
pub(crate) struct BuildRows {
    runs: Vec<Option<RunEntry>>,
    rows: usize,
    ordinary_nulls: Option<usize>,
    ordinary: Option<ProjectionRunMap>,
    group: Option<ProjectionRunMap>,
    full: Option<ProjectionRunMap>,
    _table_storage: ValueTypeStorageLifetime,
    _header_storage: ValueTypeStorageLifetime,
}
pub(crate) enum CapturesForGraphs {
    PayloadOnly,
    Ordinary {
        keys: CapturedInputRun,
    },
    Naa {
        group: Option<CapturedInputRun>,
        full: CapturedInputRun,
    },
}
pub(crate) struct CapturedInputRun {
    captures: Vec<SealedCapture>,
    _storage: ValueTypeStorageLifetime,
}
impl CapturedInputRun {
    /// Consume the funded temporary directory while its actual creator is
    /// still held. No bare funded Vec escapes into another construction scope.
    pub(crate) fn into_complete(
        self,
        domain: &BulkDomain,
        purpose: IndexPurpose,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<CompleteInput> {
        let Self {
            captures,
            _storage: storage,
        } = self;
        let result = (|| {
            let ports = capture_work(work);
            CompleteInput::from_captures(BulkInput::new(domain, captures, &ports)?, purpose, &ports)
        })();
        drop(storage);
        result
    }
}
impl BuildRows {
    /// Only O(E) metadata is moved/visited. Each E already maintained all of
    /// its page prefixes and validated each captured page's original identity.
    pub(crate) fn from_inputs(
        mut inputs: CollectedInputs<BuildInputReceipt>,
        domains: &ProjectionDomains,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<(Arc<Self>, CapturesForGraphs)> {
        check(work)?;
        let count = inputs.as_slice().len();
        let mut runs = funded_vec(work.owner, count)?;
        for i in 0..count {
            checkpoint(i, work)?;
            runs.push(None);
        }
        let mut ordinary = matches!(domains, ProjectionDomains::Ordinary { .. })
            .then(|| ProjectionRunMap::new(count, work))
            .transpose()?;
        let mut group = match domains {
            ProjectionDomains::Naa { group: Some(_), .. } => {
                Some(ProjectionRunMap::new(count, work)?)
            }
            _ => None,
        };
        let mut full = matches!(domains, ProjectionDomains::Naa { .. })
            .then(|| ProjectionRunMap::new(count, work))
            .transpose()?;
        let mut keys = funded_vec(work.owner, if ordinary.is_some() { count } else { 0 })?;
        let mut group_captures = group
            .as_ref()
            .map(|_| funded_vec(work.owner, count))
            .transpose()?;
        let mut full_captures = funded_vec(work.owner, if full.is_some() { count } else { 0 })?;
        let mut moved = 0;
        let mut ordinary_nulls = matches!(domains, ProjectionDomains::Naa { .. }).then_some(0usize);
        while let Some(receipt) = inputs.pop() {
            checkpoint(moved, work)?;
            moved += 1;
            let BuildInputReceipt {
                run,
                captures,
                pages,
            } = receipt;
            if run.index() != count - moved || run != pages.run {
                return Err(build_contract("join receipt and page run differ"));
            }
            if let Some(total) = &mut ordinary_nulls {
                let count = pages
                    .ordinary_nulls
                    .ok_or_else(|| build_contract("NAA E receipt has no original NULL count"))?;
                *total = total.checked_add(count).ok_or_else(arithmetic)?;
            }
            let slot = runs
                .get_mut(run.0)
                .ok_or_else(|| build_contract("join receipt has a foreign E slot"))?;
            if slot.is_some() {
                return Err(build_contract("join input run arrived twice"));
            }
            match (domains, captures, pages.summary) {
                (
                    ProjectionDomains::PayloadOnly,
                    InputCaptures::PayloadOnly,
                    ProjectionRunSummary::PayloadOnly,
                ) => {}
                (
                    ProjectionDomains::Ordinary { .. },
                    InputCaptures::Ordinary(capture),
                    ProjectionRunSummary::Ordinary(observed),
                ) => {
                    if capture.input_rows() != pages.rows {
                        return Err(build_contract(
                            "join physical rows differ from sealed key capture",
                        ));
                    }
                    ordinary
                        .as_mut()
                        .expect("ordinary map")
                        .insert(observed, run)?;
                    keys.push(capture);
                }
                (
                    ProjectionDomains::Naa {
                        group: expected, ..
                    },
                    InputCaptures::Naa {
                        group: captured,
                        full: capture,
                    },
                    ProjectionRunSummary::Naa {
                        group: observed_group,
                        full: observed_full,
                    },
                ) => {
                    if expected.is_some() != captured.is_some()
                        || capture.input_rows() != pages.rows
                    {
                        return Err(build_contract(
                            "join NAA capture topology or physical rows differ",
                        ));
                    }
                    if let Some(captured) = captured {
                        if captured.input_rows() != pages.rows {
                            return Err(build_contract(
                                "join group physical rows differ from full capture",
                            ));
                        }
                        group
                            .as_mut()
                            .expect("group map")
                            .insert(observed_group, run)?;
                        group_captures
                            .as_mut()
                            .expect("group directory")
                            .push(captured);
                    } else if observed_group.is_some() {
                        return Err(build_contract(
                            "join has a group page without a group domain",
                        ));
                    }
                    full.as_mut()
                        .expect("full map")
                        .insert(observed_full, run)?;
                    full_captures.push(capture);
                }
                _ => {
                    return Err(build_contract(
                        "join input projection kind changed before begin",
                    ));
                }
            }
            *slot = Some(RunEntry {
                run: pages,
                start: 0,
                end: 0,
            });
        }
        if moved != count {
            return Err(build_contract("join input directory is incomplete"));
        }
        let mut rows = 0usize;
        for (i, slot) in runs.iter_mut().enumerate() {
            checkpoint(i, work)?;
            let entry = slot
                .as_mut()
                .ok_or_else(|| build_contract("join input slot is missing"))?;
            entry.start = rows;
            rows = rows.checked_add(entry.run.rows).ok_or_else(arithmetic)?;
            entry.end = rows;
        }
        check(work)?;
        let storage = work.owner.storage_lifetime();
        let captures = match domains {
            ProjectionDomains::PayloadOnly => CapturesForGraphs::PayloadOnly,
            ProjectionDomains::Ordinary { .. } => CapturesForGraphs::Ordinary {
                keys: CapturedInputRun {
                    captures: keys,
                    _storage: storage.clone(),
                },
            },
            ProjectionDomains::Naa { .. } => CapturesForGraphs::Naa {
                group: group_captures.map(|captures| CapturedInputRun {
                    captures,
                    _storage: storage.clone(),
                }),
                full: CapturedInputRun {
                    captures: full_captures,
                    _storage: storage.clone(),
                },
            },
        };
        let rows = funded_shared(
            work.owner,
            Self {
                runs,
                rows,
                ordinary_nulls,
                ordinary,
                group,
                full,
                _table_storage: storage.clone(),
                _header_storage: storage,
            },
        )?;
        Ok((rows, captures))
    }
    pub(crate) fn len(&self) -> usize {
        self.rows
    }
    pub(crate) fn run_count(&self) -> usize {
        self.runs.len()
    }
    /// Only the actual no-group physical domain may use this whole-build
    /// summary. Scoped consumers must query their original group NULL CSR.
    pub(crate) fn ordinary_null_count(&self) -> ExecutionResult<usize> {
        self.ordinary_nulls
            .ok_or_else(|| build_contract("join data has no original NAA NULL summary"))
    }
    fn entry(&self, run: B3RunId) -> ExecutionResult<&RunEntry> {
        self.runs
            .get(run.0)
            .and_then(Option::as_ref)
            .ok_or_else(|| build_contract("join E run is outside this artifact"))
    }
    pub(crate) fn run(&self, run: B3RunId) -> ExecutionResult<&PayloadRunPages> {
        Ok(&self.entry(run)?.run)
    }
    pub(crate) fn row(&self, row: usize) -> ExecutionResult<BuildRowId> {
        if row >= self.rows {
            return Err(build_contract("join physical row is outside this artifact"));
        }
        Ok(BuildRowId(row))
    }
    pub(crate) fn page(&self, run: B3RunId, page: usize) -> ExecutionResult<BuildPageProjection> {
        self.entry(run)?.run.page(page)
    }
    pub(crate) fn locate(&self, row: BuildRowId) -> ExecutionResult<BuildPageAddress> {
        self.row(row.0)?;
        let r = self
            .runs
            .partition_point(|entry| entry.as_ref().expect("complete E directory").end <= row.0);
        let entry = self.runs[r].as_ref().expect("located run");
        let local = row.0 - entry.start;
        let p = entry
            .run
            .pages
            .partition_point(|page| page.start + page.rows <= local);
        let page = entry
            .run
            .pages
            .get(p)
            .ok_or_else(|| build_contract("join page prefix does not cover physical row"))?;
        Ok(BuildPageAddress {
            run: entry.run.run,
            page: p,
            row: local - page.start,
        })
    }
    pub(crate) fn row_id(&self, address: BuildPageAddress) -> ExecutionResult<BuildRowId> {
        let entry = self.entry(address.run)?;
        let page = entry
            .run
            .pages
            .get(address.page)
            .ok_or_else(|| build_contract("join row address has no page"))?;
        if address.row >= page.rows {
            return Err(build_contract("join row address is outside its page"));
        }
        Ok(BuildRowId(entry.start + page.start + address.row))
    }
    pub(crate) fn row_in_page(
        &self,
        run: B3RunId,
        page: usize,
        row: usize,
    ) -> ExecutionResult<BuildRowId> {
        self.row_id(BuildPageAddress { run, page, row })
    }
    /// Original E observation, not a value comparison or a per-column posting.
    pub(crate) fn has_ordinary_value_null(
        &self,
        address: BuildPageAddress,
    ) -> ExecutionResult<bool> {
        self.row_id(address)?;
        let page = self.page(address.run, address.page)?;
        let facts = page.value_nulls().ok_or_else(|| {
            build_contract("NAA NULL row selection has no original value observation")
        })?;
        Ok(facts
            .ordinary_row_indices()
            .binary_search(&address.row)
            .is_ok())
    }
    pub(crate) fn address_for_assignment(
        &self,
        kind: ProjectionKind,
        id: CapturedPageId,
        occurrence: usize,
    ) -> ExecutionResult<BuildPageAddress> {
        let map = match kind {
            ProjectionKind::Ordinary => &self.ordinary,
            ProjectionKind::Group => &self.group,
            ProjectionKind::Full => &self.full,
        };
        let run = map
            .as_ref()
            .and_then(|map| map.by_k_run.get(id.run_index()))
            .copied()
            .flatten()
            .ok_or_else(|| build_contract("join assignment has no original projection run"))?;
        let page = self.page(run, id.page_index())?;
        if page.projection_id(kind) != Some(id) || occurrence >= page.rows() {
            return Err(build_contract(
                "join assignment does not belong to its original page",
            ));
        }
        Ok(BuildPageAddress {
            run,
            page: id.page_index(),
            row: occurrence,
        })
    }
}
pub(super) fn capture_work<'a>(work: &'a BulkWork<'_>) -> CaptureWork<'a> {
    CaptureWork {
        owner: work.owner,
        allocator: work.allocator.clone(),
        host: Arc::clone(&work.host),
        control: work.control,
    }
}
pub(super) fn check(work: &BulkWork<'_>) -> ExecutionResult<()> {
    work.control.checkpoint(0).map_err(ExecutionFailure::from)
}
pub(super) fn checkpoint(index: usize, work: &BulkWork<'_>) -> ExecutionResult<()> {
    if index.is_multiple_of(256) {
        check(work)?;
    }
    Ok(())
}
pub(super) fn push_owned<T>(
    values: &mut Vec<T>,
    value: T,
    storage: &mut ValueTypeStorageLifetime,
    work: &BulkWork<'_>,
) -> ExecutionResult<()> {
    let growth =
        novarocks_type_contract::owned_resources::vec::push_growth(values).map_err(|cause| {
            match cause {
                novarocks_type_contract::ControlResourceError::Control(cause) => {
                    ConstructionFailure::Control(cause)
                }
                novarocks_type_contract::ControlResourceError::SourceModel(cause) => {
                    ConstructionFailure::InvalidSource(cause.to_owned())
                }
            }
        })?;
    if let Some(layout) = growth.requested_backing {
        work.owner.request(layout)?;
        values.try_reserve(1).map_err(|source| {
            ExecutionFailure::from_domain(novarocks_value_arrays::ValueError::Allocation {
                requested: layout.size(),
                source,
            })
        })?;
        *storage = work.owner.storage_lifetime();
    }
    values.push(value);
    Ok(())
}

#[cfg(test)]
#[path = "build_pages_tests.rs"]
pub(super) mod tests;
