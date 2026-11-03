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

//! NAA candidates over the original K projections and immutable physical pages.
//! This layer selects physical rows; value equality remains in K/V.

use super::{
    build_finalize::{BuildLookup, FinalizedBuild},
    build_pages::{B3RunId, BuildPageAddress, BuildRowId, BuildRows},
    build_projection::IndexedBuild,
    build_state::{NaaValueMode, build_contract},
    join_completion::funded_vec,
};
use crate::{
    exec::{
        hash_table::key_index::{
            BulkMatch, BulkPresenceBatch, BulkProbeBatch, BulkWork, IndexPurpose, KeyInput,
            NullPolicy, PreparedKeyBatch,
        },
        pipeline::driver_step::TurnWork,
    },
    runtime::task::ExecutionResult,
};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{ops::Range, sync::Arc};

pub(crate) struct NaaCandidateStore {
    rows: Arc<BuildRows>,
    group: Option<Arc<IndexedBuild>>,
    full: Arc<IndexedBuild>,
    values: Range<usize>,
    mode: NaaValueMode,
    eligibility: bool,
}
enum FullMatches {
    Presence(BulkPresenceBatch),
    Rows(BulkProbeBatch),
}
pub(crate) struct NaaProbePage {
    full_input: KeyInput,
    full: FullMatches,
    group: Option<BulkProbeBatch>,
    ordinary_nulls: Vec<bool>,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) enum NaaRowStart {
    Decided { suppressed: bool },
    Candidates(NaaCandidates),
}
/// The two ranges are disjoint: exact non-NULL value rows, then ordinary-NULL
/// value rows. A NULL probe instead visits the whole group exactly once.
pub(crate) struct NaaCandidates {
    exact: Range<usize>,
    remainder: RowDomain,
}
enum RowDomain {
    Empty,
    Group {
        range: Range<usize>,
        nulls_only: bool,
    },
    Physical(Range<usize>),
    GlobalNulls {
        run: usize,
        page: usize,
        row: usize,
    },
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NaaCandidateStep {
    Row(BuildRowId),
    Yielded,
    Finished,
}
impl NaaCandidateStore {
    /// `values` and `eligibility` come from the original frozen NAA roles.
    /// Index purpose is checked against those facts, never used to infer them.
    pub(crate) fn new(
        build: &FinalizedBuild,
        values: Range<usize>,
        eligibility: bool,
    ) -> ExecutionResult<Self> {
        let BuildLookup::Naa {
            value_mode,
            group,
            full,
        } = &build.lookup
        else {
            return Err(build_contract(
                "NAA requires its original published projections",
            ));
        };
        let purpose = if eligibility {
            IndexPurpose::KeyIds
        } else {
            IndexPurpose::PresenceOnly
        };
        if values.start >= values.end
            || values.end != full.view.spec().types().len()
            || full.view.purpose() != purpose
            || (eligibility && full.groups.is_none())
            || (eligibility && *value_mode != NaaValueMode::General)
            || matches!(value_mode, NaaValueMode::SingleNullable { value } if *value >= values.len())
        {
            return Err(build_contract(
                "NAA publication disagrees with original value/eligibility roles",
            ));
        }
        if let Some(group) = group
            && (group.groups.is_none() || group.ordinary_nulls.is_none())
        {
            return Err(build_contract(
                "NAA group has no complete physical/NULL CSR",
            ));
        }
        build.rows.ordinary_null_count()?;
        Ok(Self {
            rows: build.rows.clone(),
            group: group.clone(),
            full: full.clone(),
            values,
            mode: *value_mode,
            eligibility,
        })
    }
    pub(crate) fn full_spec(&self) -> &crate::exec::hash_table::key_index::KeySpec {
        self.full.view.spec()
    }
    pub(crate) fn group_spec(&self) -> Option<&crate::exec::hash_table::key_index::KeySpec> {
        self.group.as_ref().map(|group| group.view.spec())
    }
    /// Inputs borrow the already-evaluated S/C arrays and are captured for these
    /// exact K specs. No expression is re-evaluated and no key array is copied.
    pub(crate) fn prepare_probe(
        &self,
        full_input: KeyInput,
        group_input: Option<KeyInput>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<NaaProbePage> {
        let checkpoint = || work.control.checkpoint(0).map_err(Into::into);
        let prepared = PreparedKeyBatch::from_captured(
            self.full_spec(),
            &full_input,
            work.allocator.clone(),
            &checkpoint,
        )?;
        let full = if self.eligibility {
            FullMatches::Rows(
                self.full
                    .view
                    .probe(&prepared, None, work.owner, work.control)?,
            )
        } else {
            FullMatches::Presence(self.full.view.contains(
                &prepared,
                None,
                work.owner,
                work.control,
            )?)
        };
        let group = match (&self.group, group_input) {
            (None, None) => None,
            (Some(group), Some(input)) if input.len() == full_input.len() => {
                let prepared = PreparedKeyBatch::from_captured(
                    group.view.spec(),
                    &input,
                    work.allocator.clone(),
                    &checkpoint,
                )?;
                Some(
                    group
                        .view
                        .probe(&prepared, None, work.owner, work.control)?,
                )
            }
            _ => {
                return Err(build_contract(
                    "NAA probe has no exact original group input",
                ));
            }
        };
        let mut ordinary_nulls = funded_vec(work.owner, full_input.len())?;
        ordinary_nulls.resize(full_input.len(), false);
        let mut component_nulls = funded_vec(work.owner, full_input.len())?;
        component_nulls.resize(full_input.len(), false);
        for component in self.values.clone() {
            if self.full_spec().nulls()[component] == NullPolicy::Unmatchable {
                full_input.component_range_root_nulls_observed(
                    component..component + 1,
                    0..full_input.len(),
                    &mut component_nulls,
                    &checkpoint,
                )?;
                for (row, (&is_null, result)) in
                    component_nulls.iter().zip(&mut ordinary_nulls).enumerate()
                {
                    if row % 256 == 0 {
                        work.control.checkpoint(1)?;
                    }
                    *result |= is_null;
                }
            }
        }
        Ok(NaaProbePage {
            full_input,
            full,
            group,
            ordinary_nulls,
            _storage: work.owner.storage_lifetime(),
        })
    }
    /// One-key mark truth reuses the original presence lookup and NULL facts.
    /// No candidate rows are scanned and no required predicate is skipped:
    /// the frozen mark condition has no scopes or eligibility expression.
    pub(crate) fn mark_truth(
        &self,
        probe: &NaaProbePage,
        row: usize,
    ) -> ExecutionResult<Option<bool>> {
        if self.eligibility || self.group.is_some() || self.values.len() != 1 {
            return Err(build_contract(
                "mark truth requires one unscoped NULL-aware key",
            ));
        }
        let FullMatches::Presence(present) = &probe.full else {
            return Err(build_contract(
                "mark truth requires its original presence index",
            ));
        };
        let probe_null = *probe
            .ordinary_nulls
            .get(row)
            .ok_or_else(|| build_contract("mark probe row is outside its admitted page"))?;
        let exact = *present
            .present()
            .get(row)
            .ok_or_else(|| build_contract("mark lookup row is outside its admitted page"))?
            != 0;
        Ok(if self.rows.len() == 0 {
            Some(false)
        } else if !probe_null && exact {
            Some(true)
        } else if probe_null || self.rows.ordinary_null_count()? != 0 {
            None
        } else {
            Some(false)
        })
    }
    pub(crate) fn start(&self, probe: &NaaProbePage, row: usize) -> ExecutionResult<NaaRowStart> {
        let null = *probe
            .ordinary_nulls
            .get(row)
            .ok_or_else(|| build_contract("NAA probe row is outside its admitted page"))?;
        let exact_match = match &probe.full {
            FullMatches::Presence(present) => present.present()[row] != 0,
            FullMatches::Rows(matches) => matches!(matches.matches()[row], BulkMatch::Found(_)),
        };
        if !self.eligibility && !null && exact_match {
            return Ok(NaaRowStart::Decided { suppressed: true });
        }
        if self.mode == NaaValueMode::AllNonNull {
            return Ok(NaaRowStart::Decided {
                suppressed: exact_match,
            });
        }
        let (all, nulls) = match (&self.group, &probe.group) {
            (None, None) => (None, None),
            (Some(group), Some(matches)) => match matches.matches()[row] {
                BulkMatch::Found(key) => (
                    Some(
                        group
                            .groups
                            .as_ref()
                            .expect("validated group CSR")
                            .group(key)?,
                    ),
                    Some(
                        group
                            .ordinary_nulls
                            .as_ref()
                            .expect("validated NULL CSR")
                            .group(key)?,
                    ),
                ),
                BulkMatch::Miss | BulkMatch::Unmatchable => {
                    return Ok(NaaRowStart::Decided { suppressed: false });
                }
            },
            _ => {
                return Err(build_contract(
                    "NAA probe belongs to another group projection",
                ));
            }
        };
        let physical_count = all.as_ref().map_or(self.rows.len(), Range::len);
        let null_count = match &nulls {
            Some(range) => range.len(),
            None => self.rows.ordinary_null_count()?,
        };
        if matches!(self.mode, NaaValueMode::SingleNullable { .. }) {
            return Ok(NaaRowStart::Decided {
                suppressed: if null {
                    physical_count != 0
                } else {
                    exact_match || null_count != 0
                },
            });
        }
        let exact = if null {
            0..0
        } else {
            match &probe.full {
                FullMatches::Rows(matches) => match matches.matches()[row] {
                    BulkMatch::Found(key) => self
                        .full
                        .groups
                        .as_ref()
                        .expect("validated full CSR")
                        .group(key)?,
                    BulkMatch::Miss | BulkMatch::Unmatchable => 0..0,
                },
                FullMatches::Presence(_) => 0..0,
            }
        };
        let remainder = if null {
            match all {
                Some(range) => RowDomain::Group {
                    range,
                    nulls_only: false,
                },
                None => RowDomain::Physical(0..self.rows.len()),
            }
        } else if null_count == 0 {
            RowDomain::Empty
        } else {
            match nulls {
                Some(range) => RowDomain::Group {
                    range,
                    nulls_only: true,
                },
                None => RowDomain::GlobalNulls {
                    run: 0,
                    page: 0,
                    row: 0,
                },
            }
        };
        Ok(NaaRowStart::Candidates(NaaCandidates { exact, remainder }))
    }
    /// Group only pair-selection metadata by physical page. Original K/V
    /// performs each batch comparison over the retained evaluated arrays.
    pub(crate) fn compare_pairs(
        &self,
        probe: &NaaProbePage,
        probe_rows: &[usize],
        build_rows: &[BuildRowId],
        output: &mut [Option<bool>],
        work: &BulkWork<'_>,
    ) -> ExecutionResult<()> {
        if probe_rows.len() != build_rows.len() || output.len() != build_rows.len() {
            return Err(build_contract(
                "NAA comparison requires aligned original pair selections",
            ));
        }
        let checkpoint = || work.control.checkpoint(0).map_err(Into::into);
        let left = PreparedKeyBatch::from_captured(
            self.full_spec(),
            &probe.full_input,
            work.allocator.clone(),
            &checkpoint,
        )?;
        let mut addresses: Vec<BuildPageAddress> = funded_vec(work.owner, build_rows.len())?;
        let mut order = funded_vec(work.owner, build_rows.len())?;
        for (position, &row) in build_rows.iter().enumerate() {
            checkpoint()?;
            addresses.push(self.rows.locate(row)?);
            order.push(position);
        }
        order.sort_unstable_by_key(|&i| (addresses[i].run().index(), addresses[i].page()));
        checkpoint()?;
        let mut left_rows = funded_vec(work.owner, order.len())?;
        let mut right_rows = funded_vec(work.owner, order.len())?;
        let mut compared = funded_vec(work.owner, order.len())?;
        let mut start = 0;
        while start < order.len() {
            checkpoint()?;
            let address = addresses[order[start]];
            let mut end = start + 1;
            while end < order.len()
                && addresses[order[end]].run() == address.run()
                && addresses[order[end]].page() == address.page()
            {
                end += 1;
            }
            let page = self.rows.page(address.run(), address.page())?;
            let values = page
                .values()
                .ok_or_else(|| build_contract("NAA physical page has no original value capture"))?;
            if values.components() != self.values {
                return Err(build_contract(
                    "NAA build page changed its original value roles",
                ));
            }
            let right = values.prepared(work)?;
            left_rows.clear();
            right_rows.clear();
            compared.clear();
            for &position in &order[start..end] {
                left_rows.push(probe.full_input.row(probe_rows[position])?);
                right_rows.push(values.source_row(addresses[position].row_in_page())?);
                compared.push(None);
            }
            left.compare_value_pairs(
                self.values.clone(),
                &left_rows,
                &right,
                &right_rows,
                &mut compared,
                work.owner,
                &checkpoint,
            )?;
            for (&position, result) in order[start..end].iter().zip(&compared) {
                output[position] = *result;
            }
            start = end;
        }
        Ok(())
    }
}
impl NaaCandidates {
    pub(crate) fn next(
        &mut self,
        store: &NaaCandidateStore,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<NaaCandidateStep> {
        work.control.checkpoint(0)?;
        if self.exact.start < self.exact.end {
            if !turn.try_consume(1) {
                return Ok(NaaCandidateStep::Yielded);
            }
            let position = self.exact.start;
            self.exact.start += 1;
            let row = store
                .full
                .groups
                .as_ref()
                .expect("validated full CSR")
                .row(position)?;
            return Ok(NaaCandidateStep::Row(store.rows.row(row)?));
        }
        loop {
            work.control.checkpoint(0)?;
            if matches!(self.remainder, RowDomain::Empty) {
                return Ok(NaaCandidateStep::Finished);
            }
            if !turn.try_consume(1) {
                return Ok(NaaCandidateStep::Yielded);
            }
            let row = match &mut self.remainder {
                RowDomain::Empty => unreachable!(),
                RowDomain::Physical(range) => range.next(),
                RowDomain::Group { range, nulls_only } => {
                    let group = store.group.as_ref().expect("validated group projection");
                    let csr = if *nulls_only {
                        group.ordinary_nulls.as_ref()
                    } else {
                        group.groups.as_ref()
                    }
                    .expect("validated group CSR");
                    range.next().map(|position| csr.row(position)).transpose()?
                }
                RowDomain::GlobalNulls { run, page, row } => {
                    if *run == store.rows.run_count() {
                        self.remainder = RowDomain::Empty;
                        continue;
                    }
                    let run_id = B3RunId::from_input_slot(*run, store.rows.run_count())?;
                    let pages = store.rows.run(run_id)?.ordinary_null_pages()?;
                    if *page == pages.len() {
                        *run += 1;
                        *page = 0;
                        *row = 0;
                        continue;
                    }
                    let page_id = pages[*page];
                    let data = store.rows.page(run_id, page_id)?;
                    let rows = data
                        .value_nulls()
                        .ok_or_else(|| {
                            build_contract("NAA sparse NULL page lost its original facts")
                        })?
                        .ordinary_row_indices();
                    if *row == rows.len() {
                        *page += 1;
                        *row = 0;
                        continue;
                    }
                    let id = store.rows.row_in_page(run_id, page_id, rows[*row])?;
                    *row += 1;
                    return Ok(NaaCandidateStep::Row(id));
                }
            };
            match row {
                Some(row) => return Ok(NaaCandidateStep::Row(store.rows.row(row)?)),
                None => self.remainder = RowDomain::Empty,
            }
        }
    }
}

#[cfg(test)]
#[path = "naa_index_tests.rs"]
mod tests;
