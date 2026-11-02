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

//! One actual E-owned writer for evaluated projections and physical pages.
//! Group columns are an explicit K projection, never reconstructed SQL roles.

#[cfg(test)]
use super::build_pages::ValueNullRow;
use super::{
    build_pages::{
        B3RunId, BuildInputReceipt, BuildPageData, BuildPageProjection, InputCaptures,
        PageProjectionIds, PayloadRunPages, ProjectionRunSummary, RetainedNaaValues,
        ValueNullFacts, capture_work, check, checkpoint, push_owned,
    },
    build_state::{ProjectionDomains, build_contract},
    join_completion::{arithmetic, funded_shared, funded_vec},
};
use crate::{
    exec::{
        chunk::Chunk,
        hash_table::{
            key_index::{
                BulkDomain, BulkWork, CaptureBuilder, CaptureWork, EvaluatedPage, KeyInput,
                NullPolicy,
            },
            partition_keys::source_key_owner,
        },
        pipeline::operator::InputConstructionOwner,
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::array::ArrayRef;
use novarocks_type_contract::{FunctionValueType, ValueTypeError, ValueTypeStorageLifetime};
use novarocks_value_arrays::{decode::ValueConversionContext, reachability::ReachableRows};
use std::{ops::Range, sync::Arc};

pub(crate) struct EvaluatedKeyProjection<'a> {
    pub(crate) arrays: &'a [ArrayRef],
    pub(crate) bindings: &'a [FunctionValueType],
}
pub(crate) enum EvaluatedJoinKeyPage<'a> {
    Ordinary(EvaluatedKeyProjection<'a>),
    Naa {
        scope: EvaluatedKeyProjection<'a>,
        values: EvaluatedKeyProjection<'a>,
    },
}
#[expect(
    clippy::large_enum_variant,
    reason = "The E writer owns both original K capture builders inline; indirection would add another funded allocation to capture admission."
)]
enum OpenCaptures {
    PayloadOnly,
    Ordinary {
        writer: CaptureBuilder,
    },
    Naa {
        group: Option<CaptureBuilder>,
        full: CaptureBuilder,
        full_domain: BulkDomain,
        group_full_components: Vec<usize>,
        value_components: Range<usize>,
        _shape_storage: ValueTypeStorageLifetime,
    },
}
pub(crate) struct InputRunWriter {
    run: B3RunId,
    captures: Option<OpenCaptures>,
    pages: Vec<Arc<BuildPageData>>,
    rows: usize,
    ordinary_nulls: Option<usize>,
    ordinary_null_pages: Vec<usize>,
    summary: ProjectionRunSummary,
    payload_required: bool,
    failed: Option<ExecutionFailure>,
    sealed: bool,
    table_storage: ValueTypeStorageLifetime,
}
impl InputRunWriter {
    pub(crate) fn new(
        domains: &ProjectionDomains,
        run: B3RunId,
        payload_required: bool,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Self> {
        validate_domains(domains, work)?;
        let (captures, summary) = match domains {
            ProjectionDomains::PayloadOnly => {
                if !payload_required {
                    return Err(build_contract(
                        "nested-loop input must retain its physical payload",
                    ));
                }
                (OpenCaptures::PayloadOnly, ProjectionRunSummary::PayloadOnly)
            }
            ProjectionDomains::Ordinary { keys, .. } => (
                OpenCaptures::Ordinary {
                    writer: keys.begin_capture(&capture_work(work))?,
                },
                ProjectionRunSummary::Ordinary(None),
            ),
            ProjectionDomains::Naa {
                group,
                full,
                group_full_components,
                value_components,
                ..
            } => {
                let mut indices = funded_vec(work.owner, group_full_components.len())?;
                for (i, &column) in group_full_components.iter().enumerate() {
                    checkpoint(i, work)?;
                    indices.push(column);
                }
                (
                    OpenCaptures::Naa {
                        group: group
                            .as_ref()
                            .map(|domain| domain.begin_capture(&capture_work(work)))
                            .transpose()?,
                        full: full.begin_capture(&capture_work(work))?,
                        full_domain: full.clone(),
                        group_full_components: indices,
                        value_components: value_components.clone(),
                        _shape_storage: work.owner.storage_lifetime(),
                    },
                    ProjectionRunSummary::Naa {
                        group: None,
                        full: None,
                    },
                )
            }
        };
        check(work)?;
        Ok(Self {
            run,
            captures: Some(captures),
            pages: Vec::new(),
            rows: 0,
            ordinary_nulls: matches!(domains, ProjectionDomains::Naa { .. }).then_some(0),
            ordinary_null_pages: Vec::new(),
            summary,
            payload_required,
            failed: None,
            sealed: false,
            table_storage: work.owner.storage_lifetime(),
        })
    }
    fn check(&self, work: &BulkWork<'_>) -> ExecutionResult<()> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        check(work)?;
        if self.sealed {
            return Err(build_contract("join input writer was already sealed"));
        }
        Ok(())
    }
    fn latch(&mut self, cause: ExecutionFailure) -> ExecutionFailure {
        let original = self.failed.get_or_insert(cause).clone();
        // Original data is destroyed before its fields' finite creator holds.
        self.captures.take();
        self.pages = Vec::new();
        original
    }
    /// Direct NL page append: no K input, hashes, occurrence vector or router.
    pub(crate) fn push_payload(
        &mut self,
        source: &Chunk,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<BuildPageProjection> {
        let result = (|| {
            self.check(work)?;
            if !matches!(self.captures, Some(OpenCaptures::PayloadOnly)) {
                return Err(build_contract(
                    "keyed join input cannot omit its evaluated keys",
                ));
            }
            self.append_page(source, PageProjectionIds::PayloadOnly, None, None, work)
        })();
        result.map_err(|cause| self.latch(cause))
    }
    pub(crate) fn push_evaluated(
        &mut self,
        source: &Chunk,
        page: EvaluatedJoinKeyPage<'_>,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<BuildPageProjection> {
        let result = self.push_inner(source, page, conversions, work);
        result.map_err(|cause| self.latch(cause))
    }
    fn push_inner(
        &mut self,
        source: &Chunk,
        page: EvaluatedJoinKeyPage<'_>,
        conversions: &mut ValueConversionContext,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<BuildPageProjection> {
        self.check(work)?;
        let rows = source.len();
        let mut occurrences = funded_vec(work.owner, rows)?;
        for row in 0..rows {
            checkpoint(row, work)?;
            occurrences.push(row);
        }
        let readable = ReachableRows::all(rows)
            .materialize_observed(
                &mut |layout| work.owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| check(work),
            )
            .map_err(original_value_failure)?;
        let source_owner = source_key_owner(source, work.owner)?;
        let ports = CaptureWork {
            owner: source_owner.as_ref(),
            allocator: work.allocator.clone(),
            host: Arc::clone(&work.host),
            control: work.control,
        };
        let (ids, values, nulls) = match (self.captures.as_mut(), page) {
            (Some(OpenCaptures::Ordinary { writer }), EvaluatedJoinKeyPage::Ordinary(page)) => {
                validate_page(&page, rows, work)?;
                let id = writer.push_evaluated(
                    EvaluatedPage {
                        arrays: page.arrays,
                        bindings: page.bindings,
                        readable: &readable,
                        occurrences: &occurrences,
                    },
                    conversions,
                    &ports,
                )?;
                let (input, hashes) = writer.captured_page(id)?;
                if input.len() != rows || hashes.len() != rows {
                    return Err(build_contract(
                        "join page rows differ from original key receipt",
                    ));
                }
                (PageProjectionIds::Ordinary(id), None, None)
            }
            (
                Some(OpenCaptures::Naa {
                    group,
                    full,
                    full_domain,
                    group_full_components,
                    value_components,
                    ..
                }),
                EvaluatedJoinKeyPage::Naa { scope, values },
            ) => {
                validate_page(&scope, rows, work)?;
                validate_page(&values, rows, work)?;
                if scope.arrays.len() != value_components.start
                    || values.arrays.len() != value_components.end - value_components.start
                {
                    return Err(build_contract(
                        "join evaluated roles differ from original full projection",
                    ));
                }
                let mut arrays = funded_vec(work.owner, value_components.end)?;
                let mut bindings = funded_vec(work.owner, value_components.end)?;
                for (i, (array, binding)) in scope
                    .arrays
                    .iter()
                    .zip(scope.bindings)
                    .chain(values.arrays.iter().zip(values.bindings))
                    .enumerate()
                {
                    checkpoint(i, work)?;
                    arrays.push(Arc::clone(array));
                    bindings.push(binding.clone_observed(&|| check(work))?);
                }
                let group_id = if let Some(group) = group {
                    let mut group_arrays = funded_vec(work.owner, group_full_components.len())?;
                    let mut group_bindings = funded_vec(work.owner, group_full_components.len())?;
                    for (i, &column) in group_full_components.iter().enumerate() {
                        checkpoint(i, work)?;
                        group_arrays.push(Arc::clone(&arrays[column]));
                        group_bindings.push(bindings[column].clone_observed(&|| check(work))?);
                    }
                    let id = group.push_evaluated(
                        EvaluatedPage {
                            arrays: &group_arrays,
                            bindings: &group_bindings,
                            readable: &readable,
                            occurrences: &occurrences,
                        },
                        conversions,
                        &ports,
                    )?;
                    let (input, hashes) = group.captured_page(id)?;
                    if input.len() != rows || hashes.len() != rows {
                        return Err(build_contract("join group rows differ from physical page"));
                    }
                    Some(id)
                } else {
                    None
                };
                let full_id = full.push_evaluated(
                    EvaluatedPage {
                        arrays: &arrays,
                        bindings: &bindings,
                        readable: &readable,
                        occurrences: &occurrences,
                    },
                    conversions,
                    &ports,
                )?;
                let (input, hashes) = full.captured_page(full_id)?;
                if input.len() != rows || hashes.len() != rows {
                    return Err(build_contract("join full rows differ from physical page"));
                }
                let nulls = value_nulls(input, full_domain, value_components.clone(), rows, work)?;
                let retained = RetainedNaaValues {
                    input: input.clone(),
                    spec: full_domain.spec().clone(),
                    values: value_components.clone(),
                    _source: source_owner.storage_lifetime(),
                };
                (
                    PageProjectionIds::Naa {
                        group: group_id,
                        full: full_id,
                    },
                    Some(retained),
                    Some(nulls),
                )
            }
            _ => {
                return Err(build_contract(
                    "join evaluated page has the wrong closed projection kind",
                ));
            }
        };
        self.append_page(source, ids, values, nulls, work)
    }
    fn append_page(
        &mut self,
        source: &Chunk,
        ids: PageProjectionIds,
        values: Option<RetainedNaaValues>,
        nulls: Option<ValueNullFacts>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<BuildPageProjection> {
        let rows = source.len();
        let end = self.rows.checked_add(rows).ok_or_else(arithmetic)?;
        record_ids(&mut self.summary, ids, self.pages.len())?;
        if let Some(total) = &mut self.ordinary_nulls {
            let facts = nulls
                .as_ref()
                .ok_or_else(|| build_contract("NAA page has no original NULL observation"))?;
            if !facts.ordinary_row_indices().is_empty() {
                *total = total
                    .checked_add(facts.ordinary_row_indices().len())
                    .ok_or_else(arithmetic)?;
                push_owned(
                    &mut self.ordinary_null_pages,
                    self.pages.len(),
                    &mut self.table_storage,
                    work,
                )?;
            }
        }
        let payload = if self.payload_required {
            Some(super::payload_compaction::retain_payload(source, work)?)
        } else {
            None
        };
        let page = funded_shared(
            work.owner,
            BuildPageData {
                start: self.rows,
                rows,
                ids,
                payload,
                values,
                nulls,
                _storage: work.owner.storage_lifetime(),
            },
        )?;
        push_owned(
            &mut self.pages,
            Arc::clone(&page),
            &mut self.table_storage,
            work,
        )?;
        self.rows = end;
        check(work)?;
        Ok(BuildPageProjection(page))
    }
    /// The actual E caller must first finish EOS, admitted work and its RF
    /// producer. This seals data only, never query success or BuildPublished.
    pub(crate) fn seal(&mut self, work: &BulkWork<'_>) -> ExecutionResult<BuildInputReceipt> {
        let result = self.seal_inner(work);
        result.map_err(|cause| self.latch(cause))
    }
    fn seal_inner(&mut self, work: &BulkWork<'_>) -> ExecutionResult<BuildInputReceipt> {
        self.check(work)?;
        let captures = match self
            .captures
            .as_mut()
            .ok_or_else(|| build_contract("join input capture state is missing"))?
        {
            OpenCaptures::PayloadOnly => InputCaptures::PayloadOnly,
            OpenCaptures::Ordinary { writer } => {
                let capture = writer.seal(&capture_work(work))?;
                if capture.input_rows() != self.rows {
                    return Err(build_contract(
                        "join sealed key rows differ from physical pages",
                    ));
                }
                InputCaptures::Ordinary(capture)
            }
            OpenCaptures::Naa { group, full, .. } => {
                let group = group
                    .as_mut()
                    .map(|group| group.seal(&capture_work(work)))
                    .transpose()?;
                let full = full.seal(&capture_work(work))?;
                if full.input_rows() != self.rows
                    || group
                        .as_ref()
                        .is_some_and(|group| group.input_rows() != self.rows)
                {
                    return Err(build_contract(
                        "join sealed NAA rows differ from physical pages",
                    ));
                }
                InputCaptures::Naa { group, full }
            }
        };
        let pages = funded_shared(
            work.owner,
            PayloadRunPages {
                run: self.run,
                pages: std::mem::take(&mut self.pages),
                rows: self.rows,
                ordinary_nulls: self.ordinary_nulls,
                ordinary_null_pages: std::mem::take(&mut self.ordinary_null_pages),
                summary: self.summary,
                _table_storage: self.table_storage.clone(),
                _header_storage: work.owner.storage_lifetime(),
            },
        )?;
        check(work)?;
        self.sealed = true;
        self.captures.take();
        Ok(BuildInputReceipt {
            run: self.run,
            captures,
            pages,
        })
    }
}
fn record_one(
    observed: &mut Option<usize>,
    id: crate::exec::hash_table::key_index::CapturedPageId,
    page: usize,
) -> ExecutionResult<()> {
    if id.page_index() != page || observed.is_some_and(|run| run != id.run_index()) {
        return Err(build_contract(
            "join page no longer belongs to its original projection writer",
        ));
    }
    *observed = Some(id.run_index());
    Ok(())
}
fn record_ids(
    summary: &mut ProjectionRunSummary,
    ids: PageProjectionIds,
    page: usize,
) -> ExecutionResult<()> {
    match (summary, ids) {
        (ProjectionRunSummary::PayloadOnly, PageProjectionIds::PayloadOnly) => Ok(()),
        (ProjectionRunSummary::Ordinary(run), PageProjectionIds::Ordinary(id)) => {
            record_one(run, id, page)
        }
        (
            ProjectionRunSummary::Naa {
                group: group_run,
                full: full_run,
            },
            PageProjectionIds::Naa { group, full },
        ) => {
            if let Some(group) = group {
                record_one(group_run, group, page)?;
            }
            record_one(full_run, full, page)
        }
        _ => Err(build_contract("join projection kind changed during append")),
    }
}
fn validate_page(
    page: &EvaluatedKeyProjection<'_>,
    rows: usize,
    work: &BulkWork<'_>,
) -> ExecutionResult<()> {
    if page.arrays.len() != page.bindings.len() {
        return Err(build_contract(
            "join evaluated array and signed binding counts differ",
        ));
    }
    for (i, array) in page.arrays.iter().enumerate() {
        checkpoint(i, work)?;
        if array.len() != rows {
            return Err(build_contract(
                "join evaluated key does not cover the physical page",
            ));
        }
    }
    Ok(())
}
fn value_nulls(
    input: &KeyInput,
    full: &BulkDomain,
    values: Range<usize>,
    rows: usize,
    work: &BulkWork<'_>,
) -> ExecutionResult<ValueNullFacts> {
    #[cfg(test)]
    let mut any = funded_vec(work.owner, rows)?;
    let mut ordinary = funded_vec(work.owner, rows)?;
    let mut scratch = funded_vec(work.owner, rows)?;
    for row in 0..rows {
        checkpoint(row, work)?;
        #[cfg(test)]
        any.push(false);
        ordinary.push(false);
        scratch.push(false);
    }
    #[cfg(test)]
    input
        .component_range_root_nulls_observed(values.clone(), 0..rows, &mut any, &|| check(work))?;
    let mut component = values.start;
    while component < values.end {
        check(work)?;
        if full.spec().nulls()[component] != NullPolicy::Unmatchable {
            component += 1;
            continue;
        }
        let begin = component;
        while component < values.end && full.spec().nulls()[component] == NullPolicy::Unmatchable {
            checkpoint(component, work)?;
            component += 1;
        }
        input.component_range_root_nulls_observed(
            begin..component,
            0..rows,
            &mut scratch,
            &|| check(work),
        )?;
        for row in 0..rows {
            checkpoint(row, work)?;
            ordinary[row] |= scratch[row];
        }
    }
    #[cfg(test)]
    let mut facts = Vec::new();
    let mut ordinary_rows = Vec::new();
    let mut storage = work.owner.storage_lifetime();
    for (row, &ordinary_null) in ordinary.iter().enumerate() {
        checkpoint(row, work)?;
        #[cfg(test)]
        if any[row] {
            push_owned(
                &mut facts,
                ValueNullRow {
                    row,
                    ordinary: ordinary_null,
                },
                &mut storage,
                work,
            )?;
        }
        if ordinary_null {
            push_owned(&mut ordinary_rows, row, &mut storage, work)?;
        }
    }
    Ok(ValueNullFacts {
        #[cfg(test)]
        rows: facts,
        ordinary_rows,
        _storage: storage,
    })
}
fn validate_domains(domains: &ProjectionDomains, work: &BulkWork<'_>) -> ExecutionResult<()> {
    check(work)?;
    if let ProjectionDomains::Naa {
        group,
        full,
        group_full_components: indices,
        value_components: values,
        ..
    } = domains
    {
        let spec = full.spec();
        if values.start >= values.end
            || values.end != spec.types().len()
            || indices.len() < values.start
            || group.is_none() != indices.is_empty()
        {
            return Err(build_contract(
                "join NAA domains do not describe nonempty value roles and exact group columns",
            ));
        }
        let mut seen = funded_vec(
            work.owner,
            if indices.is_empty() {
                0
            } else {
                spec.types().len()
            },
        )?;
        if !indices.is_empty() {
            for column in 0..spec.types().len() {
                checkpoint(column, work)?;
                seen.push(false);
            }
        }
        for (i, &column) in indices.iter().enumerate() {
            checkpoint(i, work)?;
            if column >= spec.types().len()
                || (i < values.start && column != i)
                || (i >= values.start && (column < values.start || spec.types()[column].nullable()))
            {
                return Err(build_contract(
                    "join group omits scope or adds an unproven nullable value",
                ));
            }
            if std::mem::replace(&mut seen[column], true) {
                return Err(build_contract("join group repeats a full component"));
            }
        }
        if let Some(group) = group {
            if group.spec().types().len() != indices.len() {
                return Err(build_contract(
                    "join group signature has the wrong number of columns",
                ));
            }
            for (i, &column) in indices.iter().enumerate() {
                checkpoint(i, work)?;
                if !exact_signature(&spec.types()[column], &group.spec().types()[i], work)?
                    || spec.nulls()[column] != group.spec().nulls()[i]
                {
                    return Err(build_contract(
                        "join group signature does not originate in the full signed projection",
                    ));
                }
            }
        }
    }
    check(work)
}
enum SignatureFailure {
    Type(ValueTypeError),
    Caller(ExecutionFailure),
}
impl From<ValueTypeError> for SignatureFailure {
    fn from(value: ValueTypeError) -> Self {
        Self::Type(value)
    }
}
fn exact_signature(
    a: &FunctionValueType,
    b: &FunctionValueType,
    work: &BulkWork<'_>,
) -> ExecutionResult<bool> {
    a.exactly_equals_observed::<SignatureFailure>(b, || {
        check(work).map_err(SignatureFailure::Caller)
    })
    .map_err(|cause| match cause {
        SignatureFailure::Type(cause) => ExecutionFailure::from_domain(cause),
        SignatureFailure::Caller(cause) => cause,
    })
}
fn original_value_failure(
    cause: novarocks_value_arrays::ValueFailure<ExecutionFailure>,
) -> ExecutionFailure {
    match cause {
        novarocks_value_arrays::ValueFailure::Value(cause) => ExecutionFailure::from_domain(cause),
        novarocks_value_arrays::ValueFailure::Caller(cause) => cause,
    }
}

#[cfg(test)]
#[path = "build_capture_tests.rs"]
mod tests;
