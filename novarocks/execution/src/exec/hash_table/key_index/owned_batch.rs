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

//! One immutable representative batch. Column strategy dispatch and byte-page
//! grouping happen at batch boundaries. Row identities remain with the index.

use super::{
    KeySpec, PreparedKeyBatch, canonical_store::OwnedCanonicalColumn, checkpoint_at,
    fixed_store::OwnedFixedColumn, invariant, store::OwnedByteColumn, vector,
};
use crate::{
    exec::pipeline::operator::InputConstructionOwner,
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl};
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_value_arrays::ValueViolation;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(super) struct PageRow {
    pub(super) page: usize,
    pub(super) row: usize,
}
pub(super) enum Column {
    Fixed(OwnedFixedColumn),
    Bytes {
        pages: Vec<OwnedByteColumn>,
        locations: Vec<PageRow>,
    },
    Canonical(OwnedCanonicalColumn),
}
pub(super) struct OwnedKeyBatch {
    columns: Vec<Column>,
    spec: KeySpec,
    len: usize,
    _storage: ValueTypeStorageLifetime,
}
/// Reusable candidate scratch. Capacity is an allocation detail; larger calls
/// grow under the original owner instead of becoming a row/collision quota.
pub(super) struct CompareScratch {
    capacity: usize,
    ordinals: Vec<usize>,
    left: Vec<usize>,
    right: Vec<usize>,
    truth: Vec<u8>,
    bytes: Vec<Option<bool>>,
    _storage: ValueTypeStorageLifetime,
}
impl CompareScratch {
    pub(super) fn new() -> Self {
        Self {
            capacity: 0,
            ordinals: Vec::new(),
            left: Vec::new(),
            right: Vec::new(),
            truth: Vec::new(),
            bytes: Vec::new(),
            _storage: ValueTypeStorageLifetime::pure(),
        }
    }
    fn ensure(
        &mut self,
        count: usize,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        if count > self.capacity {
            checkpoint()?;
            let next = Self {
                capacity: count,
                ordinals: vector(owner, count)?,
                left: vector(owner, count)?,
                right: vector(owner, count)?,
                truth: vector(owner, count)?,
                bytes: vector(owner, count)?,
                _storage: owner.storage_lifetime(),
            };
            *self = next;
            checkpoint()?;
        }
        Ok(())
    }
}
impl OwnedKeyBatch {
    pub(super) fn from_compacted(
        spec: &KeySpec,
        columns: Vec<Column>,
        len: usize,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        if columns.len() != spec.types().len() {
            return Err(invariant(ValueViolation::Layout));
        }
        for column in &columns {
            checkpoint()?;
            let actual = match column {
                Column::Fixed(data) => data.len(),
                Column::Canonical(data) => data.len(),
                Column::Bytes { pages, locations } => {
                    for (index, position) in locations.iter().enumerate() {
                        checkpoint_at(index, &checkpoint)?;
                        if pages
                            .get(position.page)
                            .is_none_or(|p| position.row >= p.len())
                        {
                            return Err(invariant(ValueViolation::Bounds));
                        }
                    }
                    locations.len()
                }
            };
            if actual != len {
                return Err(invariant(ValueViolation::Bounds));
            }
        }
        checkpoint()?;
        Ok(Self {
            columns,
            spec: spec.clone(),
            len,
            _storage: owner.storage_lifetime(),
        })
    }
    pub(super) fn copy_selected(
        input: &PreparedKeyBatch<'_>,
        rows: &[usize],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        let mut columns = vector(owner, input.spec.types().len())?;
        for column in 0..input.spec.types().len() {
            checkpoint()?;
            let stored = if input.bytes(column)?.is_some() {
                let pages = OwnedByteColumn::copy_pages(
                    input,
                    column,
                    rows,
                    owner,
                    Arc::clone(&host),
                    control,
                )?;
                let mut locations = vector(owner, rows.len())?;
                for (page, data) in pages.iter().enumerate() {
                    for row in 0..data.len() {
                        checkpoint_at(locations.len(), &checkpoint)?;
                        locations.push(PageRow { page, row });
                    }
                }
                if locations.len() != rows.len() {
                    return Err(invariant(ValueViolation::Bounds));
                }
                Column::Bytes { pages, locations }
            } else if super::prepared::is_fixed(input.spec.types()[column].data_type()) {
                Column::Fixed(OwnedFixedColumn::copy_selected(
                    input,
                    column,
                    rows,
                    owner,
                    Arc::clone(&host),
                    control,
                )?)
            } else {
                Column::Canonical(OwnedCanonicalColumn::copy_selected(
                    input,
                    column,
                    rows,
                    owner,
                    Arc::clone(&host),
                    control,
                )?)
            };
            columns.push(stored);
        }
        checkpoint()?;
        Ok(Self {
            columns,
            spec: input.spec.clone(),
            len: rows.len(),
            _storage: owner.storage_lifetime(),
        })
    }
    pub(super) fn len(&self) -> usize {
        self.len
    }
    pub(super) fn append_reads<'a>(
        &'a self,
        column: usize,
        requests: &[super::read::ReadRequest],
        output: &mut Vec<super::read::KeyReadPage<'a>>,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        use super::read::{KeyReadValues, append_pages};
        checkpoint()?;
        match self
            .columns
            .get(column)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?
        {
            Column::Fixed(data) => append_pages(
                requests,
                |row| Ok((0, row)),
                |_| {
                    Ok(KeyReadValues::Fixed {
                        array: data.array_ref(),
                        binding: data.binding(),
                    })
                },
                output,
                owner,
                checkpoint,
            ),
            Column::Bytes { pages, locations } => append_pages(
                requests,
                |row| {
                    let pos = locations
                        .get(row)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                    Ok((pos.page, pos.row))
                },
                |page| {
                    let data = pages
                        .get(page)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                    Ok(if super::store::is_nominal_byte(data.binding()) {
                        KeyReadValues::NominalBytes {
                            raw: data.array_ref(),
                            logical_binding: data.binding(),
                            page: data.page(),
                        }
                    } else {
                        KeyReadValues::Bytes {
                            array: data.array_ref(),
                            binding: data.binding(),
                            page: data.page(),
                        }
                    })
                },
                output,
                owner,
                checkpoint,
            ),
            Column::Canonical(data) => data.append_reads(requests, output, owner, checkpoint),
        }
    }
    /// Pair output is conjunctive full-key equality. Any error invalidates the
    /// entire operation output, never just its unfinished suffix.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compare_pairs(
        &self,
        input: &PreparedKeyBatch<'_>,
        input_rows: &[usize],
        owned_rows: &[usize],
        output: &mut [u8],
        scratch: &mut CompareScratch,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        checkpoint()?;
        if !self.spec.same_instance(&input.spec)
            || input_rows.len() != owned_rows.len()
            || output.len() != input_rows.len()
        {
            return Err(invariant(ValueViolation::Layout));
        }
        for (i, ((&row, &owned), out)) in input_rows
            .iter()
            .zip(owned_rows)
            .zip(output.iter_mut())
            .enumerate()
        {
            checkpoint_at(i, checkpoint)?;
            if !input.input.selected().contains(row) || owned >= self.len {
                return Err(invariant(ValueViolation::UndefinedRow));
            }
            *out = 1;
        }
        scratch.ensure(output.len(), owner, checkpoint)?;
        for (column, stored) in self.columns.iter().enumerate() {
            checkpoint()?;
            scratch.ordinals.clear();
            for (i, &equal) in output.iter().enumerate() {
                checkpoint_at(i, checkpoint)?;
                if equal != 0 {
                    scratch.ordinals.push(i);
                }
            }
            if scratch.ordinals.is_empty() {
                break;
            }
            match stored {
                Column::Bytes { pages, locations } => compare_bytes(
                    pages, locations, column, input, input_rows, owned_rows, output, scratch,
                    owner, checkpoint,
                )?,
                Column::Fixed(data) => compare_values(
                    input_rows,
                    owned_rows,
                    output,
                    scratch,
                    checkpoint,
                    |left, right, out| {
                        data.compare_pairs(input, column, left, right, out, checkpoint)
                    },
                )?,
                Column::Canonical(data) => compare_values(
                    input_rows,
                    owned_rows,
                    output,
                    scratch,
                    checkpoint,
                    |left, right, out| {
                        data.compare_pairs(input, column, left, right, out, checkpoint)
                    },
                )?,
            }
        }
        checkpoint()
    }
}
fn compare_values(
    input_rows: &[usize],
    owned_rows: &[usize],
    output: &mut [u8],
    scratch: &mut CompareScratch,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
    compare: impl FnOnce(&[usize], &[usize], &mut [u8]) -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    scratch.left.clear();
    scratch.right.clear();
    scratch.truth.clear();
    for (i, &ordinal) in scratch.ordinals.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        scratch.left.push(input_rows[ordinal]);
        scratch.right.push(owned_rows[ordinal]);
        scratch.truth.push(0);
    }
    compare(&scratch.left, &scratch.right, &mut scratch.truth)?;
    for (i, &ordinal) in scratch.ordinals.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        output[ordinal] &= scratch.truth[i];
    }
    checkpoint()
}
#[allow(clippy::too_many_arguments)]
fn compare_bytes(
    pages: &[OwnedByteColumn],
    locations: &[PageRow],
    column: usize,
    input: &PreparedKeyBatch<'_>,
    input_rows: &[usize],
    owned_rows: &[usize],
    output: &mut [u8],
    scratch: &mut CompareScratch,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    super::prepared::sort_by_key(&mut scratch.ordinals, owner, checkpoint, |i| {
        (locations[owned_rows[i]].page, i)
    })?;
    let mut start = 0;
    while start < scratch.ordinals.len() {
        checkpoint()?;
        let page = locations[owned_rows[scratch.ordinals[start]]].page;
        let mut end = start + 1;
        while end < scratch.ordinals.len()
            && locations[owned_rows[scratch.ordinals[end]]].page == page
        {
            checkpoint_at(end, checkpoint)?;
            end += 1;
        }
        scratch.left.clear();
        scratch.right.clear();
        scratch.bytes.clear();
        for (i, &ordinal) in scratch.ordinals[start..end].iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            scratch.left.push(input_rows[ordinal]);
            scratch.right.push(locations[owned_rows[ordinal]].row);
            scratch.bytes.push(None);
        }
        pages[page].compare_pairs(
            input,
            column,
            &scratch.left,
            &scratch.right,
            &mut scratch.bytes,
            checkpoint,
        )?;
        for (i, &ordinal) in scratch.ordinals[start..end].iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            output[ordinal] &= u8::from(scratch.bytes[i] == Some(true));
        }
        start = end;
    }
    checkpoint()
}

/// Compaction preserves the index's complete logical domain while deriving
/// root NULL declarations from the actual representative pages.
pub(super) fn validate_compacted_binding(
    spec: &KeySpec,
    column: usize,
    binding: &novarocks_type_contract::FunctionValueType,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let expected = spec
        .types()
        .get(column)
        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
    if !expected
        .same_value_domain_observed::<super::prepared::DomainFailure>(binding, || {
            checkpoint().map_err(super::prepared::DomainFailure::Caller)
        })
        .map_err(ExecutionFailure::from)?
    {
        return Err(invariant(ValueViolation::Layout));
    }
    checkpoint()
}
