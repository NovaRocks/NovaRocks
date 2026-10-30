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

//! Short immutable page loans retain complete logical declarations, explicit
//! storage representations and ordered output coordinates. Raw nominal bytes
//! and encoded complex values never masquerade as their signed Arrow carriers.

use super::{checkpoint_at, invariant, prepared::sort_by_key, reserve_push, vector};
use crate::{exec::pipeline::operator::InputConstructionOwner, runtime::fragment::ExecutionResult};
use arrow::array::{ArrayRef, BinaryViewArray};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::{ValueViolation, batch::OwnedBytesPage};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KeyReadRow {
    /// Position in the caller's requested ID sequence, including duplicates.
    pub(crate) ordinal: usize,
    /// Physical row in this exact immutable page.
    pub(crate) row: usize,
}

pub(crate) enum KeyReadValues<'a> {
    Fixed {
        array: &'a ArrayRef,
        binding: &'a FunctionValueType,
    },
    Bytes {
        array: &'a ArrayRef,
        binding: &'a FunctionValueType,
        page: &'a OwnedBytesPage,
    },
    /// Private raw view bytes, not an Arrow array of logical_binding's type.
    /// Only original byte constructors may publish the signed logical carrier.
    NominalBytes {
        raw: &'a ArrayRef,
        logical_binding: &'a FunctionValueType,
        page: &'a OwnedBytesPage,
    },
    Canonical {
        encoded: &'a BinaryViewArray,
        logical_binding: &'a FunctionValueType,
    },
}

pub(crate) struct KeyReadPage<'a> {
    values: KeyReadValues<'a>,
    rows: Vec<KeyReadRow>,
    // Rows may move into a combined loan while the original batch exits.
    _storage: ValueTypeStorageLifetime,
}
impl<'a> KeyReadPage<'a> {
    pub(crate) fn values(&self) -> &KeyReadValues<'a> {
        &self.values
    }
    pub(crate) fn rows(&self) -> &[KeyReadRow] {
        &self.rows
    }
}
pub(crate) struct KeyReadColumn<'a> {
    pub(super) pages: Vec<KeyReadPage<'a>>,
}
impl<'a> KeyReadColumn<'a> {
    pub(crate) fn pages(&self) -> &[KeyReadPage<'a>] {
        &self.pages
    }
}
pub(crate) struct OwnedKeyReadBatch<'a> {
    pub(super) columns: Vec<KeyReadColumn<'a>>,
    pub(super) len: usize,
    // Protect all request/selection vectors until their borrowed slices exit.
    pub(super) _storage: ValueTypeStorageLifetime,
}
impl<'a> OwnedKeyReadBatch<'a> {
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    pub(crate) fn columns(&self) -> &[KeyReadColumn<'a>] {
        &self.columns
    }
}

#[derive(Clone, Copy)]
pub(super) struct ReadRequest {
    pub(super) ordinal: usize,
    pub(super) batch: usize,
    pub(super) row: usize,
}
#[derive(Clone, Copy)]
pub(super) struct PositionedRow<P> {
    pub(super) page: P,
    pub(super) selected: KeyReadRow,
}

/// Both closures are monomorphized after the column strategy has been chosen.
/// The temporary contains only metadata, never copied key values.
pub(super) fn append_pages<'a>(
    requests: &[ReadRequest],
    position: impl Fn(usize) -> ExecutionResult<(usize, usize)>,
    page: impl Fn(usize) -> ExecutionResult<KeyReadValues<'a>>,
    output: &mut Vec<KeyReadPage<'a>>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let mut ordered = vector(owner, requests.len())?;
    for (i, request) in requests.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        let (page, row) = position(request.row)?;
        ordered.push(PositionedRow {
            page,
            selected: KeyReadRow {
                ordinal: request.ordinal,
                row,
            },
        });
    }
    append_positioned_pages(ordered, page, output, owner, checkpoint)
}

/// Group already-resolved real page coordinates. Hash and direct stores share
/// this author; the coordinate type does not grant access or fabricate a batch.
/// The caller funded the moved coordinate Vec through the same actual owner.
pub(super) fn append_positioned_pages<'a, P: Copy + Ord>(
    mut ordered: Vec<PositionedRow<P>>,
    page: impl Fn(P) -> ExecutionResult<KeyReadValues<'a>>,
    output: &mut Vec<KeyReadPage<'a>>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    sort_by_key(&mut ordered, owner, checkpoint, |r| {
        (r.page, r.selected.ordinal)
    })?;
    let mut begin = 0;
    while begin < ordered.len() {
        checkpoint()?;
        let mut end = begin + 1;
        while end < ordered.len() && ordered[end].page == ordered[begin].page {
            checkpoint_at(end - begin, checkpoint)?;
            end += 1;
        }
        let values = page(ordered[begin].page)?;
        let len = match &values {
            KeyReadValues::Fixed { array, .. }
            | KeyReadValues::Bytes { array, .. }
            | KeyReadValues::NominalBytes { raw: array, .. } => array.len(),
            KeyReadValues::Canonical { encoded, .. } => {
                use arrow::array::Array;
                encoded.len()
            }
        };
        let mut rows = vector(owner, end - begin)?;
        for (i, request) in ordered[begin..end].iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            if request.selected.row >= len {
                return Err(invariant(ValueViolation::Bounds));
            }
            rows.push(request.selected);
        }
        reserve_push(output, owner, checkpoint)?;
        output.push(KeyReadPage {
            values,
            rows,
            _storage: owner.storage_lifetime(),
        });
        begin = end;
    }
    checkpoint()
}

/// Move already-funded page loans into a batch funded by this same output
/// owner. Only row metadata is reindexed; values keep their original binding.
/// Each moved page retains its real row-vector creator independently.
/// The input batch stays alive until its emptied column/page directories drop.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Moves already funded typed loans for the frozen multipartition read/export API."
    )
)]
pub(super) fn append_reindexed<'a>(
    mut source: OwnedKeyReadBatch<'a>,
    ordinals: &[usize],
    output: &mut OwnedKeyReadBatch<'a>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    if source.len != ordinals.len() || source.columns.len() != output.columns.len() {
        return Err(invariant(ValueViolation::Layout));
    }
    let len = output.len;
    for (column, from) in source.columns.iter_mut().enumerate() {
        checkpoint()?;
        let to = &mut output.columns[column].pages;
        for mut page in from.pages.drain(..) {
            checkpoint()?;
            for (i, selected) in page.rows.iter_mut().enumerate() {
                checkpoint_at(i, checkpoint)?;
                selected.ordinal = *ordinals
                    .get(selected.ordinal)
                    .filter(|&&ordinal| ordinal < len)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            }
            reserve_push(to, owner, checkpoint)?;
            to.push(page);
        }
    }
    checkpoint()
}
