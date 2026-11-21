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

//! Plan one common prefix of final output rows before building any column.
//! Output column storage, logical value bytes, and source custody are separate
//! facts. The caller owns the cursor and publishes the built columns.

mod column;
mod execution;

use crate::{
    ValueError, ValueFailure, ValueViolation,
    control::{Growth, Work},
};
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use column::Column;
use std::{alloc::Layout, num::NonZeroU32, ops::Range, sync::Arc};

/// The final producer's shared row map. Selection order and duplicate rows are
/// preserved. The shared form lets a producer retain the planner across polls.
#[derive(Clone, Debug)]
pub enum OutputRows<'a> {
    Contiguous(Range<usize>),
    Selected(&'a [u32]),
    SharedSelected(Arc<[u32]>),
}

impl OutputRows<'_> {
    fn get(&self, ordinal: usize) -> Result<usize, ValueError> {
        match self {
            Self::Contiguous(rows) => {
                let row = rows.start.checked_add(ordinal).ok_or_else(arithmetic)?;
                if row >= rows.end {
                    return Err(arithmetic());
                }
                Ok(row)
            }
            Self::Selected(rows) => rows
                .get(ordinal)
                .copied()
                .map(|row| row as usize)
                .ok_or_else(arithmetic),
            Self::SharedSelected(rows) => rows
                .get(ordinal)
                .copied()
                .map(|row| row as usize)
                .ok_or_else(arithmetic),
        }
    }

    fn len(&self) -> Result<usize, ValueError> {
        match self {
            Self::Contiguous(rows) => rows.end.checked_sub(rows.start).ok_or_else(arithmetic),
            Self::Selected(rows) => Ok(rows.len()),
            Self::SharedSelected(rows) => Ok(rows.len()),
        }
    }
}

/// A source column and its exact output carrier. Its value domain and field
/// binding remain the caller's responsibility. Keeping a dictionary asks for
/// its encoded carrier explicitly, with the values already in D(L).
#[derive(Clone, Copy)]
pub struct OutputColumn<'a> {
    source: &'a ArrayRef,
    target: &'a DataType,
    constant: bool,
}

impl<'a> OutputColumn<'a> {
    pub const fn new(source: &'a ArrayRef, target: &'a DataType) -> Self {
        Self {
            source,
            target,
            constant: false,
        }
    }

    pub const fn constant(source: &'a ArrayRef, target: &'a DataType) -> Self {
        Self {
            source,
            target,
            constant: true,
        }
    }

    pub const fn source(&self) -> &'a ArrayRef {
        self.source
    }

    pub const fn is_constant(&self) -> bool {
        self.constant
    }
}

/// Serial, producer-local planner over a single final row domain. It keeps
/// only column geometry and a rejected boundary row, not candidate arrays.
pub struct PrefixPlanner<'a> {
    columns: Vec<Column>,
    rows: OutputRows<'a>,
    row_count: usize,
    next_row: usize,
    planned_rows: usize,
    boundary_row: Option<usize>,
    uniform: bool,
}

impl<'a> PrefixPlanner<'a> {
    pub fn new<E>(
        columns: &[OutputColumn<'_>],
        rows: OutputRows<'a>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        let row_count = rows.len()?;
        let contiguous = matches!(&rows, OutputRows::Contiguous(_));
        let mut planned = allocate(columns.len(), growth)?;
        for column in columns {
            let source = column.source;
            if column.constant && source.len() != 1 {
                return Err(arithmetic().into());
            }
            if let OutputRows::Contiguous(range) = &rows
                && !column.constant
                && range.end > source.len()
            {
                return Err(arithmetic().into());
            }
            let borrow = contiguous && !column.constant && source.data_type() == column.target;
            planned.push(Column::new(
                source,
                column.target,
                column.constant,
                borrow,
                growth,
                work,
                0,
            )?);
        }
        let uniform = planned.iter().all(Column::uniform);
        Ok(Self {
            columns: planned,
            rows,
            row_count,
            next_row: 0,
            planned_rows: 0,
            boundary_row: None,
            uniform,
        })
    }

    pub const fn remaining_rows(&self) -> usize {
        self.row_count - self.next_row
    }

    /// Plan complete rows. A hard representation boundary returns the valid
    /// preceding prefix, or the original typed failure when one row cannot
    /// fit. The soft target always permits one representable row.
    pub fn plan_prefix<E>(
        &mut self,
        max_rows: NonZeroU32,
        soft_target_new_column_bytes: u64,
        work: &mut Work<'_, E>,
    ) -> Result<OutputPrefix<'_>, ValueFailure<E>> {
        self.plan_prefix_with_additional_storage(max_rows, soft_target_new_column_bytes, None, work)
    }

    /// Include the caller's additional, row-indexed materialized column
    /// storage in the same soft prefix decision. The callback consumes
    /// source row indices; the original build still owns its physical shape.
    pub fn plan_prefix_with_additional_storage<E>(
        &mut self,
        max_rows: NonZeroU32,
        soft_target_new_column_bytes: u64,
        mut additional_storage: Option<&mut dyn FnMut(usize) -> Result<u128, E>>,
        work: &mut Work<'_, E>,
    ) -> Result<OutputPrefix<'_>, ValueFailure<E>> {
        work(0).map_err(ValueFailure::Caller)?;
        if self.planned_rows != 0 {
            return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
        }
        for column in &mut self.columns {
            column.clear_prefix();
        }
        let cap = (max_rows.get() as usize).min(self.remaining_rows());
        let soft_target = u128::from(soft_target_new_column_bytes);
        let mut logical_bytes = 0;
        let mut new_column_bytes = 0;
        let mut shares_source = false;
        let mut len = 0;
        if self.uniform && cap > 0 && additional_storage.is_none() {
            let ordinal = self.next_row;
            self.prepare_boundary_row(ordinal, work)?;
            // A column's constant/fixed-width row facts can be scaled without
            // an array-sized pass. The predicate is monotone over the prefix.
            let one = self.candidate(0, 1)?;
            let mut low = 1;
            let mut high = cap;
            while low < high {
                work(self.columns.len()).map_err(ValueFailure::Caller)?;
                let middle = low + (high - low).div_ceil(2);
                match self.candidate(0, middle) {
                    Ok(candidate) if candidate.new_bytes <= soft_target => low = middle,
                    Ok(_) | Err(ValueError::Representation(_)) => high = middle - 1,
                    Err(error) => return Err(error.into()),
                }
            }
            let result = if low == 1 {
                one
            } else {
                self.candidate(0, low)?
            };
            (logical_bytes, new_column_bytes, shares_source) =
                (result.logical_bytes, result.new_bytes, result.shares_source);
            for column in &mut self.columns {
                column.commit_candidate(low)?;
            }
            len = low;
        } else {
            let mut additional_bytes = 0_u128;
            while len < cap {
                let ordinal = self.next_row + len;
                self.prepare_boundary_row(ordinal, work)?;
                let additional = match additional_storage.as_deref_mut() {
                    Some(measure) => additional_bytes
                        .checked_add(
                            measure(self.rows.get(ordinal)?).map_err(ValueFailure::Caller)?,
                        )
                        .ok_or_else(arithmetic)?,
                    None => 0,
                };
                let result = match self.candidate(len, 1) {
                    Ok(mut result) => {
                        result.new_bytes = result
                            .new_bytes
                            .checked_add(additional)
                            .ok_or_else(arithmetic)?;
                        if len > 0 && result.new_bytes > soft_target {
                            break;
                        }
                        result
                    }
                    Err(ValueError::Representation(_)) if len > 0 => break,
                    Err(error) => return Err(error.into()),
                };
                for column in &mut self.columns {
                    column.commit_candidate(1)?;
                }
                (logical_bytes, new_column_bytes, shares_source) =
                    (result.logical_bytes, result.new_bytes, result.shares_source);
                additional_bytes = additional;
                len += 1;
            }
        }
        if let OutputRows::Contiguous(range) = &self.rows {
            let start = range.start + self.next_row;
            for column in &self.columns {
                logical_bytes = logical_bytes
                    .checked_add(column.count_fixed_slice(start..start + len, work)?)
                    .ok_or_else(arithmetic)?;
            }
        }
        self.planned_rows = len;
        Ok(OutputPrefix {
            columns: &self.columns,
            row_map: &self.rows,
            start: self.next_row,
            len,
            logical_bytes,
            new_column_bytes,
            shares_source,
        })
    }

    fn prepare_boundary_row<E>(
        &mut self,
        ordinal: usize,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        if self.boundary_row == Some(ordinal) {
            return Ok(());
        }
        self.boundary_row = None;
        let row = self.rows.get(ordinal)?;
        for column in &mut self.columns {
            column.prepare_candidate(row, work)?;
        }
        self.boundary_row = Some(ordinal);
        Ok(())
    }

    fn candidate(&self, rows: usize, run: usize) -> Result<ColumnTotals, ValueError> {
        let mut total = ColumnTotals::default();
        for column in &self.columns {
            column.candidate_totals(rows, run, &mut total)?;
        }
        Ok(total)
    }

    /// Advance by exactly the plan that was built and accepted by the caller.
    pub fn advance(&mut self, published_rows: usize) -> Result<(), ValueError> {
        if self.planned_rows != published_rows {
            return Err(ValueError::Invariant(ValueViolation::RowTransaction));
        }
        self.next_row += published_rows;
        self.planned_rows = 0;
        Ok(())
    }
}

/// The selected row interval and the physical construction facts derived from it.
pub struct OutputPrefix<'a> {
    columns: &'a [Column],
    row_map: &'a OutputRows<'a>,
    start: usize,
    len: usize,
    logical_bytes: u128,
    new_column_bytes: u128,
    shares_source: bool,
}

impl OutputPrefix<'_> {
    pub const fn rows(&self) -> usize {
        self.len
    }

    /// New physical column storage, excluding scratch, wrapper allocations,
    /// and source backing. This is a construction estimate, not a capacity grant.
    pub const fn new_column_bytes(&self) -> u128 {
        self.new_column_bytes
    }

    pub const fn logical_bytes(&self) -> u128 {
        self.logical_bytes
    }

    /// At least one column uses a source buffer or the original dictionary.
    pub const fn shares_source(&self) -> bool {
        self.shares_source
    }

    pub fn build_observed<E>(
        &self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<Vec<ArrayRef>, ValueFailure<E>> {
        execution::build_prefix(self, growth, work)
    }
}

#[derive(Default)]
struct ColumnTotals {
    logical_bytes: u128,
    new_bytes: u128,
    shares_source: bool,
}

fn arithmetic() -> ValueError {
    ValueError::Invariant(ValueViolation::Bounds)
}

fn allocate<T, E>(count: usize, growth: &mut Growth<'_, E>) -> Result<Vec<T>, ValueFailure<E>> {
    let layout = Layout::array::<T>(count).map_err(|_| arithmetic())?;
    let mut out = Vec::new();
    if layout.size() != 0 {
        growth(layout).map_err(ValueFailure::Caller)?;
        out.try_reserve_exact(count)
            .map_err(|source| ValueError::Allocation {
                requested: layout.size(),
                source,
            })?;
    }
    Ok(out)
}
