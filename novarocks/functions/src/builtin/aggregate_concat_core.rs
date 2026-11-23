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

use crate::aggregate_scalar::{
    ScalarStateAllocator, ScalarWork, TrackedAggScalarValue, aggregate_vec_with_capacity,
    tracked_scalar_from_array, tracked_scalar_heap_capacity,
};
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{ArrayRef, builder::StringBuilder};
use std::{cmp::Ordering, sync::Arc};

#[path = "listagg_emission.rs"]
mod listagg_emission;

#[derive(Debug)]
pub struct ListaggState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub rows: ScalarVec<ScalarVec<Option<TrackedAggScalarValue<A>>, A>, A>,
    pub failed: bool,
    payload_bytes: usize,
}
impl<A: ScalarStateAllocator> ListaggState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            rows: ScalarVec::new_in(allocator.clone()),
            allocator,
            failed: false,
            payload_bytes: 0,
        }
    }
    pub fn retained_bytes(&self) -> usize {
        self.rows.capacity() * std::mem::size_of::<ScalarVec<Option<TrackedAggScalarValue<A>>, A>>()
            + self.payload_bytes
    }
    pub fn latch_failure(&mut self) {
        self.failed = true;
        self.payload_bytes = 0;
        self.rows = ScalarVec::new_in(self.allocator.clone());
    }
    pub fn push_row(
        &mut self,
        row: ScalarVec<Option<TrackedAggScalarValue<A>>, A>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        work.flush()?;
        self.rows.try_reserve(1).map_err(|source| {
            self.allocator
                .scalar_allocation_error("reserve LISTAGG row", source)
        })?;
        work.flush()?;
        let mut retained = row.capacity() * std::mem::size_of::<Option<TrackedAggScalarValue<A>>>();
        for value in row.iter().flatten() {
            work.step()?;
            retained = retained
                .checked_add(tracked_scalar_heap_capacity(value, work)?)
                .ok_or(crate::KernelFailure::ResourceExhausted)?;
        }
        self.payload_bytes = self
            .payload_bytes
            .checked_add(retained)
            .ok_or(crate::KernelFailure::ResourceExhausted)?;
        self.rows.push(row);
        Ok(())
    }
    pub fn update_row(
        &mut self,
        columns: &[(&ArrayRef, usize)],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        let mut values = aggregate_vec_with_capacity(
            &self.allocator,
            columns.len(),
            "reserve LISTAGG row values",
            work,
        )?;
        for (idx, (column, row)) in columns.iter().enumerate() {
            work.step()?;
            let value = tracked_scalar_from_array(column, *row, &self.allocator, work)?;
            if idx < 5 && value.is_none() {
                return Ok(());
            }
            values.push(value);
        }
        self.push_row(values, work)
    }
}

fn compare_optional_scalar<A: ScalarStateAllocator>(
    left: &Option<TrackedAggScalarValue<A>>,
    right: &Option<TrackedAggScalarValue<A>>,
    asc: bool,
    nulls_first: bool,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Ordering, crate::KernelFailure> {
    let ord = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let ord = crate::aggregate_scalar::ExtremaDirection::Minimum
                .compare_tracked(left, right, work)?;
            if asc { ord } else { ord.reverse() }
        }
    };
    Ok(ord)
}

fn sort_indices<A: ScalarStateAllocator>(
    state: &ListaggState<A>,
    is_asc_order: &[bool],
    nulls_first: &[bool],
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<usize, A>, crate::KernelFailure> {
    let rows = &state.rows;
    let mut indices = aggregate_vec_with_capacity(
        &state.allocator,
        rows.len(),
        "reserve aggregate sort indices",
        work,
    )?;
    for index in 0..rows.len() {
        work.step()?;
        indices.push(index);
    }
    work.flush()?;
    if is_asc_order.len() == 0 {
        return Ok(indices);
    }

    work.flush()?;
    let mut error: Option<crate::KernelFailure> = None;
    indices.sort_unstable_by(|l, r| {
        if error.is_some() {
            return Ordering::Equal;
        }
        for key in 0..is_asc_order.len() {
            let col = 5 + key;
            match compare_optional_scalar(
                &rows[*l][col],
                &rows[*r][col],
                is_asc_order[key],
                nulls_first[key],
                work,
            ) {
                Ok(Ordering::Equal) => continue,
                Ok(ord) => return ord,
                Err(e) => {
                    error = Some(e);
                    return Ordering::Equal;
                }
            }
        }
        l.cmp(r)
    });
    if let Some(err) = error {
        return Err(err);
    }
    work.flush()?;
    Ok(indices)
}

pub(super) fn build_listagg_array<
    's,
    A: ScalarStateAllocator,
    I: ExactSizeIterator<Item = &'s ListaggState<A>>,
>(
    states: I,
    distinct: bool,
    ascending: &[bool],
    nulls_first: &[bool],
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, crate::KernelFailure> {
    let mut builder = StringBuilder::new();
    for state in states {
        work.step()?;
        let indices = sort_indices(state, ascending, nulls_first, work)?;
        let ordered = indices.iter().map(|index| state.rows[*index].as_slice());
        if let Some(output) = listagg_emission::emit(ordered, distinct, &state.allocator, work)? {
            let value = std::str::from_utf8(&output).map_err(|_| {
                crate::kernel_control::internal("LISTAGG emission did not preserve UTF-8")
            })?;
            work.flush()?;
            builder.append_value(value);
            work.flush()?;
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}
