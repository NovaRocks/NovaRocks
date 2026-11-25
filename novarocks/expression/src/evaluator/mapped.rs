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

//! Compact logical rows borrowing previously evaluated source backing.

use super::{Work, gather, internal};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, UInt64Array, new_empty_array, new_null_array},
    compute::concat,
    datatypes::DataType,
};
use novarocks_functions::{
    AggregateStateAllocator, KernelFailure, RowDataError,
    opaque_memory::{OpaqueReservation, OpaqueRetainedCharge},
    selected_copy::{CopyIndices, CopyOperationError, concat_copy_in, slice_copy_in, take_copy_in},
};
use std::{alloc::Layout, collections::HashMap, hash::Hash, sync::Arc};

pub(super) struct MappedValues {
    ty: DataType,
    sources: Vec<ArrayRef>,
    rows: Vec<Option<(usize, usize)>>,
    errors: Box<[RowDataError]>,
    allocator: Option<Arc<dyn AggregateStateAllocator>>,
    _mapping_reservation: Option<OpaqueReservation>,
}

pub(super) fn reserve(
    allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    bytes: usize,
) -> Result<Option<OpaqueReservation>, KernelFailure> {
    let Some(host) = allocator else {
        return Ok(None);
    };
    if bytes == 0 {
        return Ok(None);
    }
    let authority = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    authority.reserve_operation(bytes).map(Some)
}

pub(super) fn bytes<T>(len: usize) -> Result<usize, KernelFailure> {
    Layout::array::<T>(len)
        .map(|layout| layout.size())
        .map_err(|_| KernelFailure::ResourceExhausted)
}

pub(super) fn buffer<T>(len: usize) -> Result<Vec<T>, KernelFailure> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(values)
}

pub(super) struct ScopedMap<K, V> {
    entries: HashMap<K, V>,
    _reservation: Option<OpaqueReservation>,
}
impl<K: Eq + Hash, V> ScopedMap<K, V> {
    pub(super) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            _reservation: None,
        }
    }

    pub(super) fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key)
    }

    pub(super) fn insert(
        &mut self,
        key: K,
        value: V,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<(), KernelFailure> {
        if self.entries.len() == self.entries.capacity() {
            let capacity = self
                .entries
                .len()
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .ok_or(KernelFailure::ResourceExhausted)?;
            let facts =
                novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<K, V>(
                    capacity,
                )
                .map_err(|cause| internal(&cause.to_string()))?;
            work.flush()?;
            let reservation = reserve(allocator, facts.request_bytes_upper_bound)?;
            let mut grown = HashMap::new();
            grown
                .try_reserve(capacity)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            work.flush()?;
            for (key, value) in self.entries.drain() {
                grown.insert(key, value);
                work.step()?;
            }
            self.entries = grown;
            self._reservation = reservation;
        }
        self.entries.insert(key, value);
        work.step()
    }
}

impl MappedValues {
    pub(super) fn new(
        ty: &DataType,
        source_capacity: usize,
        rows: usize,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    ) -> Result<Self, KernelFailure> {
        let bytes = bytes::<Option<(usize, usize)>>(rows)?
            .checked_add(bytes::<ArrayRef>(source_capacity)?)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let reservation = reserve(allocator, bytes)?;
        Ok(Self {
            ty: ty.clone(),
            sources: buffer(source_capacity)?,
            rows: buffer(rows)?,
            errors: Box::default(),
            allocator: allocator.cloned(),
            _mapping_reservation: reservation,
        })
    }

    pub(super) fn with_errors(mut self, errors: Box<[RowDataError]>) -> Self {
        self.errors = errors;
        self
    }

    pub(super) fn errors(&self) -> &[RowDataError] {
        &self.errors
    }

    pub(super) fn into_errors(self) -> Box<[RowDataError]> {
        self.errors
    }

    pub(super) fn catch_try_errors(
        &mut self,
        catches: impl Fn(&RowDataError) -> bool,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<(), KernelFailure> {
        let mut errors = std::mem::take(&mut self.errors).into_vec();
        let mut keep = 0;
        for ordinal in 0..errors.len() {
            if catches(&errors[ordinal]) {
                self.rows[errors[ordinal].selected_ordinal()] = None;
            } else {
                errors.swap(ordinal, keep);
                keep += 1;
            }
            work.step()?;
        }
        errors.truncate(keep);
        self.errors = errors.into_boxed_slice();
        Ok(())
    }

    pub(super) fn data_type(&self) -> &DataType {
        &self.ty
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn sources(&self) -> &[ArrayRef] {
        &self.sources
    }

    pub(super) fn allocator(&self) -> Option<&Arc<dyn AggregateStateAllocator>> {
        self.allocator.as_ref()
    }

    pub(super) fn copy_mapping(
        &self,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<Self, KernelFailure> {
        let mut result = Self::new(
            &self.ty,
            self.sources.len(),
            self.rows.len(),
            self.allocator(),
        )?;
        for source in &self.sources {
            result.sources.push(Arc::clone(source));
            work.step()?;
        }
        for &row in &self.rows {
            result.rows.push(row);
            work.step()?;
        }
        let mut errors = buffer(self.errors.len())?;
        for error in &self.errors {
            errors.push(error.clone());
            work.step()?;
        }
        Ok(result.with_errors(errors.into_boxed_slice()))
    }

    pub(super) fn address(&self, ordinal: usize) -> Option<(usize, usize)> {
        self.rows[ordinal]
    }

    pub(super) fn value(&self, ordinal: usize) -> Option<(&ArrayRef, usize)> {
        self.address(ordinal)
            .map(|(source, row)| (&self.sources[source], row))
    }

    pub(super) fn is_null(
        &self,
        ordinal: usize,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<bool, KernelFailure> {
        match self.value(ordinal) {
            Some((array, row)) => {
                novarocks_functions::inspect_logical_null(array.as_ref(), row, || work.step())
            }
            None => Ok(true),
        }
    }

    pub(super) fn boolean(&self, ordinal: usize) -> Result<Option<bool>, KernelFailure> {
        let Some((array, row)) = self.value(ordinal) else {
            return Ok(None);
        };
        let values = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                super::invalid("mapped Boolean operand differs from its frozen carrier")
            })?;
        Ok((!values.is_null(row)).then(|| values.value(row)))
    }

    pub(super) fn push_source(&mut self, source: ArrayRef) -> Result<usize, KernelFailure> {
        if self.sources.len() >= self.sources.capacity() {
            return Err(internal("mapped result exceeded its admitted source table"));
        }
        let index = self.sources.len();
        self.sources.push(source);
        Ok(index)
    }

    pub(super) fn push(&mut self, row: Option<(usize, usize)>) -> Result<(), KernelFailure> {
        if self.rows.len() >= self.rows.capacity() {
            return Err(internal("mapped result exceeded its admitted row extent"));
        }
        if row.is_some_and(|(source, row)| {
            self.sources
                .get(source)
                .is_none_or(|array| row >= array.len())
        }) {
            return Err(internal("mapped row is outside its original backing"));
        }
        self.rows.push(row);
        Ok(())
    }

    pub(super) fn materialize(
        &self,
        ordinals: impl ExactSizeIterator<Item = usize> + Clone,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<ArrayRef, KernelFailure> {
        let rows = ordinals.len();
        if rows == 0 {
            return Ok(new_empty_array(&self.ty));
        }
        let mut selected = None;
        let mut multiple = false;
        let mut first = None;
        let mut contiguous = true;
        for (position, ordinal) in ordinals.clone().enumerate() {
            let address = self
                .rows
                .get(ordinal)
                .ok_or_else(|| internal("mapped selection exceeds its actual row extent"))?;
            if let Some((source, row)) = address {
                multiple |= selected.is_some_and(|selected| selected != *source);
                selected = Some(*source);
                if position == 0 {
                    first = Some(*row);
                }
                contiguous &= first.and_then(|first| first.checked_add(position)) == Some(*row);
            } else {
                contiguous = false;
            }
            work.step()?;
        }
        if multiple {
            return self.materialize_mixed(ordinals, work);
        }
        let Some(source) = selected else {
            let null = new_null_array(&self.ty, 1);
            return take_rows(
                &null,
                (0..rows).map(|_| None),
                self.allocator.as_ref(),
                work,
            );
        };
        if contiguous {
            return slice_rows(
                &self.sources[source],
                first.expect("nonempty contiguous mapping"),
                rows,
                self.allocator.as_ref(),
                work,
            );
        }
        take_rows(
            &self.sources[source],
            ordinals.map(|ordinal| self.rows[ordinal].map(|(_, row)| row)),
            self.allocator.as_ref(),
            work,
        )
    }

    fn materialize_mixed(
        &self,
        ordinals: impl ExactSizeIterator<Item = usize> + Clone,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<ArrayRef, KernelFailure> {
        // Each original copy receives only the selected physical rows of that
        // source. The final permutation reads exclusively these compact pieces.
        let capacity = self.sources.len();
        let rows = ordinals.len();
        let bytes = [
            bytes::<(usize, usize)>(rows)?,
            bytes::<Option<usize>>(rows)?,
            bytes::<ArrayRef>(capacity)?,
            bytes::<usize>(capacity)?,
            bytes::<usize>(
                capacity
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?,
            )?,
        ]
        .into_iter()
        .try_fold(0usize, |sum, size| {
            sum.checked_add(size)
                .ok_or(KernelFailure::ResourceExhausted)
        })?;
        let _scratch = reserve(self.allocator.as_ref(), bytes)?;
        let mut permutation = buffer::<Option<usize>>(rows)?;
        permutation.resize(rows, None);
        let mut counts = buffer::<usize>(capacity)?;
        counts.resize(capacity, 0);
        for ordinal in ordinals.clone() {
            if let Some((source, _)) = self.rows[ordinal] {
                counts[source] += 1;
            }
            work.step()?;
        }
        let mut offsets = buffer(capacity + 1)?;
        offsets.push(0usize);
        for &count in &counts {
            offsets.push(offsets.last().copied().unwrap() + count);
            work.step()?;
        }
        let mut grouped = buffer(rows)?;
        grouped.resize(rows, (0, 0));
        counts.fill(0);
        for (output, ordinal) in ordinals.enumerate() {
            if let Some((source, row)) = self.rows[ordinal] {
                grouped[offsets[source] + counts[source]] = (output, row);
                counts[source] += 1;
            }
            work.step()?;
        }
        let mut pieces = buffer(capacity)?;
        let mut offset = 0usize;
        for (source, bounds) in offsets.windows(2).enumerate() {
            let group = &grouped[bounds[0]..bounds[1]];
            if group.is_empty() {
                continue;
            }
            for (local, &(row, _)) in group.iter().enumerate() {
                permutation[row] = Some(
                    offset
                        .checked_add(local)
                        .ok_or(KernelFailure::ResourceExhausted)?,
                );
                work.step()?;
            }
            pieces.push(take_rows(
                &self.sources[source],
                group.iter().map(|&(_, row)| Some(row)),
                self.allocator.as_ref(),
                work,
            )?);
            offset = offset
                .checked_add(group.len())
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        work.flush()?;
        let joined = match &self.allocator {
            Some(host) => concat_copy_in(pieces, (), Arc::clone(host), work.control)
                .map_err(copy_failure)?
                .into_values(),
            None => concat(
                &pieces
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<_>>(),
            )
            .map_err(|cause| {
                novarocks_functions::control_values::AssemblyFailure::arrow("", cause).into_kernel()
            })?,
        };
        work.flush()?;
        take_rows(
            &joined,
            permutation.into_iter(),
            self.allocator.as_ref(),
            work,
        )
    }
}

pub(super) fn slice_rows(
    array: &ArrayRef,
    offset: usize,
    rows: usize,
    allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<ArrayRef, KernelFailure> {
    if offset == 0 && rows == array.len() {
        return Ok(Arc::clone(array));
    }
    work.flush()?;
    match allocator {
        Some(host) => Ok(slice_copy_in(
            Arc::clone(array),
            offset,
            rows,
            (),
            Arc::clone(host),
            work.control,
        )
        .map_err(copy_failure)?
        .into_values()),
        None => {
            let values = array.slice(offset, rows);
            work.flush()?;
            Ok(values)
        }
    }
}

pub(super) fn take_rows(
    array: &ArrayRef,
    rows: impl ExactSizeIterator<Item = Option<usize>>,
    allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<ArrayRef, KernelFailure> {
    let len = rows.len();
    let index_width = bytes::<u64>(len)?
        .checked_add(len.div_ceil(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    let indices_charge = reserve(allocator, index_width)?;
    let _projection_charge = reserve(allocator, bytes::<Option<u64>>(len)?)?;
    let mut indices = buffer(len)?;
    for row in rows {
        indices.push(
            row.map(u64::try_from)
                .transpose()
                .map_err(|_| KernelFailure::ResourceExhausted)?,
        );
        work.step()?;
    }
    match allocator {
        Some(host) => {
            work.flush()?;
            let indices = CopyIndices::UInt64(Arc::new(UInt64Array::from_iter(indices)));
            let copied = take_copy_in(
                Arc::clone(array),
                indices,
                indices_charge,
                Arc::clone(host),
                work.control,
            )
            .map_err(copy_failure)?;
            #[cfg(test)]
            super::lambda_observation::record(|| super::lambda_observation::Event::Gather {
                is_collection: matches!(
                    array.data_type(),
                    DataType::List(_) | DataType::LargeList(_) | DataType::Map(..)
                ),
                new_stock: copied.new_buffer_stock(),
            });
            Ok(copied.into_values())
        }
        None => gather(array, &indices, work),
    }
}

fn copy_failure(error: CopyOperationError) -> KernelFailure {
    match error {
        CopyOperationError::Control(error) => error,
        CopyOperationError::Preflight(error) => error.into_kernel(),
        CopyOperationError::OriginalData(error) => internal(error.text()),
    }
}
