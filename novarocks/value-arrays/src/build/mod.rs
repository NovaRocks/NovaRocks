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

//! Frozen-layout byte construction. A row is published only by commit; every
//! refused operation rolls back that entire row. Scheduling belongs to callers.
use crate::arrow_raw::{self, ByteSpan};
use crate::control::{Growth, Work};
use crate::error::field_representation;
use crate::{ByteLayout, ValueError, ValueFailure, ValueViolation};
use allocator_api2::alloc::{Allocator, Global};
use allocator_api2::vec::Vec as AllocVec;
use arrow_array::{Array, ArrayRef};
use arrow_buffer::Buffer;
use novarocks_failure_contract::RepresentationBoundary;
use std::alloc::Layout;
use std::ops::Range;

const BLOCK_TARGET: usize = 64 * 1024;
const COPY_CHECKPOINT: usize = 64 * 1024;

/// A bounded inline plan for the builder's own storage. It includes the view
/// publication-region table: reserve_planned_observed allocates and holds that
/// table before appending, and finish_observed consumes it without another
/// growth request. Arrow's private wrapping metadata remains outside this plan.
#[derive(Clone, Debug, Default)]
pub struct PlannedLayouts {
    layouts: [Option<Layout>; 5],
    len: usize,
}

impl PlannedLayouts {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &Layout> {
        self.layouts.iter().flatten()
    }

    fn push<T>(&mut self, count: usize) -> Result<(), ValueError> {
        if count != 0 {
            self.layouts[self.len] = Some(planned_layout::<T>(count)?);
            self.len += 1;
        }
        Ok(())
    }
}

impl IntoIterator for PlannedLayouts {
    type Item = Layout;
    type IntoIter = std::iter::Flatten<std::array::IntoIter<Option<Layout>, 5>>;

    fn into_iter(self) -> Self::IntoIter {
        self.layouts.into_iter().flatten()
    }
}

#[derive(Debug)]
pub enum WriteFailure<E> {
    /// The current row fits an empty output, but not this output's real fields.
    Full,
    Failure(ValueFailure<E>),
}
impl<E> From<ValueError> for WriteFailure<E> {
    fn from(error: ValueError) -> Self {
        Self::Failure(error.into())
    }
}
impl<E> From<ValueFailure<E>> for WriteFailure<E> {
    fn from(error: ValueFailure<E>) -> Self {
        Self::Failure(error)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BuildFacts {
    pub rows: usize,
    /// Logical volume includes repeated adopted values and can exceed the
    /// volume of all addressable payload storage.
    pub logical_bytes: u128,
    /// Actual bytes copied by writes, excluding rolled-back rows.
    pub copied_bytes: u128,
    /// Bytes copied from foreign-allocator buffers into standard Arrow
    /// buffers during publication, including offsets/descriptors/validity.
    pub publication_copied_bytes: u128,
    pub adopted_regions: usize,
    /// Sum of the mounted payload regions' declared capacities. Shared
    /// allocations may appear more than once; unknown owner capacity stays
    /// unknown and this is never a physical-retention ledger.
    pub declared_payload_capacity: u128,
}

pub struct BuiltArray {
    pub array: ArrayRef,
    pub facts: BuildFacts,
}

enum Region<A: Allocator> {
    Owned(AllocVec<u8, A>),
    Shared(Buffer),
}

/// Its actual mutable allocations use A. Publication transfers Global buffers
/// to standard Arrow storage; a foreign A is released after a standard copy.
pub struct ValueBuilder<A: Allocator + Clone + 'static = Global> {
    layout: ByteLayout,
    allocator: A,
    payload: AllocVec<u8, A>,
    offsets32: AllocVec<i32, A>,
    offsets64: AllocVec<i64, A>,
    views: AllocVec<u128, A>,
    validity: AllocVec<u8, A>,
    regions: AllocVec<Region<A>, A>,
    current: Option<usize>,
    rows: usize,
    null_count: usize,
    logical_bytes: u128,
    copied_bytes: u128,
    planned_payload_region: Option<usize>,
    publication_regions: Option<Vec<Buffer>>,
    _serial: std::marker::PhantomData<std::cell::Cell<()>>,
}

impl ValueBuilder<Global> {
    pub fn new(layout: ByteLayout) -> Self {
        Self::new_in(layout, Global)
    }
}

impl<A: Allocator + Clone + 'static> ValueBuilder<A> {
    pub fn new_in(layout: ByteLayout, allocator: A) -> Self {
        Self {
            _serial: std::marker::PhantomData,
            layout,
            payload: AllocVec::new_in(allocator.clone()),
            offsets32: AllocVec::new_in(allocator.clone()),
            offsets64: AllocVec::new_in(allocator.clone()),
            views: AllocVec::new_in(allocator.clone()),
            validity: AllocVec::new_in(allocator.clone()),
            regions: AllocVec::new_in(allocator.clone()),
            allocator,
            current: None,
            rows: 0,
            null_count: 0,
            logical_bytes: 0,
            copied_bytes: 0,
            planned_payload_region: None,
            publication_regions: None,
        }
    }

    pub fn layout(&self) -> ByteLayout {
        self.layout
    }
    pub fn len(&self) -> usize {
        self.rows
    }
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Plan exact initial capacity requests for append_array_value conversion.
    /// Offsets use the sum of output bytes. Views adopt long source regions;
    /// payload_bytes is the sum of copied inline bytes and region_bound bounds
    /// distinct adopted regions. One additional owned inline region is included
    /// automatically. The plan itself has no heap storage. Foreign-allocator
    /// publication copies remain separate finalization growth observations;
    /// their ordinary destination storage is not held by this mutable plan.
    pub fn planned_layouts(
        layout: ByteLayout,
        rows: usize,
        payload_bytes: usize,
        adopted_region_bound: usize,
    ) -> Result<PlannedLayouts, ValueError> {
        let counts = planned_counts(layout, rows, payload_bytes, adopted_region_bound)?;
        let mut plan = PlannedLayouts::default();
        if layout.is_view() {
            plan.push::<u128>(rows)?;
        } else if matches!(layout, ByteLayout::Utf8 | ByteLayout::Binary) {
            plan.push::<i32>(counts.offsets)?;
        } else {
            plan.push::<i64>(counts.offsets)?;
        }
        plan.push::<u8>(counts.validity)?;
        plan.push::<u8>(payload_bytes)?;
        if layout.is_view() {
            plan.push::<Region<A>>(counts.regions)?;
            plan.push::<Buffer>(counts.regions)?;
        }
        Ok(plan)
    }

    /// Prepare a pristine builder using the checked plan above. Allocation or
    /// control refusal leaves self pristine, so the same request can be retried
    /// without re-requesting retained storage. Only after every reservation
    /// succeeds does this builder take custody of the prepared vectors.
    ///
    /// The capacity guarantee covers append_array_value, NULLs and empty values
    /// within the supplied row/byte/region counts. General fragment writes may
    /// request additional storage through their ordinary growth callback.
    pub fn reserve_planned_observed<E>(
        &mut self,
        rows: usize,
        payload_bytes: usize,
        adopted_region_bound: usize,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        work(0).map_err(ValueFailure::Caller)?;
        if self.rows != 0
            || self.payload.capacity() != 0
            || self.offsets32.capacity() != 0
            || self.offsets64.capacity() != 0
            || self.views.capacity() != 0
            || self.validity.capacity() != 0
            || self.regions.capacity() != 0
            || self.current.is_some()
            || self.planned_payload_region.is_some()
            || self.publication_regions.is_some()
        {
            return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
        }
        let counts = planned_counts(self.layout, rows, payload_bytes, adopted_region_bound)?;
        // Check the entire plan before the first allocation or growth request.
        Self::planned_layouts(self.layout, rows, payload_bytes, adopted_region_bound)?;
        let mut prepared = Self::new_in(self.layout, self.allocator.clone());
        if self.layout.is_view() {
            reserve_initial(&mut prepared.views, rows, growth, work)?;
        } else if matches!(self.layout, ByteLayout::Utf8 | ByteLayout::Binary) {
            reserve_initial(&mut prepared.offsets32, counts.offsets, growth, work)?;
        } else {
            reserve_initial(&mut prepared.offsets64, counts.offsets, growth, work)?;
        }
        reserve_initial(&mut prepared.validity, counts.validity, growth, work)?;
        if self.layout.is_view() && rows != 0 {
            let mut inline = AllocVec::new_in(self.allocator.clone());
            reserve_initial(&mut inline, payload_bytes, growth, work)?;
            reserve_initial(&mut prepared.regions, counts.regions, growth, work)?;
            prepared.regions.push(Region::Owned(inline));
            prepared.current = Some(0);
            prepared.planned_payload_region = Some(0);
            let table_layout = planned_layout::<Buffer>(counts.regions)?;
            work(1).map_err(ValueFailure::Caller)?;
            growth(table_layout).map_err(ValueFailure::Caller)?;
            let mut publication = Vec::new();
            publication
                .try_reserve_exact(counts.regions)
                .map_err(|source| ValueError::Allocation {
                    requested: table_layout.size(),
                    source,
                })?;
            prepared.publication_regions = Some(publication);
        } else {
            reserve_initial(&mut prepared.payload, payload_bytes, growth, work)?;
        }
        *self = prepared;
        Ok(())
    }

    pub fn begin_row(&mut self) -> RowWriter<'_, A> {
        let current_len = self.current.map(|i| match &self.regions[i] {
            Region::Owned(bytes) => bytes.len(),
            Region::Shared(_) => unreachable!("current block is owned"),
        });
        let mark = RowMark {
            payload: self.payload.len(),
            region_count: self.regions.len(),
            current: self.current,
            current_len,
            copied_bytes: self.copied_bytes,
        };
        RowWriter {
            builder: self,
            mark,
            data: Pending::Empty,
            done: false,
        }
    }

    pub fn append_str<E>(
        &mut self,
        value: &str,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        let mut row = self.begin_row();
        row.write_str(value, growth, work)?;
        row.commit(growth, work)
    }

    pub fn append_bytes<E>(
        &mut self,
        value: &[u8],
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        let mut row = self.begin_row();
        row.write_bytes(value, growth, work)?;
        row.commit(growth, work)
    }

    pub fn append_null<E>(
        &mut self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.begin_row().commit_null(growth, work)
    }

    /// Adopt a value from a valid Arrow string/binary source without UTF8
    /// revalidation. A NULL source appends a logical NULL, not an empty value.
    pub fn append_array_value<E>(
        &mut self,
        source: &dyn Array,
        index: usize,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        let mut row = self.begin_row();
        row.write_array_value(source, index, growth, work)?;
        row.commit(growth, work)
    }

    /// Publish with ordinary global allocation and no caller control policy.
    /// Foreign allocator buffers are copied; their original allocator releases
    /// the mutable storage. Owners requiring growth/cancellation observation
    /// use finish_observed instead.
    pub fn finish(self) -> Result<BuiltArray, ValueError> {
        self.finish_observed(
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .map_err(ValueFailure::into_error)
    }

    /// Publish the committed prefix. Global buffers transfer to standard Arrow
    /// without recopying or a new payload-growth observation. Foreign buffers
    /// first observe the exact standard destination Layout and bounded copying;
    /// refusal drops all unpublished storage with its original allocator.
    /// Arrow's additional private headers remain a public-library boundary.
    pub fn finish_observed<E>(
        self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<BuiltArray, ValueFailure<E>> {
        self.finish_structured_observed(growth, work, &mut |_| Ok(()))
    }
    pub fn finish_structured_observed<E>(
        mut self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut crate::control::StructureObserver<'_, E>,
    ) -> Result<BuiltArray, ValueFailure<E>> {
        let owners = if self.layout.is_view() {
            1 + if self.rows == 0 {
                0
            } else {
                self.regions
                    .iter()
                    .filter(|region| matches!(region, Region::Owned(_)))
                    .count()
                    + usize::from(self.null_count != 0)
            }
        } else {
            2 + usize::from(self.rows != 0 && self.null_count != 0)
        };
        observe(crate::control::ValueStructure::ByteArray {
            layout: self.layout,
            new_buffer_owners: owners,
        })
        .map_err(ValueFailure::Caller)?;
        if self.layout.is_view() {
            // Arrow already stored Arc<[Buffer]> in 58.4. Publication consumes
            // our separately observed Vec while allocating this shared table.
            // Admit both live blocks under the same original caller scope.
            crate::control::shared_slice_owner::<Buffer, E>(
                if self.rows == 0 {
                    0
                } else {
                    self.regions.len()
                },
                observe,
            )?;
        }
        work(0).map_err(ValueFailure::Caller)?;
        let mut facts = self.facts();
        if self.rows == 0 {
            let array = if self.layout.is_view() {
                // The view empty constructor has no offsets sentinel or data
                // allocation. Do not transfer any aborted mutable capacity.
                arrow_array::new_empty_array(&self.layout.data_type())
            } else {
                // An offsets array still owns one zero sentinel. Construct it
                // through our observed allocator rather than Arrow's rounded
                // internal empty-offset allocation, reusing an existing plan.
                let offsets = if matches!(self.layout, ByteLayout::Utf8 | ByteLayout::Binary) {
                    if self.offsets32.capacity() == 0 {
                        reserve_initial(&mut self.offsets32, 1, growth, work)?;
                    }
                    self.offsets32.push(0);
                    publish_buffer(
                        self.offsets32,
                        &mut facts.publication_copied_bytes,
                        growth,
                        work,
                    )?
                } else {
                    if self.offsets64.capacity() == 0 {
                        reserve_initial(&mut self.offsets64, 1, growth, work)?;
                    }
                    self.offsets64.push(0);
                    publish_buffer(
                        self.offsets64,
                        &mut facts.publication_copied_bytes,
                        growth,
                        work,
                    )?
                };
                arrow_raw::make_offsets(
                    self.layout,
                    offsets,
                    Buffer::from_vec(Vec::<u8>::new()),
                    None,
                )?
            };
            facts.declared_payload_capacity = arrow_raw::byte_payload_capacity(array.as_ref())?;
            debug_assert!(array.to_data().validate_full().is_ok());
            work(0).map_err(ValueFailure::Caller)?;
            return Ok(BuiltArray { array, facts });
        }
        let nulls = if self.null_count == 0 {
            None
        } else {
            arrow_raw::make_nulls(
                publish_buffer(
                    self.validity,
                    &mut facts.publication_copied_bytes,
                    growth,
                    work,
                )?,
                self.rows,
                self.null_count,
            )
        };
        let array = if self.layout.is_view() {
            // This is our output-region table, distinct from the mutable Region
            // enum table and from Arrow's opaque internal header allocation.
            let table_layout = Layout::array::<Buffer>(self.regions.len()).map_err(|_| {
                allocation_extent(
                    self.rows,
                    self.regions
                        .len()
                        .checked_mul(std::mem::size_of::<Buffer>())
                        .and_then(|n| u64::try_from(n).ok()),
                )
            })?;
            let mut buffers = self.publication_regions.unwrap_or_default();
            if self.regions.len() > buffers.capacity() {
                growth(table_layout).map_err(ValueFailure::Caller)?;
                buffers
                    .try_reserve_exact(self.regions.len())
                    .map_err(|source| ValueError::Allocation {
                        requested: table_layout.size(),
                        source,
                    })?;
            }
            for region in self.regions {
                work(1).map_err(ValueFailure::Caller)?;
                buffers.push(match region {
                    Region::Owned(bytes) => {
                        publish_buffer(bytes, &mut facts.publication_copied_bytes, growth, work)?
                    }
                    Region::Shared(buffer) => buffer,
                });
            }
            let views = publish_buffer(
                self.views,
                &mut facts.publication_copied_bytes,
                growth,
                work,
            )?
            .into();
            // SAFETY: commits create only checked descriptors into retained
            // regions; strings originate from str or admitted Arrow strings.
            // NULL slots use a valid empty inline descriptor, and bitmap length
            // equals the committed descriptor count.
            unsafe { arrow_raw::make_view_unchecked(self.layout, views, buffers, nulls)? }
        } else {
            let offsets = if matches!(self.layout, ByteLayout::Utf8 | ByteLayout::Binary) {
                publish_buffer(
                    self.offsets32,
                    &mut facts.publication_copied_bytes,
                    growth,
                    work,
                )?
            } else {
                publish_buffer(
                    self.offsets64,
                    &mut facts.publication_copied_bytes,
                    growth,
                    work,
                )?
            };
            arrow_raw::make_offsets(
                self.layout,
                offsets,
                publish_buffer(
                    self.payload,
                    &mut facts.publication_copied_bytes,
                    growth,
                    work,
                )?,
                nulls,
            )?
        };
        facts.declared_payload_capacity = arrow_raw::byte_payload_capacity(array.as_ref())?;
        debug_assert!(array.to_data().validate_full().is_ok());
        work(0).map_err(ValueFailure::Caller)?;
        Ok(BuiltArray { array, facts })
    }

    pub fn facts(&self) -> BuildFacts {
        let mut facts = BuildFacts {
            rows: self.rows,
            logical_bytes: self.logical_bytes,
            copied_bytes: self.copied_bytes,
            publication_copied_bytes: 0,
            adopted_regions: 0,
            declared_payload_capacity: self.payload.capacity() as u128,
        };
        for region in &self.regions {
            let capacity = match region {
                Region::Owned(bytes) => bytes.capacity(),
                Region::Shared(bytes) => {
                    facts.adopted_regions += 1;
                    bytes.capacity()
                }
            };
            facts.declared_payload_capacity += capacity as u128;
        }
        facts
    }
}

/// The helper returns its actual copy count, so the builder does not guess
/// allocator identity or conflate standard zero-copy transfer with publication.
fn publish_buffer<T: arrow_buffer::ArrowNativeType, A: Allocator + 'static, E>(
    values: AllocVec<T, A>,
    copied: &mut u128,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<Buffer, ValueFailure<E>> {
    let (buffer, bytes) = arrow_raw::into_arrow_buffer_observed(values, growth, work)?;
    *copied += bytes as u128;
    Ok(buffer)
}

struct RowMark {
    payload: usize,
    region_count: usize,
    current: Option<usize>,
    current_len: Option<usize>,
    copied_bytes: u128,
}

enum Pending {
    Empty,
    Null,
    Owned {
        region: Option<usize>,
        start: usize,
        len: usize,
    },
    Shared {
        buffer: Buffer,
        offset: usize,
        len: usize,
    },
}
impl Pending {
    fn len(&self) -> usize {
        match self {
            Self::Owned { len, .. } | Self::Shared { len, .. } => *len,
            _ => 0,
        }
    }
}

/// An uncommitted row. Dropping it is an abort. A failed operation aborts
/// immediately and closes this writer so subsequent calls cannot publish it.
pub struct RowWriter<'a, A: Allocator + Clone + 'static = Global> {
    builder: &'a mut ValueBuilder<A>,
    mark: RowMark,
    data: Pending,
    done: bool,
}

impl<A: Allocator + Clone + 'static> Drop for RowWriter<'_, A> {
    fn drop(&mut self) {
        self.rollback();
    }
}

impl<A: Allocator + Clone + 'static> RowWriter<'_, A> {
    fn rollback(&mut self) {
        if self.done {
            return;
        }
        self.builder.payload.truncate(self.mark.payload);
        self.builder.regions.truncate(self.mark.region_count);
        self.builder.current = self.mark.current;
        if let (Some(index), Some(len)) = (self.mark.current, self.mark.current_len) {
            match &mut self.builder.regions[index] {
                Region::Owned(bytes) => bytes.truncate(len),
                Region::Shared(_) => unreachable!("current block is owned"),
            }
        }
        self.builder.copied_bytes = self.mark.copied_bytes;
        self.data = Pending::Empty;
        self.done = true;
    }

    pub fn abort(mut self) {
        self.rollback();
    }

    fn checked<E>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<(), WriteFailure<E>>,
    ) -> Result<(), WriteFailure<E>> {
        if self.done {
            return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
        }
        let result = operation(self);
        if result.is_err() {
            self.rollback();
        }
        result
    }

    pub fn write_str<E>(
        &mut self,
        value: &str,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if !this.builder.layout.is_utf8() {
                return Err(ValueError::Invariant(ValueViolation::Layout).into());
            }
            this.write_admitted(value.as_bytes(), growth, work)
        })
    }

    pub fn write_bytes<E>(
        &mut self,
        value: &[u8],
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if this.builder.layout.is_utf8() {
                return Err(ValueError::Invariant(ValueViolation::Layout).into());
            }
            this.write_admitted(value, growth, work)
        })
    }

    pub fn write_array_value<E>(
        &mut self,
        source: &dyn Array,
        index: usize,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            let source_layout = ByteLayout::from_data_type(source.data_type())
                .ok_or(ValueError::Invariant(ValueViolation::Carrier))?;
            if source_layout.is_utf8() != this.builder.layout.is_utf8() {
                return Err(ValueError::Invariant(ValueViolation::Layout).into());
            }
            match arrow_raw::byte_span(source, index)? {
                None => {
                    if !matches!(this.data, Pending::Empty) {
                        return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
                    }
                    this.data = Pending::Null;
                    Ok(())
                }
                Some(ByteSpan::Inline(bytes)) => this.write_admitted(bytes, growth, work),
                Some(ByteSpan::Region {
                    buffer,
                    offset,
                    len,
                }) => this.adopt_admitted(buffer, offset..offset + len, growth, work),
            }
        })
    }

    pub fn write_binary_region<E>(
        &mut self,
        buffer: &Buffer,
        range: Range<usize>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if this.builder.layout.is_utf8() {
                return Err(ValueError::Invariant(ValueViolation::Layout).into());
            }
            this.adopt_admitted(buffer, range, growth, work)
        })
    }

    /// The str's pointer and length must describe exactly the selected region.
    /// That proof avoids rescanning a previously validated string's bytes.
    pub fn write_string_region<E>(
        &mut self,
        buffer: &Buffer,
        range: Range<usize>,
        value: &str,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if !this.builder.layout.is_utf8() || range.start > range.end || range.end > buffer.len()
            {
                return Err(ValueError::Invariant(ValueViolation::Bounds).into());
            }
            let bytes = &buffer.as_slice()[range.clone()];
            if bytes.as_ptr() != value.as_ptr() || bytes.len() != value.len() {
                return Err(ValueError::Invariant(ValueViolation::Utf8Boundary).into());
            }
            this.adopt_admitted(buffer, range, growth, work)
        })
    }

    fn adopt_admitted<E>(
        &mut self,
        buffer: &Buffer,
        range: Range<usize>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        if range.start > range.end || range.end > buffer.len() {
            return Err(ValueError::Invariant(ValueViolation::Bounds).into());
        }
        if self.builder.layout.is_view() && matches!(self.data, Pending::Empty) && range.len() > 12
        {
            check_value_extent(
                self.builder.layout,
                0,
                range.len() as u64,
                self.builder.rows,
            )?;
            work(1).map_err(ValueFailure::Caller)?;
            self.data = Pending::Shared {
                buffer: buffer.clone(),
                offset: range.start,
                len: range.len(),
            };
            Ok(())
        } else {
            self.write_admitted(&buffer.as_slice()[range], growth, work)
        }
    }

    fn write_admitted<E>(
        &mut self,
        value: &[u8],
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        if matches!(self.data, Pending::Null) {
            return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
        }
        let old_len = self.data.len();
        let total = old_len
            .checked_add(value.len())
            .ok_or_else(|| allocation_extent(self.builder.rows, None))?;
        check_value_extent(
            self.builder.layout,
            self.mark.payload as u64,
            total as u64,
            self.builder.rows,
        )?;
        // A planned conversion can keep all copied inline bytes in one region:
        // inline descriptors have no offset field. If a general fragment writer
        // later extends such a draft into an external value, move its at-most
        // twelve bytes into a fresh addressable block before appending.
        if let Pending::Owned {
            region: Some(index),
            start,
            len,
        } = &self.data
            && *start > i32::MAX as usize
            && total > 12
        {
            let mut inline = [0u8; 12];
            let Region::Owned(bytes) = &self.builder.regions[*index] else {
                unreachable!()
            };
            inline[..*len].copy_from_slice(&bytes[*start..*start + *len]);
            let len = *len;
            self.data = Pending::Empty;
            self.builder.current = None;
            if len != 0 {
                self.write_admitted(&inline[..len], growth, work)?;
            }
        }
        if matches!(self.data, Pending::Shared { .. }) {
            let previous = std::mem::replace(&mut self.data, Pending::Empty);
            if let Pending::Shared {
                buffer,
                offset,
                len,
            } = previous
            {
                self.write_admitted(&buffer.as_slice()[offset..offset + len], growth, work)?;
            }
        }
        if matches!(self.data, Pending::Empty) {
            if self.builder.layout.is_view() {
                let planned = self.builder.planned_payload_region;
                let reusable = self
                    .builder
                    .current
                    .filter(|&i| match &self.builder.regions[i] {
                        Region::Owned(bytes) => {
                            (planned == Some(i) && total <= 12)
                                || (bytes.len() < BLOCK_TARGET && bytes.len() <= i32::MAX as usize)
                        }
                        Region::Shared(_) => false,
                    });
                let index = if let Some(index) = reusable {
                    index
                } else {
                    if self.builder.regions.len() > i32::MAX as usize {
                        return Err(WriteFailure::Full);
                    }
                    reserve(&mut self.builder.regions, 1, self.builder.rows, growth)?;
                    let index = self.builder.regions.len();
                    self.builder.regions.push(Region::Owned(AllocVec::new_in(
                        self.builder.allocator.clone(),
                    )));
                    self.builder.current = Some(index);
                    index
                };
                let start = match &self.builder.regions[index] {
                    Region::Owned(bytes) => bytes.len(),
                    _ => unreachable!(),
                };
                self.data = Pending::Owned {
                    region: Some(index),
                    start,
                    len: 0,
                };
            } else {
                self.data = Pending::Owned {
                    region: None,
                    start: self.mark.payload,
                    len: 0,
                };
            }
        }
        let Pending::Owned { region, len, .. } = &mut self.data else {
            unreachable!()
        };
        let bytes = match region {
            Some(index) => match &mut self.builder.regions[*index] {
                Region::Owned(bytes) => bytes,
                _ => unreachable!(),
            },
            None => &mut self.builder.payload,
        };
        reserve(bytes, value.len(), self.builder.rows, growth)?;
        for part in value.chunks(COPY_CHECKPOINT) {
            work(part.len()).map_err(ValueFailure::Caller)?;
            bytes.extend_from_slice(part);
        }
        *len += value.len();
        self.builder.copied_bytes = add_byte_volume(self.builder.copied_bytes, value.len());
        Ok(())
    }

    pub fn commit_null<E>(
        mut self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| {
            if !matches!(this.data, Pending::Empty | Pending::Null) {
                return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
            }
            this.data = Pending::Null;
            this.commit_inner(growth, work)
        })
    }

    pub fn commit<E>(
        mut self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        self.checked(|this| this.commit_inner(growth, work))
    }

    fn commit_inner<E>(
        &mut self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), WriteFailure<E>> {
        let rows = self.builder.rows;
        let next_rows = rows
            .checked_add(1)
            .ok_or_else(|| allocation_extent(rows, None))?;
        let valid = !matches!(self.data, Pending::Null);
        let len = self.data.len();
        let next_logical = add_byte_volume(self.builder.logical_bytes, len);
        work(1).map_err(ValueFailure::Caller)?;
        if rows.is_multiple_of(8) {
            reserve(&mut self.builder.validity, 1, rows, growth)?;
        }
        if self.builder.layout.is_view() {
            reserve(&mut self.builder.views, 1, rows, growth)?;
        } else if matches!(self.builder.layout, ByteLayout::Utf8 | ByteLayout::Binary) {
            reserve(
                &mut self.builder.offsets32,
                if rows == 0 { 2 } else { 1 },
                rows,
                growth,
            )?;
        } else {
            reserve(
                &mut self.builder.offsets64,
                if rows == 0 { 2 } else { 1 },
                rows,
                growth,
            )?;
        }
        let descriptor = if self.builder.layout.is_view() {
            match &self.data {
                Pending::Empty | Pending::Null => arrow_raw::inline_view(&[])?,
                Pending::Owned {
                    region: Some(index),
                    start,
                    len,
                } => {
                    let Region::Owned(bytes) = &self.builder.regions[*index] else {
                        unreachable!()
                    };
                    arrow_raw::external_view(
                        self.builder.layout,
                        &bytes[*start..*start + *len],
                        *index,
                        *start,
                    )?
                }
                Pending::Shared {
                    buffer,
                    offset,
                    len,
                } => {
                    // Match only the same accessible region. A shared allocation
                    // with a different slice is registered separately.
                    let same = self.builder.regions.iter().position(|region| matches!(region, Region::Shared(other)
                        if other.data_ptr() == buffer.data_ptr() && other.ptr_offset() == buffer.ptr_offset() && other.len() == buffer.len()));
                    let (index, offset) =
                        if let Some(index) = same.filter(|_| *offset <= i32::MAX as usize) {
                            (index, *offset)
                        } else {
                            if self.builder.regions.len() > i32::MAX as usize {
                                return Err(WriteFailure::Full);
                            }
                            reserve(&mut self.builder.regions, 1, rows, growth)?;
                            let index = self.builder.regions.len();
                            let (region, relative) = if *offset > i32::MAX as usize {
                                (buffer.slice_with_length(*offset, *len), 0)
                            } else {
                                (buffer.clone(), *offset)
                            };
                            self.builder.regions.push(Region::Shared(region));
                            (index, relative)
                        };
                    arrow_raw::external_view(
                        self.builder.layout,
                        &buffer.as_slice()[*self.data_offset()..*self.data_offset() + *len],
                        index,
                        offset,
                    )?
                }
                Pending::Owned { region: None, .. } => unreachable!(),
            }
        } else {
            0
        };
        // All fallible work is complete. Publish the row and bitmap together.
        if rows.is_multiple_of(8) {
            self.builder.validity.push(0);
        }
        if valid {
            self.builder.validity[rows / 8] |= 1 << (rows % 8);
        } else {
            self.builder.null_count += 1;
        }
        if self.builder.layout.is_view() {
            self.builder.views.push(descriptor);
        } else if matches!(self.builder.layout, ByteLayout::Utf8 | ByteLayout::Binary) {
            if rows == 0 {
                self.builder.offsets32.push(0);
            }
            self.builder
                .offsets32
                .push(self.builder.payload.len() as i32);
        } else {
            if rows == 0 {
                self.builder.offsets64.push(0);
            }
            self.builder
                .offsets64
                .push(self.builder.payload.len() as i64);
        }
        self.builder.rows = next_rows;
        self.builder.logical_bytes = next_logical;
        self.done = true;
        Ok(())
    }

    fn data_offset(&self) -> &usize {
        match &self.data {
            Pending::Shared { offset, .. } => offset,
            _ => unreachable!(),
        }
    }
}

/// A committed row has at most i64::MAX bytes, and there are at most usize::MAX
/// rows. Even on a 64-bit host, their product fits u128. A pending row is checked
/// against its layout first. Relocating an inline draft can add at most twelve
/// copied bytes per row; that larger product still fits u128. Statistics
/// therefore add no input rejection.
fn add_byte_volume(volume: u128, bytes: usize) -> u128 {
    volume + bytes as u128
}

struct PlannedCounts {
    offsets: usize,
    validity: usize,
    regions: usize,
}

fn planned_counts(
    layout: ByteLayout,
    rows: usize,
    payload_bytes: usize,
    adopted_region_bound: usize,
) -> Result<PlannedCounts, ValueError> {
    if rows == 0 {
        if payload_bytes != 0 {
            return Err(ValueError::Invariant(ValueViolation::Bounds));
        }
        return Ok(PlannedCounts {
            offsets: usize::from(!layout.is_view()),
            validity: 0,
            regions: 0,
        });
    }
    if !layout.is_view() && payload_bytes as u128 > layout.extent_limit() as u128 {
        return Err(ValueError::Representation(layout.representation(
            if rows == 1 {
                RepresentationBoundary::MinimumUnit
            } else {
                RepresentationBoundary::SingleOutput
            },
            u64::try_from(payload_bytes).ok(),
            Some(layout.extent_limit()),
            None,
        )));
    }
    let overflow = || planned_extent(None);
    Ok(PlannedCounts {
        offsets: if layout.is_view() {
            0
        } else {
            rows.checked_add(1).ok_or_else(overflow)?
        },
        // Dividing first avoids overflowing rows + 7 for a valid usize count.
        validity: rows / 8 + usize::from(!rows.is_multiple_of(8)),
        regions: if layout.is_view() {
            adopted_region_bound.checked_add(1).ok_or_else(overflow)?
        } else {
            0
        },
    })
}

fn planned_layout<T>(count: usize) -> Result<Layout, ValueError> {
    Layout::array::<T>(count).map_err(|_| {
        planned_extent(
            count
                .checked_mul(std::mem::size_of::<T>())
                .and_then(|n| u64::try_from(n).ok()),
        )
    })
}

fn planned_extent(required: Option<u64>) -> ValueError {
    ValueError::Representation(field_representation(
        11,
        RepresentationBoundary::SingleOutput,
        required,
        Some(isize::MAX as u64),
        None,
    ))
}

fn reserve_initial<T, A: Allocator, E>(
    values: &mut AllocVec<T, A>,
    count: usize,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<(), ValueFailure<E>> {
    if count == 0 {
        return Ok(());
    }
    debug_assert_eq!(values.capacity(), 0);
    let layout = planned_layout::<T>(count)?;
    work(1).map_err(ValueFailure::Caller)?;
    growth(layout).map_err(ValueFailure::Caller)?;
    values
        .try_reserve_exact(count)
        .map_err(|source| ValueError::Allocator {
            requested: layout.size(),
            source,
        })?;
    Ok(())
}

/// Pure arithmetic, also used by planners to test real limits without allocating
/// gigabytes. For offsets, used_payload is the committed prefix's payload.
pub fn check_value_extent<E>(
    layout: ByteLayout,
    used_payload: u64,
    value_bytes: u64,
    committed_rows: usize,
) -> Result<(), WriteFailure<E>> {
    let limit = layout.extent_limit();
    if value_bytes > limit {
        return Err(ValueError::Representation(layout.representation(
            RepresentationBoundary::MinimumUnit,
            Some(value_bytes),
            Some(limit),
            None,
        ))
        .into());
    }
    if !layout.is_view()
        && used_payload
            .checked_add(value_bytes)
            .is_none_or(|total| total > limit)
    {
        return Err(WriteFailure::Full);
    }
    let _ = committed_rows;
    Ok(())
}

fn allocation_extent(rows: usize, required: Option<u64>) -> ValueError {
    ValueError::Representation(field_representation(
        11,
        RepresentationBoundary::SingleOutput,
        required,
        Some(isize::MAX as u64),
        u64::try_from(rows).ok(),
    ))
}

fn reserve<T, A: Allocator, E>(
    values: &mut AllocVec<T, A>,
    additional: usize,
    rows: usize,
    growth: &mut Growth<'_, E>,
) -> Result<(), WriteFailure<E>> {
    let overflow = |required| {
        if rows > 0 {
            WriteFailure::Full
        } else {
            ValueError::Representation(field_representation(
                11,
                RepresentationBoundary::MinimumUnit,
                required,
                Some(isize::MAX as u64),
                None,
            ))
            .into()
        }
    };
    let needed = values
        .len()
        .checked_add(additional)
        .ok_or_else(|| overflow(None))?;
    if needed <= values.capacity() {
        return Ok(());
    }
    // Doubling affects efficiency only. Near the real Layout limit, fall back
    // to the exact required extent rather than rejecting a representable row.
    let preferred = values
        .capacity()
        .checked_mul(2)
        .unwrap_or(needed)
        .max(needed);
    let (capacity, layout) = match Layout::array::<T>(preferred) {
        Ok(layout) => (preferred, layout),
        Err(_) => (
            needed,
            Layout::array::<T>(needed).map_err(|_| {
                overflow(
                    needed
                        .checked_mul(std::mem::size_of::<T>())
                        .and_then(|bytes| u64::try_from(bytes).ok()),
                )
            })?,
        ),
    };
    growth(layout).map_err(ValueFailure::Caller)?;
    values
        .try_reserve_exact(capacity - values.len())
        .map_err(|source| ValueError::Allocator {
            requested: layout.size(),
            source,
        })?;
    Ok(())
}

#[cfg(test)]
mod tests;
