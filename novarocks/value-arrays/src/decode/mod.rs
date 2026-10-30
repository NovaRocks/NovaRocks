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

//! A single computation representation: an Arrow view bottom, an explicit row
//! mapping, and separately checked input and bottom coverage. The context is
//! owned by the actual serial caller; its budget only changes reuse decisions.
use crate::{
    ByteLayout, ValueError, ValueFailure, ValueViolation,
    access::{ByteRegion, ByteSource},
    arrow_raw,
    control::{Growth, StructureObserver, ValueStructure, Work, shared_owner},
    reachability::{ReachableRows, SelectionSet},
};
use arrow_array::{Array, ArrayRef, Int32Array};
use arrow_buffer::Buffer;
use std::{alloc::Layout, convert::Infallible, sync::Arc};
fn violation(v: ValueViolation) -> ValueError {
    ValueError::Invariant(v)
}
fn reserve<T, E>(
    values: &mut Vec<T>,
    count: usize,
    growth: &mut Growth<'_, E>,
) -> Result<(), ValueFailure<E>> {
    if count <= values.capacity() {
        return Ok(());
    }
    let layout = Layout::array::<T>(count).map_err(|_| violation(ValueViolation::Bounds))?;
    growth(layout).map_err(ValueFailure::Caller)?;
    values
        .try_reserve_exact(count - values.len())
        .map_err(|source| ValueError::Allocation {
            requested: layout.size(),
            source,
        })?;
    Ok(())
}

/// A pre-reserved vector is initialized or copied in bounded synchronous
/// turns. No continuation or byte position escapes the current operation.
fn resize_observed<T: Clone, E>(
    values: &mut Vec<T>,
    count: usize,
    value: T,
    work: &mut Work<'_, E>,
) -> Result<(), ValueFailure<E>> {
    let per_turn = (64 * 1024 / std::mem::size_of::<T>().max(1)).max(1);
    while values.len() < count {
        let chunk = (count - values.len()).min(per_turn);
        work(chunk.saturating_mul(std::mem::size_of::<T>())).map_err(ValueFailure::Caller)?;
        values.resize(values.len() + chunk, value.clone());
    }
    Ok(())
}
fn extend_observed<T: Clone, E>(
    values: &mut Vec<T>,
    input: &[T],
    work: &mut Work<'_, E>,
) -> Result<(), ValueFailure<E>> {
    let per_turn = (64 * 1024 / std::mem::size_of::<T>().max(1)).max(1);
    for chunk in input.chunks(per_turn) {
        work(chunk.len().saturating_mul(std::mem::size_of::<T>())).map_err(ValueFailure::Caller)?;
        values.extend_from_slice(chunk);
    }
    Ok(())
}

/// Mapping validity is distinct from bottom validity. A valid key pointing at
/// NULL remains a valid key, and a NULL key's physical integer is never read.
#[derive(Clone, Debug)]
pub enum RowMapping {
    Identity { len: usize },
    Constant { len: usize, index: usize },
    Dictionary(Int32Array),
}
impl RowMapping {
    pub fn len(&self) -> usize {
        match self {
            Self::Identity { len } | Self::Constant { len, .. } => *len,
            Self::Dictionary(k) => k.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn mapped_row(&self, row: usize) -> Result<Option<usize>, ValueError> {
        if row >= self.len() {
            return Err(violation(ValueViolation::Bounds));
        }
        Ok(match self {
            Self::Identity { .. } => Some(row),
            Self::Constant { index, .. } => Some(*index),
            Self::Dictionary(keys) => {
                if keys.is_null(row) {
                    None
                } else {
                    Some(
                        usize::try_from(keys.value(row))
                            .map_err(|_| violation(ValueViolation::Bounds))?,
                    )
                }
            }
        })
    }
    pub fn dictionary_keys(&self) -> Option<&Int32Array> {
        if let Self::Dictionary(keys) = self {
            Some(keys)
        } else {
            None
        }
    }
}
#[derive(Debug)]
struct DecodedBottom {
    array: ArrayRef,
    covered: SelectionSet,
    owned_descriptor_bytes: usize,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
/// A decoded result owns Arrow aliases and never borrows a mutable cache. This
/// remains valid if the context is cleared, replaced or moved to another thread.
#[derive(Clone, Debug)]
pub struct DecodedBytes {
    bottom: Arc<DecodedBottom>,
    mapping: RowMapping,
    readable: Arc<SelectionSet>,
    dictionary: bool,
    flattened: bool,
    utf8: bool,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl DecodedBytes {
    pub fn len(&self) -> usize {
        self.mapping.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn is_utf8(&self) -> bool {
        self.utf8
    }
    pub fn is_flattened(&self) -> bool {
        self.flattened
    }
    pub fn mapping(&self) -> &RowMapping {
        &self.mapping
    }
    /// The bottom array may include placeholders. Its coverage must accompany
    /// it whenever a consumer reads original bottom coordinates.
    pub fn bottom(&self) -> &ArrayRef {
        &self.bottom.array
    }
    pub fn bottom_coverage(&self) -> &SelectionSet {
        &self.bottom.covered
    }
    pub fn readable_rows(&self) -> &SelectionSet {
        &self.readable
    }
    fn checked_mapping(&self, row: usize) -> Result<Option<usize>, ValueError> {
        if !self.readable.contains(row) {
            return Err(violation(ValueViolation::UndefinedRow));
        }
        let mapped = self.mapping.mapped_row(row)?;
        if mapped.is_some_and(|r| !self.bottom.covered.contains(r)) {
            return Err(violation(ValueViolation::UndefinedRow));
        }
        Ok(mapped)
    }
    pub fn value(&self, row: usize) -> Result<Option<&[u8]>, ValueError> {
        let Some(row) = self.checked_mapping(row)? else {
            return Ok(None);
        };
        Ok(arrow_raw::byte_span(self.bottom.array.as_ref(), row)?.map(|s| s.bytes()))
    }
    /// UTF8 was established by the admitted source, not by a repeated parser.
    pub fn string(&self, row: usize) -> Result<Option<&str>, ValueError> {
        if !self.utf8 {
            return Err(violation(ValueViolation::Layout));
        }
        // SAFETY: utf8 is set only from an admitted STRING source; conversion
        // copies checked descriptors or borrows that source without changing
        // byte ranges. Mapping and coverage are checked by value().
        Ok(self
            .value(row)?
            .map(|bytes| unsafe { std::str::from_utf8_unchecked(bytes) }))
    }
    pub fn value_len(&self, row: usize) -> Result<Option<usize>, ValueError> {
        let Some(row) = self.checked_mapping(row)? else {
            return Ok(None);
        };
        if self.bottom.array.is_null(row) {
            return Ok(None);
        };
        Ok(Some(
            arrow_raw::view_inline_prefix(self.bottom.array.as_ref(), row)?.0,
        ))
    }
    /// At most four prefix bytes, read from the descriptor even for an external
    /// value. Bytes beyond the value's length are not part of this prefix.
    pub fn prefix(&self, row: usize) -> Result<Option<([u8; 4], usize)>, ValueError> {
        let Some(row) = self.checked_mapping(row)? else {
            return Ok(None);
        };
        if self.bottom.array.is_null(row) {
            return Ok(None);
        };
        let (len, prefix, _) = arrow_raw::view_inline_prefix(self.bottom.array.as_ref(), row)?;
        Ok(Some((prefix, len.min(4))))
    }
    pub fn inline_bytes(&self, row: usize) -> Result<Option<&[u8]>, ValueError> {
        let Some(row) = self.checked_mapping(row)? else {
            return Ok(None);
        };
        if self.bottom.array.is_null(row) {
            return Ok(None);
        };
        Ok(arrow_raw::view_inline_prefix(self.bottom.array.as_ref(), row)?.2)
    }
    pub fn region(&self, row: usize) -> Result<Option<ByteRegion<'_>>, ValueError> {
        let Some(row) = self.checked_mapping(row)? else {
            return Ok(None);
        };
        Ok(
            match arrow_raw::byte_span(self.bottom.array.as_ref(), row)? {
                Some(arrow_raw::ByteSpan::Region {
                    buffer,
                    offset,
                    len,
                }) => Some(ByteRegion {
                    buffer,
                    offset,
                    len,
                }),
                _ => None,
            },
        )
    }
    /// Character semantics are chosen by the consumer. For STRING, the caller's
    /// byte boundaries must already be UTF8 character boundaries.
    pub fn substring(
        &self,
        row: usize,
        range: std::ops::Range<usize>,
    ) -> Result<Option<&[u8]>, ValueError> {
        let Some(bytes) = self.value(row)? else {
            return Ok(None);
        };
        if range.start > range.end || range.end > bytes.len() {
            return Err(violation(ValueViolation::Bounds));
        }
        if self.utf8 {
            // SAFETY: utf8 records the admitted STRING source's guarantee,
            // preserved by checked view conversion and value() coverage.
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            if !text.is_char_boundary(range.start) || !text.is_char_boundary(range.end) {
                return Err(violation(ValueViolation::Utf8Boundary));
            }
        }
        Ok(Some(&bytes[range]))
    }
    /// Borrow a substring together with the exact adopting region. Inline
    /// bytes remain inline; external bytes retain their original Arrow owner.
    pub fn substring_region(
        &self,
        row: usize,
        range: std::ops::Range<usize>,
    ) -> Result<Option<ByteRegion<'_>>, ValueError> {
        if self.substring(row, range.clone())?.is_none() {
            return Ok(None);
        };
        Ok(self.region(row)?.map(|region| ByteRegion {
            buffer: region.buffer,
            offset: region.offset + range.start,
            len: range.end - range.start,
        }))
    }
    pub fn peel(&self, selection: &SelectionSet) -> Result<Option<PeelInput>, ValueError> {
        self.peel_observed(selection, &mut |_| Ok::<_, Infallible>(()), &mut |_| {
            Ok::<_, Infallible>(())
        })
        .map_err(ValueFailure::into_error)
    }
    pub fn peel_observed<E>(
        &self,
        selection: &SelectionSet,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<Option<PeelInput>, ValueFailure<E>> {
        if !selection.is_subset(&self.readable) {
            return Err(violation(ValueViolation::UndefinedRow).into());
        }
        if !self.dictionary || self.flattened {
            return Ok(None);
        };
        let mut required =
            SelectionSet::try_new_with_control_observed(self.bottom.array.len(), growth, work)?;
        for row in selection.iter() {
            work(1).map_err(ValueFailure::Caller)?;
            if let Some(bottom) = self.checked_mapping(row)? {
                required.insert(bottom)?;
            }
        }
        Ok(Some(PeelInput {
            input: self.clone(),
            selected: Arc::new(selection.try_clone_with_control_observed(growth, work)?),
            required,
        }))
    }
}
/// An original-coordinate peel request. Selection membership does not discard
/// mapping order, repeated keys or the keys' own validity bits.
#[derive(Debug)]
pub struct PeelInput {
    input: DecodedBytes,
    selected: Arc<SelectionSet>,
    required: SelectionSet,
}
impl PeelInput {
    pub fn required_rows(&self) -> &SelectionSet {
        &self.required
    }
    pub fn bottom(&self) -> &ArrayRef {
        self.input.bottom()
    }
    pub fn mapping(&self) -> &RowMapping {
        self.input.mapping()
    }
    pub fn selected_rows(&self) -> &SelectionSet {
        &self.selected
    }
    pub fn rewrap(&self, result: &PeelResult) -> Result<ArrayRef, ValueError> {
        self.rewrap_observed(result, &mut |_| Ok::<_, Infallible>(()), &mut |_| {
            Ok::<_, Infallible>(())
        })
        .map_err(ValueFailure::into_error)
    }
    pub fn rewrap_observed<E>(
        &self,
        result: &PeelResult,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<ArrayRef, ValueFailure<E>> {
        if result.values.len() != self.input.bottom.array.len()
            || !self.required.is_subset(&result.defined_rows)
        {
            return Err(violation(ValueViolation::UndefinedRow).into());
        }
        let RowMapping::Dictionary(keys) = &self.input.mapping else {
            return Err(violation(ValueViolation::Dictionary).into());
        };
        // Unselected rows are not published as meaningful results. Keys keep
        // their own validity and do not inherit the old bottom's NULL mask.
        let mut validity = Vec::<u8>::new();
        let bytes = keys.len().div_ceil(8);
        reserve(&mut validity, bytes, growth)?;
        resize_observed(&mut validity, bytes, 0, work)?;
        let mut valid = 0;
        for row in self.selected.iter() {
            work(1).map_err(ValueFailure::Caller)?;
            if keys.is_valid(row) {
                validity[row / 8] |= 1 << (row % 8);
                valid += 1;
            }
        }
        let nulls =
            arrow_raw::make_nulls(Buffer::from_vec(validity), keys.len(), keys.len() - valid);
        Ok(arrow_raw::rewrap_dictionary(
            keys,
            result.values.clone(),
            nulls,
        )?)
    }
}
/// NULL is a computed value when its row belongs to defined_rows. Placeholder
/// values outside that set cannot satisfy a later peel request.
#[derive(Debug)]
pub struct PeelResult {
    values: ArrayRef,
    defined_rows: SelectionSet,
}
impl PeelResult {
    pub fn new(
        values: ArrayRef,
        defined_rows: SelectionSet,
        output_layout: ByteLayout,
        dictionary_values: Option<ByteLayout>,
    ) -> Result<Self, ValueError> {
        let expected = dictionary_values
            .map(ByteLayout::data_type)
            .or_else(|| output_layout.dictionary_value_layout())
            .ok_or_else(|| violation(ValueViolation::Dictionary))?;
        if values.data_type() != &expected {
            return Err(violation(ValueViolation::Layout));
        }
        if values.len() != defined_rows.len() {
            return Err(violation(ValueViolation::Bounds));
        }
        Ok(Self {
            values,
            defined_rows,
        })
    }
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }
    pub fn defined_rows(&self) -> &SelectionSet {
        &self.defined_rows
    }
}

/// A frozen computation view prepared with the real caller-owned context.
/// Descriptor conversion has already acquired storage through the planning
/// Growth callback. Execution publishes only an Arrow alias of this snapshot;
/// it does not allocate another descriptor bottom or choose another strategy.
#[derive(Debug)]
pub struct DecodePlan {
    _source: ArrayRef,
    decoded: DecodedBytes,
    held_storage_bytes: usize,
    new_storage_bytes: usize,
    shared_cached_storage_bytes: usize,
    planning_growth_requested_bytes: u128,
}
impl DecodePlan {
    /// Existing and newly created descriptor/coverage storage held by this
    /// plan. This is retention information, not a future allocation request.
    /// Arrow wrapper/Arc metadata remains at its public R4 boundary.
    pub fn held_plan_storage_bytes(&self) -> usize {
        self.held_storage_bytes
    }
    /// Actual owned descriptor/coverage capacities first created in this plan.
    /// This is already held and must not be charged again during execution.
    pub fn new_plan_storage_bytes(&self) -> usize {
        self.new_storage_bytes
    }
    /// Cached bottom storage merely aliased by planning, not newly allocated.
    pub fn shared_cached_storage_bytes(&self) -> usize {
        self.shared_cached_storage_bytes
    }
    /// Sum of successful planning Growth request sizes, including transient
    /// scratch and replacements. This is neither peak retention nor a full
    /// physical-memory estimate; new requests were admitted when they occurred.
    pub fn planning_growth_requested_bytes(&self) -> u128 {
        self.planning_growth_requested_bytes
    }
    /// Conversion already acquired its self-owned storage during planning.
    pub fn output_layouts(&self) -> &[Layout] {
        &[]
    }
    pub fn execution_scratch_layouts(&self) -> &[Layout] {
        &[]
    }
    pub fn decoded(&self) -> &DecodedBytes {
        &self.decoded
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConversionStats {
    pub cache_hits: u64,
    pub converted_values: u64,
    pub cache_extensions: u64,
    pub cache_skips: u64,
    pub cache_evictions: u64,
    pub descriptors_created: u64,
}
/// These are separate observations, not an extra physical-retention ledger.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConversionStorageFacts {
    pub owned_descriptor_bytes: u128,
    pub declared_backing_bytes: u128,
    pub known_owner_capacity: Option<usize>,
}
#[derive(Debug)]
struct CacheEntry {
    identity: [usize; 12],
    _source: ArrayRef,
    bottom: Arc<DecodedBottom>,
    charge: usize,
    declared_backing_bytes: usize,
    last_use: u64,
}
/// A Send, serial context. The budget only controls efficiency; it never
/// authorizes or rejects logical input and is not a memory-capacity authority.
#[derive(Debug)]
pub struct ValueConversionContext {
    budget: usize,
    entries: Vec<CacheEntry>,
    stats: ConversionStats,
    clock: u64,
}
impl Default for ValueConversionContext {
    fn default() -> Self {
        Self::new(8 * 1024 * 1024)
    }
}
impl ValueConversionContext {
    pub fn new(efficiency_budget: usize) -> Self {
        Self {
            budget: efficiency_budget,
            entries: Vec::new(),
            stats: ConversionStats::default(),
            clock: 0,
        }
    }
    pub fn clear(&mut self) {
        self.stats.cache_evictions = self
            .stats
            .cache_evictions
            .saturating_add(self.entries.len() as u64);
        self.entries.clear();
    }
    pub fn stats(&self) -> ConversionStats {
        self.stats
    }
    pub fn storage_facts(&self) -> ConversionStorageFacts {
        ConversionStorageFacts {
            owned_descriptor_bytes: self
                .entries
                .iter()
                .map(|e| e.bottom.owned_descriptor_bytes as u128)
                .sum(),
            declared_backing_bytes: self
                .entries
                .iter()
                .map(|e| e.declared_backing_bytes as u128)
                .sum(),
            known_owner_capacity: None,
        }
    }
    /// Prepare using the context's actual reuse state and efficiency strategy.
    /// Only the supplied reachable logical rows are converted. Filling a
    /// sparse bottom may create legal placeholders at unreferenced coordinates,
    /// but never reads those values or imposes their representation limits.
    pub fn plan_decode_observed<E>(
        &mut self,
        source: &ByteSource<'_>,
        rows: &ReachableRows<'_>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<DecodePlan, ValueFailure<E>> {
        self.plan_decode_structured_observed(source, rows, growth, work, &mut |_| Ok(()))
    }
    pub fn plan_decode_structured_observed<E>(
        &mut self,
        source: &ByteSource<'_>,
        rows: &ReachableRows<'_>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut StructureObserver<'_, E>,
    ) -> Result<DecodePlan, ValueFailure<E>> {
        let previous = if source.is_dictionary() && !source.value_layout().is_view() {
            let identity = arrow_raw::byte_storage_identity(source.values().as_ref())?;
            self.entries
                .iter()
                .find(|entry| entry.identity == identity)
                .map(|entry| entry.bottom.clone())
        } else {
            None
        };
        let mut planning_growth_requested_bytes = 0u128;
        let decoded = self.decode_structured_observed(
            source,
            rows,
            &mut |layout| {
                growth(layout)?;
                planning_growth_requested_bytes =
                    planning_growth_requested_bytes.saturating_add(layout.size() as u128);
                Ok(())
            },
            work,
            observe,
        )?;
        let shared_cached_storage_bytes = previous
            .filter(|bottom| Arc::ptr_eq(bottom, &decoded.bottom))
            .map_or(0, |bottom| bottom.owned_descriptor_bytes);
        let held_storage_bytes = decoded
            .bottom
            .owned_descriptor_bytes
            .checked_add(decoded.readable.descriptor_bytes())
            .ok_or_else(|| violation(ValueViolation::Bounds))?;
        let new_storage_bytes = held_storage_bytes - shared_cached_storage_bytes;
        Ok(DecodePlan {
            _source: source.input().clone(),
            decoded,
            held_storage_bytes,
            new_storage_bytes,
            shared_cached_storage_bytes,
            planning_growth_requested_bytes,
        })
    }
    /// Publication performs no self-owned value allocation. A plan remains
    /// valid after cache eviction, clear, replacement, or a context handoff.
    pub fn decode_planned(&mut self, plan: &DecodePlan) -> DecodedBytes {
        plan.decoded.clone()
    }
    pub fn decode(
        &mut self,
        source: &ByteSource<'_>,
        rows: &ReachableRows<'_>,
    ) -> Result<DecodedBytes, ValueError> {
        self.decode_observed(source, rows, &mut |_| Ok::<_, Infallible>(()), &mut |_| {
            Ok::<_, Infallible>(())
        })
        .map_err(ValueFailure::into_error)
    }
    pub fn decode_observed<E>(
        &mut self,
        source: &ByteSource<'_>,
        rows: &ReachableRows<'_>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<DecodedBytes, ValueFailure<E>> {
        self.decode_structured_observed(source, rows, growth, work, &mut |_| Ok(()))
    }
    pub fn decode_structured_observed<E>(
        &mut self,
        source: &ByteSource<'_>,
        rows: &ReachableRows<'_>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut StructureObserver<'_, E>,
    ) -> Result<DecodedBytes, ValueFailure<E>> {
        if rows.len() != source.len() {
            return Err(violation(ValueViolation::Bounds).into());
        }
        work(0).map_err(ValueFailure::Caller)?;
        let readable_rows = rows.materialize_observed(growth, work)?;
        shared_owner::<SelectionSet, E>(observe)?;
        let readable = Arc::new(readable_rows);
        let mapping = if let Some(keys) = source.dictionary_keys() {
            RowMapping::Dictionary(keys.clone())
        } else if source.is_constant() {
            RowMapping::Constant {
                len: source.len(),
                index: 0,
            }
        } else {
            RowMapping::Identity { len: source.len() }
        };
        let utf8 = source.value_layout().is_utf8();
        if !source.is_dictionary() && !source.value_layout().is_view() {
            self.clear();
            let mut required =
                SelectionSet::try_new_with_control_observed(source.values().len(), growth, work)?;
            for row in readable.iter() {
                work(1).map_err(ValueFailure::Caller)?;
                if let Some(index) = source.mapped_row(row)? {
                    required.insert(index)?;
                }
            }
            let bottom = convert_original(
                source.values(),
                &required,
                None,
                growth,
                work,
                &mut self.stats,
                &source.storage(),
                observe,
            )?;
            return Ok(DecodedBytes {
                bottom,
                mapping,
                readable,
                dictionary: false,
                flattened: false,
                utf8,
                _storage: source.storage(),
            });
        }
        let identity = if source.value_layout().is_view() {
            [0; 12]
        } else {
            arrow_raw::byte_storage_identity(source.values().as_ref())?
        };
        let descriptor_bytes = source.values().len().saturating_mul(16);
        let bitmap_bytes = source.values().len().div_ceil(8);
        // Reuse quotes include fixed cache holders even for zero-value
        // dictionaries. A zero efficiency budget must never retain sources.
        // Declared backing remains an estimate, not a full-owner capacity fact.
        let declared_backing_bytes =
            arrow_raw::byte_declared_backing_capacity(source.values().as_ref())?;
        let charge = descriptor_bytes
            .saturating_add(bitmap_bytes)
            .saturating_add(declared_backing_bytes)
            .saturating_add(std::mem::size_of::<CacheEntry>())
            .saturating_add(std::mem::size_of::<DecodedBottom>())
            .saturating_add(4 * std::mem::size_of::<usize>());
        let found = self
            .entries
            .iter()
            .position(|entry| entry.identity == identity);
        // Make the efficiency decision before allocating an original-coordinate
        // bitmap. A huge unreferenced dictionary must not impose that scratch
        // cost on the flattening path. Logical occurrence count is an upper
        // bound; the subsequently materialized set records exact distinct rows.
        let retain = source.is_dictionary()
            && charge <= self.budget
            && (found.is_some() || source.values().len() <= readable.count().saturating_mul(8));
        if !source.value_layout().is_view() && !retain {
            self.clear();
            self.stats.cache_skips = self.stats.cache_skips.saturating_add(1);
            return self.flatten(source, readable, growth, work, observe);
        }
        let mut required =
            SelectionSet::try_new_with_control_observed(source.values().len(), growth, work)?;
        for row in readable.iter() {
            work(1).map_err(ValueFailure::Caller)?;
            if let Some(index) = source.mapped_row(row)? {
                required.insert(index)?;
            }
        }
        if source.value_layout().is_view() {
            self.clear();
            shared_owner::<DecodedBottom, E>(observe)?;
            return Ok(DecodedBytes {
                bottom: Arc::new(DecodedBottom {
                    array: source.values().clone(),
                    owned_descriptor_bytes: required.descriptor_bytes(),
                    covered: required,
                    _storage: source.storage(),
                }),
                mapping,
                readable,
                dictionary: source.is_dictionary(),
                flattened: false,
                utf8,
                _storage: source.storage(),
            });
        }
        self.clock = self.clock.wrapping_add(1);
        let bottom = if let Some(index) = found {
            self.stats.cache_hits = self.stats.cache_hits.saturating_add(1);
            let entry = &self.entries[index];
            if required.is_subset(&entry.bottom.covered) {
                entry.bottom.clone()
            } else {
                let bottom = convert_original(
                    source.values(),
                    &required,
                    Some(&entry.bottom),
                    growth,
                    work,
                    &mut self.stats,
                    &source.storage(),
                    observe,
                )?;
                self.stats.cache_extensions = self.stats.cache_extensions.saturating_add(1);
                self.entries[index].bottom = bottom.clone();
                bottom
            }
        } else {
            let bottom = convert_original(
                source.values(),
                &required,
                None,
                growth,
                work,
                &mut self.stats,
                &source.storage(),
                observe,
            )?;
            while self
                .entries
                .iter()
                .fold(0usize, |total, e| total.saturating_add(e.charge))
                .saturating_add(charge)
                > self.budget
            {
                let Some((oldest, _)) = self
                    .entries
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, e)| e.last_use)
                else {
                    break;
                };
                self.entries.remove(oldest);
                self.stats.cache_evictions = self.stats.cache_evictions.saturating_add(1);
            }
            let entry_count = self.entries.len() + 1;
            reserve(&mut self.entries, entry_count, growth)?;
            self.entries.push(CacheEntry {
                identity,
                _source: source.values().clone(),
                bottom: bottom.clone(),
                charge,
                declared_backing_bytes,
                last_use: self.clock,
            });
            bottom
        };
        if let Some(entry) = self.entries.iter_mut().find(|e| e.identity == identity) {
            entry.last_use = self.clock;
        }
        Ok(DecodedBytes {
            bottom,
            mapping,
            readable,
            dictionary: true,
            flattened: false,
            utf8,
            _storage: source.storage(),
        })
    }
    fn flatten<E>(
        &mut self,
        source: &ByteSource<'_>,
        readable: Arc<SelectionSet>,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut StructureObserver<'_, E>,
    ) -> Result<DecodedBytes, ValueFailure<E>> {
        let count = source.len();
        let layout = if source.value_layout().is_utf8() {
            ByteLayout::Utf8View
        } else {
            ByteLayout::BinaryView
        };
        let mut views = Vec::new();
        reserve(&mut views, count, growth)?;
        resize_observed(&mut views, count, 0, work)?;
        let mut regions = Vec::new();
        reserve(&mut regions, readable.count(), growth)?;
        let mut validity = Vec::<u8>::new();
        let validity_bytes = count.div_ceil(8);
        reserve(&mut validity, validity_bytes, growth)?;
        resize_observed(&mut validity, validity_bytes, 0, work)?;
        let mut valid_count = 0;
        for row in readable.iter() {
            work(1).map_err(ValueFailure::Caller)?;
            if let Some(span) = source.span(row)? {
                views[row] = arrow_raw::append_span_view(layout, span, &mut regions)?;
                validity[row / 8] |= 1 << (row % 8);
                valid_count += 1;
            }
            self.stats.converted_values = self.stats.converted_values.saturating_add(1);
        }
        self.stats.descriptors_created =
            self.stats.descriptors_created.saturating_add(count as u64);
        // SAFETY: every non-placeholder descriptor comes from the checked span
        // adapter; placeholders are legal empty views, NULLs have exact length,
        // and STRING bytes originate from an admitted STRING source.
        let owned_descriptor_bytes = views.capacity().saturating_mul(16).saturating_add(
            regions
                .capacity()
                .saturating_mul(std::mem::size_of::<Buffer>()),
        );
        observe(ValueStructure::ByteArray {
            layout,
            new_buffer_owners: 2,
        })
        .map_err(ValueFailure::Caller)?;
        crate::control::shared_slice_owner::<Buffer, E>(regions.len(), observe)?;
        let array = unsafe {
            arrow_raw::make_view_unchecked(
                layout,
                views.into(),
                regions,
                arrow_raw::make_nulls(Buffer::from_vec(validity), count, count - valid_count),
            )
        }?;
        let covered = readable.try_clone_with_control_observed(growth, work)?;
        shared_owner::<DecodedBottom, E>(observe)?;
        Ok(DecodedBytes {
            bottom: Arc::new(DecodedBottom {
                array,
                owned_descriptor_bytes: owned_descriptor_bytes
                    .saturating_add(covered.descriptor_bytes()),
                covered,
                _storage: source.storage(),
            }),
            mapping: RowMapping::Identity { len: count },
            readable,
            dictionary: false,
            flattened: source.is_dictionary(),
            utf8: layout.is_utf8(),
            _storage: source.storage(),
        })
    }
}
fn convert_original<E>(
    source: &ArrayRef,
    required: &SelectionSet,
    previous: Option<&DecodedBottom>,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
    stats: &mut ConversionStats,
    storage: &novarocks_type_contract::ValueTypeStorageLifetime,
    observe: &mut StructureObserver<'_, E>,
) -> Result<Arc<DecodedBottom>, ValueFailure<E>> {
    let layout = if ByteLayout::from_data_type(source.data_type())
        .ok_or_else(|| violation(ValueViolation::Layout))?
        .is_utf8()
    {
        ByteLayout::Utf8View
    } else {
        ByteLayout::BinaryView
    };
    let mut views = Vec::new();
    reserve(&mut views, source.len(), growth)?;
    let mut regions = Vec::new();
    let mut covered = if let Some(previous) = previous {
        let (existing, backing) = arrow_raw::view_parts(previous.array.as_ref())?;
        extend_observed(&mut views, existing.as_ref(), work)?;
        reserve(
            &mut regions,
            backing.len().saturating_add(required.count()),
            growth,
        )?;
        extend_observed(&mut regions, backing, work)?;
        previous
            .covered
            .try_clone_with_control_observed(growth, work)?
    } else {
        resize_observed(&mut views, source.len(), 0, work)?;
        reserve(&mut regions, required.count(), growth)?;
        SelectionSet::try_new_with_control_observed(source.len(), growth, work)?
    };
    for row in required.iter() {
        if covered.contains(row) {
            continue;
        }
        work(1).map_err(ValueFailure::Caller)?;
        if let Some(span) = arrow_raw::byte_span(source.as_ref(), row)? {
            views[row] = arrow_raw::append_span_view(layout, span, &mut regions)?;
        }
        covered.insert(row)?;
        stats.converted_values = stats.converted_values.saturating_add(1);
    }
    stats.descriptors_created = stats
        .descriptors_created
        .saturating_add(source.len() as u64);
    // SAFETY: checked source spans establish descriptors and STRING validity;
    // unconverted slots are legal empty placeholders protected by coverage.
    // The source's own validity is retained at the same original coordinates.
    let owned_descriptor_bytes = views
        .capacity()
        .saturating_mul(16)
        .saturating_add(
            regions
                .capacity()
                .saturating_mul(std::mem::size_of::<Buffer>()),
        )
        .saturating_add(covered.descriptor_bytes());
    observe(ValueStructure::ByteArray {
        layout,
        new_buffer_owners: 1,
    })
    .map_err(ValueFailure::Caller)?;
    crate::control::shared_slice_owner::<Buffer, E>(regions.len(), observe)?;
    let array = unsafe {
        arrow_raw::make_view_unchecked(layout, views.into(), regions, source.nulls().cloned())
    }?;
    shared_owner::<DecodedBottom, E>(observe)?;
    Ok(Arc::new(DecodedBottom {
        array,
        covered,
        owned_descriptor_bytes,
        _storage: storage.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        BinaryArray, BinaryViewArray, DictionaryArray, LargeStringArray, StringArray,
        StringViewArray, types::Int32Type,
    };
    fn dictionary(values: ArrayRef, keys: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), values).unwrap())
    }
    fn source<'a>(input: &'a ArrayRef, layout: ByteLayout) -> ByteSource<'a> {
        ByteSource::parse(input, layout, None).unwrap()
    }
    fn domain(len: usize, indices: &[usize]) -> SelectionSet {
        let mut out = SelectionSet::try_new(len).unwrap();
        for &row in indices {
            out.insert(row).unwrap();
        }
        out
    }
    fn oracle(input: &DecodedBytes, expected: &[Option<&str>]) {
        for (row, value) in expected.iter().enumerate() {
            assert_eq!(input.string(row).unwrap(), *value);
            assert_eq!(input.value_len(row).unwrap(), value.map(str::len));
        }
        input.bottom().to_data().validate_full().unwrap();
    }
    #[test]
    fn plain_slices_constant_and_binary_follow_independent_logical_values() {
        let expected = [
            Some(""),
            None,
            Some("twelve bytes"),
            Some("thirteen byte"),
            Some("a longer external string"),
        ];
        let arrays: Vec<(ArrayRef, ByteLayout)> = vec![
            (
                Arc::new(StringArray::from(expected.to_vec())),
                ByteLayout::Utf8,
            ),
            (
                Arc::new(LargeStringArray::from(expected.to_vec())),
                ByteLayout::LargeUtf8,
            ),
            (
                Arc::new(StringViewArray::from(expected.to_vec())),
                ByteLayout::Utf8View,
            ),
        ];
        for (array, layout) in arrays {
            let slice = array.slice(1, 4);
            let decoded = ValueConversionContext::default()
                .decode(&source(&slice, layout), &ReachableRows::all(4))
                .unwrap();
            oracle(&decoded, &expected[1..]);
        }
        let value: ArrayRef = Arc::new(StringArray::from(vec![Some(
            "constant externally stored value",
        )]));
        let decoded = ValueConversionContext::default()
            .decode(
                &ByteSource::constant(&value, ByteLayout::Utf8, 3).unwrap(),
                &ReachableRows::all(3),
            )
            .unwrap();
        oracle(&decoded, &[Some("constant externally stored value"); 3]);
        assert_eq!(decoded.bottom().len(), 1);
        assert!(matches!(decoded.mapping(), RowMapping::Constant { .. }));
        for (array, layout) in [
            (
                Arc::new(BinaryArray::from(vec![
                    Some(b"\xff\x00 bytes".as_slice()),
                    None,
                ])) as ArrayRef,
                ByteLayout::Binary,
            ),
            (
                Arc::new(BinaryViewArray::from(vec![
                    Some(b"\xff\x00 bytes".as_slice()),
                    None,
                ])) as ArrayRef,
                ByteLayout::BinaryView,
            ),
        ] {
            let decoded = ValueConversionContext::default()
                .decode(&source(&array, layout), &ReachableRows::all(2))
                .unwrap();
            assert_eq!(
                decoded.value(0).unwrap(),
                Some(b"\xff\x00 bytes".as_slice())
            );
            assert_eq!(decoded.value(1).unwrap(), None);
            assert!(decoded.string(0).is_err());
        }
    }
    #[test]
    fn original_coordinate_sparse_cache_extends_without_mutating_published_alias() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("first external value"),
            Some("second external value"),
            None,
        ]));
        let first_input = dictionary(values.clone(), vec![Some(0), Some(0), None]);
        let mut context = ValueConversionContext::new(4096);
        let first = context
            .decode(
                &source(&first_input, ByteLayout::Utf8),
                &ReachableRows::all(3),
            )
            .unwrap();
        oracle(
            &first,
            &[
                Some("first external value"),
                Some("first external value"),
                None,
            ],
        );
        assert!(!first.bottom_coverage().contains(1));
        assert_eq!(context.stats().converted_values, 1);
        assert_eq!(context.stats().descriptors_created, 3);
        // A fresh ArrayRef with exactly the same storage must hit the cache.
        let reconstructed: ArrayRef = Arc::new(
            values
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone(),
        );
        let second_input = dictionary(reconstructed, vec![Some(1), Some(2), Some(0)]);
        let second = context
            .decode(
                &source(&second_input, ByteLayout::Utf8),
                &ReachableRows::all(3),
            )
            .unwrap();
        oracle(
            &second,
            &[
                Some("second external value"),
                None,
                Some("first external value"),
            ],
        );
        assert_eq!(context.stats().cache_hits, 1);
        assert_eq!(context.stats().cache_extensions, 1);
        assert_eq!(context.stats().converted_values, 3);
        assert!(second.bottom_coverage().contains(1));
        assert!(!first.bottom_coverage().contains(1));
        assert!(!Arc::ptr_eq(first.bottom(), second.bottom()));
        let hit = context
            .decode(
                &source(&second_input, ByteLayout::Utf8),
                &ReachableRows::all(3),
            )
            .unwrap();
        assert!(Arc::ptr_eq(hit.bottom(), second.bottom()));
        assert_eq!(context.stats().converted_values, 3);
        assert_eq!(context.storage_facts().known_owner_capacity, None);
        context.clear();
        oracle(
            &first,
            &[
                Some("first external value"),
                Some("first external value"),
                None,
            ],
        );
        assert_eq!(context.storage_facts().owned_descriptor_bytes, 0);
    }
    #[test]
    fn flatten_budget_and_density_are_efficiency_choices_and_unselected_rows_are_undefined() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b"), None]));
        let input = dictionary(values, vec![Some(0), Some(1), None, Some(2)]);
        let decoded = ValueConversionContext::new(0)
            .decode(&source(&input, ByteLayout::Utf8), &ReachableRows::all(4))
            .unwrap();
        oracle(&decoded, &[Some("a"), Some("b"), None, None]);
        assert!(decoded.is_flattened());
        assert!(decoded.peel(&domain(4, &[0, 1, 2, 3])).unwrap().is_none());
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..1000).map(|_| "unreferenced value"),
        ));
        let input = dictionary(values, vec![Some(999)]);
        let mut context = ValueConversionContext::new(1024 * 1024);
        let decoded = context
            .decode(&source(&input, ByteLayout::Utf8), &ReachableRows::all(1))
            .unwrap();
        assert!(decoded.is_flattened());
        assert_eq!(context.stats().descriptors_created, 1);
        assert_eq!(context.stats().converted_values, 1);
        let input: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let decoded = context
            .decode(
                &source(&input, ByteLayout::Utf8),
                &ReachableRows::indices(3, &[2]),
            )
            .unwrap();
        assert!(matches!(
            decoded.value(0),
            Err(ValueError::Invariant(ValueViolation::UndefinedRow))
        ));
        assert_eq!(decoded.value(2).unwrap(), Some(b"c".as_slice()));
    }
    #[test]
    fn view_dictionary_borrows_bottom_and_matches_offsets_dictionary() {
        let expected = [Some("a longer external string"), None, None, Some("short")];
        let keys = vec![Some(0), Some(1), None, Some(2)];
        let plain: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a longer external string"),
            None,
            Some("short"),
        ]));
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("a longer external string"),
            None,
            Some("short"),
        ]));
        let input = dictionary(view.clone(), keys.clone());
        let parsed =
            ByteSource::parse(&input, ByteLayout::Utf8View, Some(ByteLayout::Utf8View)).unwrap();
        let mut context = ValueConversionContext::default();
        let decoded = context.decode(&parsed, &ReachableRows::all(4)).unwrap();
        oracle(&decoded, &expected);
        assert!(Arc::ptr_eq(decoded.bottom(), &view));
        assert_eq!(context.stats().converted_values, 0);
        let input = dictionary(plain, keys);
        oracle(
            &context
                .decode(&source(&input, ByteLayout::Utf8), &ReachableRows::all(4))
                .unwrap(),
            &expected,
        );
    }
    #[test]
    fn peel_keeps_key_validity_and_requires_actual_result_coverage() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("b")]));
        let input = dictionary(values, vec![Some(0), None, Some(1)]);
        let decoded = ValueConversionContext::default()
            .decode(&source(&input, ByteLayout::Utf8), &ReachableRows::all(3))
            .unwrap();
        let first = decoded.peel(&domain(3, &[0, 1])).unwrap().unwrap();
        assert_eq!(first.required_rows().iter().collect::<Vec<_>>(), vec![0]);
        // New result is not forced NULL by the input's logical NULL mask.
        let result = PeelResult::new(
            Arc::new(StringArray::from(vec![
                Some("generated"),
                Some("poison placeholder"),
            ])),
            domain(2, &[0]),
            ByteLayout::Utf8View,
            None,
        )
        .unwrap();
        let restored = first.rewrap(&result).unwrap();
        let restored = source(&restored, ByteLayout::Utf8);
        assert_eq!(restored.value(0).unwrap(), Some(b"generated".as_slice()));
        assert_eq!(restored.mapped_row(1).unwrap(), None);
        let second = decoded.peel(&domain(3, &[2])).unwrap().unwrap();
        assert!(matches!(
            second.rewrap(&result),
            Err(ValueError::Invariant(ValueViolation::UndefinedRow))
        ));
        let intersection = second
            .required_rows()
            .intersection(result.defined_rows())
            .unwrap();
        let missing = second
            .required_rows()
            .difference(result.defined_rows())
            .unwrap();
        assert!(intersection.is_empty());
        assert_eq!(missing.iter().collect::<Vec<_>>(), vec![1]);
        let result = PeelResult::new(
            Arc::new(StringArray::from(vec![None, None::<&str>])),
            domain(2, &[0, 1]),
            ByteLayout::Utf8View,
            None,
        )
        .unwrap();
        let restored = second.rewrap(&result).unwrap();
        assert_eq!(source(&restored, ByteLayout::Utf8).value(2).unwrap(), None);
        // A computed NULL remains covered. Primitive D1 tests may explicitly
        // choose view-valued dictionary results without changing engine D(L).
        let result = PeelResult::new(
            Arc::new(StringViewArray::from(vec![None, Some("view result")])),
            domain(2, &[0, 1]),
            ByteLayout::Utf8View,
            Some(ByteLayout::Utf8View),
        )
        .unwrap();
        let restored = second.rewrap(&result).unwrap();
        let parsed =
            ByteSource::parse(&restored, ByteLayout::Utf8View, Some(ByteLayout::Utf8View)).unwrap();
        assert_eq!(parsed.value(2).unwrap(), Some(b"view result".as_slice()));
    }
    #[test]
    fn descriptor_fast_paths_and_utf8_substrings_preserve_regions() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            "é short",
            "a much longer external value",
        ]));
        let decoded = ValueConversionContext::default()
            .decode(&source(&input, ByteLayout::Utf8), &ReachableRows::all(2))
            .unwrap();
        assert_eq!(
            decoded.prefix(0).unwrap(),
            Some(([0xc3, 0xa9, b' ', b's'], 4))
        );
        assert_eq!(decoded.inline_bytes(0).unwrap(), Some("é short".as_bytes()));
        assert_eq!(decoded.inline_bytes(1).unwrap(), None);
        assert_eq!(decoded.prefix(1).unwrap(), Some((*b"a mu", 4)));
        assert_eq!(decoded.substring(0, 0..2).unwrap(), Some("é".as_bytes()));
        assert!(matches!(
            decoded.substring(0, 1..2),
            Err(ValueError::Invariant(ValueViolation::Utf8Boundary))
        ));
        let parent = decoded.region(1).unwrap().unwrap();
        let child = decoded.substring_region(1, 2..13).unwrap().unwrap();
        assert_eq!(parent.buffer.as_ptr(), child.buffer.as_ptr());
        assert_eq!(child.offset, parent.offset + 2);
        assert_eq!(
            &child.buffer.as_slice()[child.offset..child.offset + child.len],
            b"much longer"
        );
    }
    #[test]
    fn original_control_refusals_propagate_and_context_is_send() {
        let input: ArrayRef = Arc::new(StringArray::from(vec!["value"]));
        let parsed = source(&input, ByteLayout::Utf8);
        let mut context = ValueConversionContext::default();
        assert!(matches!(
            context.decode_observed(
                &parsed,
                &ReachableRows::all(1),
                &mut |_| Err(41),
                &mut |_| Ok(())
            ),
            Err(ValueFailure::Caller(41))
        ));
        assert!(matches!(
            context.decode_observed(
                &parsed,
                &ReachableRows::all(1),
                &mut |_| Ok(()),
                &mut |_| Err(42)
            ),
            Err(ValueFailure::Caller(42))
        ));
        let output = std::thread::spawn(move || {
            context
                .decode(&source(&input, ByteLayout::Utf8), &ReachableRows::all(1))
                .unwrap()
        })
        .join()
        .unwrap();
        assert_eq!(output.value(0).unwrap(), Some(b"value".as_slice()));
    }
    #[test]
    fn cache_release_and_final_payload_alias_drop_are_separate_events() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let drops = Arc::new(AtomicUsize::new(0));
        let values = arrow_raw::tracked_string_source(
            &["a long custom-owned external string"],
            drops.clone(),
        );
        let weak = Arc::downgrade(&values);
        let input = dictionary(values.clone(), vec![Some(0)]);
        let mut context = ValueConversionContext::default();
        let decoded = context
            .decode(&source(&input, ByteLayout::Utf8), &ReachableRows::all(1))
            .unwrap();
        let output = decoded.clone();
        drop(values);
        drop(input);
        drop(decoded);
        assert!(weak.upgrade().is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        context.clear();
        assert!(weak.upgrade().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            output.value(0).unwrap(),
            Some(b"a long custom-owned external string".as_slice())
        );
        drop(output);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn slices_and_null_bitmap_offsets_do_not_alias_cache_identity() {
        let base = StringArray::from(vec![Some("same"), None, Some("same"), None]);
        let first_values: ArrayRef = Arc::new(base.slice(0, 2));
        let second_values: ArrayRef = Arc::new(base.slice(1, 2));
        let first = dictionary(first_values, vec![Some(0), Some(1)]);
        let second = dictionary(second_values, vec![Some(0), Some(1)]);
        let mut context = ValueConversionContext::default();
        oracle(
            &context
                .decode(&source(&first, ByteLayout::Utf8), &ReachableRows::all(2))
                .unwrap(),
            &[Some("same"), None],
        );
        oracle(
            &context
                .decode(&source(&second, ByteLayout::Utf8), &ReachableRows::all(2))
                .unwrap(),
            &[None, Some("same")],
        );
        assert_eq!(context.stats().cache_hits, 0);
        assert_eq!(context.stats().converted_values, 4);
    }
    #[test]
    fn zero_efficiency_budget_does_not_retain_even_an_empty_dictionary_source() {
        for budget in [0, 1] {
            let values: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
            let weak = Arc::downgrade(&values);
            let input = dictionary(values.clone(), vec![None, None]);
            let mut context = ValueConversionContext::new(budget);
            let decoded = context
                .decode(&source(&input, ByteLayout::Utf8), &ReachableRows::all(2))
                .unwrap();
            assert!(decoded.is_flattened());
            assert_eq!(context.stats().cache_skips, 1);
            assert_eq!(context.stats().cache_hits, 0);
            drop(input);
            drop(values);
            assert!(
                weak.upgrade().is_none(),
                "efficiency rejection cannot pin the original source"
            );
            assert_eq!(decoded.value(0).unwrap(), None);
            assert_eq!(decoded.value(1).unwrap(), None);
        }
    }

    #[test]
    fn cache_bottom_and_published_aliases_keep_actual_source_and_creator_custody() {
        use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
        struct Hold;
        impl ValueTypeStorageHold for Hold {}
        let hold = Arc::new(Hold);
        let weak = Arc::downgrade(&hold);
        let storage = ValueTypeStorageLifetime::held(hold);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["retained external payload"]));
        let input = dictionary(values, vec![Some(0), Some(0)]);
        let mut context = ValueConversionContext::default();
        let mut publications = 0;
        let decoded = context
            .decode_structured_observed(
                &source(&input, ByteLayout::Utf8).with_storage(&storage),
                &ReachableRows::all(2),
                &mut |_| Ok::<_, Infallible>(()),
                &mut |_| Ok(()),
                &mut |event| {
                    if matches!(event, ValueStructure::ByteArray { .. }) {
                        publications += 1;
                    }
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(publications, 1);
        let hit = context
            .decode_structured_observed(
                &source(&input, ByteLayout::Utf8).with_storage(&storage),
                &ReachableRows::all(2),
                &mut |_| Ok::<_, Infallible>(()),
                &mut |_| Ok(()),
                &mut |event| {
                    assert!(!matches!(event, ValueStructure::ByteArray { .. }));
                    Ok(())
                },
            )
            .unwrap();
        drop(storage);
        drop(input);
        context.clear();
        assert!(weak.upgrade().is_some());
        assert_eq!(hit.string(0).unwrap(), Some("retained external payload"));
        drop(hit);
        assert!(weak.upgrade().is_some());
        drop(decoded);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn copy_on_write_bottom_keeps_current_creator_without_chaining_previous_creators() {
        use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
        struct Hold;
        impl ValueTypeStorageHold for Hold {}
        let first_hold = Arc::new(Hold);
        let first_weak = Arc::downgrade(&first_hold);
        let second_hold = Arc::new(Hold);
        let second_weak = Arc::downgrade(&second_hold);
        let first_storage = ValueTypeStorageLifetime::held(first_hold);
        let second_storage = ValueTypeStorageLifetime::held(second_hold);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "first external payload",
            "second external payload",
        ]));
        let input1 = dictionary(values.clone(), vec![Some(0)]);
        let input2 = dictionary(values, vec![Some(1)]);
        let mut context = ValueConversionContext::default();
        let first = context
            .decode(
                &source(&input1, ByteLayout::Utf8).with_storage(&first_storage),
                &ReachableRows::all(1),
            )
            .unwrap();
        let second = context
            .decode(
                &source(&input2, ByteLayout::Utf8).with_storage(&second_storage),
                &ReachableRows::all(1),
            )
            .unwrap();
        drop(first_storage);
        drop(second_storage);
        drop(input1);
        drop(input2);
        drop(first);
        assert!(first_weak.upgrade().is_none());
        context.clear();
        assert!(second_weak.upgrade().is_some());
        assert_eq!(second.string(0).unwrap(), Some("second external payload"));
        drop(second);
        assert!(second_weak.upgrade().is_none());
    }

    #[test]
    fn decode_plan_freezes_real_context_reuse_and_never_reallocates_on_publication() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "first long external value",
            "second long external value",
        ]));
        let first_input = dictionary(values.clone(), vec![Some(0)]);
        let second_input = dictionary(values, vec![Some(1)]);
        let mut context = ValueConversionContext::new(4096);
        let mut requested = 0u128;
        let first = context
            .plan_decode_observed(
                &source(&first_input, ByteLayout::Utf8),
                &ReachableRows::all(1),
                &mut |layout| {
                    requested += layout.size() as u128;
                    Ok::<_, Infallible>(())
                },
                &mut |_| Ok::<_, Infallible>(()),
            )
            .unwrap();
        assert_eq!(first.planning_growth_requested_bytes(), requested);
        assert_eq!(first.shared_cached_storage_bytes(), 0);
        assert_eq!(
            first.new_plan_storage_bytes(),
            first.held_plan_storage_bytes()
        );
        assert!(first.output_layouts().is_empty());
        assert!(first.execution_scratch_layouts().is_empty());
        let hit = context
            .plan_decode_observed(
                &source(&first_input, ByteLayout::Utf8),
                &ReachableRows::all(1),
                &mut |_| Ok::<_, Infallible>(()),
                &mut |_| Ok::<_, Infallible>(()),
            )
            .unwrap();
        assert!(hit.shared_cached_storage_bytes() > 0);
        assert_eq!(
            hit.new_plan_storage_bytes() + hit.shared_cached_storage_bytes(),
            hit.held_plan_storage_bytes()
        );
        assert_eq!(context.stats().converted_values, 1);
        let second = context
            .plan_decode_observed(
                &source(&second_input, ByteLayout::Utf8),
                &ReachableRows::all(1),
                &mut |_| Ok::<_, Infallible>(()),
                &mut |_| Ok::<_, Infallible>(()),
            )
            .unwrap();
        assert_eq!(context.stats().converted_values, 2);
        assert_eq!(context.stats().cache_extensions, 1);
        assert_eq!(second.shared_cached_storage_bytes(), 0);
        assert!(!first.decoded().bottom_coverage().contains(1));
        context.clear();
        let after_clear = context.stats();
        let published = context.decode_planned(&first);
        oracle(&published, &[Some("first long external value")]);
        assert_eq!(context.stats(), after_clear);
        oracle(
            &context.decode_planned(&second),
            &[Some("second long external value")],
        );
    }
    #[test]
    fn decode_plan_preserves_caller_refusals_and_flattening_choice() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let input = dictionary(values, vec![Some(1)]);
        let mut context = ValueConversionContext::new(0);
        assert!(matches!(
            context.plan_decode_observed(
                &source(&input, ByteLayout::Utf8),
                &ReachableRows::all(1),
                &mut |_| Err(81),
                &mut |_| Ok(())
            ),
            Err(ValueFailure::Caller(81))
        ));
        assert!(matches!(
            context.plan_decode_observed(
                &source(&input, ByteLayout::Utf8),
                &ReachableRows::all(1),
                &mut |_| Ok(()),
                &mut |_| Err(82)
            ),
            Err(ValueFailure::Caller(82))
        ));
        let plan = context
            .plan_decode_observed(
                &source(&input, ByteLayout::Utf8),
                &ReachableRows::all(1),
                &mut |_| Ok::<_, Infallible>(()),
                &mut |_| Ok::<_, Infallible>(()),
            )
            .unwrap();
        assert!(plan.decoded().is_flattened());
        context.budget = 4096;
        let published = context.decode_planned(&plan);
        assert!(published.is_flattened());
        assert_eq!(published.value(0).unwrap(), Some(b"b".as_slice()));
    }
    #[test]
    fn shared_view_table_refusal_preserves_original_cause_source_and_retry() {
        use std::cell::Cell;
        let values: ArrayRef = Arc::new(StringArray::from(vec!["original external value"]));
        for (input, budget) in [
            (values.clone(), 8 * 1024 * 1024),
            (dictionary(values.clone(), vec![Some(0)]), 8 * 1024 * 1024),
            // Zero cache budget forces the original flatten constructor rather
            // than retaining/reusing the dictionary conversion bottom.
            (dictionary(values.clone(), vec![Some(0)]), 0),
        ] {
            let mut context = ValueConversionContext::new(budget);
            let cause = Arc::new(83);
            let published = Cell::new(false);
            let refused = Cell::new(false);
            let table = novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::array::<Buffer>(1).unwrap(),
            )
            .unwrap();
            let result = context.decode_structured_observed(
                &source(&input, ByteLayout::Utf8),
                &ReachableRows::all(1),
                &mut |_| {
                    assert!(!refused.get(), "growth after original table refusal");
                    Ok(())
                },
                &mut |_| {
                    assert!(!refused.get(), "work after original table refusal");
                    Ok(())
                },
                &mut |event| {
                    assert!(!refused.get(), "structure after original table refusal");
                    match event {
                        ValueStructure::ByteArray { .. } => published.set(true),
                        ValueStructure::SharedOwner(actual) if published.get() => {
                            assert_eq!(actual, table);
                            refused.set(true);
                            return Err(cause.clone());
                        }
                        _ => {}
                    }
                    Ok(())
                },
            );
            let Err(ValueFailure::Caller(actual)) = result else {
                panic!("actual view table observer refusal")
            };
            assert!(Arc::ptr_eq(&actual, &cause));
            assert!(refused.get());
            let output = context
                .decode(&source(&input, ByteLayout::Utf8), &ReachableRows::all(1))
                .unwrap();
            oracle(&output, &[Some("original external value")]);
            if budget == 0 {
                assert!(
                    output.is_flattened(),
                    "zero cache budget uses flatten on retry"
                );
            }
            assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "original external value"
            );
        }
    }
}
