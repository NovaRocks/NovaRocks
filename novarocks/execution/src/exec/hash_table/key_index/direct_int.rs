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

//! Direct signed-integer storage and resumable data operations. The parent
//! graph owns phase barriers, exclusive task claims and Ready publication.
//! These cursors do not own scheduling, worker pools or completion registries.

use super::{
    IndexPurpose, KeyId, KeyInput, KeyMatch, KeySpec, NullPolicy, PreparedKeyBatch, checkpoint_at,
    invariant, shared,
    statistics::{IntegerDomain, visit_integer_occurrences},
    vector,
};
use crate::{
    exec::{
        hash_table::allocation::AggregateAllocator,
        pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::{
    array::{ArrayRef, PrimitiveArray, new_null_array},
    datatypes::{ArrowPrimitiveType, DataType, Int8Type, Int16Type, Int32Type, Int64Type},
};
use arrow_buffer::ScalarBuffer;
use novarocks_functions::{
    AggregateStateAllocator, KernelEvaluationControl,
    selected_copy::{
        CopyConstructionError, preflight_arrow_node_structure_construction,
        preflight_null_arrow_structure_observed, preflight_null_construction,
    },
};
use novarocks_type_contract::{FunctionValueType, ValueSchemaCreation, ValueTypeStorageLifetime};
use novarocks_value_arrays::ValueViolation;
use std::{
    alloc::Layout,
    convert::Infallible,
    marker::PhantomData,
    num::NonZeroUsize,
    ops::Range,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

// Private work/storage segmentation. A larger domain creates more pages;
// neither constant imposes an input, value, span or distinct-class quota.
const PAGE_WORDS: usize = 1024;
const REPRESENTATIVE_ROWS: usize = 1024;
#[derive(Clone, Copy, Debug)]
enum IntegerKind {
    I8,
    I16,
    I32,
    I64,
}
impl IntegerKind {
    fn from_spec(spec: &KeySpec) -> Option<Self> {
        if spec.types().len() != 1 {
            return None;
        }
        Some(match spec.types()[0].data_type() {
            DataType::Int8 => Self::I8,
            DataType::Int16 => Self::I16,
            DataType::Int32 => Self::I32,
            DataType::Int64 => Self::I64,
            _ => return None,
        })
    }
    fn width(self) -> usize {
        match self {
            Self::I8 => 1,
            Self::I16 => 2,
            Self::I32 => 4,
            Self::I64 => 8,
        }
    }
    fn data_type(self) -> DataType {
        match self {
            Self::I8 => DataType::Int8,
            Self::I16 => DataType::Int16,
            Self::I32 => DataType::Int32,
            Self::I64 => DataType::Int64,
        }
    }
}
/// Pure representation geometry for an already selected exact integer domain.
/// None means this optimization cannot represent its own planned layout. It is
/// never a refusal of legal input and never authorizes a fallback after OOM.
#[derive(Clone, Copy, Debug)]
pub(super) struct DirectGeometry {
    min_max: Option<(i64, i64)>,
    slots: usize,
    words: usize,
    shards: usize,
    pages: usize,
    purpose: IndexPurpose,
    kind: IntegerKind,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct DirectCosts {
    pub(super) bitmap_bytes: u128,
    pub(super) slot_id_bytes: u128,
    pub(super) storage_headers_bytes: u128,
    pub(super) count_prefix_peak_bytes: u128,
    pub(super) representative_payload_estimate: u128,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the original selector cost/provenance fact; it grants no allocation authority."
        )
    )]
    pub(super) representative_pages_upper: usize,
    pub(super) representative_node_metadata_upper: u128,
    pub(super) null_constructor_peak_upper: u128,
    // SourceBackingOwner custody remains its real, separately paid author.
    // This is a sizing model, not a complete invoice or memory grant.
}
impl DirectGeometry {
    pub(super) fn for_selected(
        spec: &KeySpec,
        domain: IntegerDomain,
        purpose: IndexPurpose,
        workers: NonZeroUsize,
    ) -> Option<Self> {
        let kind = IntegerKind::from_spec(spec)?;
        let slots = match domain.min_max {
            Some((min, max)) => {
                usize::try_from((max as i128).checked_sub(min as i128)?.checked_add(1)?).ok()?
            }
            None => 0,
        };
        let words = ceiling(slots, 64)?;
        let shards = workers.get().min(words.max(1));
        Layout::array::<OnceLock<Arc<Shard>>>(shards).ok()?;
        let max_words = ceiling(words, shards)?;
        Layout::array::<Arc<Page>>(ceiling(max_words, PAGE_WORDS)?).ok()?;
        Layout::array::<AtomicU64>(PAGE_WORDS).ok()?;
        if purpose == IndexPurpose::KeyIds {
            Layout::array::<AtomicUsize>(PAGE_WORDS.checked_mul(64)?).ok()?;
            Layout::array::<usize>(max_words).ok()?;
        }
        arc_bytes::<Storage>()?;
        arc_bytes::<Shard>()?;
        arc_bytes::<Page>()?;
        let small = words / shards;
        let remainder = words % shards;
        let pages = (shards - remainder)
            .checked_mul(ceiling(small, PAGE_WORDS)?)?
            .checked_add(remainder.checked_mul(ceiling(small.checked_add(1)?, PAGE_WORDS)?)?)?;
        Some(Self {
            min_max: domain.min_max,
            slots,
            words,
            shards,
            pages,
            purpose,
            kind,
        })
    }
    #[cfg(test)]
    pub(super) fn slots(self) -> usize {
        self.slots
    }
    pub(super) fn shard_count(self) -> usize {
        self.shards
    }
    pub(super) fn purpose(self) -> IndexPurpose {
        self.purpose
    }
    pub(super) fn costs(
        self,
        estimated_nonnull_classes: usize,
        null_class_possible: bool,
    ) -> Option<DirectCosts> {
        let ids = self.purpose == IndexPurpose::KeyIds;
        let distinct = if ids {
            estimated_nonnull_classes.min(self.slots)
        } else {
            0
        };
        let representative_pages_upper = if distinct == 0 {
            0
        } else {
            ceiling(distinct, REPRESENTATIVE_ROWS)?.checked_add(distinct.min(self.shards) - 1)?
        };
        let directory = Layout::array::<OnceLock<Arc<Shard>>>(self.shards)
            .ok()?
            .size() as u128;
        let page_headers = (self.pages as u128)
            .checked_mul(arc_bytes::<Page>()? as u128 + std::mem::size_of::<Arc<Page>>() as u128)?;
        let shard_headers = (self.shards as u128).checked_mul(arc_bytes::<Shard>()? as u128)?;
        let header = directory
            .checked_add(page_headers)?
            .checked_add(shard_headers)?
            .checked_add(arc_bytes::<Storage>()? as u128)?;
        let representative_directory = (representative_pages_upper as u128)
            .checked_mul(std::mem::size_of::<ArrayRef>() as u128)?;
        Some(DirectCosts {
            bitmap_bytes: (self.words as u128)
                .checked_mul(std::mem::size_of::<AtomicU64>() as u128)?,
            slot_id_bytes: if ids {
                (self.slots as u128).checked_mul(std::mem::size_of::<AtomicUsize>() as u128)?
            } else {
                0
            },
            storage_headers_bytes: header.checked_add(representative_directory)?,
            count_prefix_peak_bytes: if ids {
                (self.words as u128).checked_mul(std::mem::size_of::<usize>() as u128)?
            } else {
                0
            },
            representative_payload_estimate: (distinct as u128)
                .checked_mul(self.kind.width() as u128)?,
            representative_pages_upper,
            representative_node_metadata_upper: (representative_pages_upper as u128)
                .checked_mul(node_upper(self.kind)? as u128)?,
            null_constructor_peak_upper: if ids && null_class_possible {
                null_upper(self.kind)? as u128
            } else {
                0
            },
        })
    }
    fn word_range(self, shard: usize) -> Option<Range<usize>> {
        if shard >= self.shards {
            return None;
        }
        let quotient = self.words / self.shards;
        let remainder = self.words % self.shards;
        let start = shard
            .checked_mul(quotient)?
            .checked_add(shard.min(remainder))?;
        Some(start..start.checked_add(quotient + usize::from(shard < remainder))?)
    }
    fn slot(self, value: i64) -> Option<usize> {
        let (min, max) = self.min_max?;
        if value < min || value > max {
            return None;
        }
        usize::try_from(value as i128 - min as i128).ok()
    }
    fn value(self, slot: usize) -> ExecutionResult<i64> {
        if slot >= self.slots {
            return Err(invariant(ValueViolation::Bounds));
        }
        let min = self
            .min_max
            .ok_or_else(|| invariant(ValueViolation::Bounds))?
            .0;
        i64::try_from(min as i128 + slot as i128).map_err(|_| arithmetic())
    }
    fn shard_for_word(self, word: usize) -> Option<usize> {
        if word >= self.words {
            return None;
        }
        let q = self.words / self.shards;
        let r = self.words % self.shards;
        let large_end = (q + 1).checked_mul(r)?;
        if word < large_end {
            Some(word / (q + 1))
        } else {
            Some(r + (word - large_end) / q)
        }
    }
}
fn ceiling(value: usize, unit: usize) -> Option<usize> {
    (value / unit).checked_add(usize::from(!value.is_multiple_of(unit)))
}
fn arc_bytes<T>() -> Option<usize> {
    novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
        .ok()
        .map(|l| l.size())
}
fn node_upper(kind: IntegerKind) -> Option<usize> {
    let mut exact = Some(0usize);
    let mut opaque = Some(0usize);
    let mut request = |l: Layout| {
        exact = exact.and_then(|n| n.checked_add(l.size()));
        Ok::<_, Infallible>(())
    };
    let mut bound = |bytes| {
        opaque = opaque.and_then(|n| n.checked_add(bytes));
        Ok::<_, Infallible>(())
    };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut bound);
    preflight_arrow_node_structure_construction(
        &kind.data_type(),
        false,
        &mut creation,
        &mut |_| Ok::<_, Infallible>(()),
    )
    .ok()?;
    exact?.checked_add(opaque?)
}
fn null_upper(kind: IntegerKind) -> Option<usize> {
    let mut request = |_: Layout| Ok::<_, Infallible>(());
    let mut opaque = |_| Ok::<_, Infallible>(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    // Cost observation only. This original author's facts grant no capacity;
    // the real null constructor below obtains its actual envelope again.
    preflight_null_arrow_structure_observed(&kind.data_type(), 1, &mut creation, &mut |_| {
        Ok::<_, Infallible>(())
    })
    .ok()
    .map(|f| f.operation_peak_bytes())
}

pub(crate) struct DirectWork<'a> {
    pub(crate) owner: &'a dyn InputConstructionOwner,
    pub(crate) allocator: AggregateAllocator,
    pub(crate) host: Arc<dyn AggregateStateAllocator>,
    pub(crate) control: &'a dyn KernelEvaluationControl,
}
struct Page {
    bits: Vec<AtomicU64>,
    ids: Option<Vec<AtomicUsize>>,
    _storage: ValueTypeStorageLifetime,
}
struct Shard {
    words: Range<usize>,
    pages: Vec<Arc<Page>>,
    representatives: OnceLock<RepresentativePages>,
    _storage: ValueTypeStorageLifetime,
}
pub(super) struct RepresentativePages {
    pub(super) base: usize,
    pub(super) len: usize,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains actual Direct key representative arrays for the frozen typed read/export API."
        )
    )]
    pages: Vec<ArrayRef>,
    _storage: ValueTypeStorageLifetime,
}
impl RepresentativePages {
    #[cfg(test)]
    pub(super) fn arrays(&self) -> &[ArrayRef] {
        &self.pages
    }
}
pub(super) struct Storage {
    geometry: DirectGeometry,
    spec: KeySpec,
    binding: FunctionValueType,
    shards: Vec<OnceLock<Arc<Shard>>>,
    null_present: AtomicBool,
    null_id: AtomicUsize,
    null_representative: OnceLock<Option<ArrayRef>>,
    completed_ids: OnceLock<usize>,
    failed: Mutex<Option<ExecutionFailure>>,
    _storage: ValueTypeStorageLifetime,
}
impl Storage {
    /// O(W) headers only. All span-sized initialization belongs to InitCursor.
    pub(super) fn new(
        geometry: DirectGeometry,
        spec: &KeySpec,
        actual_root_nullable_union: Option<bool>,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Arc<Self>> {
        checkpoint(work)?;
        if IntegerKind::from_spec(spec).map(|k| k.width()) != Some(geometry.kind.width()) {
            return Err(invariant(ValueViolation::Layout));
        }
        // No actual captured page means the explicit empty-input constructor
        // uses the frozen Spec. Otherwise use the real captured declaration OR.
        let nullable = actual_root_nullable_union.unwrap_or(spec.types()[0].nullable());
        let binding = if nullable == spec.types()[0].nullable() {
            spec.types()[0].clone_observed(|| checkpoint(work))?
        } else {
            spec.types()[0]
                .with_nullable_observed::<ConstructionFailure>(
                    nullable,
                    work.owner.storage_lifetime(),
                    |l| work.owner.request(l),
                    |_| checkpoint(work).map_err(ConstructionFailure::from),
                )
                .map_err(ExecutionFailure::from)?
        };
        let mut shards = vector(work.owner, geometry.shards)?;
        for shard in 0..geometry.shards {
            checkpoint_at(shard, &|| checkpoint(work))?;
            shards.push(OnceLock::new());
        }
        shared(
            work.owner,
            Self {
                geometry,
                spec: spec.clone(),
                binding,
                shards,
                null_present: AtomicBool::new(false),
                null_id: AtomicUsize::new(0),
                null_representative: OnceLock::new(),
                completed_ids: OnceLock::new(),
                failed: Mutex::new(None),
                _storage: work.owner.storage_lifetime(),
            },
        )
    }
    pub(super) fn spec(&self) -> &KeySpec {
        &self.spec
    }
    pub(super) fn geometry(&self) -> DirectGeometry {
        self.geometry
    }
    #[cfg(test)]
    pub(super) fn binding(&self) -> &FunctionValueType {
        &self.binding
    }
    pub(super) fn check_failure(&self) -> ExecutionResult<()> {
        self.failed
            .lock()
            .map_err(|_| invariant(ValueViolation::RowTransaction))?
            .as_ref()
            .map_or(Ok(()), |e| Err(e.clone()))
    }
    fn latch(&self, cause: ExecutionFailure) {
        if let Ok(mut first) = self.failed.lock()
            && first.is_none()
        {
            *first = Some(cause);
        }
    }
    fn shard(&self, shard: usize) -> ExecutionResult<&Arc<Shard>> {
        self.shards
            .get(shard)
            .and_then(OnceLock::get)
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))
    }
    fn page_word(&self, word: usize) -> ExecutionResult<(&Page, usize)> {
        let shard = self.shard(
            self.geometry
                .shard_for_word(word)
                .ok_or_else(|| invariant(ValueViolation::Bounds))?,
        )?;
        let local = word - shard.words.start;
        Ok((
            shard
                .pages
                .get(local / PAGE_WORDS)
                .ok_or_else(|| invariant(ValueViolation::Bounds))?,
            local % PAGE_WORDS,
        ))
    }
    fn bits(&self, word: usize) -> ExecutionResult<u64> {
        let (page, local) = self.page_word(word)?;
        let mut bits = page
            .bits
            .get(local)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?
            .load(Ordering::Acquire);
        let remaining = self.geometry.slots - word * 64;
        if remaining < 64 {
            bits &= (1u64 << remaining) - 1;
        }
        Ok(bits)
    }
    fn write_id(&self, slot: usize, id: usize) -> ExecutionResult<()> {
        let (page, word) = self.page_word(slot / 64)?;
        page.ids
            .as_ref()
            .and_then(|v| v.get(word * 64 + slot % 64))
            .ok_or_else(|| invariant(ValueViolation::Layout))?
            .store(id, Ordering::Relaxed);
        Ok(())
    }
    fn mark_value(&self, value: Option<i64>) -> ExecutionResult<()> {
        if let Some(value) = value {
            let slot = self
                .geometry
                .slot(value)
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            let (page, word) = self.page_word(slot / 64)?;
            page.bits[word].fetch_or(1u64 << (slot % 64), Ordering::AcqRel);
        } else if self.spec.nulls()[0] == NullPolicy::Equal {
            if !self.binding.nullable() {
                return Err(invariant(ValueViolation::RequiredNull));
            }
            self.null_present.store(true, Ordering::Release);
        }
        Ok(())
    }
    /// Caller must have crossed the graph's global Mark barrier. No hash is
    /// needed, and no phase completion is inferred from one loaded word.
    pub(super) fn contains_value_after_mark(&self, value: Option<i64>) -> ExecutionResult<bool> {
        self.check_failure()?;
        Ok(match value {
            None => {
                self.spec.nulls()[0] == NullPolicy::Equal
                    && self.null_present.load(Ordering::Acquire)
            }
            Some(v) => match self.geometry.slot(v) {
                None => false,
                Some(slot) => self.bits(slot / 64)? & (1u64 << (slot % 64)) != 0,
            },
        })
    }
    /// Caller must have crossed the global Fill barrier before ID queries.
    pub(super) fn lookup_value_after_fill(&self, value: Option<i64>) -> ExecutionResult<KeyMatch> {
        self.check_failure()?;
        if self.geometry.purpose != IndexPurpose::KeyIds {
            return Err(invariant(ValueViolation::Layout));
        }
        if value.is_none() && self.spec.nulls()[0] == NullPolicy::Unmatchable {
            return Ok(KeyMatch::Unmatchable);
        }
        if !self.contains_value_after_mark(value)? {
            return Ok(KeyMatch::Miss);
        }
        if let Some(value) = value {
            let slot = self
                .geometry
                .slot(value)
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            let word = slot / 64;
            let shard = self.shard(
                self.geometry
                    .shard_for_word(word)
                    .ok_or_else(|| invariant(ValueViolation::Bounds))?,
            )?;
            if shard.representatives.get().is_none() {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            let (page, local) = self.page_word(word)?;
            Ok(KeyMatch::Found(KeyId(
                page.ids
                    .as_ref()
                    .and_then(|v| v.get(local * 64 + slot % 64))
                    .ok_or_else(|| invariant(ValueViolation::Layout))?
                    .load(Ordering::Acquire),
            )))
        } else {
            if self.null_representative.get().is_none() {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            Ok(KeyMatch::Found(KeyId(self.null_id.load(Ordering::Acquire))))
        }
    }
    pub(super) fn representatives(&self, shard: usize) -> ExecutionResult<&RepresentativePages> {
        self.shard(shard)?
            .representatives
            .get()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Direct typed read/export locates the separately constructed NULL class."
        )
    )]
    pub(super) fn null_array(&self) -> ExecutionResult<Option<&ArrayRef>> {
        self.null_representative
            .get()
            .map(Option::as_ref)
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))
    }
    /// NULL is a separate final class after all non-NULL value slots. Main
    /// supplies the exact sum of W counted totals, after the Fill prerequisites.
    pub(super) fn finish_null(
        &self,
        nonnull_classes: usize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<()> {
        let result: ExecutionResult<_> = (|| {
            self.check_failure()?;
            checkpoint(work)?;
            if self.geometry.purpose != IndexPurpose::KeyIds
                || nonnull_classes > self.geometry.slots
            {
                return Err(invariant(ValueViolation::Layout));
            }
            let mut next_base = 0usize;
            for shard in 0..self.geometry.shards {
                checkpoint_at(shard, &|| checkpoint(work))?;
                let representatives = self.representatives(shard)?;
                if representatives.base != next_base {
                    return Err(invariant(ValueViolation::RowTransaction));
                }
                next_base = next_base
                    .checked_add(representatives.len)
                    .ok_or_else(arithmetic)?;
            }
            if next_base != nonnull_classes {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            let has_null = self.null_present.load(Ordering::Acquire);
            let class_count = nonnull_classes
                .checked_add(usize::from(has_null))
                .ok_or_else(arithmetic)?;
            let array = if has_null {
                if !self.binding.nullable() {
                    return Err(invariant(ValueViolation::RequiredNull));
                }
                let mut request = |l| work.owner.request(l).map_err(ExecutionFailure::from);
                let mut opaque = |b| {
                    work.owner
                        .observe_opaque_upper_bound(b)
                        .map_err(ExecutionFailure::from)
                };
                let mut creation = ValueSchemaCreation::new(
                    work.owner.storage_lifetime(),
                    &mut request,
                    &mut opaque,
                );
                preflight_null_construction(
                    self.binding.typed_binding().layout(),
                    1,
                    &mut creation,
                    work.control,
                )
                .map_err(copy_error)?;
                checkpoint(work)?;
                let array = new_null_array(self.binding.data_type(), 1);
                checkpoint(work)?;
                self.null_id.store(nonnull_classes, Ordering::Relaxed);
                Some(super::store::retain_created_array(
                    array,
                    &self.binding,
                    &work.owner.storage_lifetime(),
                    Arc::clone(&work.host),
                    work.control,
                )?)
            } else {
                None
            };
            self.null_representative
                .set(array)
                .map_err(|_| invariant(ValueViolation::RowTransaction))?;
            checkpoint(work)?;
            self.completed_ids
                .set(class_count)
                .map_err(|_| invariant(ValueViolation::RowTransaction))
        })();
        if let Err(cause) = &result {
            self.latch(cause.clone());
        }
        result
    }
}

// This private coordinate keeps REPRESENTATIVE_ROWS inside its constructing
// author. A NULL page is the original typed one-row array, never a fake value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Private location tag for the frozen Direct typed read/export API."
    )
)]
enum DirectPageId {
    Values { shard: usize, page: usize },
    Null,
}
impl Storage {
    /// This records constructed representative data only. Main still owns the
    /// complete input/task barriers and immutable Ready publication.
    pub(super) fn id_count_after_fill(&self) -> ExecutionResult<usize> {
        self.check_failure()?;
        if self.geometry.purpose != IndexPurpose::KeyIds {
            return Err(invariant(ValueViolation::Layout));
        }
        self.completed_ids
            .get()
            .copied()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Direct typed read/export preserves dense KeyIds and the original representative author."
        )
    )]
    fn locate_id(
        &self,
        id: KeyId,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<(DirectPageId, usize)> {
        checkpoint()?;
        let count = self.id_count_after_fill()?;
        if id.index() >= count {
            return Err(invariant(ValueViolation::Bounds));
        }
        if self.null_array()?.is_some() && id.index() == self.null_id.load(Ordering::Acquire) {
            return Ok((DirectPageId::Null, 0));
        }
        // Find the first exclusive class end above this ID. Empty shards may
        // share bases/ends, so searching only by base would choose the wrong one.
        let mut lower = 0usize;
        let mut upper = self.geometry.shards;
        while lower < upper {
            checkpoint()?;
            let middle = lower + (upper - lower) / 2;
            let representatives = self.representatives(middle)?;
            let end = representatives
                .base
                .checked_add(representatives.len)
                .ok_or_else(arithmetic)?;
            if end <= id.index() {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let representatives = self.representatives(lower)?;
        let local = id
            .index()
            .checked_sub(representatives.base)
            .filter(|local| *local < representatives.len)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        Ok((
            DirectPageId::Values {
                shard: lower,
                page: local / REPRESENTATIVE_ROWS,
            },
            local % REPRESENTATIVE_ROWS,
        ))
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Direct typed read/export preserves dense KeyIds and the original representative author."
        )
    )]
    fn read_array(&self, page: DirectPageId) -> ExecutionResult<&ArrayRef> {
        match page {
            DirectPageId::Values { shard, page } => self
                .representatives(shard)?
                .pages
                .get(page)
                .ok_or_else(|| invariant(ValueViolation::Bounds)),
            DirectPageId::Null => self
                .null_array()?
                .ok_or_else(|| invariant(ValueViolation::Bounds)),
        }
    }
    /// Borrow exact immutable representative arrays in requested ID order.
    /// Every location is O(log W); no slot scan, per-key Arrow array or codec.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Direct typed read/export preserves dense KeyIds and the original representative author."
        )
    )]
    pub(super) fn read_keys(
        &self,
        ids: &[KeyId],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::read::OwnedKeyReadBatch<'_>> {
        self.id_count_after_fill()?;
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        let mut coordinates = vector(owner, ids.len())?;
        for (ordinal, &id) in ids.iter().enumerate() {
            checkpoint_at(ordinal, &checkpoint)?;
            let (page, row) = self.locate_id(id, &checkpoint)?;
            coordinates.push(super::read::PositionedRow {
                page,
                selected: super::read::KeyReadRow { ordinal, row },
            });
        }
        let mut pages = Vec::new();
        super::read::append_positioned_pages(
            coordinates,
            |page| {
                Ok(super::read::KeyReadValues::Fixed {
                    array: self.read_array(page)?,
                    binding: &self.binding,
                })
            },
            &mut pages,
            owner,
            &checkpoint,
        )?;
        let mut columns = vector(owner, 1)?;
        columns.push(super::read::KeyReadColumn { pages });
        checkpoint()?;
        Ok(super::read::OwnedKeyReadBatch {
            columns,
            len: ids.len(),
            _storage: owner.storage_lifetime(),
        })
    }
    /// The existing output author enforces exact signed target/NULL semantics
    /// and owns actual selected copy plus result/Buffer creator custody.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Direct typed read/export preserves dense KeyIds and the original representative author."
        )
    )]
    pub(super) fn export_keys(
        &self,
        ids: &[KeyId],
        targets: &[FunctionValueType],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::ExportedKeys> {
        self.check_failure()?;
        super::export::validate_targets(&self.spec, targets, control)?;
        let read = self.read_keys(ids, owner, control)?;
        super::export::construct(&read, &self.spec, targets, owner, host, control)
    }
}

struct PendingPage {
    bits: Vec<AtomicU64>,
    ids: Option<Vec<AtomicUsize>>,
    target_words: usize,
    target_slots: usize,
    _storage: ValueTypeStorageLifetime,
}
pub(super) struct InitCursor {
    store: Arc<Storage>,
    shard: usize,
    words: Range<usize>,
    next_word: usize,
    pages: Vec<Arc<Page>>,
    pending: Option<PendingPage>,
    directory_storage: ValueTypeStorageLifetime,
    done: bool,
}
impl InitCursor {
    pub(super) fn new(
        store: Arc<Storage>,
        shard: usize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Self> {
        let result: ExecutionResult<_> = (|| {
            store.check_failure()?;
            checkpoint(work)?;
            let words = store
                .geometry
                .word_range(shard)
                .ok_or_else(|| invariant(ValueViolation::Bounds))?;
            if store.shards[shard].get().is_some() {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            let pages = vector(
                work.owner,
                ceiling(words.len(), PAGE_WORDS).ok_or_else(arithmetic)?,
            )?;
            Ok(Self {
                store: Arc::clone(&store),
                shard,
                next_word: words.start,
                words,
                pages,
                pending: None,
                directory_storage: work.owner.storage_lifetime(),
                done: false,
            })
        })();
        if let Err(cause) = &result {
            store.latch(cause.clone());
        }
        result
    }
    pub(super) fn advance(
        &mut self,
        mut quota: usize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<bool> {
        let result = self.advance_inner(&mut quota, work);
        if let Err(cause) = &result {
            self.store.latch(cause.clone());
        }
        result
    }
    fn advance_inner(&mut self, quota: &mut usize, work: &DirectWork<'_>) -> ExecutionResult<bool> {
        self.store.check_failure()?;
        checkpoint(work)?;
        if self.done {
            return Ok(true);
        }
        while *quota > 0 && self.next_word < self.words.end {
            if self.pending.is_none() {
                let words = (self.words.end - self.next_word).min(PAGE_WORDS);
                let slots = self
                    .store
                    .geometry
                    .slots
                    .saturating_sub(self.next_word * 64)
                    .min(words * 64);
                let bits = vector(work.owner, words)?;
                let ids = if self.store.geometry.purpose == IndexPurpose::KeyIds {
                    Some(vector(work.owner, slots)?)
                } else {
                    None
                };
                self.pending = Some(PendingPage {
                    bits,
                    ids,
                    target_words: words,
                    target_slots: slots,
                    _storage: work.owner.storage_lifetime(),
                });
            }
            let pending = self.pending.as_mut().expect("created pending page");
            while *quota > 0 && pending.bits.len() < pending.target_words {
                checkpoint(work)?;
                pending.bits.push(AtomicU64::new(0));
                *quota -= 1;
            }
            if let Some(ids) = pending.ids.as_mut() {
                while *quota > 0 && ids.len() < pending.target_slots {
                    checkpoint(work)?;
                    ids.push(AtomicUsize::new(0));
                    *quota -= 1;
                }
            }
            let complete = pending.bits.len() == pending.target_words
                && pending
                    .ids
                    .as_ref()
                    .is_none_or(|v| v.len() == pending.target_slots);
            if !complete {
                break;
            }
            let pending = self.pending.take().expect("complete pending page");
            let storage = join_storage(work, &pending._storage)?;
            let page = shared(
                work.owner,
                Page {
                    bits: pending.bits,
                    ids: pending.ids,
                    _storage: storage,
                },
            )?;
            self.next_word += pending.target_words;
            self.pages.push(page);
        }
        if self.next_word == self.words.end && self.pending.is_none() {
            let storage = join_storage(work, &self.directory_storage)?;
            let shard = shared(
                work.owner,
                Shard {
                    words: self.words.clone(),
                    pages: std::mem::take(&mut self.pages),
                    representatives: OnceLock::new(),
                    _storage: storage,
                },
            )?;
            self.store.shards[self.shard]
                .set(shard)
                .map_err(|_| invariant(ValueViolation::RowTransaction))?;
            self.done = true;
        }
        checkpoint(work)?;
        Ok(self.done)
    }
}

pub(super) struct MarkCursor {
    store: Arc<Storage>,
    input: KeyInput,
    position: usize,
    end: usize,
}
impl MarkCursor {
    pub(super) fn new(
        store: Arc<Storage>,
        input: &KeyInput,
        range: Range<usize>,
    ) -> ExecutionResult<Self> {
        let origin = Arc::clone(&store);
        let result: ExecutionResult<_> = (|| {
            store.check_failure()?;

            if range.start > range.end || range.end > input.len() {
                return Err(invariant(ValueViolation::Bounds));
            }
            Ok(Self {
                store,
                input: input.clone(),
                position: range.start,
                end: range.end,
            })
        })();
        if let Err(cause) = &result {
            origin.latch(cause.clone());
        }
        result
    }
    pub(super) fn advance(&mut self, quota: usize, work: &DirectWork<'_>) -> ExecutionResult<bool> {
        let result: ExecutionResult<_> = (|| {
            self.store.check_failure()?;
            checkpoint(work)?;
            let prepared = PreparedKeyBatch::from_captured(
                &self.store.spec,
                &self.input,
                work.allocator.clone(),
                &|| checkpoint(work),
            )?;
            if prepared.component_binding(0)?.nullable() && !self.store.binding.nullable() {
                return Err(invariant(ValueViolation::Layout));
            }
            let end = self.position.saturating_add(quota).min(self.end);
            visit_integer_occurrences(
                &prepared,
                self.position..end,
                &|| checkpoint(work),
                &mut |_, value| self.store.mark_value(value),
            )?;
            self.position = end;
            checkpoint(work)?;
            Ok(self.position == self.end)
        })();
        if let Err(cause) = &result {
            self.store.latch(cause.clone());
        }
        result
    }
}

pub(super) struct CountCursor {
    store: Arc<Storage>,
    shard: usize,
    position: usize,
    counts: Vec<usize>,
    total: usize,
    storage: ValueTypeStorageLifetime,
}
pub(super) struct CountedShard {
    store: Arc<Storage>,
    shard: usize,
    counts: Vec<usize>,
    total: usize,
    storage: ValueTypeStorageLifetime,
}
impl CountedShard {
    pub(super) fn total(&self) -> usize {
        self.total
    }
    pub(super) fn shard(&self) -> usize {
        self.shard
    }
}
impl CountCursor {
    pub(super) fn new(
        store: Arc<Storage>,
        shard: usize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Self> {
        let result: ExecutionResult<_> = (|| {
            store.check_failure()?;
            checkpoint(work)?;
            if store.geometry.purpose != IndexPurpose::KeyIds {
                return Err(invariant(ValueViolation::Layout));
            }
            let words = store.shard(shard)?.words.len();
            Ok(Self {
                store: Arc::clone(&store),
                shard,
                position: 0,
                counts: vector(work.owner, words)?,
                total: 0,
                storage: work.owner.storage_lifetime(),
            })
        })();
        if let Err(cause) = &result {
            store.latch(cause.clone());
        }
        result
    }
    pub(super) fn advance(&mut self, quota: usize, work: &DirectWork<'_>) -> ExecutionResult<bool> {
        let result: ExecutionResult<_> = (|| {
            self.store.check_failure()?;
            let range = self.store.shard(self.shard)?.words.clone();
            let end = self.position.saturating_add(quota).min(range.len());
            while self.position < end {
                checkpoint(work)?;
                let count = self.store.bits(range.start + self.position)?.count_ones() as usize;
                self.total = self.total.checked_add(count).ok_or_else(arithmetic)?;
                self.counts.push(count);
                self.position += 1;
            }
            checkpoint(work)?;
            Ok(self.position == range.len())
        })();
        if let Err(cause) = &result {
            self.store.latch(cause.clone());
        }
        result
    }
    pub(super) fn finish(self) -> ExecutionResult<CountedShard> {
        let origin = Arc::clone(&self.store);
        let result: ExecutionResult<_> = (|| {
            self.store.check_failure()?;
            if self.position != self.store.shard(self.shard)?.words.len() {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            Ok(CountedShard {
                store: self.store,
                shard: self.shard,
                counts: self.counts,
                total: self.total,
                storage: self.storage,
            })
        })();
        if let Err(cause) = &result {
            origin.latch(cause.clone());
        }
        result
    }
}
pub(super) struct PrefixCursor {
    counted: CountedShard,
    base: usize,
    position: usize,
    running: usize,
}
pub(super) struct PrefixedShard {
    store: Arc<Storage>,
    shard: usize,
    prefixes: Vec<usize>,
    base: usize,
    total: usize,
    _storage: ValueTypeStorageLifetime,
}
impl PrefixCursor {
    pub(super) fn new(counted: CountedShard, base: usize) -> ExecutionResult<Self> {
        let origin = Arc::clone(&counted.store);
        let result: ExecutionResult<_> = (|| {
            counted.store.check_failure()?;
            if base
                .checked_add(counted.total)
                .is_none_or(|v| v > counted.store.geometry.slots)
            {
                return Err(invariant(ValueViolation::Bounds));
            }
            Ok(Self {
                counted,
                base,
                position: 0,
                running: 0,
            })
        })();
        if let Err(cause) = &result {
            origin.latch(cause.clone());
        }
        result
    }
    pub(super) fn advance(&mut self, quota: usize, work: &DirectWork<'_>) -> ExecutionResult<bool> {
        let result: ExecutionResult<_> = (|| {
            self.counted.store.check_failure()?;
            let end = self
                .position
                .saturating_add(quota)
                .min(self.counted.counts.len());
            while self.position < end {
                checkpoint(work)?;
                let count = self.counted.counts[self.position];
                self.counted.counts[self.position] = self.running;
                self.running = self.running.checked_add(count).ok_or_else(arithmetic)?;
                self.position += 1;
            }
            checkpoint(work)?;
            Ok(self.position == self.counted.counts.len())
        })();
        if let Err(cause) = &result {
            self.counted.store.latch(cause.clone());
        }
        result
    }
    pub(super) fn finish(self) -> ExecutionResult<PrefixedShard> {
        let origin = Arc::clone(&self.counted.store);
        let result: ExecutionResult<_> = (|| {
            self.counted.store.check_failure()?;
            if self.position != self.counted.counts.len() || self.running != self.counted.total {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            Ok(PrefixedShard {
                store: self.counted.store,
                shard: self.counted.shard,
                prefixes: self.counted.counts,
                base: self.base,
                total: self.running,
                _storage: self.counted.storage,
            })
        })();
        if let Err(cause) = &result {
            origin.latch(cause.clone());
        }
        result
    }
}

struct Values<T: ArrowPrimitiveType> {
    values: Vec<T::Native>,
    value_storage: ValueTypeStorageLifetime,
    pages: Vec<ArrayRef>,
    directory_storage: ValueTypeStorageLifetime,
    word: usize,
    bits: u64,
    loaded: bool,
    emitted: usize,
    _type: PhantomData<T>,
}
enum FillValues {
    I8(Values<Int8Type>),
    I16(Values<Int16Type>),
    I32(Values<Int32Type>),
    I64(Values<Int64Type>),
}
pub(super) struct FillCursor {
    prefix: PrefixedShard,
    values: FillValues,
    done: bool,
}
impl FillCursor {
    pub(super) fn new(prefix: PrefixedShard, work: &DirectWork<'_>) -> ExecutionResult<Self> {
        let origin = Arc::clone(&prefix.store);
        let result: ExecutionResult<_> = (|| {
            prefix.store.check_failure()?;
            checkpoint(work)?;
            fn values<T: ArrowPrimitiveType>(
                count: usize,
                work: &DirectWork<'_>,
            ) -> ExecutionResult<Values<T>> {
                Ok(Values {
                    values: Vec::new(),
                    value_storage: ValueTypeStorageLifetime::pure(),
                    pages: vector(
                        work.owner,
                        ceiling(count, REPRESENTATIVE_ROWS).ok_or_else(arithmetic)?,
                    )?,
                    directory_storage: work.owner.storage_lifetime(),
                    word: 0,
                    bits: 0,
                    loaded: false,
                    emitted: 0,
                    _type: PhantomData,
                })
            }
            let values = match prefix.store.geometry.kind {
                IntegerKind::I8 => FillValues::I8(values(prefix.total, work)?),
                IntegerKind::I16 => FillValues::I16(values(prefix.total, work)?),
                IntegerKind::I32 => FillValues::I32(values(prefix.total, work)?),
                IntegerKind::I64 => FillValues::I64(values(prefix.total, work)?),
            };
            Ok(Self {
                prefix,
                values,
                done: false,
            })
        })();
        if let Err(cause) = &result {
            origin.latch(cause.clone());
        }
        result
    }
    pub(super) fn advance(&mut self, quota: usize, work: &DirectWork<'_>) -> ExecutionResult<bool> {
        let result: ExecutionResult<_> = (|| {
            self.prefix.store.check_failure()?;
            checkpoint(work)?;
            if self.done {
                return Ok(true);
            }
            let done = match &mut self.values {
                FillValues::I8(v) => fill(v, &self.prefix, quota, work)?,
                FillValues::I16(v) => fill(v, &self.prefix, quota, work)?,
                FillValues::I32(v) => fill(v, &self.prefix, quota, work)?,
                FillValues::I64(v) => fill(v, &self.prefix, quota, work)?,
            };
            self.done = done;
            Ok(done)
        })();
        if let Err(cause) = &result {
            self.prefix.store.latch(cause.clone());
        }
        result
    }
}
fn fill<T: ArrowPrimitiveType>(
    values: &mut Values<T>,
    prefix: &PrefixedShard,
    mut quota: usize,
    work: &DirectWork<'_>,
) -> ExecutionResult<bool>
where
    T::Native: TryFrom<i64>,
{
    let shard = prefix.store.shard(prefix.shard)?;
    if prefix.total == 0 {
        values.word = shard.words.len();
    }
    while quota > 0 && values.word < shard.words.len() {
        checkpoint(work)?;
        if !values.loaded {
            if prefix.prefixes[values.word] != values.emitted {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            values.bits = prefix.store.bits(shard.words.start + values.word)?;
            values.loaded = true;
        }
        if values.bits == 0 {
            values.word += 1;
            values.loaded = false;
            quota -= 1;
            continue;
        }
        let bit = values.bits.trailing_zeros() as usize;
        let slot = (shard.words.start + values.word) * 64 + bit;
        if values.emitted >= prefix.total {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        if values.values.is_empty() {
            let capacity = (prefix.total - values.emitted).min(REPRESENTATIVE_ROWS);
            values.values = vector(work.owner, capacity)?;
            values.value_storage = work.owner.storage_lifetime();
        }
        let scalar = T::Native::try_from(prefix.store.geometry.value(slot)?)
            .map_err(|_| invariant(ValueViolation::Carrier))?;
        let id = prefix
            .base
            .checked_add(values.emitted)
            .ok_or_else(arithmetic)?;
        prefix.store.write_id(slot, id)?;
        values.values.push(scalar);
        values.emitted += 1;
        values.bits &= values.bits - 1;
        quota -= 1;
        if values.values.len() == REPRESENTATIVE_ROWS || values.emitted == prefix.total {
            publish_values::<T>(values, prefix, work)?;
        }
    }
    if values.word == shard.words.len() {
        if values.emitted != prefix.total || !values.values.is_empty() {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        let output = RepresentativePages {
            base: prefix.base,
            len: prefix.total,
            pages: std::mem::take(&mut values.pages),
            _storage: values.directory_storage.clone(),
        };
        shard
            .representatives
            .set(output)
            .map_err(|_| invariant(ValueViolation::RowTransaction))?;
        checkpoint(work)?;
        return Ok(true);
    }
    Ok(false)
}
fn publish_values<T: ArrowPrimitiveType>(
    values: &mut Values<T>,
    prefix: &PrefixedShard,
    work: &DirectWork<'_>,
) -> ExecutionResult<()> {
    let mut request = |l| work.owner.request(l).map_err(ExecutionFailure::from);
    let mut opaque = |b| {
        work.owner
            .observe_opaque_upper_bound(b)
            .map_err(ExecutionFailure::from)
    };
    let mut creation =
        ValueSchemaCreation::new(work.owner.storage_lifetime(), &mut request, &mut opaque);
    preflight_arrow_node_structure_construction(
        prefix.store.binding.data_type(),
        false,
        &mut creation,
        &mut |_| checkpoint(work),
    )
    .map_err(copy_error)?;
    checkpoint(work)?;
    let array: ArrayRef = Arc::new(PrimitiveArray::<T>::new(
        ScalarBuffer::from(std::mem::take(&mut values.values)),
        None,
    ));
    checkpoint(work)?;
    let storage = join_storage(work, &values.value_storage)?;
    let array = super::store::retain_created_array(
        array,
        &prefix.store.binding,
        &storage,
        Arc::clone(&work.host),
        work.control,
    )?;
    values.pages.push(array);
    values.value_storage = ValueTypeStorageLifetime::pure();
    Ok(())
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Preserves the tested independent K preparation/task interface in production; current consumers use the shared captured graph path."
    )
)]
pub(super) struct AssignmentPage {
    pub(super) start: usize,
    matches: Vec<KeyMatch>,
    _storage: ValueTypeStorageLifetime,
}
impl AssignmentPage {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Preserves the tested independent K preparation/task interface in production; current consumers use the shared captured graph path."
        )
    )]
    pub(super) fn matches(&self) -> &[KeyMatch] {
        &self.matches
    }
}
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Preserves the tested independent K preparation/task interface in production; current consumers use the shared captured graph path."
    )
)]
pub(super) struct AssignCursor {
    store: Arc<Storage>,
    input: KeyInput,
    start: usize,
    position: usize,
    end: usize,
    matches: Vec<KeyMatch>,
    storage: ValueTypeStorageLifetime,
}
impl AssignCursor {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Preserves the tested independent K preparation/task interface in production; current consumers use the shared captured graph path."
        )
    )]
    pub(super) fn new(
        store: Arc<Storage>,
        input: &KeyInput,
        range: Range<usize>,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Self> {
        let origin = Arc::clone(&store);
        let result: ExecutionResult<_> = (|| {
            store.check_failure()?;
            checkpoint(work)?;
            if store.geometry.purpose != IndexPurpose::KeyIds {
                return Err(invariant(ValueViolation::Layout));
            }
            if range.start > range.end || range.end > input.len() {
                return Err(invariant(ValueViolation::Bounds));
            }
            Ok(Self {
                store,
                input: input.clone(),
                start: range.start,
                position: range.start,
                end: range.end,
                matches: vector(work.owner, range.len())?,
                storage: work.owner.storage_lifetime(),
            })
        })();
        if let Err(cause) = &result {
            origin.latch(cause.clone());
        }
        result
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Preserves the tested independent K preparation/task interface in production; current consumers use the shared captured graph path."
        )
    )]
    pub(super) fn advance(&mut self, quota: usize, work: &DirectWork<'_>) -> ExecutionResult<bool> {
        let result: ExecutionResult<_> = (|| {
            self.store.check_failure()?;
            let prepared = PreparedKeyBatch::from_captured(
                &self.store.spec,
                &self.input,
                work.allocator.clone(),
                &|| checkpoint(work),
            )?;
            let end = self.position.saturating_add(quota).min(self.end);
            visit_integer_occurrences(
                &prepared,
                self.position..end,
                &|| checkpoint(work),
                &mut |_, value| {
                    let found = self.store.lookup_value_after_fill(value)?;
                    if found == KeyMatch::Miss {
                        return Err(invariant(ValueViolation::RowTransaction));
                    }
                    self.matches.push(found);
                    Ok(())
                },
            )?;
            self.position = end;
            checkpoint(work)?;
            Ok(self.position == self.end)
        })();
        if let Err(cause) = &result {
            self.store.latch(cause.clone());
        }
        result
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Preserves the tested independent K preparation/task interface in production; current consumers use the shared captured graph path."
        )
    )]
    pub(super) fn finish(self) -> ExecutionResult<AssignmentPage> {
        let origin = Arc::clone(&self.store);
        let result: ExecutionResult<_> = (|| {
            self.store.check_failure()?;
            if self.position != self.end || self.matches.len() != self.end - self.start {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            Ok(AssignmentPage {
                start: self.start,
                matches: self.matches,
                _storage: self.storage,
            })
        })();
        if let Err(cause) = &result {
            origin.latch(cause.clone());
        }
        result
    }
}
fn checkpoint(work: &DirectWork<'_>) -> ExecutionResult<()> {
    work.control.checkpoint(0).map_err(ExecutionFailure::from)
}
fn arithmetic() -> ExecutionFailure {
    ConstructionFailure::from(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
    .into()
}
fn join_storage(
    work: &DirectWork<'_>,
    other: &ValueTypeStorageLifetime,
) -> ExecutionResult<ValueTypeStorageLifetime> {
    work.owner
        .storage_lifetime()
        .retaining_observed(other, |l| work.owner.request(l))
        .map_err(ExecutionFailure::from)
}
fn copy_error(cause: CopyConstructionError<ExecutionFailure>) -> ExecutionFailure {
    match cause {
        CopyConstructionError::Creator(e) => e,
        CopyConstructionError::Resource(e) => ConstructionFailure::Resource(e).into(),
        CopyConstructionError::Copy(e) => e.into_kernel().into(),
    }
}

#[cfg(test)]
#[path = "direct_int_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "direct_read_tests.rs"]
mod read_tests;
