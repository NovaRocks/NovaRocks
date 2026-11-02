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

//! ONE selected-copy extent author feeds this opaque take admission. Geometry
//! is not authority: only the supplied host grants its operation reservation.
#[cfg(test)]
use super::ScratchCoverage;
use super::copy_buffer_peak::CopyBufferPeak;
use super::{CopyError, CopyMode, add, buffer_extent, mul, zip};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{
    AggregateStateAllocator, KernelControlObservation, KernelEvaluationControl, KernelFailure,
};
use arrow_array::{Array, ArrayRef, UInt32Array, UInt64Array};
use arrow_buffer::ArrowNativeType;
use arrow_data::{ArrayData, transform::MutableArrayData};
use arrow_schema::{DataType, UnionMode};
use std::sync::Arc;

/// These are the two actual gather-index carriers used by the existing hosts.
/// The caller retains their original backing owner separately from a new grant.
#[derive(Clone)]
pub enum CopyIndices {
    UInt32(Arc<UInt32Array>),
    UInt64(Arc<UInt64Array>),
}
impl CopyIndices {
    pub(super) fn as_array(&self) -> &dyn Array {
        match self {
            Self::UInt32(a) => a.as_ref(),
            Self::UInt64(a) => a.as_ref(),
        }
    }
}
/// Borrowed actual index vocabulary. No Arc or semantic declaration is created
/// to inspect the original gather plan.
#[derive(Clone, Copy)]
pub enum CopyIndexView<'a> {
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
}
impl CopyIndices {
    pub fn borrowed(&self) -> CopyIndexView<'_> {
        match self {
            Self::UInt32(a) => CopyIndexView::UInt32(a),
            Self::UInt64(a) => CopyIndexView::UInt64(a),
        }
    }
}
impl CopyIndexView<'_> {
    pub(super) fn len(&self) -> usize {
        self.as_array().len()
    }
    pub(super) fn as_array(&self) -> &dyn Array {
        match self {
            Self::UInt32(a) => *a,
            Self::UInt64(a) => *a,
        }
    }
    pub(super) fn raw_value(&self, row: usize) -> usize {
        match self {
            Self::UInt32(a) => a.value(row).as_usize(),
            Self::UInt64(a) => a.value(row).as_usize(),
        }
    }
    pub(super) fn index_maximum(&self) -> usize {
        match self {
            Self::UInt32(_) => u32::MAX as usize,
            Self::UInt64(_) => usize::MAX,
        }
    }
}

#[derive(Debug)]
pub enum CopyOperationError {
    Preflight(CopyError),
    OriginalData(super::copy_diagnostic::OriginalCopyData),
    Control(KernelFailure),
}
impl From<CopyError> for CopyOperationError {
    fn from(e: CopyError) -> Self {
        match e {
            CopyError::Control(c) => Self::Control(c),
            other => Self::Preflight(other),
        }
    }
}
impl From<KernelFailure> for CopyOperationError {
    fn from(e: KernelFailure) -> Self {
        Self::Control(e)
    }
}

/// Unforgeable successful output of the SAME constructor/selection traversal.
/// No caller budget, input stock or function name can construct these facts.
#[derive(Debug)]
pub struct CopyOperationFacts {
    new_retained: usize,
    operation_peak: usize,
    metadata: usize,
    copied_root: Option<usize>,
}
impl CopyOperationFacts {
    pub fn retained_new_backing_upper(&self) -> usize {
        self.new_retained
    }
    pub fn operation_peak_bytes(&self) -> usize {
        self.operation_peak
    }
    pub fn metadata_upper_bytes(&self) -> usize {
        self.metadata
    }
    pub(super) fn borrowed_slice(metadata: usize) -> Result<Self, CopyError> {
        buffer_extent(metadata, 1)?;
        Ok(Self {
            new_retained: 0,
            operation_peak: metadata,
            metadata,
            copied_root: None,
        })
    }
    pub(super) fn admit_original_take_diagnostic(
        &mut self,
        diagnostic: &super::copy_diagnostic::OriginalTakeDiagnosticFacts,
    ) -> Result<(), CopyError> {
        self.operation_peak = add(self.operation_peak, diagnostic.operation_peak_bytes())?;
        Ok(())
    }
    pub(super) fn admit_original_concat_diagnostic(
        &mut self,
        diagnostic: &super::concat_diagnostic::OriginalConcatDiagnosticFacts,
    ) -> Result<(), CopyError> {
        self.operation_peak = add(self.operation_peak, diagnostic.operation_peak_bytes())?;
        Ok(())
    }
    pub(super) fn bind_original_copy(&mut self, original: &ArrayRef) {
        assert!(self.copied_root.is_none(), "copy facts bound only once");
        self.copied_root = Some(Arc::as_ptr(original) as *const () as usize);
    }
    pub(crate) fn matches_original_copy(&self, original: &ArrayRef) -> bool {
        self.copied_root == Some(Arc::as_ptr(original) as *const () as usize)
    }
}
fn original_unreserved_vec_upper<T>(count: usize) -> Result<usize, CopyError> {
    novarocks_type_contract::owned_resources::vec::original_fresh_push_requests::<
        T,
        novarocks_type_contract::ControlResourceError,
    >(count, &mut || Ok(()))
    .map_err(|cause| CopyError::Control(KernelFailure::from_domain(cause)))
}

fn original_filter_null_words_bytes(selected_count: usize) -> Result<usize, CopyError> {
    mul(add(selected_count / 64, 1)?, size_of::<u64>())
}

#[derive(Default)]
pub(super) struct CopyInvoiceTotals {
    initial_buffers: usize,
    selected_buffers: usize,
    exact_buffers: usize,
    metadata: usize,
    // These classifications are populated only for the paired structural port.
    // The existing full-operation facts and arithmetic remain unchanged.
    classify_structure: bool,
    result_metadata: usize,
    temporary_structure_backing: usize,
}
impl CopyInvoiceTotals {
    pub(super) fn structural() -> Self {
        Self {
            classify_structure: true,
            ..Self::default()
        }
    }
    pub(super) fn temporary_metadata_upper(&self) -> Result<usize, CopyError> {
        add(self.metadata, self.temporary_structure_backing)
    }
    pub(super) fn result_metadata_upper(&self) -> usize {
        self.result_metadata
    }

    /// Called by the existing selected-node visitor, once per actually emitted
    /// node. Borrowed Dictionary/ListView children are not walked a second time.
    pub(super) fn observe_result_node(&mut self, sources: &[&dyn Array]) -> Result<(), CopyError> {
        if !self.classify_structure {
            return Ok(());
        }
        let array = *sources
            .first()
            .ok_or(CopyError::Invalid("copy has no result carrier"))?;
        let ty = array.data_type();
        let (buffers, children) = match ty {
            DataType::Null => (0, 0),
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
                (2, 0)
            }
            DataType::Utf8View | DataType::BinaryView => {
                let mut buffers = 1;
                for source in sources {
                    let count = if matches!(ty, DataType::Utf8View) {
                        source
                            .as_any()
                            .downcast_ref::<arrow_array::StringViewArray>()
                            .ok_or(CopyError::Invalid("copy view has wrong concrete carrier"))?
                            .data_buffers()
                            .len()
                    } else {
                        source
                            .as_any()
                            .downcast_ref::<arrow_array::BinaryViewArray>()
                            .ok_or(CopyError::Invalid("copy view has wrong concrete carrier"))?
                            .data_buffers()
                            .len()
                    };
                    buffers = add(buffers, count)?;
                }
                (buffers, 0)
            }
            DataType::List(_) | DataType::LargeList(_) | DataType::Map(..) => (1, 1),
            DataType::ListView(_) | DataType::LargeListView(_) => (2, 1),
            DataType::FixedSizeList(..) => (0, 1),
            DataType::Struct(fields) => (0, fields.len()),
            DataType::Dictionary(..) => (1, 1),
            DataType::Union(fields, mode) => {
                (if *mode == UnionMode::Dense { 2 } else { 1 }, fields.len())
            }
            DataType::RunEndEncoded(ends, _) => {
                // The run-end primitive is constructed beside the visited values.
                self.result_metadata = add(
                    self.result_metadata,
                    result_node_metadata_upper(ends.data_type(), 1, false, 0)?,
                )?;
                (0, 2)
            }
            _ => (1, 0),
        };
        self.result_metadata = add(
            self.result_metadata,
            result_node_metadata_upper(ty, buffers, true, children)?,
        )?;
        Ok(())
    }
    fn initial(&mut self, bytes: usize) -> Result<(), CopyError> {
        self.initial_buffers = add(self.initial_buffers, zip::rounded_capacity(bytes)?)?;
        Ok(())
    }
    fn required(&mut self, bytes: usize) -> Result<(), CopyError> {
        self.selected_buffers = add(self.selected_buffers, zip::rounded_capacity(bytes)?)?;
        Ok(())
    }
    fn exact(&mut self, bytes: usize) -> Result<(), CopyError> {
        self.exact_buffers = add(
            self.exact_buffers,
            CopyBufferPeak::exact(bytes)?.retained_upper(),
        )?;
        Ok(())
    }
    fn temporary_exact(&mut self, bytes: usize) -> Result<(), CopyError> {
        // The original author already bounds these concrete index/map Vecs.
        // Reuse that SAME extent for the temporary structural classification.
        if self.classify_structure {
            self.temporary_structure_backing = add(
                self.temporary_structure_backing,
                CopyBufferPeak::exact(bytes)?.retained_upper(),
            )?;
        }
        self.exact(bytes)
    }
    pub(super) fn interleave_exact_payload(&mut self, bytes: usize) -> Result<(), CopyError> {
        self.exact(bytes)
    }
    pub(super) fn interleave_validity(&mut self, rows: usize) -> Result<(), CopyError> {
        self.exact(zip::rounded_capacity(add(
            rows / 8,
            usize::from(!rows.is_multiple_of(8)),
        )?)?)
    }
    pub(super) fn empty_offsets(&mut self, width: usize) -> Result<(), CopyError> {
        // new_empty's from_len_zeroed has one initial rounded allocation and
        // no append/realloc path. This is a locked upper bound, not Layout.
        self.exact(zip::rounded_capacity(width)?)
    }
    pub(super) fn original_comparator_scratch(&mut self, bytes: usize) -> Result<(), CopyError> {
        self.metadata = add(self.metadata, bytes)?;
        Ok(())
    }
    pub(super) fn original_nullable_union_scratch(
        &mut self,
        fields: usize,
        rows: usize,
        mode: CopyMode,
        union_mode: UnionMode,
    ) -> Result<(), CopyError> {
        // take_union_type_ids keeps take_native's bytes while collecting the
        // replacement type ids. Only the replacement survives in the result.
        self.temporary_exact(rows)?;
        if union_mode == UnionMode::Sparse {
            let index_width = match mode {
                CopyMode::Take { index_maximum, .. } if index_maximum > u32::MAX as usize => 8,
                _ => 4,
            };
            // Each non-selected child independently builds non-null dummy
            // indices. ArrayIter has an exact hint but is not TrustedLen, so
            // use the original fresh Vec author, including minimum capacity.
            // The empty child skips this request.
            let dummy = if index_width == 8 {
                original_unreserved_vec_upper::<u64>(rows)?
            } else {
                original_unreserved_vec_upper::<u32>(rows)?
            };
            self.temporary_exact(mul(fields.saturating_sub(1), dummy)?)?;
        } else {
            for _ in 0..fields {
                // Dense offsets retain indices.nulls. A mixed per-child mask
                // calls filter_bits_compress: Vec<u64>(count /64 +1), not a
                // MutableBuffer round64 bitmap. count <= this selected length.
                self.temporary_exact(original_filter_null_words_bytes(rows)?)?;
            }
        }
        Ok(())
    }
    pub(super) fn original_union_padding_scratch(&mut self, nulls: usize) -> Result<(), CopyError> {
        self.temporary_exact(nulls)
    }
    pub(super) fn original_null_scratch_metadata(
        &mut self,
        metadata: usize,
    ) -> Result<(), CopyError> {
        // new_null_array -> to_data -> build -> make_array temporarily keeps
        // the original packet/wrapper beside the replacement packet/wrapper.
        // Reuse the same source-derived node envelope for those operations.
        self.metadata = add(self.metadata, mul(metadata, 3)?)?;
        if self.classify_structure {
            self.result_metadata = add(self.result_metadata, metadata)?;
        }
        Ok(())
    }
    pub(super) fn original_union_validation_scratch(&mut self) -> Result<(), CopyError> {
        // ArrayDataBuilder::build validates this Union root; validate_values
        // authors one Vec<Option<usize>> indexed by the permitted type-id set.
        self.temporary_exact(mul(128, size_of::<Option<usize>>())?)
    }
    pub(super) fn selected_payload(
        &mut self,
        bytes: usize,
        mode: CopyMode,
    ) -> Result<(), CopyError> {
        if mode.is_take() {
            self.exact(bytes)
        } else {
            self.required(bytes)
        }
    }
    /// This is buffer geometry, not another value traversal. Exact payload and
    /// child extents are supplied by the existing bytes/map_ranges/run authors.
    pub(super) fn selected_node(
        &mut self,
        array: &dyn Array,
        rows: usize,
        mode: CopyMode,
    ) -> Result<(), CopyError> {
        let ty = array.data_type();
        let bitmap = add(rows / 8, usize::from(!rows.is_multiple_of(8)))?;
        // The null bitmap may borrow indices in take, or be fresh. A fresh
        // rounded envelope is conservative; existing input bytes are NOT stock.
        self.required(bitmap)?;
        if let Some(width) = ty.primitive_width() {
            return self.required(mul(rows, width)?);
        }
        match ty {
            DataType::Null => Ok(()),
            DataType::Boolean => self.required(bitmap),
            DataType::FixedSizeBinary(w) => self.required(mul(
                rows,
                usize::try_from(*w)
                    .map_err(|_| CopyError::Invalid("copy frozen carrier has a negative width"))?,
            )?),
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
                let width = if matches!(ty, DataType::LargeUtf8 | DataType::LargeBinary) {
                    8
                } else {
                    4
                };
                // extend_offsets reserves an input slice with one additional
                // offset before appending. It can transiently request rows+2.
                self.required(mul(add(rows, 2)?, width)?)
            }
            DataType::Utf8View | DataType::BinaryView => self.required(mul(rows, 16)?),
            DataType::Dictionary(key, _) => self.required(mul(
                rows,
                key.primitive_width()
                    .ok_or(CopyError::Invalid("copy frozen primitive width is absent"))?,
            )?),
            DataType::List(_) | DataType::Map(..) => self.required(mul(add(rows, 2)?, 4)?),
            DataType::LargeList(_) => self.required(mul(add(rows, 2)?, 8)?),
            DataType::ListView(_) => self.required(mul(rows, 8)?),
            DataType::LargeListView(_) => self.required(mul(rows, 16)?),
            DataType::Struct(_) => Ok(()),
            DataType::FixedSizeList(_, w) => {
                if mode.is_take() {
                    // Original take_value_indices_from_fixed_size_list builds
                    // a UInt32 child index and validity, including NULL parents.
                    let child = mul(
                        rows,
                        usize::try_from(*w).map_err(|_| {
                            CopyError::Invalid("copy frozen carrier has a negative width")
                        })?,
                    )?;
                    self.required(mul(child, 4)?)?;
                    self.required(add(child / 8, usize::from(!child.is_multiple_of(8)))?)?;
                }
                Ok(())
            }
            DataType::Union(fields, union_mode) => {
                self.required(rows)?;
                if *union_mode == UnionMode::Dense {
                    // Original offsets and final offsets coexist. Each field's
                    // filter has a mask and filtered offsets. Its Int32 route
                    // does not enable FilterBuilder's heap strategy tables.
                    self.required(mul(rows, 8)?)?;
                    for _ in fields.iter() {
                        self.required(bitmap)?;
                        self.required(mul(rows, 4)?)?;
                        let headers = add(
                            size_of::<arrow_array::BooleanArray>(),
                            mul(2, size_of::<arrow_array::Int32Array>())?,
                        )?;
                        self.metadata = add(
                            self.metadata,
                            add(headers, mul(3, 2 * size_of::<usize>())?)?,
                        )?;
                    }
                }
                Ok(())
            }
            DataType::RunEndEncoded(ends, _) => {
                let width = ends
                    .data_type()
                    .primitive_width()
                    .ok_or(CopyError::Invalid("copy frozen primitive width is absent"))?;
                if mode.is_take() {
                    // physical_indices_for_take retains Vec<Option<usize>>.
                    // With nullable indices it first collects valid logical
                    // integers (input width <=8) and maps only those. The
                    // RunEndBuffer mapping owns ordered+physical usize tables.
                    // Non-null input also converts its mapped Vec to Option.
                    self.temporary_exact(original_unreserved_vec_upper::<Option<usize>>(rows)?)?;
                    let valid_logical = original_unreserved_vec_upper::<u64>(rows)?;
                    self.temporary_exact(valid_logical)?;
                    self.temporary_exact(mul(2, mul(rows, size_of::<usize>())?)?)?;
                    // take_run's new per-run validity builder has at most one
                    // bit per logical occurrence, independently of value merge.
                    self.required(bitmap)?;
                    // Both original builders start at one and double; final
                    // emitted runs cannot exceed actual selected logical rows.
                    self.initial(width)?;
                    self.initial(8)?;
                    self.required(mul(rows, width)?)?;
                    self.required(mul(rows, 8)?)
                } else {
                    // Original MutableArrayData run::build_extend_arrays owns
                    // a growing Vec<u8> beside the destination run-end buffer.
                    // Its selected physical run count is bounded by THIS
                    // invocation's logical rows; no source stock is copied here.
                    self.required(mul(rows, width)?)?;
                    self.required(mul(rows, width)?)
                }
            }
            other => Err(CopyError::unsupported(other)),
        }
    }
    pub(super) fn original_concat_node(
        &mut self,
        array: &dyn Array,
        source_count: usize,
        rows: usize,
    ) -> Result<(), CopyError> {
        // Each specialized original concat owns a per-source reference table.
        // List/Struct/Run paths additionally own Arc slice and output tables.
        let borrowed = mul(source_count, size_of::<&dyn Array>())?;
        let slices = mul(source_count, size_of::<ArrayRef>())?;
        self.metadata = add(self.metadata, add(borrowed, slices)?)?;
        let fields = match array.data_type() {
            DataType::Struct(fields) => fields.len(),
            DataType::Union(fields, _) => fields.len(),
            _ => 0,
        };
        self.metadata = add(self.metadata, mul(fields, size_of::<ArrayRef>())?)?;
        if matches!(
            array.data_type(),
            DataType::Null | DataType::RunEndEncoded(..)
        ) {
            // Null concat creates no row-sized payload. Run is invoiced by the
            // SAME run child-range author using actual physical ends below.
            return Ok(());
        }
        self.selected_node(array, rows, CopyMode::Extend)
    }

    pub(super) fn original_run_concat_scratch(
        &mut self,
        physical_rows: usize,
        source_count: usize,
        end_width: usize,
    ) -> Result<(), CopyError> {
        // Pinned concat_run_arrays stores a borrowed Run vector, an adjustment
        // vector (N+1), values_slice Arc vector and borrowed child-concat vector.
        self.metadata = add(self.metadata, mul(source_count, size_of::<&dyn Array>())?)?;
        self.exact(mul(add(source_count, 1)?, end_width)?)?;
        // A recursively reached run child of original Mutable fallback can
        // retain its growing extension Vec beside the destination end buffer.
        self.required(mul(physical_rows, end_width)?)?;
        self.required(mul(physical_rows, end_width)?)
    }

    pub(super) fn original_dictionary_merge_scratch(
        &mut self,
        values: &[&dyn Array],
        key: &DataType,
        source_count: usize,
    ) -> Result<(), CopyError> {
        let value_type = values
            .first()
            .ok_or(CopyError::Invalid(
                "original dictionary concat has no value source",
            ))?
            .data_type();
        if !value_type.is_primitive()
            && !matches!(
                value_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
            )
        {
            return Ok(());
        }
        let mut domain = 0;
        for array in values {
            domain = add(domain, array.len())?;
            self.required(add(
                array.len() / 8,
                usize::from(!array.len().is_multiple_of(8)),
            )?)?;
        }
        let key_width = key
            .primitive_width()
            .ok_or(CopyError::Invalid("copy frozen primitive width is absent"))?;
        // Actual masked-values Vec tuples (index, optional borrowed bytes),
        // one native mapping per domain element and interleave source choices.
        self.temporary_exact(mul(domain, size_of::<(usize, Option<&[u8]>)>())?)?;
        self.temporary_exact(mul(domain, key_width)?)?;
        self.temporary_exact(mul(domain, size_of::<(usize, usize)>())?)?;
        self.metadata = add(
            self.metadata,
            mul(source_count, size_of::<Vec<(usize, Option<&[u8]>)>>())?,
        )?;
        self.metadata = add(self.metadata, mul(source_count, size_of::<Vec<u64>>())?)?;
        // ONE original fixed interner formula, with capacity bounded by the
        // actual full domain; no hash/equality computation or key-limit gate.
        // u64 has the largest installed dictionary native key width/alignment.
        let capacity = u64::try_from(domain).map_err(|_| CopyError::arithmetic(None))?;
        let shift = capacity
            .checked_add(128)
            .ok_or_else(|| CopyError::arithmetic(None))?
            .leading_zeros();
        let buckets = (u64::MAX >> shift).saturating_add(1);
        let buckets = usize::try_from(buckets).map_err(|_| CopyError::arithmetic(None))?;
        self.temporary_exact(mul(buckets, size_of::<Option<(Option<&[u8]>, u64)>>())?)
    }

    pub(super) fn constructor(
        &mut self,
        data: &ArrayData,
        sources: usize,
        capacity: usize,
    ) -> Result<(), CopyError> {
        // Actual Arrow MutableArrayData body and vectors, with two per-source
        // Box<dyn Fn> tables. Across pinned transform/*.rs, the largest capture
        // is two borrowed slices plus two usize scalars; no cloned value heap.
        let refs = mul(sources, size_of::<&ArrayData>())?;
        let tables = mul(sources, mul(2, size_of::<Box<dyn Fn()>>())?)?;
        let captures = mul(sources, mul(2, size_of::<(&[u8], &[u8], usize, usize)>())?)?;
        let children = mul(
            data.child_data().len(),
            size_of::<MutableArrayData<'static>>(),
        )?;
        let buffers = mul(data.buffers().len(), size_of::<arrow_buffer::Buffer>())?;
        let own = add(
            size_of::<MutableArrayData<'static>>(),
            add(add(refs, tables)?, add(captures, add(children, buffers)?)?)?,
        )?;
        // Vec geometric growth can retain two capacities and its predecessor.
        self.metadata = add(self.metadata, mul(own, 3)?)?;
        self.initial(add(capacity / 8, usize::from(!capacity.is_multiple_of(8)))?)?;
        let ty = data.data_type();
        if let Some(width) = ty.primitive_width() {
            return self.initial(mul(capacity, width)?);
        }
        match ty {
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
                self.initial(mul(
                    add(capacity, 1)?,
                    if matches!(ty, DataType::LargeUtf8 | DataType::LargeBinary) {
                        8
                    } else {
                        4
                    },
                )?)?;
                self.initial(capacity)
            }
            DataType::Boolean => {
                self.initial(add(capacity / 8, usize::from(!capacity.is_multiple_of(8)))?)
            }
            DataType::FixedSizeBinary(w) => self.initial(mul(
                capacity,
                usize::try_from(*w)
                    .map_err(|_| CopyError::Invalid("copy frozen carrier has a negative width"))?,
            )?),
            DataType::List(_) | DataType::Map(..) => self.initial(mul(add(capacity, 1)?, 4)?),
            DataType::LargeList(_) => self.initial(mul(add(capacity, 1)?, 8)?),
            DataType::ListView(_) => self.initial(mul(capacity, 8)?),
            DataType::LargeListView(_) => self.initial(mul(capacity, 16)?),
            DataType::Utf8View | DataType::BinaryView => self.initial(mul(capacity, 16)?),
            DataType::Dictionary(key, _) => self.initial(mul(
                capacity,
                key.primitive_width()
                    .ok_or(CopyError::Invalid("copy frozen primitive width is absent"))?,
            )?),
            DataType::Union(_, mode) => {
                self.initial(capacity)?;
                if *mode == UnionMode::Dense {
                    self.initial(mul(capacity, 4)?)?;
                }
                Ok(())
            }
            DataType::Null
            | DataType::Struct(_)
            | DataType::FixedSizeList(..)
            | DataType::RunEndEncoded(..) => Ok(()),
            other => Err(CopyError::unsupported(other)),
        }
    }
    pub(super) fn finish(self, source_metadata: usize) -> Result<CopyOperationFacts, CopyError> {
        // Summing constructor and selected envelopes avoids pairing distinct
        // nodes by guessed identity. Each actual buffer's initial/required is
        // <= their sum. Pinned reserve max(round64(required),2*previous) gives
        // retained <=2*sum and old+new <=3*sum, independently of in-place realloc.
        let high = add(self.initial_buffers, self.selected_buffers)?;
        let mutable_peak = CopyBufferPeak::mutable(0, high)?;
        let retained = add(self.exact_buffers, mutable_peak.retained_upper())?;
        let metadata = add(self.metadata, source_metadata)?;
        let peak = add(
            add(self.exact_buffers, mutable_peak.transient_upper())?,
            metadata,
        )?;
        buffer_extent(peak, 1)?;
        Ok(CopyOperationFacts {
            new_retained: retained,
            operation_peak: peak,
            metadata,
            copied_root: None,
        })
    }
}

/// The sole result-custody node author. This conservative construction
/// envelope contains private owners and the concrete Array Arc; callers must
/// not charge an additional wrapper Arc for the same constructor.
pub(super) fn result_node_metadata_upper(
    ty: &DataType,
    buffers: usize,
    has_nulls: bool,
    children: usize,
) -> Result<usize, CopyError> {
    crate::arrow_result_custody::custody_typed_node_metadata_upper_bound(
        ty, buffers, has_nulls, children,
    )
    .map_err(CopyError::Control)
}

struct CopySources<C> {
    source: ArrayRef,
    indices: CopyIndices,
    _original_owner: C,
}
impl<C> crate::arrow_result_custody::CopyInputBacking for CopySources<C> {
    fn source_count(&self) -> usize {
        1
    }
    fn source_array_at(&self, ordinal: usize) -> Option<&dyn Array> {
        (ordinal == 0).then_some(self.source.as_ref())
    }
    fn index_array(&self) -> Option<&dyn Array> {
        Some(self.indices.as_array())
    }
}

pub struct RetainedTakeResult {
    values: ArrayRef,
    facts: CopyOperationFacts,
    new_backing_envelope: usize,
    new_buffer_stock: usize,
}
impl RetainedTakeResult {
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }
    pub fn facts(&self) -> &CopyOperationFacts {
        &self.facts
    }
    pub fn new_backing_envelope(&self) -> usize {
        self.new_backing_envelope
    }
    pub fn new_buffer_stock(&self) -> usize {
        self.new_buffer_stock
    }
    pub fn into_values(self) -> ArrayRef {
        self.values
    }
}

/// Complete standalone take: actual host scratch + pre-copy opaque admission +
/// ONE original Arrow take + ONE Buffer custody handoff. Existing entry points
/// and all compiled/public callers remain unchanged until separately activated.
/// `original_owner` is the caller's actual source/index lease, not a new budget.
/// Original Arrow Data strings and panics propagate without reinterpretation.
pub fn take_copy_in<C: Send + Sync + 'static>(
    source: ArrayRef,
    indices: CopyIndices,
    original_owner: C,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<RetainedTakeResult, CopyOperationError> {
    // Pin input/index backing before its original owner on EVERY exit,
    // including constructor refusal before any actual output exists.
    let sources = CopySources {
        source,
        indices,
        _original_owner: original_owner,
    };
    let observed = KernelControlObservation::new(control);
    observed.checkpoint(0)?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
    observed.checkpoint(0)?;
    let allocator = HostAggregateAllocator::try_new(host)?;
    let mut work = EvaluationCheckpoints::new(&observed);
    // SAME borrowed structural author; no allocating to_data precedes grant.
    let source_metadata = add(
        crate::array_backing_geometry::copy_metadata_bytes(sources.source.as_ref(), &mut work)?,
        crate::array_backing_geometry::copy_metadata_bytes(sources.indices.as_array(), &mut work)?,
    )?;
    let metadata_scope = charge.reserve_operation(source_metadata)?;
    work.flush()?;
    let mut invoice = CopyInvoiceTotals::default();
    super::preflight_original_take_with_invoice(
        sources.source.as_ref(),
        &sources.indices,
        |boundary| if boundary { work.flush() } else { work.step() },
        &allocator,
        &mut invoice,
    )?;
    // Both original input carriers are still borrowed by their source owner.
    // Every raw-index block/range was backed by the actual host scratch vector.
    drop(metadata_scope);
    let mut facts = invoice.finish(source_metadata)?;
    work.flush()?;
    let diagnostic = super::copy_diagnostic::OriginalTakeDiagnosticFacts::try_new_observed(
        sources.source.data_type(),
        &mut |boundary| {
            if boundary {
                work.flush().map_err(CopyError::Control)
            } else {
                work.step().map_err(CopyError::Control)
            }
        },
    )?;
    facts.admit_original_take_diagnostic(&diagnostic)?;
    let mut reservation = charge.reserve_operation(facts.operation_peak_bytes())?;
    work.flush()?;
    let copied =
        match arrow_select::take::take(sources.source.as_ref(), sources.indices.as_array(), None) {
            Ok(copied) => copied,
            Err(error) => {
                // Error construction and the ONE original Display-to-String author
                // are under the earlier actual diagnostic peak grant. Keep the
                // returned full text charged until its real last Drop.
                return Err(CopyOperationError::OriginalData(diagnostic.retain(
                    error,
                    charge,
                    &mut reservation,
                )?));
            }
        };
    facts.bind_original_copy(&copied);
    work.flush()?;
    let result = crate::arrow_result_custody::retain_copied_result_backing(
        copied,
        sources,
        charge,
        &mut reservation,
        allocator,
        &facts,
        &mut work,
    )?;
    work.finish()?;
    Ok(RetainedTakeResult {
        values: result.values,
        facts,
        new_backing_envelope: result.new_backing_envelope,
        new_buffer_stock: result.new_buffer_stock,
    })
}

#[cfg(test)]
#[path = "take_host_tests.rs"]
mod tests;
