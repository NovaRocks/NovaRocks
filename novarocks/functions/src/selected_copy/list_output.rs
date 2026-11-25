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

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, MapArray, StructArray, make_array};
use arrow_buffer::{NullBuffer, NullBufferBuilder, OffsetBuffer};
use arrow_data::{ArrayData, transform::MutableArrayData};
use arrow_schema::{DataType, FieldRef, Fields};

use super::{
    ChildScratchVec, CopyError, CopyLayout, CopyObservation, ExtendSegment, ScratchCoverage,
    WorkObservation, add, limit, mul, preflight_extend_multi_with_observation,
    take_host::{CopyInvoiceTotals, result_node_metadata_upper},
};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::array_projection::{ArrayProjectionFailure, ArrayProjectionOperation};
use crate::arrow_result_custody::{CopyInputBacking, retain_copied_result_backing};
use crate::kernel_control::internal;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{AggregateStateAllocator, KernelFailure};

struct Sources(ChildScratchVec<ArrayRef>);

impl CopyInputBacking for Sources {
    fn source_count(&self) -> usize {
        self.0.len()
    }

    fn source_array_at(&self, ordinal: usize) -> Option<&dyn Array> {
        self.0.get(ordinal).map(AsRef::as_ref)
    }

    fn index_array(&self) -> Option<&dyn Array> {
        None
    }
}

/// Construct compact List rows from the original selected ranges. The same
/// copy traversal preflights the child format and authors the backing envelope.
pub(crate) fn list_from_segments_in(
    field: &FieldRef,
    sources: &[ArrayRef],
    segments: &[ExtendSegment],
    row_lengths: &[Option<usize>],
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let layout = ListOutput::prepare(field, row_lengths, work)?;
    copy_segments_in(
        sources,
        segments,
        layout.total,
        Some(layout),
        host,
        allocator,
        work,
    )
}

/// Exact child gather for callers whose result is the selected element, not a List.
pub(crate) fn array_from_segments_in(
    sources: &[ArrayRef],
    segments: &[ExtendSegment],
    total: usize,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    copy_segments_in(sources, segments, total, None, host, allocator, work)
}

/// Preflight the outer List before producing any of its child values.
pub(crate) fn list_child_count(
    row_lengths: &[Option<usize>],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    Ok(list_geometry(row_lengths, work)?.0)
}

fn list_geometry(
    row_lengths: &[Option<usize>],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(usize, bool), KernelFailure> {
    let copy = CopyError::into_kernel;
    let mut total = 0usize;
    let mut has_null = false;
    for len in row_lengths {
        work.step()?;
        has_null |= len.is_none();
        total = add(total, len.unwrap_or(0)).map_err(copy)?;
        limit(total, i32::MAX as usize, CopyLayout::List32).map_err(copy)?;
    }
    Ok((total, has_null))
}

struct ListOutput<'a> {
    field: &'a FieldRef,
    row_lengths: &'a [Option<usize>],
    total: usize,
    has_null: bool,
}

impl<'a> ListOutput<'a> {
    fn prepare(
        field: &'a FieldRef,
        row_lengths: &'a [Option<usize>],
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let (total, has_null) = list_geometry(row_lengths, work)?;
        Ok(Self {
            field,
            row_lengths,
            total,
            has_null,
        })
    }

    fn invoice(&self, invoice: &mut CopyInvoiceTotals) -> Result<(), CopyError> {
        let count = add(self.row_lengths.len(), 1)?;
        invoice.interleave_exact_payload(mul(count, size_of::<i32>())?)?;
        if self.has_null {
            invoice.interleave_validity(self.row_lengths.len())?;
        }
        invoice.original_comparator_scratch(result_node_metadata_upper(
            &DataType::List(Arc::clone(self.field)),
            1,
            self.has_null,
            1,
        )?)
    }

    fn build(
        &self,
        child: ArrayRef,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<ArrayRef, KernelFailure> {
        let (offsets, nulls) = offsets_and_validity(self.row_lengths, self.has_null, work)?;
        Ok(Arc::new(
            ListArray::try_new(Arc::clone(self.field), offsets, child, nulls)
                .map_err(arrow_failure)?,
        ))
    }
}

fn offsets_and_validity(
    row_lengths: &[Option<usize>],
    has_null: bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(OffsetBuffer<i32>, Option<NullBuffer>), KernelFailure> {
    let offset_count = add(row_lengths.len(), 1).map_err(CopyError::into_kernel)?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(offset_count)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut nulls = has_null.then(|| NullBufferBuilder::new(row_lengths.len()));
    let mut offset = 0i32;
    offsets.push(offset);
    for len in row_lengths {
        work.step()?;
        if let Some(nulls) = &mut nulls {
            nulls.append(len.is_some());
        }
        offset += len.unwrap_or(0) as i32;
        offsets.push(offset);
    }
    work.flush()?;
    Ok((
        OffsetBuffer::new(offsets.into()),
        nulls.and_then(|mut nulls| nulls.finish()),
    ))
}

fn arrow_failure(source: arrow_schema::ArrowError) -> KernelFailure {
    KernelFailure::from_domain(ArrayProjectionFailure::Arrow {
        operation: ArrayProjectionOperation::RebuildArray,
        source,
    })
}

fn observe_sources(
    sources: &[ArrayRef],
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(Sources, usize), KernelFailure> {
    let copy = CopyError::into_kernel;
    let mut backing =
        ChildScratchVec::try_with_capacity(sources.len(), Some(allocator)).map_err(copy)?;
    let mut metadata = 0usize;
    for source in sources {
        work.step()?;
        metadata = add(
            metadata,
            crate::array_backing_geometry::copy_metadata_bytes(source.as_ref(), work)?,
        )
        .map_err(copy)?;
        backing.try_push(Arc::clone(source)).map_err(copy)?;
    }
    Ok((Sources(backing), metadata))
}

/// Build zip's anonymous, all-valid ROW carrier from independently funded fields.
/// Only the two outer nodes invest new backing; every selected field is a source.
pub(crate) fn list_from_struct_children_in(
    item: &FieldRef,
    fields: &Fields,
    children: &[ArrayRef],
    row_lengths: &[Option<usize>],
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let copy = CopyError::into_kernel;
    let layout = ListOutput::prepare(item, row_lengths, work)?;
    let (backing, metadata) = observe_sources(children, allocator, work)?;
    let mut invoice = CopyInvoiceTotals::default();
    layout.invoice(&mut invoice).map_err(copy)?;
    invoice
        .original_comparator_scratch(
            result_node_metadata_upper(item.data_type(), 0, false, children.len()).map_err(copy)?,
        )
        .map_err(copy)?;
    invoice
        .original_comparator_scratch(metadata)
        .map_err(copy)?;
    let mut facts = invoice
        .finish(mul(metadata, 2).map_err(copy)?)
        .map_err(copy)?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    work.flush()?;
    let mut reservation = charge.reserve_operation(facts.operation_peak_bytes())?;
    work.flush()?;
    let mut fields_copy = Vec::new();
    fields_copy
        .try_reserve_exact(children.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for child in children {
        work.step()?;
        fields_copy.push(Arc::clone(child));
    }
    work.flush()?;
    let child = StructArray::try_new_with_length(fields.clone(), fields_copy, None, layout.total)
        .map_err(arrow_failure)?;
    let output = layout.build(Arc::new(child), work)?;
    facts.bind_original_copy(&output);
    Ok(retain_copied_result_backing(
        output,
        backing,
        charge,
        &mut reservation,
        allocator.clone(),
        &facts,
        work,
    )?
    .values)
}

/// Assemble compact MAP rows from the original, already funded entry columns.
/// Only the MAP offsets/validity and the all-valid entry structure are new.
pub(crate) fn map_from_struct_children_in(
    target: &DataType,
    children: [ArrayRef; 2],
    row_lengths: &[Option<usize>],
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let DataType::Map(entries, sorted) = target else {
        return Err(internal("MAP result does not have its frozen carrier"));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(internal("MAP result has no frozen entry structure"));
    };
    let copy = CopyError::into_kernel;
    let mut total = 0i32;
    let mut has_null = false;
    for length in row_lengths {
        total = crate::aggregate_scalar::checked_map_output_offset(total, length.unwrap_or(0))?;
        has_null |= length.is_none();
        work.step()?;
    }
    let (backing, metadata) = observe_sources(&children, allocator, work)?;
    let mut invoice = CopyInvoiceTotals::default();
    invoice
        .interleave_exact_payload(
            mul(add(row_lengths.len(), 1).map_err(copy)?, size_of::<i32>()).map_err(copy)?,
        )
        .map_err(copy)?;
    if has_null {
        invoice
            .interleave_validity(row_lengths.len())
            .map_err(copy)?;
    }
    invoice
        .original_comparator_scratch(
            result_node_metadata_upper(target, 1, has_null, 1).map_err(copy)?,
        )
        .map_err(copy)?;
    invoice
        .original_comparator_scratch(
            result_node_metadata_upper(entries.data_type(), 0, false, 2).map_err(copy)?,
        )
        .map_err(copy)?;
    invoice
        .original_comparator_scratch(metadata)
        .map_err(copy)?;
    let mut facts = invoice
        .finish(mul(metadata, 2).map_err(copy)?)
        .map_err(copy)?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    work.flush()?;
    let mut reservation = charge.reserve_operation(facts.operation_peak_bytes())?;
    work.flush()?;
    let (offsets, nulls) = offsets_and_validity(row_lengths, has_null, work)?;
    let entries_array =
        StructArray::try_new_with_length(fields.clone(), Vec::from(children), None, total as usize)
            .map_err(arrow_failure)?;
    let output: ArrayRef = Arc::new(
        MapArray::try_new(Arc::clone(entries), offsets, entries_array, nulls, *sorted)
            .map_err(arrow_failure)?,
    );
    facts.bind_original_copy(&output);
    Ok(retain_copied_result_backing(
        output,
        backing,
        charge,
        &mut reservation,
        allocator.clone(),
        &facts,
        work,
    )?
    .values)
}

fn copy_segments_in(
    sources: &[ArrayRef],
    segments: &[ExtendSegment],
    total: usize,
    layout: Option<ListOutput<'_>>,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let copy = CopyError::into_kernel;
    let first = sources
        .first()
        .ok_or_else(|| internal("selected output has no source"))?;
    let (backing, source_metadata) = observe_sources(sources, allocator, work)?;
    let mut invoice = CopyInvoiceTotals::default();
    if let Some(layout) = &layout {
        layout.invoice(&mut invoice).map_err(copy)?;
    }

    // The one-span result keeps the original child backing. The shared slice
    // metadata observation also covers the zero-length view of the child.
    let span = match segments {
        [] if total == 0 => Some((first, 0, 0)),
        [segment] if segment.repeats == 1 && segment.nulls == 0 && segment.len == total => {
            let source = sources
                .get(segment.source)
                .ok_or_else(|| internal("List output references a missing source"))?;
            if segment.start > source.len() || segment.len > source.len() - segment.start {
                return Err(internal("List output span exceeds source"));
            }
            Some((source, segment.start, segment.len))
        }
        _ => None,
    };
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    if let Some((source, _, _)) = span {
        invoice
            .original_comparator_scratch(crate::array_backing_geometry::copy_metadata_bytes(
                source.as_ref(),
                work,
            )?)
            .map_err(copy)?;
    } else {
        let mut refs =
            ChildScratchVec::try_with_capacity(sources.len(), Some(allocator)).map_err(copy)?;
        for source in sources {
            work.step()?;
            refs.try_push(source.as_ref()).map_err(copy)?;
        }
        work.flush()?;
        let preflight = charge.reserve_operation(source_metadata)?;
        let mut observe = |boundary| if boundary { work.flush() } else { work.step() };
        let mut observation = CopyObservation(
            WorkObservation::Value(&mut observe),
            Some(allocator),
            ScratchCoverage::RecursiveSelections,
            Some(&mut invoice),
            None,
        );
        preflight_extend_multi_with_observation(&refs, segments, total, &mut observation)
            .map_err(copy)?;
        invoice
            .original_comparator_scratch(mul(sources.len(), size_of::<&ArrayData>()).map_err(copy)?)
            .map_err(copy)?;
        drop(preflight);
    }

    let mut facts = invoice
        .finish(mul(source_metadata, 2).map_err(copy)?)
        .map_err(copy)?;
    work.flush()?;
    let mut reservation = charge.reserve_operation(facts.operation_peak_bytes())?;
    work.flush()?;
    let child = if let Some((source, start, len)) = span {
        if start == 0 && len == source.len() {
            Arc::clone(source)
        } else {
            source.slice(start, len)
        }
    } else {
        let mut data =
            ChildScratchVec::try_with_capacity(sources.len(), Some(allocator)).map_err(copy)?;
        for source in sources {
            work.flush()?;
            data.try_push(source.to_data()).map_err(copy)?;
        }
        work.flush()?;
        let mut inputs = Vec::new();
        inputs
            .try_reserve_exact(data.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        inputs.extend(data.iter());
        let mut padded = false;
        for segment in segments {
            work.step()?;
            if segment.repeats != 0 && segment.nulls != 0 {
                padded = true;
                break;
            }
        }
        work.flush()?;
        // The original constructor invoice already includes initial validity
        // for the root and its children. Enable padding even for all-valid inputs.
        let mut values = MutableArrayData::try_new(inputs, padded, total).map_err(arrow_failure)?;
        for segment in segments {
            for _ in 0..segment.repeats {
                work.step()?;
                if segment.len != 0 {
                    values
                        .try_extend(segment.source, segment.start, segment.start + segment.len)
                        .map_err(arrow_failure)?;
                }
                if segment.nulls != 0 {
                    values
                        .try_extend_nulls(segment.nulls)
                        .map_err(arrow_failure)?;
                }
            }
        }
        work.flush()?;
        make_array(values.freeze())
    };
    if child.len() != total {
        return Err(internal("List output differs from selected child length"));
    }
    let result = if let Some(layout) = layout {
        layout.build(child, work)?
    } else {
        child
    };
    facts.bind_original_copy(&result);
    Ok(retain_copied_result_backing(
        result,
        backing,
        charge,
        &mut reservation,
        allocator.clone(),
        &facts,
        work,
    )?
    .values)
}
