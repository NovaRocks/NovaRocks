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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

use crate::exec::pipeline::operator::InputConstructionFactory;
use novarocks_functions::control_values::{
    indexed_value_offset_count, visit_indexed_value_offsets,
};
use novarocks_functions::selected_copy::{CopyOperationError, OriginalCopyData, concat_copy_in};
use novarocks_functions::{SourceBackingOwner, retain_source_backing};

pub(super) struct PreparedPartitionCall {
    pub(super) range: WindowRowRange,
    pub(super) ordinal: usize,
    pub(super) continuation: Option<WindowEvaluationPartition<'static>>,
}

pub(super) enum BatchedCall {
    Complete(CallOutput),
    Owned(WindowInvocationScratch<PreparedPartitionCall>),
}

impl BatchedCall {
    pub(super) fn complete(&self) -> Option<&CallOutput> {
        match self {
            Self::Complete(output) => Some(output),
            Self::Owned(_) => None,
        }
    }

    fn visit_offsets(
        &self,
        row: usize,
        visit: &mut dyn FnMut(usize, usize) -> Result<(), KernelFailure>,
        control: &RuntimeKernelControl,
    ) -> Result<(), KernelFailure> {
        match self {
            Self::Complete(output) => {
                visit_indexed_value_offsets(output.values.as_ref(), row, 0, visit, control)
            }
            Self::Owned(partitions) => {
                let i = partitions.partition_point(|part| part.range.end <= row);
                let part = partitions
                    .get(i)
                    .filter(|part| part.range.start <= row)
                    .ok_or_else(|| invalid_batch("window result row has no prepared partition"))?;
                part.continuation
                    .as_ref()
                    .ok_or_else(|| invalid_batch("window geometry used a finished partition"))?
                    .visit_output_row_offsets(row - part.range.start, visit, control)
            }
        }
    }

    fn evaluate(
        &mut self,
        range: std::ops::Range<usize>,
        call: usize,
        source: &Chunk,
        creation: &SortCreation<'_>,
        owner: &dyn InputConstructionOwner,
        source_storage: &ValueTypeStorageLifetime,
        host: &Arc<dyn AggregateStateAllocator>,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<ArrayRef> {
        match self {
            Self::Complete(output) => {
                let values = std::slice::from_ref(&output.values);
                Ok(slice_observed(values, range.start, range.len(), owner)?
                    .pop()
                    .expect("one window result column"))
            }
            Self::Owned(partitions) => {
                let first = partitions.partition_point(|part| part.range.end <= range.start);
                let last = partitions.partition_point(|part| part.range.start < range.end);
                let mut pieces = WindowInvocationScratch::try_with_capacity(
                    last - first,
                    Arc::clone(host),
                    control,
                )?;
                for part in &mut partitions.as_mut_slice()[first..last] {
                    let rows = part.range.end - part.range.start;
                    let begin = range.start.max(part.range.start) - part.range.start;
                    let end = range.end.min(part.range.end) - part.range.start;
                    let len = end - begin;
                    let mut sparse = WindowInvocationScratch::try_with_capacity(
                        if len == rows { 0 } else { len },
                        Arc::clone(host),
                        control,
                    )?;
                    let selection = if len == rows {
                        Selection::all(rows)
                    } else {
                        for row in begin..end {
                            sparse.try_push(row, control)?;
                        }
                        Selection::try_sparse_observed::<KernelFailure>(rows, &sparse, || {
                            control.checkpoint(1)
                        })?
                    };
                    let continuation = part
                        .continuation
                        .as_mut()
                        .ok_or_else(|| invalid_batch("window output used a finished partition"))?;
                    let output = continuation.evaluate_evaluated(selection, len, control)?;
                    if end == rows {
                        continuation.finish_evaluated(control)?;
                        part.continuation = None;
                    }
                    let (_, values, errors) = output.into_parts();
                    validate_unmasked_window_output(
                        call,
                        part.ordinal,
                        len,
                        values.len(),
                        errors.len(),
                    )?;
                    pieces.try_push(values, control)?;
                }
                match pieces.as_slice() {
                    [] => {
                        Err(invalid_batch("window output interval has no checked partition").into())
                    }
                    [only] => Ok(Arc::clone(only)),
                    many => {
                        let mut arrays = creation.vector::<ArrayRef>(many.len(), owner)?;
                        arrays.extend(many.iter().cloned());
                        let storage = source
                            .source_storage_lifetime_observed::<ConstructionFailure>(
                                source_storage.clone(),
                                |layout| owner.request(layout),
                            )?;
                        retain_concat(arrays, storage, host, control)
                    }
                }
            }
        }
    }
}

pub(super) struct PreparedWindowOutput {
    pub(super) calls: WindowInvocationScratch<BatchedCall>,
    pub(super) source: Chunk,
    pub(super) argument_storage: ValueTypeStorageLifetime,
}

// Design: ADR-0274 (docs/adr/ADR-0274-window-setup-precedes-tracked-batch-output.md)
pub(super) struct WindowBatchCursor {
    // A group contains one ordinary partition or the existing complete BY
    // invocation source. Completed groups are released as they advance.
    groups: WindowInvocationScratch<Option<PreparedWindowOutput>>,
    group: usize,
    row: usize,
    arrival_ends: WindowInvocationScratch<usize>,
    arrival: usize,
    // The original input charges back any BY aliases on an emitted batch.
    invocation_input: Option<InvocationInputBuffer>,
    pub(super) eos: bool,
}

impl WindowBatchCursor {
    pub(super) fn new(
        groups: WindowInvocationScratch<Option<PreparedWindowOutput>>,
        arrival_ends: WindowInvocationScratch<usize>,
        invocation_input: Option<InvocationInputBuffer>,
        eos: bool,
    ) -> Self {
        Self {
            groups,
            group: 0,
            row: 0,
            arrival_ends,
            arrival: 0,
            invocation_input,
            eos,
        }
    }

    pub(super) fn drained(&self) -> bool {
        self.group == self.groups.len()
    }

    pub(super) fn next(
        &mut self,
        row_target: usize,
        columns: &[OutputSource],
        schema: &ChunkSchemaRef,
        factory: &dyn InputConstructionFactory,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<Chunk> {
        let host = control
            .allocator()
            .ok_or_else(|| invalid_batch("batched window output requires its actual allocator"))?;
        let available = self
            .arrival_ends
            .get(self.arrival)
            .map_or(row_target, |end| row_target.min(*end - self.row));
        let prefix = self.prefix(available, columns, schema, &host, control)?;
        let mut remaining = prefix;
        let mut output = WindowInvocationScratch::try_new(Arc::clone(&host), control)?;
        while remaining > 0 {
            let group = self.groups.as_mut_slice()[self.group]
                .as_mut()
                .expect("live prepared window group");
            let len = remaining.min(group.source.len() - self.row);
            let range = self.row..self.row + len;
            let chunk = emit_group(
                group,
                range,
                columns,
                schema,
                factory,
                &host,
                self.invocation_input.as_ref(),
                control,
            )?;
            output.try_push(chunk, control)?;
            remaining -= len;
            self.row += len;
            if self.arrival_ends.get(self.arrival) == Some(&self.row) {
                self.arrival += 1;
            }
            if self.row == group.source.len() {
                self.groups.as_mut_slice()[self.group] = None;
                self.group += 1;
                self.row = 0;
            }
        }
        combine_output(&output, schema, factory, &host, control)
    }

    fn prefix(
        &self,
        target: usize,
        columns: &[OutputSource],
        schema: &ChunkSchemaRef,
        host: &Arc<dyn AggregateStateAllocator>,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<usize> {
        let mut widths =
            WindowInvocationScratch::try_with_capacity(columns.len(), Arc::clone(host), control)?;
        let mut cells = 0usize;
        for slot in schema.slots() {
            let width = indexed_value_offset_count(slot.field().data_type())?;
            widths.try_push((cells, width), control)?;
            cells = cells
                .checked_add(width)
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        let mut totals =
            WindowInvocationScratch::try_with_capacity(cells, Arc::clone(host), control)?;
        for _ in 0..cells {
            totals.try_push(0usize, control)?;
        }
        let mut count = 0;
        for group in self.groups[self.group..].iter().flatten() {
            let begin = if count == 0 { self.row } else { 0 };
            for row in begin..group.source.len() {
                let mut oversized = None;
                for ((source, &(first, width)), slot) in
                    columns.iter().zip(widths.iter()).zip(schema.slots())
                {
                    if width == 0 {
                        continue;
                    }
                    let mut add = |ordinal: usize, units: usize| {
                        if ordinal >= width {
                            return Err(invalid_batch(
                                "window output geometry differs from its bound result type",
                            ));
                        }
                        let total = &mut totals.as_mut_slice()[first + ordinal];
                        *total = total.saturating_add(units);
                        if *total > i32::MAX as usize {
                            oversized = Some((slot.field().data_type(), ordinal, *total));
                        }
                        Ok(())
                    };
                    match *source {
                        OutputSource::Input(ordinal) => visit_indexed_value_offsets(
                            group.source.record_batch().column(ordinal).as_ref(),
                            row,
                            0,
                            &mut add,
                            control,
                        )?,
                        OutputSource::Call(call) => {
                            group.calls[call].visit_offsets(row, &mut add, control)?
                        }
                    }
                    if oversized.is_some() {
                        break;
                    }
                }
                if let Some((ty, ordinal, required)) = oversized {
                    if count == 0 {
                        return Err(
                            novarocks_functions::control_values::window_i32_offset_failure(
                                ty, ordinal, required,
                            )
                            .into(),
                        );
                    }
                    return Ok(count);
                }
                count += 1;
                if count == target {
                    return Ok(count);
                }
            }
        }
        if count == 0 {
            return Err(invalid_batch("prepared window cursor cannot yield an empty batch").into());
        }
        Ok(count)
    }
}

fn emit_group(
    group: &mut PreparedWindowOutput,
    range: std::ops::Range<usize>,
    sources: &[OutputSource],
    schema: &ChunkSchemaRef,
    factory: &dyn InputConstructionFactory,
    host: &Arc<dyn AggregateStateAllocator>,
    invocation_input: Option<&InvocationInputBuffer>,
    control: &RuntimeKernelControl,
) -> ExecutionResult<Chunk> {
    let source = &group.source;
    let construction = factory.begin_input_construction(source)?;
    let creation = SortCreation::new(factory, schema);
    let mut columns = creation.vector::<ArrayRef>(sources.len(), construction.as_ref())?;
    let mut calls =
        WindowInvocationScratch::try_with_capacity(group.calls.len(), Arc::clone(host), control)?;
    for (ordinal, call) in group.calls.as_mut_slice().iter_mut().enumerate() {
        calls.try_push(
            call.evaluate(
                range.clone(),
                ordinal,
                source,
                &creation,
                construction.as_ref(),
                &group.argument_storage,
                host,
                control,
            )?,
            control,
        )?;
    }
    for column in sources {
        let value = match *column {
            OutputSource::Input(ordinal) => {
                let value = source.record_batch().column(ordinal);
                construction.observe(
                    ordinal,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::SliceArray {
                        source: value,
                        offset: range.start,
                        length: range.len(),
                    },
                )?;
                value.slice(range.start, range.len())
            }
            OutputSource::Call(ordinal) => Arc::clone(&calls[ordinal]),
        };
        columns.push(value);
    }

    let local_storage = construction
        .storage_lifetime()
        .retaining_observed::<ConstructionFailure>(&group.argument_storage, |layout| {
            construction.request(layout)
        })?;
    let local_storage = source
        .source_storage_lifetime_observed::<ConstructionFailure>(local_storage, |layout| {
            construction.request(layout)
        })?;
    let storage = if let Some(input) = invocation_input {
        Chunk::source_set_storage_lifetime_observed::<ConstructionFailure>(
            input.chunks(),
            local_storage,
            |layout| construction.request(layout),
        )?
    } else {
        local_storage
    };
    // Some result columns alias an evaluated argument rather than the raw
    // batch. Bind the real custody to the buffers before publishing columns
    // that may outlive this Chunk or the current continuation.
    let backing = SourceBackingOwner::try_from_owned(storage.clone(), Arc::clone(host), control)?;
    for column in &mut columns {
        *column = retain_source_backing(Arc::clone(column), &backing, control)?;
    }
    Ok(Chunk::try_new_typed_with_columns_owned_observed::<
        ConstructionFailure,
        _,
    >(
        schema.clone(),
        columns,
        range.len(),
        storage,
        |step| construction.observe(0, &[], step),
    )?)
}

fn combine_output(
    output: &[Chunk],
    schema: &ChunkSchemaRef,
    factory: &dyn InputConstructionFactory,
    host: &Arc<dyn AggregateStateAllocator>,
    control: &RuntimeKernelControl,
) -> ExecutionResult<Chunk> {
    match output {
        [] => Err(invalid_batch("prepared window cursor has no output").into()),
        [one] => Ok(one.clone()),
        _ => {
            let creation = SortCreation::new(factory, schema);
            let retained = factory.begin_input_construction(&output[0])?;
            let storage = Chunk::source_set_storage_lifetime_observed::<ConstructionFailure>(
                output,
                retained.storage_lifetime(),
                |layout| retained.request(layout),
            )?;
            let rows = output.iter().try_fold(0usize, |rows, chunk| {
                rows.checked_add(chunk.len())
                    .ok_or(KernelFailure::ResourceExhausted)
            })?;
            let mut columns =
                creation.vector::<ArrayRef>(schema.slots().len(), retained.as_ref())?;
            for ordinal in 0..schema.slots().len() {
                let mut sources = creation.vector::<ArrayRef>(output.len(), retained.as_ref())?;
                for chunk in output {
                    control.checkpoint(1)?;
                    sources.push(Arc::clone(chunk.record_batch().column(ordinal)));
                }
                columns.push(retain_concat(sources, storage.clone(), host, control)?);
            }
            Ok(Chunk::try_new_typed_with_columns_owned_observed::<
                ConstructionFailure,
                _,
            >(
                schema.clone(),
                columns,
                rows,
                storage,
                |step| retained.observe(0, &[], step),
            )?)
        }
    }
}

fn retain_concat(
    arrays: Vec<ArrayRef>,
    storage: ValueTypeStorageLifetime,
    host: &Arc<dyn AggregateStateAllocator>,
    control: &RuntimeKernelControl,
) -> ExecutionResult<ArrayRef> {
    match concat_copy_in(arrays, storage, Arc::clone(host), control) {
        Ok(result) => Ok(result.into_values()),
        Err(CopyOperationError::Control(cause)) => Err(cause.into()),
        Err(CopyOperationError::Preflight(cause)) => Err(cause.into_kernel().into()),
        Err(CopyOperationError::OriginalData(cause)) => Err(ExecutionFailure::from_domain(
            WindowBatchConcatFailure(cause),
        )),
    }
}

#[derive(Debug)]
struct WindowBatchConcatFailure(OriginalCopyData);

impl std::fmt::Display for WindowBatchConcatFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "compiled window ConcatenatePartitions: {}", self.0)
    }
}

impl std::error::Error for WindowBatchConcatFailure {}

impl novarocks_failure_contract::FailureProjection for WindowBatchConcatFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::WindowArrow.reason(),
        )
    }
}

pub(super) fn invalid_batch(message: &str) -> KernelFailure {
    KernelFailure::InvalidProgram(novarocks_functions::KernelDiagnostic::new(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::runtime::mem_tracker::MemTracker;
    use arrow::array::{ListArray, NullArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};
    use novarocks_types::SlotId;

    #[test]
    fn compiled_window_cursor_splits_cumulative_i32_offsets_across_groups() {
        // NullArray has no per-entry backing. Each selected list is valid on
        // its own, while the two lists cannot share an i32-offset output.
        let width = i32::MAX / 2 + 1;
        let item = Arc::new(Field::new("item", DataType::Null, true));
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(0),
                Field::new("lists", DataType::List(Arc::clone(&item)), true),
                None,
                None,
            )])
            .unwrap(),
        );
        let tracker = MemTracker::new_root("window offset prefix fixture");
        let mut control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
        control.bind_mem_tracker(Arc::clone(&tracker));
        let host = control.allocator().unwrap();
        let mut groups =
            WindowInvocationScratch::try_with_capacity(2, Arc::clone(&host), &control).unwrap();
        for _ in 0..2 {
            let array: ArrayRef = Arc::new(ListArray::new(
                Arc::clone(&item),
                OffsetBuffer::new(vec![0, width].into()),
                Arc::new(NullArray::new(width as usize)),
                None,
            ));
            let source = Chunk::try_new_with_columns(Arc::clone(&schema), vec![array]).unwrap();
            let calls = WindowInvocationScratch::try_new(Arc::clone(&host), &control).unwrap();
            groups
                .try_push(
                    Some(PreparedWindowOutput {
                        calls,
                        source,
                        argument_storage: ValueTypeStorageLifetime::pure(),
                    }),
                    &control,
                )
                .unwrap();
        }
        let arrival_ends = WindowInvocationScratch::try_new(Arc::clone(&host), &control).unwrap();
        let mut cursor = WindowBatchCursor::new(groups, arrival_ends, None, true);
        let output = [OutputSource::Input(0)];
        assert_eq!(
            cursor.prefix(2, &output, &schema, &host, &control).unwrap(),
            1,
        );
        cursor.group = 1;
        assert_eq!(
            cursor.prefix(2, &output, &schema, &host, &control).unwrap(),
            1,
        );
        drop(cursor);
        assert_eq!(tracker.current(), 0);
    }
}
