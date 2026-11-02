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

//! Borrowed longest-zip UNNEST cursor. The host owns outer/LEFT assembly.
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::{Array, ArrayRef, ListArray, make_array};
use arrow_data::transform::MutableArrayData;
use novarocks_type_contract::CompileCheckpoints;
use std::{alloc::Layout, sync::Arc};

#[derive(Debug)]
struct Prepared {
    contract: Arc<TableCallContract>,
}
pub(super) fn prepare(
    contract: Arc<TableCallContract>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<dyn PreparedTableKernel>, KernelFailure> {
    let channels = contract.argument_types().len();
    let valid = channels > 0 && channels == contract.result_types().len();
    work.step().map_err(compile_failure)?;
    if !valid {
        return Err(invalid(
            "UNNEST requires one result for every List argument",
        ));
    }
    for value in contract.argument_types() {
        let list = matches!(value.data_type(), arrow_schema::DataType::List(_));
        work.step().map_err(compile_failure)?;
        if !list {
            return Err(invalid("UNNEST requires canonical List channels"));
        }
    }
    work.flush().map_err(compile_failure)?;
    let result: Arc<dyn PreparedTableKernel> = Arc::new(Prepared { contract });
    work.flush().map_err(compile_failure)?;
    Ok(result)
}
impl PreparedTableKernel for Prepared {
    fn contract(&self) -> &Arc<TableCallContract> {
        &self.contract
    }
    fn cursor_retained_upper_bound(&self, _parents: usize) -> Result<usize, KernelFailure> {
        Ok(std::mem::size_of::<Cursor<'static>>())
    }
    fn begin_selected<'input>(
        self: Arc<Self>,
        input: SelectedTableInput<'input, 'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn TableKernelCursor + 'input>, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let result = (|| {
            let same = std::ptr::eq(input.contract(), self.contract.as_ref());
            work.step()?;
            if !same {
                return Err(invalid("UNNEST input differs from its prepared contract"));
            }
            for arg in input.arguments() {
                let valid = arg.array().as_any().downcast_ref::<ListArray>().is_some();
                work.step()?;
                if !valid {
                    return Err(invalid("UNNEST source is not an actual ListArray"));
                }
            }
            work.flush()?;
            let result: Box<dyn TableKernelCursor + 'input> = Box::new(Cursor {
                owner: self,
                input,
                parent: 0,
                offset: 0,
                finished: false,
            });
            work.flush()?;
            Ok(result)
        })();
        finish(work, result)
    }
}
struct Cursor<'input> {
    owner: Arc<Prepared>,
    input: SelectedTableInput<'input, 'input>,
    parent: usize,
    offset: usize,
    finished: bool,
}
fn finish<T>(
    work: EvaluationCheckpoints<'_>,
    result: Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    if result.as_ref().is_err_and(|cause| {
        cause.is_interruption() || super::collection_arrow::is_extension_failure(cause)
    }) {
        return result;
    }
    work.finish()?;
    result
}
fn reserve<T>(count: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut output = Vec::new();
    if output.try_reserve_exact(count).is_err() {
        return Err(KernelFailure::ResourceExhausted);
    }
    work.flush()?;
    Ok(output)
}
fn boxed<T>(
    input: Vec<T>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Box<[T]>, KernelFailure> {
    work.flush()?;
    let output = input.into_boxed_slice();
    work.flush()?;
    Ok(output)
}
fn list_range<'a>(
    argument: EvaluatedArgument<'a>,
    ordinal: usize,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(&'a ListArray, usize, usize), KernelFailure> {
    let list = argument
        .array()
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| invalid("UNNEST source lost its ListArray class"))?;
    let address = argument.value_row(ordinal, row);
    let in_bounds = address < list.len();
    work.step()?;
    if !in_bounds {
        return Err(invalid("UNNEST selected parent is outside its source"));
    }
    if list.is_null(address) {
        work.step()?;
        return Ok((list, 0, 0));
    }
    let offsets = list.value_offsets();
    let range = usize::try_from(offsets[address])
        .ok()
        .zip(usize::try_from(offsets[address + 1]).ok());
    work.step()?;
    let (start, end) = range.ok_or_else(|| invalid("UNNEST List offset is negative"))?;
    if end < start || end > list.values().len() {
        return Err(invalid("UNNEST List range is outside child values"));
    }
    Ok((list, start, end - start))
}
impl Cursor<'_> {
    fn step(
        &mut self,
        capacity: TableStepCapacity,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<TableCursorStep, KernelFailure> {
        if self.finished {
            return Err(KernelFailure::InstanceFailed);
        }
        let row = self.input.selection().row(self.parent);
        let mut length = 0;
        if let Some(row) = row {
            for arg in self.input.arguments() {
                let (_, _, len) = list_range(*arg, self.parent, row, work)?;
                length = length.max(len);
                work.step()?;
            }
        }
        let remaining = length
            .checked_sub(self.offset)
            .ok_or_else(|| internal("UNNEST progress exceeds parent extent"))?;
        if row.is_some() && remaining > 0 && capacity.page.rows == 0 {
            return Ok(TableCursorStep::CapacityRequired(
                TableCapacityRequirements {
                    row: true,
                    completion: false,
                    parent_error: false,
                },
            ));
        }
        if row.is_some() && remaining == 0 && capacity.page.completions == 0 {
            return Ok(TableCursorStep::CapacityRequired(
                TableCapacityRequirements {
                    row: false,
                    completion: true,
                    parent_error: false,
                },
            ));
        }
        let rows = remaining.min(capacity.page.rows);
        // The cursor has no unbounded per-parent expansion. Mutable copy
        // temporaries/page backing are separate host-owned requests; the inline
        // retained bound does not claim their memory is funded.
        let mut parents = reserve(rows, work)?;
        for _ in 0..rows {
            parents.push(self.parent);
            work.step()?;
        }
        let terminal = row.is_some() && rows == remaining && capacity.page.completions > 0;
        let mut completions = reserve(usize::from(terminal), work)?;
        if terminal {
            completions.push(self.parent);
            work.step()?;
        }
        let mut columns = reserve::<ArrayRef>(self.input.arguments().len(), work)?;
        for arg in self.input.arguments() {
            let (list, start, len) = match row {
                Some(row) => list_range(*arg, self.parent, row, work)?,
                None => (
                    arg.array()
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| invalid("UNNEST source lost List class"))?,
                    0,
                    0,
                ),
            };
            let copied = len.saturating_sub(self.offset).min(rows);
            let copy_start = start
                .checked_add(self.offset.min(len))
                .ok_or(KernelFailure::ResourceExhausted)?;
            let nulls = rows - copied;
            work.step()?;
            work.flush()?;
            let preflight = crate::selected_copy::preflight_extend(
                list.values().as_ref(),
                copy_start,
                copied,
                nulls,
                rows,
                |opaque| if opaque { work.flush() } else { work.step() },
            );
            match preflight {
                Ok(()) => work.flush()?,
                Err(source) => return Err(source.into_kernel()),
            }
            work.flush()?;
            let data = list.values().to_data();
            work.flush()?;
            let mut sources = reserve(1, work)?;
            sources.push(&data);
            work.step()?;
            work.flush()?;
            let mut mutable = MutableArrayData::new(sources, true, rows);
            work.flush()?;
            let copy_end = copy_start
                .checked_add(copied)
                .ok_or(KernelFailure::ResourceExhausted)?;
            let mut position = copy_start;
            while position < copy_end {
                let count = (copy_end - position).min(256);
                work.flush()?;
                mutable
                    .try_extend(0, position, position + count)
                    .map_err(|source| {
                        super::collection_arrow::arrow_failure(
                            "unnest",
                            super::collection_arrow::ArrowSite::Extend,
                            source,
                        )
                    })?;
                work.flush()?;
                for _ in 0..count {
                    work.step()?;
                }
                position += count;
            }
            let mut remaining_nulls = nulls;
            while remaining_nulls > 0 {
                let count = remaining_nulls.min(256);
                work.flush()?;
                mutable.try_extend_nulls(count).map_err(|source| {
                    super::collection_arrow::arrow_failure(
                        "unnest",
                        super::collection_arrow::ArrowSite::Extend,
                        source,
                    )
                })?;
                work.flush()?;
                for _ in 0..count {
                    work.step()?;
                }
                remaining_nulls -= count;
            }
            work.flush()?;
            let output = make_array(mutable.freeze());
            work.flush()?;
            columns.push(output);
            work.step()?;
        }
        let page = OwnedTableOutputPage {
            columns: boxed(columns, work)?,
            parent_ordinals: boxed(parents, work)?,
            completed_parents: boxed(completions, work)?,
            parent_errors: Box::default(),
            eof: row.is_none() || (terminal && self.parent + 1 == self.input.selection().len()),
        };
        // Publish progress only after every checked copy and final observation.
        work.flush()?;
        if terminal {
            self.parent += 1;
            self.offset = 0;
        } else {
            self.offset += rows;
        }
        Ok(TableCursorStep::Page(page))
    }
}
impl TableKernelCursor for Cursor<'_> {
    fn next(
        &mut self,
        capacity: TableStepCapacity,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TableCursorStep, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        // The invocation wrapper owns the failed latch and forbids replay after
        // any failure, including a final observation after cursor progress.
        let result = self.step(capacity, &mut work);
        finish(work, result)
    }
    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        control.checkpoint(0)?;
        if self.finished {
            return Err(KernelFailure::InstanceFailed);
        }
        if self.parent != self.input.selection().len() {
            return Err(invalid(
                "UNNEST normal finish requires all parent completions",
            ));
        }
        control.checkpoint(0)?;
        self.finished = true;
        Ok(())
    }
    fn retained_bytes(&self) -> usize {
        // Shared immutable owner/input remain loans, not uniquely owned backing.
        let _ = &self.owner;
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod extension_failure_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn actual_extension_failure_wins_without_a_completion_checkpoint() {
        struct RefuseCompletion(AtomicUsize);
        impl KernelEvaluationControl for RefuseCompletion {
            fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Err(KernelFailure::Cancelled)
            }
            fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
                self.checkpoint(0)
            }
        }
        let values = arrow_array::Int32Array::from(vec![1]);
        let data = values.to_data();
        let mut mutable = MutableArrayData::new(vec![&data], true, 1);
        let source = mutable.try_extend(0, 0, 2).unwrap_err();
        let cause = super::super::collection_arrow::arrow_failure(
            "unnest",
            super::super::collection_arrow::ArrowSite::Extend,
            source,
        );
        let control = RefuseCompletion(AtomicUsize::new(0));
        let result = finish::<()>(EvaluationCheckpoints::new(&control), Err(cause.clone()));
        assert_eq!(result.unwrap_err(), cause);
        assert_eq!(control.0.load(Ordering::Relaxed), 0);
    }
}
