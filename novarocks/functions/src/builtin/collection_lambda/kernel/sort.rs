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

//! Stable bottom-up position merges. Every parent requests its next required
//! comparison, since an invalid result terminates that parent's sort.

use arrow_array::Int32Array;

use super::*;

pub(super) struct Sort<'input> {
    selection: Selection<'input>,
    contract: Arc<HigherOrderCallContract>,
    values: [ArrayRef; 2],
    parents: Scratch<SortParent<'input>>,
    pending: Scratch<(usize, usize)>,
    batch_parents: bool,
    next_parent: usize,
    remaining: usize,
    storage: Storage,
    _invocation: OpaqueReservation,
}
impl<'input> Sort<'input> {
    pub(super) fn new(
        input: HigherOrderCallInput<'_, 'input>,
        contract: Arc<HigherOrderCallContract>,
        batch_parents: bool,
        storage: Storage,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let _invocation = storage.reserve(size_of::<Self>())?;
        let selection = input.selection();
        let [ordinary] = input.arguments() else {
            return Err(invalid(
                "array_sort comparator requires one collection argument",
            ));
        };
        let source = ordinary.array();
        let Columns::List(values) = Columns::from_input(source)? else {
            return Err(invalid("array_sort comparator requires a List carrier"));
        };
        let offsets = source_ranges(source)?;
        let mut parents = storage.vec(selection.len())?;
        let mut remaining = 0;
        for (parent, row) in selection.iter().enumerate() {
            let state = SortParent::new(
                parent_range(source, offsets, ordinary.value_row(parent, row), work)?,
                &storage,
                work,
            )?;
            if state.active() {
                remaining += 1;
            }
            parents.push(state);
        }
        Ok(Self {
            selection,
            contract,
            values: [Arc::clone(&values), values],
            parents,
            pending: storage.vec(0)?,
            batch_parents,
            next_parent: 0,
            remaining,
            storage,
            _invocation,
        })
    }

    fn request(
        &mut self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<LambdaBodyBatch, KernelFailure> {
        let first_parent = self.next_parent;
        let mut pairs = self.storage.vec(0)?;
        for _ in 0..self.parents.len() {
            let parent = self.next_parent;
            self.next_parent = (self.next_parent + 1) % self.parents.len();
            let row = &mut self.parents[parent];
            work.step()?;
            if !row.active() {
                continue;
            }
            row.ensure_pass(&self.storage, work)?;
            for _ in 0..row.merges.len() {
                let merge = &row.merges[row.next_merge];
                work.step()?;
                if merge.done() {
                    row.next_merge = (row.next_merge + 1) % row.merges.len();
                    continue;
                }
                let pair = [row.positions[merge.left], row.positions[merge.right]];
                self.storage.push(&mut pairs, pair)?;
                self.storage
                    .push(&mut self.pending, (parent, row.next_merge))?;
                break;
            }
            if !self.batch_parents {
                self.next_parent = parent;
                break;
            }
            if pairs.len() == BATCH_ROWS {
                break;
            }
        }
        if let Some(wrap) = parent_wrap(
            self.pending.iter().map(|(parent, _)| *parent),
            first_parent,
            work,
        )? {
            self.pending.rotate_left(wrap);
            pairs.rotate_left(wrap);
            work.flush()?;
        }
        let mut batch =
            LambdaBodyBatchBuilder::try_new(&self.storage.host, &self.values, pairs.len())?;
        for (&(parent, _), pair) in self.pending.iter().zip(pairs) {
            work.step()?;
            batch.push(parent, &pair)?;
        }
        Ok(batch.finish())
    }

    fn consume(
        &mut self,
        response: &LambdaBodyBatchOutput<'input>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let values = response
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| internal("array_sort comparator must return INTEGER"))?;
        let mut errors = response.errors().iter().enumerate().peekable();
        for (ordinal, &(parent, run)) in self.pending.iter().enumerate() {
            let row = &mut self.parents[parent];
            let error = errors.next_if(|(_, error)| error.selected_ordinal() == ordinal);
            work.step()?;
            if !row.active() {
                continue;
            }
            if let Some((error_index, _)) = error {
                row.fail(required_error(response, error_index)?);
            } else if values.is_null(ordinal) || !(-1..=1).contains(&values.value(ordinal)) {
                row.fail(bad_argument(parent, "Comparator must return -1, 0, or 1"));
            } else {
                row.merge(run, values.value(ordinal) <= 0, work)?;
            }
            if !row.active() {
                self.remaining -= 1;
            }
        }
        self.pending.clear();
        Ok(())
    }

    fn finish(
        &mut self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<HigherOrderOutput<'input>, KernelFailure> {
        let target = self.contract.result_type().data_type();
        let DataType::List(field) = target else {
            return Err(internal("array_sort lost its frozen List type"));
        };
        let mut lengths = self.storage.vec(self.parents.len())?;
        let mut segments = self.storage.vec::<ExtendSegment>(0)?;
        for row in &self.parents {
            work.step()?;
            if !row.present || row.error.is_some() {
                lengths.push(None);
                continue;
            }
            lengths.push(Some(row.positions.len()));
            for &position in &row.positions {
                work.step()?;
                if let Some(last) = segments.last_mut()
                    && last.start + last.len == position
                {
                    last.len += 1;
                } else {
                    self.storage.push(
                        &mut segments,
                        ExtendSegment {
                            source: 0,
                            start: position,
                            len: 1,
                            repeats: 1,
                            nulls: 0,
                        },
                    )?;
                }
            }
        }
        let result = list_from_segments_in(
            field,
            &self.values[..1],
            &segments,
            &lengths,
            &self.storage.host,
            &self.storage.allocator,
            work,
        )?;
        finish_output(
            self.selection,
            target,
            result,
            self.parents.iter_mut().filter_map(|row| row.error.take()),
            &self.storage,
            work,
        )
    }
}
impl<'input> HigherOrderCall<'input> for Sort<'input> {
    fn advance(
        &mut self,
        completed: Option<LambdaBodyBatchOutput<'input>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<HigherOrderStep<'input>, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        if let Some(completed) = completed {
            self.consume(&completed, &mut work)?;
        }
        let step = if self.remaining != 0 {
            HigherOrderStep::Request(self.request(&mut work)?)
        } else {
            HigherOrderStep::Complete(self.finish(&mut work)?)
        };
        work.finish()?;
        Ok(step)
    }

    fn instance_retained_bytes(&self) -> usize {
        size_of::<Instance>()
    }
}

struct SortParent<'input> {
    present: bool,
    width: usize,
    positions: Scratch<usize>,
    target: Scratch<usize>,
    merges: Scratch<Merge>,
    next_merge: usize,
    remaining_merges: usize,
    error: Option<ParentError<'input>>,
}
impl<'input> SortParent<'input> {
    fn new(
        range: Option<Range<usize>>,
        storage: &Storage,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let present = range.is_some();
        let range = range.unwrap_or_default();
        let mut positions = storage.vec(range.len())?;
        let mut target = storage.vec(if range.len() > 1 { range.len() } else { 0 })?;
        for position in range {
            positions.push(position);
            if target.capacity() != 0 {
                target.push(position);
            }
            work.step()?;
        }
        Ok(Self {
            present,
            width: 1,
            positions,
            target,
            merges: storage.vec(0)?,
            next_merge: 0,
            remaining_merges: 0,
            error: None,
        })
    }

    fn active(&self) -> bool {
        self.error.is_none() && self.width < self.positions.len()
    }

    fn fail(&mut self, error: ParentError<'input>) {
        self.error = Some(error);
        self.positions = Scratch::new_in(self.positions.allocator().clone());
        self.target = Scratch::new_in(self.target.allocator().clone());
        self.merges = Scratch::new_in(self.merges.allocator().clone());
    }

    fn ensure_pass(
        &mut self,
        storage: &Storage,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        if !self.merges.is_empty() {
            return Ok(());
        }
        let len = self.positions.len();
        let stride = self.width * 2;
        for start in (0..len).step_by(stride) {
            work.step()?;
            let middle = (start + self.width).min(len);
            let end = (start + stride).min(len);
            if middle == end {
                for index in start..end {
                    work.step()?;
                    self.target[index] = self.positions[index];
                }
            } else {
                storage.push(
                    &mut self.merges,
                    Merge {
                        left: start,
                        middle,
                        right: middle,
                        end,
                        output: start,
                    },
                )?;
            }
        }
        self.next_merge = 0;
        self.remaining_merges = self.merges.len();
        Ok(())
    }

    fn merge(
        &mut self,
        run: usize,
        choose_left: bool,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let merge = &mut self.merges[run];
        // Taking the left on ties is the whole stability rule.
        let source = if choose_left {
            let source = merge.left;
            merge.left += 1;
            source
        } else {
            let source = merge.right;
            merge.right += 1;
            source
        };
        self.target[merge.output] = self.positions[source];
        merge.output += 1;
        if merge.left == merge.middle || merge.right == merge.end {
            for rest in (merge.left..merge.middle).chain(merge.right..merge.end) {
                work.step()?;
                self.target[merge.output] = self.positions[rest];
                merge.output += 1;
            }
            self.remaining_merges -= 1;
            if self.remaining_merges == 0 {
                std::mem::swap(&mut self.positions, &mut self.target);
                self.merges.clear();
                self.width *= 2;
            }
        }
        Ok(())
    }
}
struct Merge {
    left: usize,
    middle: usize,
    right: usize,
    end: usize,
    output: usize,
}
impl Merge {
    fn done(&self) -> bool {
        self.output == self.end
    }
}
