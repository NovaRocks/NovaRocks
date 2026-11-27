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

//! Aggregate OVER through an installed aggregate kernel with inline state.
//!
//! Each row's value is the aggregate's own result over exactly the rows of
//! that row's frame, folded in partition order through the aggregate's own
//! update and emitter. NULL handling, exact accumulation, type rules and the
//! finalize-time result checks are therefore the plain aggregate's, frame by
//! frame. An empty frame folds no row and emits the aggregate's value for a
//! group without input (NULL for SUM, MIN and MAX).
//!
//! Cost: a frame that starts at the partition start and ends no earlier than
//! the rows already folded (UNBOUNDED PRECEDING frames, ROWS or RANGE) extends
//! one running state and copies it, O(rows) for the partition. A frame equal
//! to its predecessor copies the predecessor's state. Every other frame folds
//! its own rows from a fresh state, O(rows x frame): the installed aggregates
//! have no retraction, so a sliding frame is refolded, never slid.
//!
//! Required work belongs to the partition begin: every frame is folded and
//! every frame's final value is built there once, so a result failure fails
//! the partition even when no output row is demanded. Only a frame whose own
//! result fails fails: an exact running state that passes through a prefix
//! outside the result range raises nothing unless a frame ends there.
//!
//! The partition retains one inline state per row. The adapter admits only
//! kernels whose state owns no memory beyond its inline body, so that bound is
//! exact and copying a state is a plain value copy.

use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateKernelPhase, AggregateStateMemoryPolicy, KernelEvaluationControl, KernelFailure,
    PreparedAggregateKernel, PreparedWindowKernel, SelectedAggregateUpdateInput, SelectedValues,
    Selection, WindowCallContract, WindowKernelPartition, WindowPartitionInput, WindowRowRange,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, WindowFrameExclusion,
};
use std::{alloc::Layout, fmt, sync::Arc};

#[path = "aggregate_window_adapter_tracked.rs"]
mod tracked;
pub(super) use tracked::{TrackedWindowOwner, admit_frame_modifiers};

/// States whose final values one partition begin builds per emitter call.
/// The values are checked and dropped; output emission builds its own.
const RESULT_CHECK_BLOCK: usize = 4096;

/// An installed aggregate kernel whose window frames copy its inline state.
/// The copy is exact, owns no heap and runs no aggregate work; a kernel whose
/// prepared state retains heap reports it here as a refusal.
pub(super) trait InlineAggregateWindowKernel: PreparedAggregateKernel {
    fn copy_state(&self, state: &Self::State) -> Result<Self::State, KernelFailure>;
}

pub(super) struct PreparedAggregateWindow<K: InlineAggregateWindowKernel> {
    aggregate: Arc<K>,
    contract: Arc<WindowCallContract>,
}

impl<K: InlineAggregateWindowKernel> fmt::Debug for PreparedAggregateWindow<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedAggregateWindow")
            .field("aggregate", &self.aggregate)
            .finish_non_exhaustive()
    }
}

/// The same installed owner passes its already-prepared Single aggregate.
pub(super) fn prepare<K: InlineAggregateWindowKernel>(
    aggregate: Arc<K>,
    contract: Arc<WindowCallContract>,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
        .map_err(compile_failure)?;
    let result = (|| {
        let source = aggregate.contract();
        let exact = contract
            .aggregate()
            .is_some_and(|window| Arc::ptr_eq(window, source))
            && source.phase() == AggregateKernelPhase::Single
            && source.state_input_type().is_none();
        work.step().map_err(compile_failure)?;
        if !exact {
            return Err(invalid(
                "aggregate OVER requires its exact Single aggregate preparation",
            ));
        }
        let distinct = source.distinct();
        work.step().map_err(compile_failure)?;
        if distinct {
            return Err(invalid("aggregate OVER DISTINCT has no window adapter"));
        }
        let ordered = !source.order_keys().is_empty();
        work.step().map_err(compile_failure)?;
        if ordered {
            return Err(invalid(
                "aggregate OVER with function ORDER BY has no window adapter",
            ));
        }
        let inline = aggregate.memory_policy() == AggregateStateMemoryPolicy::FixedZero;
        work.step().map_err(compile_failure)?;
        if !inline {
            return Err(invalid(
                "aggregate OVER requires an inline state without retained heap",
            ));
        }
        let exclusion = contract
            .options()
            .frame()
            .is_some_and(|frame| frame.exclusion != WindowFrameExclusion::NoOthers);
        work.step().map_err(compile_failure)?;
        if exclusion {
            return Err(invalid("aggregate OVER frame exclusion is unsupported"));
        }
        // An aggregate's own NULL rule decides which inputs it folds; the
        // adapter has no second rule that IGNORE NULLS could select.
        let ignore_nulls = contract.options().ignore_nulls();
        work.step().map_err(compile_failure)?;
        if ignore_nulls {
            return Err(invalid("aggregate OVER IGNORE NULLS is unsupported"));
        }
        work.flush().map_err(compile_failure)?;
        let prepared: Arc<dyn PreparedWindowKernel> = Arc::new(PreparedAggregateWindow {
            aggregate,
            contract,
        });
        work.flush().map_err(compile_failure)?;
        Ok(prepared)
    })();
    if result.as_ref().is_err_and(KernelFailure::is_interruption) {
        return result;
    }
    work.finish().map_err(compile_failure)?;
    result
}

struct AggregateWindowPartition<K: InlineAggregateWindowKernel> {
    prepared: Arc<PreparedAggregateWindow<K>>,
    /// One folded frame state per partition row.
    states: Box<[K::State]>,
    closed: bool,
}

/// Run one lifecycle step under an observation of the original control, so
/// every original failure stays primary over this scope's own tail.
fn observed<T>(
    original: &dyn KernelEvaluationControl,
    operation: impl FnOnce(
        &mut EvaluationCheckpoints<'_>,
        &dyn KernelEvaluationControl,
    ) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let control = KernelControlObservation::new(original);
    let result = (|| {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(&control);
        let result = operation(&mut work, &control);
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish()?;
        result
    })();
    control.finish(result)
}

fn reserve<T>(rows: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}

impl<K: InlineAggregateWindowKernel> PreparedAggregateWindow<K> {
    /// Fold every frame of one complete partition into its own state.
    fn fold_frames(
        &self,
        input: WindowPartitionInput<'_>,
        states: &mut Vec<K::State>,
        work: &mut EvaluationCheckpoints<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let full = input.full_input();
        let rows = full.partition_rows();
        let update = SelectedAggregateUpdateInput::try_new(
            self.aggregate.contract(),
            Selection::all(rows),
            full.logical_arguments(),
            &[],
            control,
        )?;
        let prepared = self.aggregate.prepare_update(update, control)?;
        // The running state holds exactly the rows [0, covered).
        let mut running = self.aggregate.create_state(control)?;
        let mut covered = 0;
        let mut previous: Option<WindowRowRange> = None;
        for frame in input.frames() {
            work.flush()?;
            let state = match states.last() {
                Some(last) if previous == Some(*frame) => self.aggregate.copy_state(last)?,
                _ if frame.start == 0 && frame.end >= covered => {
                    for row in covered..frame.end {
                        self.aggregate
                            .update_row(&mut running, &prepared, row, control)?;
                        work.step()?;
                    }
                    covered = frame.end;
                    self.aggregate.copy_state(&running)?
                }
                _ => {
                    let mut state = self.aggregate.create_state(control)?;
                    for row in frame.start..frame.end {
                        self.aggregate
                            .update_row(&mut state, &prepared, row, control)?;
                        work.step()?;
                    }
                    state
                }
            };
            states.push(state);
            previous = Some(*frame);
            work.step()?;
        }
        if states.len() != rows {
            return Err(internal(
                "aggregate OVER frames differ from their complete partition",
            ));
        }
        Ok(())
    }
}

impl<K: InlineAggregateWindowKernel> PreparedWindowKernel for PreparedAggregateWindow<K> {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }

    fn partition_retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure> {
        let heap = Layout::array::<K::State>(rows)
            .map_err(|_| KernelFailure::ResourceExhausted)?
            .size();
        size_of::<AggregateWindowPartition<K>>()
            .checked_add(heap)
            .ok_or(KernelFailure::ResourceExhausted)
    }

    fn begin_partition<'a>(
        self: Arc<Self>,
        input: WindowPartitionInput<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'a>, KernelFailure> {
        observed(control, |work, control| {
            let exact = std::ptr::eq(input.full_input().contract(), self.contract.as_ref());
            work.step()?;
            if !exact {
                return Err(invalid(
                    "aggregate OVER partition differs from its exact prepared contract",
                ));
            }
            let rows = input.full_input().partition_rows();
            self.partition_retained_upper_bound(rows)?;
            let mut states = reserve(rows, work)?;
            if rows > 0 {
                self.fold_frames(input, &mut states, work, control)?;
            }
            // Every frame's result is required, whatever output is demanded.
            for block in states.chunks(RESULT_CHECK_BLOCK) {
                work.flush()?;
                drop(self.aggregate.build_final(block.iter(), control)?);
                work.flush()?;
            }
            let states = states.into_boxed_slice();
            work.flush()?;
            let partition = Box::new(AggregateWindowPartition {
                prepared: self,
                states,
                closed: false,
            }) as Box<dyn WindowKernelPartition>;
            work.flush()?;
            Ok(partition)
        })
    }
}

impl<K: InlineAggregateWindowKernel> WindowKernelPartition for AggregateWindowPartition<K> {
    fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        if self.closed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = observed(control, |work, control| {
            let valid =
                selection.batch_rows() == self.states.len() && selection.len() <= row_capacity;
            work.step()?;
            if !valid {
                return Err(invalid(
                    "aggregate OVER output differs from its partition or host row grant",
                ));
            }
            let aggregate = &self.prepared.aggregate;
            let output = if selection.is_all() {
                aggregate.build_final(self.states.iter(), control)?
            } else {
                let mut selected = reserve(selection.len(), work)?;
                for row in selection.iter() {
                    selected.push(&self.states[row]);
                    work.step()?;
                }
                work.flush()?;
                aggregate.build_final(selected.into_iter(), control)?
            };
            SelectedValues::try_new_observed::<KernelFailure>(
                selection,
                self.prepared.contract.result_type().data_type(),
                output,
                Box::default(),
                || work.step(),
            )
        });
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        if self.closed {
            return Err(KernelFailure::InstanceFailed);
        }
        self.closed = true;
        observed(control, |work, _| {
            self.states = Box::default();
            work.step()
        })
    }

    fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.states.len() * size_of::<K::State>()
    }
}

#[cfg(test)]
#[path = "aggregate_window_adapter_tests.rs"]
mod tests;
