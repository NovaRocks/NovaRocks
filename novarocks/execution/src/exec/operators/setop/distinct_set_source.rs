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

//! Generic source side for DISTINCT set-operation result emission.
//!
//! Responsibilities:
//! - Reads shared state and emits rows selected by operation-specific set semantics.
//! - Handles completion ordering and shared error propagation before producing output.
//!
//! Key exported interfaces:
//! - Types: `DistinctSetSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, OperatorLifecycle,
};
use crate::runtime::fragment::ExecutionResult;

use std::marker::PhantomData;
use std::sync::Arc;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{
    Source, SourceContractFailure, SourceKernel, SourceStart, source_contract_failure,
};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};
use crate::runtime::runtime_state::RuntimeState;
use arrow::array::ArrayRef;
use std::num::NonZeroUsize;

use super::distinct_set_shared::{
    DistinctSetSemantics, DistinctSetSharedState, SetSelectionCursor, SetSelectionProgress,
};

pub struct DistinctSetSourceFactory<S: DistinctSetSemantics> {
    name: String,
    state: DistinctSetSharedState<S>,
}

impl<S: DistinctSetSemantics> DistinctSetSourceFactory<S> {
    #[cfg(test)]
    pub(crate) fn new(state: DistinctSetSharedState<S>, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("{} (id={node_id})", S::SOURCE_OPERATOR_NAME)
        } else {
            S::SOURCE_OPERATOR_NAME.to_string()
        };
        Self { name, state }
    }
}

impl<S: DistinctSetSemantics> OperatorFactory for DistinctSetSourceFactory<S> {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(Source::new(DistinctSetSourceOperator::<S> {
            name: self.name.clone(),
            state: self.state.clone(),
            output_chunk_schema: self.state.output_chunk_schema(),
            output: None,
            active: driver_id == 0,
            chunk_size: None,
            construction: None,
            control: None,
            mem_tracker: None,
            _semantics: PhantomData,
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct DistinctSetSourceOperator<S: DistinctSetSemantics> {
    name: String,
    state: DistinctSetSharedState<S>,
    output_chunk_schema: ChunkSchemaRef,
    output: Option<OutputCursor>,
    active: bool,
    chunk_size: Option<NonZeroUsize>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: Option<RuntimeKernelControl>,
    mem_tracker: Option<Arc<crate::runtime::mem_tracker::MemTracker>>,
    _semantics: PhantomData<S>,
}

struct OutputCursor {
    selection: SetSelectionCursor,
}

impl<S: DistinctSetSemantics> OperatorLifecycle for DistinctSetSourceOperator<S> {
    fn name(&self) -> &str {
        &self.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        if let Some(control) = self.control.as_mut() {
            control.bind_construction_factory(Arc::clone(&factory));
        }
        self.construction = Some(factory);
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        if let Some(control) = self.control.as_mut() {
            control.bind_mem_tracker(Arc::clone(&tracker));
        }
        self.mem_tracker = Some(tracker);
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        let mut control = RuntimeKernelControl::new(state.error_state());
        control.bind_runtime_memory(state);
        if let Some(tracker) = &self.mem_tracker {
            control.bind_mem_tracker(Arc::clone(tracker));
        }
        if let Some(factory) = &self.construction {
            control.bind_construction_factory(Arc::clone(factory));
        }
        self.control = Some(control);
        self.chunk_size = Some(NonZeroUsize::new(state.chunk_size()).ok_or_else(|| {
            source_contract_failure(SourceContractFailure::MissingRuntimeBinding)
        })?);
        Ok(())
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.output = None;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.output = None;
        Ok(())
    }
    fn cancel(&mut self) {
        self.output = None;
    }
}

impl<S: DistinctSetSemantics> SourceKernel for DistinctSetSourceOperator<S> {
    fn start(&self) -> SourceStart {
        if self.active {
            SourceStart::Ready
        } else {
            SourceStart::Finished
        }
    }
    fn poll_output(
        &mut self,
        ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
    ) -> ExecutionResult<StepResult> {
        let chunk_size = self
            .chunk_size
            .ok_or_else(|| source_contract_failure(SourceContractFailure::MissingRuntimeBinding))?;
        let observable = self.state.controller().observable();
        let observed_generation = observable.generation();
        let output_stage = self.state.controller().stage_total();
        if !self.state.controller().is_stage_ready(output_stage) {
            return Ok(StepResult::new(
                None,
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Dependency,
                    observable,
                    observed_generation,
                    deadline: None,
                }),
                0,
            ));
        }
        if ctx.work().remaining() == 0 {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        if self.output.is_none() {
            self.output = Some(OutputCursor {
                selection: SetSelectionCursor::new(
                    crate::exec::hash_table::allocation::AggregateAllocator::new(Arc::clone(
                        self.mem_tracker
                            .as_ref()
                            .ok_or(ConstructionFailure::MissingOwner)?,
                    )),
                ),
            });
        }
        let cursor = self
            .output
            .as_mut()
            .expect("the output snapshot is installed");
        let progress = self
            .state
            .advance_selection(&mut cursor.selection, chunk_size, || {
                ctx.checkpoint()?;
                Ok(ctx.work().try_consume(1))
            })?;
        let SetSelectionProgress::Ready { exhausted } = progress else {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        };
        if cursor.selection.is_empty() {
            self.output = None;
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        ctx.checkpoint()?;
        let control = self
            .control
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        control.checkpoint_execution()?;
        let owner = control.construction_owner(&self.output_chunk_schema)?;
        let positions = cursor.selection.take_indices()?;
        let exported = self
            .state
            .export_selected_positions(positions.as_slice(), owner.as_ref(), control)
            .map_err(|failure| control.project_required_failure(failure))?;
        if self.output_chunk_schema.slot_ids().len() != exported.arrays().len() {
            return Err(source_contract_failure(
                SourceContractFailure::ColumnCount {
                    expected: self.output_chunk_schema.slot_ids().len(),
                    actual: exported.arrays().len(),
                },
            ));
        }
        let layout = std::alloc::Layout::array::<ArrayRef>(exported.arrays().len())
            .map_err(|_| ConstructionFailure::from(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?;
        if layout.size() != 0 {
            owner.request(layout)?;
        }
        let mut out_arrays = Vec::new();
        out_arrays
            .try_reserve_exact(exported.arrays().len())
            .map_err(|source| {
                crate::runtime::fragment::ExecutionFailure::from_domain(
                    novarocks_value_arrays::ValueError::Allocation {
                        requested: layout.size(),
                        source,
                    },
                )
            })?;
        for column in exported.arrays() {
            control.checkpoint_execution()?;
            out_arrays.push(Arc::clone(column));
        }
        let output = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            Arc::clone(&self.output_chunk_schema),
            out_arrays,
            positions.len(),
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?;
        let next = if exhausted {
            NextStep::Finished
        } else {
            NextStep::Runnable
        };
        if matches!(next, NextStep::Finished) {
            self.output = None;
        }
        Ok(StepResult::new(Some(output), next, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{
        operators::setop::{except_shared::ExceptSemantics, set_op_stage::SetOpStageController},
        pipeline::step_context::{StepContext, TurnWork},
    };
    use crate::runtime::{mem_tracker::MemTracker, query_options::QueryOptions};
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use novarocks_types::SlotId;
    use std::num::NonZeroU64;

    fn chunk(values: Vec<i32>) -> Chunk {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap();
        crate::exec::chunk::typed_fixture::chunk(
            batch,
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        )
    }
    #[test]
    fn selection_yields_with_one_cursor_and_emits_the_expected_rows_once() {
        let interval = novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize;
        let count = interval * 2 + 7;
        let input = chunk((0..count as i32).collect());
        let state = RuntimeState::for_local_execution(
            Some(QueryOptions {
                batch_size: Some(3),
                ..Default::default()
            }),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let tracker = MemTracker::new_root("set source original construction fixture");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            Arc::clone(&tracker), state.error_state()).unwrap();
        let owner = factory
            .begin_typed_construction(input.chunk_schema())
            .unwrap();
        let kernel_control = RuntimeKernelControl::new(state.error_state());
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let shared = DistinctSetSharedState::<ExceptSemantics>::new(
            SetOpStageController::new("EXCEPT", vec![1, 1]).unwrap(),
            input.chunk_schema_ref(),
        );
        shared
            .ingest_build(
                &input,
                Arc::clone(&tracker),
                owner.as_ref(),
                &kernel_control,
                &mut conversions,
            )
            .unwrap();
        shared.finish_stage(0).unwrap();
        let probe = chunk((0..(interval * 2) as i32).collect());
        let probe_owner = factory
            .begin_typed_construction(probe.chunk_schema())
            .unwrap();
        shared
            .ingest_probe(
                1,
                &probe,
                Arc::clone(&tracker),
                probe_owner.as_ref(),
                &kernel_control,
                &mut conversions,
            )
            .unwrap();
        shared.finish_stage(1).unwrap();
        let mut source = DistinctSetSourceFactory::new(shared.clone(), 7).create(1, 0);
        source.set_mem_tracker(Arc::clone(&tracker));
        source.set_construction_factory(Arc::clone(&factory));
        source.bind_runtime_state(&state).unwrap();
        let control = state.error_state();
        let mut rows = Vec::new();
        let mut yields = 0;
        let mut ended = false;
        let expected = ((interval * 2) as i32..count as i32).collect::<Vec<_>>();
        let max_turns = count.div_ceil(interval) + expected.len().div_ceil(3);
        for turn in 1..=max_turns {
            let mut work = TurnWork::new(NonZeroU64::new(turn as u64).unwrap(), 1);
            let mut value_conversions =
                novarocks_value_arrays::decode::ValueConversionContext::default();
            let mut context = StepContext::new(&control, &mut work, &mut value_conversions);
            let step = source.as_mut().drive(&mut context).unwrap();
            if let Some(output) = step.output {
                assert!(output.len() <= 3);
                let column = output.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                rows.extend(column.values().iter().copied());
            }
            if matches!(step.next, NextStep::Yield) {
                yields += 1;
            }
            if matches!(step.next, NextStep::Finished) {
                ended = true;
                break;
            }
            if matches!(step.next, NextStep::Runnable) {
                let spent = source.as_mut().drive(&mut context).unwrap();
                assert!(spent.output.is_none());
                assert!(matches!(spent.next, NextStep::Yield));
                yields += 1;
            }
        }
        assert!(ended);
        assert!(
            yields >= 2,
            "the long unselected prefix must yield instead of replaying or blocking"
        );
        assert_eq!(rows, expected);
        let mut inactive = DistinctSetSourceFactory::new(shared, 7).create(2, 1);
        inactive.set_mem_tracker(Arc::clone(&tracker));
        inactive.set_construction_factory(Arc::clone(&factory));
        inactive.bind_runtime_state(&state).unwrap();
        let mut work = TurnWork::new(NonZeroU64::MIN, 1);
        let step = inactive
            .as_mut()
            .drive(&mut StepContext::new(
                &control,
                &mut work,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            ))
            .unwrap();
        assert!(step.output.is_none());
        assert!(matches!(step.next, NextStep::Finished));
    }
}
