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
//! Closed compiled integer-series processor over its exact bounds Values port.
//!
//! The preceding Values source owns parameter evaluation through the original
//! compiled Frame. This processor receives arrays only and calls the one
//! original Functions computation. Whole-call original Data remains the
//! function-domain cause; controls keep their typed first cause.
//! Expansion/Arrow retained memory and opaque allocation are still OPEN.

use super::compiled_expression::RuntimeKernelControl;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
    generate_series_core::{self, SeriesFailure, SeriesObservation},
};
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};
use std::sync::Arc;

#[path = "compiled_series_failure.rs"]
mod failure;
use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use failure::CompiledSeriesFailure;
fn binding(kind: FragmentBindingErrorKind, detail: &'static str) -> FragmentBindingError {
    FragmentBindingError::new(FragmentBindingTarget::Program, kind, detail)
}

pub(crate) struct CompiledGenerateSeriesProcessorFactory {
    name: String,
    input: SchemaRef,
    parameters: Arc<[usize]>,
    output_layout: novarocks_local_program::StaticLayout,
    error: Arc<RuntimeErrorState>,
}
impl CompiledGenerateSeriesProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, FragmentBindingError> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            binding(
                FragmentBindingErrorKind::MissingAssignment,
                "compiled series node is absent",
            )
        })?;
        let ProgramNodeKind::GenerateSeries {
            input,
            parameter_slots,
        } = graph_node.kind()
        else {
            return Err(binding(
                FragmentBindingErrorKind::WrongAssignmentKind,
                "compiled node is not an integer-series source",
            ));
        };
        let bounds = program.graph().nodes().get(input.index()).ok_or_else(|| {
            binding(
                FragmentBindingErrorKind::MissingAssignment,
                "compiled series bounds are absent",
            )
        })?;
        let ProgramNodeKind::Values { values } = bounds.kind() else {
            return Err(binding(
                FragmentBindingErrorKind::WrongAssignmentKind,
                "compiled series bounds are not Values",
            ));
        };
        let layout = bounds.output_layout();
        if values.num_rows() != 1
            || !matches!(parameter_slots.len(), 2 | 3)
            || graph_node.output_layout().slots().len() != 1
        {
            return Err(binding(
                FragmentBindingErrorKind::LayoutMismatch,
                "compiled series has an invalid bounds or output shape",
            ));
        }
        let parameters = parameter_slots
            .iter()
            .map(|slot| {
                layout
                    .slots()
                    .iter()
                    .position(|candidate| candidate == slot)
                    .ok_or_else(|| {
                        binding(
                            FragmentBindingErrorKind::LayoutMismatch,
                            "compiled series parameter is outside its bounds port",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output_layout = graph_node.output_layout().clone();
        Ok(Self {
            name: format!("COMPILED_GENERATE_SERIES (node={})", node.index()),
            input: layout.schema().clone(),
            parameters: Arc::from(parameters),
            output_layout,
            error,
        })
    }
    fn instance(
        &self,
        control: Arc<dyn KernelEvaluationControl>,
    ) -> CompiledGenerateSeriesProcessor {
        CompiledGenerateSeriesProcessor {
            name: self.name.clone(),
            input: Arc::clone(&self.input),
            parameters: Arc::clone(&self.parameters),
            output_layout: self.output_layout.clone(),
            output: None,
            construction: None,
            control,
            pending: None,
            offset: 0,
            accepted: false,
            batch_rows: None,
            failed: false,
        }
    }
}
impl OperatorFactory for CompiledGenerateSeriesProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(self.instance(Arc::new(
            RuntimeKernelControl::new(Arc::clone(&self.error)),
        ))))
    }
}

struct Work<'a> {
    control: &'a dyn KernelEvaluationControl,
    pending: u32,
}
impl<'a> Work<'a> {
    fn begin(control: &'a dyn KernelEvaluationControl) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        Ok(Self {
            control,
            pending: 0,
        })
    }
    fn flush(&mut self) -> Result<(), KernelFailure> {
        let units = std::mem::take(&mut self.pending);
        self.control.checkpoint(units)
    }
    fn observe(&mut self, event: SeriesObservation) -> Result<(), KernelFailure> {
        match event {
            SeriesObservation::Step => {
                self.pending += 1;
                if self.pending == MAX_UNOBSERVED_KERNEL_WORK {
                    self.flush()?;
                }
            }
            SeriesObservation::OpaqueBoundary => self.flush()?,
        };
        Ok(())
    }
}

struct CompiledGenerateSeriesProcessor {
    name: String,
    input: SchemaRef,
    parameters: Arc<[usize]>,
    output_layout: novarocks_local_program::StaticLayout,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    control: Arc<dyn KernelEvaluationControl>,
    pending: Option<Chunk>,
    offset: usize,
    accepted: bool,
    batch_rows: Option<std::num::NonZeroUsize>,
    failed: bool,
}
impl CompiledGenerateSeriesProcessor {
    fn prepared_output(&self) -> ExecutionResult<&ChunkSchemaRef> {
        self.output.as_ref().ok_or_else(|| {
            crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into()
        })
    }
    fn invoke(&self, chunk: &Chunk) -> ExecutionResult<Option<Chunk>> {
        let output_schema = self.prepared_output()?;
        let mut work = Work::begin(self.control.as_ref())?;
        if chunk.len() != 1 || chunk.schema().as_ref() != self.input.as_ref() {
            return Err(CompiledSeriesFailure::InputPort.into());
        }
        let batch = chunk.record_batch();
        let start = batch.column(self.parameters[0]);
        let stop = batch.column(self.parameters[1]);
        let step = self.parameters.get(2).map(|ordinal| batch.column(*ordinal));
        let expanded =
            generate_series_core::expand_observed(start, stop, step, 1, false, &mut |event| {
                work.observe(event)
            });
        let expanded = match expanded {
            Ok(value) => value,
            Err(SeriesFailure::Control(cause)) => return Err(cause.into()),
            Err(SeriesFailure::Cause(cause)) => return Err(ExecutionFailure::from_domain(cause)),
        };
        if expanded.total_rows == 0 {
            work.flush()?;
            return Ok(None);
        }
        let data_type = output_schema
            .arrow_schema_ref()
            .field(0)
            .data_type()
            .clone();
        let column = generate_series_core::result_column_observed(
            expanded.values,
            &[data_type],
            &mut |event| work.observe(event),
        );
        let column = match column {
            Ok(column) => column,
            Err(SeriesFailure::Control(cause)) => return Err(cause.into()),
            Err(SeriesFailure::Cause(cause)) => return Err(ExecutionFailure::from_domain(cause)),
        };
        work.flush()?;
        let output = if output_schema.is_typed() {
            use crate::exec::pipeline::operator::ConstructionFailure;
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let owner = factory.begin_typed_construction(output_schema)?;
            owner.observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns { channels: 1 },
            )?;
            Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
                Arc::clone(output_schema),
                vec![column],
                expanded.total_rows,
                owner.storage_lifetime(),
                |step| owner.observe(0, &[], step),
            )?
        } else {
            let batch = RecordBatch::try_new(output_schema.arrow_schema_ref(), vec![column])
                .map_err(CompiledSeriesFailure::Arrow)?;
            Chunk::try_new_with_chunk_schema(batch, Arc::clone(output_schema))?
        };
        work.flush()?;
        Ok(Some(output))
    }
    fn alive(&self) -> ExecutionResult<()> {
        if self.failed {
            Err(CompiledSeriesFailure::AlreadyFailed.into())
        } else {
            Ok(())
        }
    }
}
impl OperatorLifecycle for CompiledGenerateSeriesProcessor {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        self.alive()?;
        if self.output.is_some() {
            return Ok(());
        }
        let result = (|| -> ExecutionResult<ChunkSchemaRef> {
            use crate::exec::pipeline::operator::ConstructionFailure;
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            Ok(factory.create_compiled_chunk_schema(&self.output_layout)?)
        })();
        match result {
            Ok(output) => {
                self.output = Some(output);
                Ok(())
            }
            Err(cause) => {
                self.failed = true;
                Err(cause)
            }
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.batch_rows = Some(
            std::num::NonZeroUsize::new(state.chunk_size())
                .ok_or_else(|| CompiledSeriesFailure::InvalidBatchRows)?,
        );
        Ok(())
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.pending = None;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.failed = true;
        self.pending = None;
    }
}
impl InputEncodingPolicy for CompiledGenerateSeriesProcessor {}
impl TransformKernel for CompiledGenerateSeriesProcessor {
    fn apply(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        self.alive()?;
        self.prepared_output()?;
        if self.accepted {
            self.failed = true;
            return Err(CompiledSeriesFailure::DuplicateBounds.into());
        }
        self.batch_rows
            .ok_or_else(|| CompiledSeriesFailure::MissingBatchRows)?;
        self.accepted = true;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        match self.invoke(&chunk) {
            Ok(output) => {
                let next = if output.is_some() {
                    NextStep::Runnable
                } else {
                    NextStep::Finished
                };
                self.pending = output;
                Ok(StepResult::new(None, next, rows))
            }
            Err(error) => {
                self.failed = true;
                self.pending = None;
                Err(error)
            }
        }
    }
    fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.alive()?;
        if let Err(cause) = self.control.checkpoint(0) {
            self.failed = true;
            self.pending = None;
            return Err(cause.into());
        }
        let Some(chunk) = &self.pending else {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        };
        let total = chunk.len();
        let rows = self
            .batch_rows
            .ok_or_else(|| CompiledSeriesFailure::MissingBatchRows)?
            .get()
            .min(total - self.offset);
        let sliced = (|| -> ExecutionResult<Chunk> {
            let output = if chunk.chunk_schema().is_typed() {
                use crate::exec::pipeline::operator::ConstructionFailure;
                let factory = self
                    .construction
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?;
                let owner = factory.begin_input_construction(chunk)?;
                chunk.slice_typed_observed::<ConstructionFailure, _>(
                    self.offset,
                    rows,
                    owner.storage_lifetime(),
                    |step| owner.observe(0, &[], step),
                )?
            } else {
                chunk.slice(self.offset, rows)
            };
            Ok(output)
        })();
        let output = match sliced {
            Ok(output) => output,
            Err(cause) => {
                self.failed = true;
                self.pending = None;
                return Err(cause);
            }
        };
        if let Err(cause) = self.control.checkpoint(0) {
            self.failed = true;
            self.pending = None;
            return Err(cause.into());
        }
        self.offset += rows;
        let next = if self.offset == total {
            self.pending = None;
            NextStep::Finished
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(output), next, 0))
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.alive()?;
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
}

#[cfg(test)]
#[path = "compiled_generate_series_control_tests.rs"]
mod control_tests;
