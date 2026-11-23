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
//! Demand-driven generation over the exact, once-evaluated bounds Values port.
//!
//! The processor retains a cursor. Each driver step constructs and hands off
//! at most one batch using its actual construction scope.

use super::compiled_expression::RuntimeKernelControl;
use crate::exec::chunk::{ArrayNormalizationStep, Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner, InputEncodingPolicy,
    Operator, OperatorLifecycle,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
};
use crate::runtime::runtime_state::RuntimeErrorState;
use crate::runtime::task::ExecutionResult;
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, PrimitiveArray};
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowNativeType, DataType, Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef,
};
use novarocks_functions::{
    EvaluationCheckpoints, KernelEvaluationControl, KernelFailure,
    array_projection::ARROW_BUFFER_OWNER_METADATA_BOUND,
    generate_series_core::{IntegerDiagnosticContext, SeriesError, integer_argument},
};
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};
use novarocks_type_contract::{
    ValueTypeStorageLifetime,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};
use std::panic::RefUnwindSafe;
use std::sync::Arc;
use std::{alloc::Layout, mem};

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
    batch_rows: std::num::NonZeroU32,
}
impl CompiledGenerateSeriesProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
        batch_rows: std::num::NonZeroU32,
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
            batch_rows,
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
            cursor: None,
            accepted: false,
            batch_rows: self.batch_rows,
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

#[derive(Clone, Copy)]
// Design: ADR-0255 (docs/adr/ADR-0255-collection-calls-and-relational-sequence.md)
struct SeriesCursor {
    current: i128,
    stop: i128,
    step: i128,
}

impl SeriesCursor {
    fn new(start: i128, stop: i128, step: i128) -> Result<Option<Self>, SeriesError> {
        if step == 0 {
            return Err(SeriesError::ZeroStep);
        }
        if (step > 0 && start > stop) || (step < 0 && start < stop) {
            return Ok(None);
        }
        Ok(Some(Self {
            current: start,
            stop,
            step,
        }))
    }

    fn batch(&self, max_rows: std::num::NonZeroU32) -> (usize, bool) {
        // Count steps after the current value. Adding a row for the current
        // value only after taking the batch bound also handles full i128 spans.
        let steps = self.current.abs_diff(self.stop) / self.step.unsigned_abs();
        let rows = steps.min(u128::from(max_rows.get() - 1)) + 1;
        (rows as usize, steps >= rows)
    }

    fn fill(
        &mut self,
        rows: usize,
        has_successor: bool,
        work: &mut EvaluationCheckpoints<'_>,
        mut emit: impl FnMut(i128) -> Result<(), SeriesError>,
    ) -> ExecutionResult<()> {
        for row in 0..rows {
            work.step()?;
            emit(self.current)?;
            if row + 1 < rows || has_successor {
                self.current =
                    self.current
                        .checked_add(self.step)
                        .ok_or(SeriesError::ValueProgress {
                            current: self.current,
                            step: self.step,
                        })?;
            }
        }
        Ok(())
    }
}

/// The output payload and its admitted construction lifetime follow the last
/// actual Arrow Buffer alias, including arrays detached from their Chunk.
struct BatchValues<T> {
    values: Vec<T>,
    _storage: ValueTypeStorageLifetime,
}

// The array is immutable after publication. Drop only releases owned storage.
impl<T: RefUnwindSafe> RefUnwindSafe for BatchValues<T> {}

fn series_column<T: ArrowNativeType + RefUnwindSafe, A: Array + 'static>(
    elements: usize,
    owner: &dyn InputConstructionOwner,
    work: &mut EvaluationCheckpoints<'_>,
    fill: impl FnOnce(&mut Vec<T>, &mut EvaluationCheckpoints<'_>) -> ExecutionResult<()>,
    wrap: impl FnOnce(Buffer) -> Result<A, CompiledSeriesFailure>,
) -> ExecutionResult<ArrayRef> {
    work.flush()?;
    let layout = Layout::array::<T>(elements)
        .map_err(|_| ConstructionFailure::Resource(LayoutResourceError::Arithmetic))?;
    owner.request(layout)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(elements)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    fill(&mut values, work)?;
    work.flush()?;

    owner
        .request(arc_layout(Layout::new::<BatchValues<T>>()).map_err(ConstructionFailure::from)?)?;
    owner.observe_opaque_upper_bound(ARROW_BUFFER_OWNER_METADATA_BOUND)?;
    let backing = Arc::new(BatchValues {
        values,
        _storage: owner.storage_lifetime(),
    });
    let bytes = mem::size_of_val(backing.values.as_slice());
    let pointer = std::ptr::NonNull::from(backing.values.as_slice()).cast();
    // SAFETY: the initialized, correctly aligned Vec payload is immutable
    // and is owned by the Buffer's custom allocation for its full lifetime.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, bytes, backing) };
    owner.request(arc_layout(Layout::new::<A>()).map_err(ConstructionFailure::from)?)?;
    Ok(Arc::new(wrap(buffer)?))
}

struct CompiledGenerateSeriesProcessor {
    name: String,
    input: SchemaRef,
    parameters: Arc<[usize]>,
    output_layout: novarocks_local_program::StaticLayout,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: Arc<dyn KernelEvaluationControl>,
    cursor: Option<SeriesCursor>,
    accepted: bool,
    batch_rows: std::num::NonZeroU32,
    failed: bool,
}
impl CompiledGenerateSeriesProcessor {
    fn prepared_output(&self) -> ExecutionResult<&ChunkSchemaRef> {
        self.output
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }

    fn bind_bounds(&self, chunk: &Chunk) -> ExecutionResult<Option<SeriesCursor>> {
        let mut work = EvaluationCheckpoints::new(self.control.as_ref());
        work.flush()?;
        if chunk.len() != 1 || chunk.schema().as_ref() != self.input.as_ref() {
            return Err(CompiledSeriesFailure::InputPort.into());
        }
        let batch = chunk.record_batch();
        let mut argument = |index: usize| -> ExecutionResult<Option<i128>> {
            work.step()?;
            Ok(integer_argument(
                batch.column(self.parameters[index]),
                0,
                index,
                IntegerDiagnosticContext::GenerateSeries,
            )?)
        };
        let start = argument(0)?;
        let stop = argument(1)?;
        let step = if self.parameters.len() == 3 {
            argument(2)?
        } else {
            Some(1)
        };
        let cursor = match (start, stop, step) {
            (Some(start), Some(stop), Some(step)) => SeriesCursor::new(start, stop, step)?,
            _ => None,
        };
        work.finish()?;
        Ok(cursor)
    }

    fn build_batch(
        &self,
        mut cursor: SeriesCursor,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> ExecutionResult<(Chunk, Option<SeriesCursor>)> {
        let (rows, has_successor) = cursor.batch(self.batch_rows);
        let output_schema = self.prepared_output()?;
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_typed_construction(output_schema)?;
        let ty = output_schema.arrow_schema_ref();
        let data_type = ty.field(0).data_type();
        macro_rules! signed_column {
            ($arrow:ty, $native:ty) => {
                series_column(
                    rows,
                    owner.as_ref(),
                    work,
                    |values, work| {
                        cursor.fill(rows, has_successor, work, |value| {
                            let converted = <$native>::try_from(value).map_err(|_| {
                                SeriesError::ValueOutOfRange {
                                    value,
                                    data_type: data_type.clone(),
                                }
                            })?;
                            values.push(converted);
                            Ok(())
                        })
                    },
                    |buffer| {
                        Ok(PrimitiveArray::<$arrow>::new(
                            ScalarBuffer::new(buffer, 0, rows),
                            None,
                        ))
                    },
                )?
            };
        }
        let column = match data_type {
            DataType::Int8 => signed_column!(Int8Type, i8),
            DataType::Int16 => signed_column!(Int16Type, i16),
            DataType::Int32 => signed_column!(Int32Type, i32),
            DataType::Int64 => signed_column!(Int64Type, i64),
            DataType::FixedSizeBinary(width)
                if *width == novarocks_functions::largeint::LARGEINT_BYTE_WIDTH =>
            {
                let bytes =
                    rows.checked_mul(*width as usize)
                        .ok_or(ConstructionFailure::Resource(
                            LayoutResourceError::Arithmetic,
                        ))?;
                series_column(
                    bytes,
                    owner.as_ref(),
                    work,
                    |values, work| {
                        cursor.fill(rows, has_successor, work, |value| {
                            values.extend_from_slice(&value.to_be_bytes());
                            Ok(())
                        })
                    },
                    |buffer| {
                        FixedSizeBinaryArray::try_new(*width, buffer, None)
                            .map_err(CompiledSeriesFailure::Arrow)
                    },
                )?
            }
            other => {
                return Err(SeriesError::ReturnType {
                    data_type: other.clone(),
                }
                .into());
            }
        };
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::CreateChunkColumns { channels: 1 },
        )?;
        let output = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            Arc::clone(output_schema),
            vec![column],
            rows,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?;
        Ok((output, has_successor.then_some(cursor)))
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
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        self.alive()?;
        if self.output.is_some() {
            return Ok(());
        }
        let result = (|| -> ExecutionResult<ChunkSchemaRef> {
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
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.cursor = None;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.failed = true;
        self.cursor = None;
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
            self.cursor = None;
            return Err(CompiledSeriesFailure::DuplicateBounds.into());
        }
        self.accepted = true;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        match self.bind_bounds(&chunk) {
            Ok(cursor) => {
                let next = if cursor.is_some() {
                    NextStep::Runnable
                } else {
                    NextStep::Finished
                };
                self.cursor = cursor;
                Ok(StepResult::new(None, next, rows))
            }
            Err(error) => {
                self.failed = true;
                self.cursor = None;
                Err(error)
            }
        }
    }

    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.alive()?;
        let Some(cursor) = self.cursor else {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        };
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let result = (|| {
            let mut work = EvaluationCheckpoints::new(self.control.as_ref());
            work.flush()?;
            let batch = self.build_batch(cursor, &mut work)?;
            work.finish()?;
            Ok(batch)
        })();
        match result {
            Ok((output, cursor)) => {
                self.cursor = cursor;
                let next = if cursor.is_some() {
                    NextStep::Runnable
                } else {
                    NextStep::Finished
                };
                Ok(StepResult::new(Some(output), next, 0))
            }
            Err(error) => {
                self.failed = true;
                self.cursor = None;
                Err(error)
            }
        }
    }

    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.alive()?;
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
}

#[cfg(test)]
#[path = "compiled_generate_series_control_tests.rs"]
mod control_tests;
