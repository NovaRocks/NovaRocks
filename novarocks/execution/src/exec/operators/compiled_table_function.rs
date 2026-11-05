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

//! Compiled TableFunction: every input row is one parent of one exact table
//! call, and the output is the relation it produces joined to that parent.
//!
//! The kernel is the program's prepared table cursor at its `Table` call site,
//! never a function looked up by name. Each input batch is one invocation over
//! all its rows: its parameters are the frozen parameter channels of the
//! argument Project that feeds this node, which already evaluated every
//! argument root through compiled instances. The host drives the cursor with
//! an explicit full step grant -- a page of up to one chunk of rows, every
//! parent's completion and every parent's error -- so a step that still asks
//! for capacity can never make progress and is refused instead of retried.
//!
//! The host owns what the kernel does not: it rejects a row or a completion
//! for a parent that already completed, and an end before every parent
//! completed; a parent row error is a required error and fails the operator;
//! LEFT OUTER emits one NULL-extended row for each parent that completed
//! without a row. Output rows follow parent order, a parent's rows in the
//! order the kernel produced them, so the operator preserves its input's
//! order and every pass-through ordering. Rows stay on their driver.
//!
//! The invocation currently retains its relation and parent-order selection.
//! Output arrays are gathered one page per driver step. The function's grant
//! bounds each kernel page, not the expansion of a whole input batch; memory
//! admission and incremental invocation remain obligations of the MEM work.

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
#[path = "compiled_table_failure.rs"]
mod failure;
use failure::{CompiledTableFailure, CompiledTableFailureKind};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt64Array, new_empty_array};
use arrow::compute::{concat, take};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{
    EvaluatedArgument, PreparedTableKernel, SelectedTableInput, Selection, TableCursorStep,
    TableEvaluationCursor, TablePageCapacity, TableStepCapacity,
};
use novarocks_local_program::{
    LocalProgram, ProgramCallSite, ProgramNodeId, ProgramNodeKind, ProgramStateTemplate,
    TableFunctionOutputSlot,
};
use novarocks_types::SlotId;

use super::compiled_expression::RuntimeKernelControl;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

/// The producer of one output channel, in output order.
#[derive(Clone, Copy, Debug)]
enum Producer {
    /// An outer pass-through channel at this input ordinal.
    Outer(usize),
    /// This produced relation column.
    Result(usize),
}

pub struct CompiledTableFunctionProcessorFactory {
    name: String,
    node: usize,
    kernel: Arc<dyn PreparedTableKernel>,
    /// The frozen input port; channels are read by position.
    input: SchemaRef,
    input_layout: novarocks_local_program::StaticLayout,
    params: Arc<[usize]>,
    producers: Arc<[Producer]>,
    left_outer: bool,
    output_layout: novarocks_local_program::StaticLayout,
    error: Arc<RuntimeErrorState>,
}

impl CompiledTableFunctionProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let at = node.index();
        let nodes = program.graph().nodes();
        let graph_node = nodes.get(at).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled TableFunction node {at} is absent"),
            )
        })?;
        let ProgramNodeKind::TableFunction {
            input,
            param_slots,
            outer_slots,
            fn_result_slots,
            fn_result_required,
            is_left_join,
            output_slot_sources,
            ..
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled local node {at} is not a TableFunction"),
            ));
        };
        let invalid = |what: String| {
            program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled TableFunction at local node {at} {what}"),
            )
        };
        if !*fn_result_required {
            return Err(invalid(
                "with optional function results is not executable yet".to_string(),
            ));
        }
        let Some(ProgramStateTemplate::TableCursor { kernel, .. }) =
            program.state_template(ProgramCallSite::Table { node })
        else {
            return Err(invalid("has no prepared table cursor".to_string()));
        };
        let kernel = Arc::clone(kernel);
        let contract = Arc::clone(kernel.contract());
        let argument_types = contract.argument_types().collect::<Vec<_>>();
        let result_types = contract.result_types();
        if param_slots.len() != argument_types.len() || fn_result_slots.len() != result_types.len()
        {
            return Err(invalid(format!(
                "has {} parameters and {} results for a call of {} arguments and {} results",
                param_slots.len(),
                fn_result_slots.len(),
                argument_types.len(),
                result_types.len()
            )));
        }
        let input_layout = nodes
            .get(input.index())
            .ok_or_else(|| invalid("has no input".to_string()))?
            .output_layout();
        let layout = graph_node.output_layout();
        let input_ordinal = |slot: SlotId| {
            input_layout
                .slots()
                .iter()
                .position(|candidate| *candidate == slot)
                .ok_or_else(|| invalid(format!("reads slot {slot} outside its input")))
        };
        let input_type = |ordinal: usize| input_layout.schema().field(ordinal).data_type();
        let mut params = Vec::with_capacity(param_slots.len());
        for (slot, expected) in param_slots.iter().zip(&argument_types) {
            let ordinal = input_ordinal(*slot)?;
            if input_layout.is_typed()
                && input_layout.value_binding_at(ordinal) != Some(expected.typed_binding())
            {
                return Err(invalid(format!(
                    "parameter slot {slot} differs from its complete frozen binding"
                )));
            }
            if input_type(ordinal) != expected.data_type() {
                return Err(invalid(format!(
                    "parameter slot {slot} has type {:?}, not {:?}",
                    input_type(ordinal),
                    expected.data_type()
                )));
            }
            params.push(ordinal);
        }
        if output_slot_sources.len() != layout.slots().len() {
            return Err(invalid(
                "has an output source count different from its layout".to_string(),
            ));
        }
        let mut producers = Vec::with_capacity(output_slot_sources.len());
        for (ordinal, source) in output_slot_sources.iter().enumerate() {
            let output_type = layout.schema().field(ordinal).data_type();
            let (producer, source_type) = match source {
                TableFunctionOutputSlot::Outer { slot } => {
                    if !outer_slots.contains(slot) {
                        return Err(invalid(format!(
                            "passes through slot {slot} that is not an outer slot"
                        )));
                    }
                    let input = input_ordinal(*slot)?;
                    (Producer::Outer(input), input_type(input))
                }
                TableFunctionOutputSlot::Result { index } => {
                    let result = result_types.get(*index).ok_or_else(|| {
                        invalid(format!("publishes result {index} outside its relation"))
                    })?;
                    (Producer::Result(*index), result.data_type())
                }
            };
            if source_type != output_type {
                return Err(invalid(format!(
                    "output channel {ordinal} has type {output_type:?}, not {source_type:?}"
                )));
            }
            if layout.is_typed() {
                let target = layout
                    .value_binding_at(ordinal)
                    .ok_or_else(|| invalid("has an output without a complete binding".into()))?;
                let source = match producer {
                    Producer::Outer(input) => {
                        input_layout.value_binding_at(input).ok_or_else(|| {
                            invalid("has an outer input without a complete binding".into())
                        })?
                    }
                    Producer::Result(index) => result_types[index].typed_binding(),
                };
                if (source.nullable() && !target.nullable())
                    || !source
                        .borrowed()
                        .same_physical_binding_ignoring_root_nullability_observed(
                            target.borrowed(),
                            || Ok::<(), crate::runtime::task::ExecutionFailure>(()),
                        )?
                {
                    return Err(invalid(format!(
                        "output channel {ordinal} differs from its complete source binding"
                    )));
                }
            } else if input_layout.is_typed() {
                return Err(invalid("is missing its complete output bindings".into()));
            }
            producers.push(producer);
        }
        let output_layout = layout.clone();
        Ok(Self {
            name: format!("COMPILED_TABLE_FUNCTION (node={at})"),
            node: at,
            kernel,
            input: input_layout.schema().clone(),
            input_layout: input_layout.clone(),
            params: Arc::from(params),
            producers: Arc::from(producers),
            left_outer: *is_left_join,
            output_layout,
            error,
        })
    }
}

impl OperatorFactory for CompiledTableFunctionProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(CompiledTableFunctionProcessor {
            name: self.name.clone(),
            node: self.node,
            kernel: Arc::clone(&self.kernel),
            input_schema: Arc::clone(&self.input),
            input_layout: self.input_layout.clone(),
            params: Arc::clone(&self.params),
            producers: Arc::clone(&self.producers),
            left_outer: self.left_outer,
            output_layout: self.output_layout.clone(),
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            cursor: None,
            batch_rows: None,
            failed: false,
        }))
    }
}

struct CompiledTableFunctionProcessor {
    name: String,
    node: usize,
    kernel: Arc<dyn PreparedTableKernel>,
    input_schema: SchemaRef,
    input_layout: novarocks_local_program::StaticLayout,
    params: Arc<[usize]>,
    producers: Arc<[Producer]>,
    left_outer: bool,
    output_layout: novarocks_local_program::StaticLayout,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: RuntimeKernelControl,
    cursor: Option<OutputCursor>,
    batch_rows: Option<std::num::NonZeroUsize>,
    failed: bool,
}

/// The rows one invocation produced, in arrival order, with their parents.
struct Produced {
    /// Each produced row's parent ordinal.
    parents: Vec<usize>,
    /// Every produced relation column, concatenated in arrival order.
    columns: Vec<ArrayRef>,
    /// Rows produced for each parent.
    counts: Vec<usize>,
}

/// One invocation's relation and its parent-order selection. Output arrays are
/// gathered only when the driver makes a free output edge available.
struct OutputCursor {
    input: Chunk,
    columns: Vec<ArrayRef>,
    parent_rows: Vec<u64>,
    result_rows: Vec<Option<u64>>,
    offset: usize,
}

impl CompiledTableFunctionProcessor {
    fn alive(&self) -> ExecutionResult<()> {
        if self.failed {
            Err(self.failure(CompiledTableFailureKind::AlreadyFailed))
        } else {
            Ok(())
        }
    }
    fn prepared_output(&self) -> ExecutionResult<&ChunkSchemaRef> {
        self.output
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }
    fn failure(&self, reason: CompiledTableFailureKind) -> ExecutionFailure {
        CompiledTableFailure {
            node: self.node,
            reason,
        }
        .into()
    }

    /// Run one exact invocation over every row of `batch` to its end.
    fn invoke(&self, batch: &RecordBatch, page_rows: usize) -> ExecutionResult<Produced> {
        let parents = batch.num_rows();
        let contract = self.kernel.contract();
        let arguments = self
            .params
            .iter()
            .map(|ordinal| EvaluatedArgument::Column(batch.column(*ordinal)))
            .collect::<Vec<_>>();
        let selection = Selection::all(parents);
        let input =
            SelectedTableInput::try_new(contract.as_ref(), selection, &arguments, &self.control)?;
        let mut cursor =
            TableEvaluationCursor::begin(Arc::clone(&self.kernel), input, &self.control)?;
        // The full grant: one chunk of rows, and every parent's completion
        // and error. Each channel needs at most one fact to make progress.
        let capacity = TableStepCapacity {
            page: TablePageCapacity {
                rows: page_rows,
                completions: parents,
            },
            parent_errors: parents,
        };
        let mut completed = vec![false; parents];
        let mut counts = vec![0usize; parents];
        let mut produced = Vec::new();
        let mut pages = Vec::new();
        loop {
            let page = match cursor.next(capacity, &self.control)? {
                TableCursorStep::Page(page) => page,
                TableCursorStep::CapacityRequired(required) => {
                    return Err(self.failure(CompiledTableFailureKind::Capacity {
                        required,
                        granted: capacity,
                    }));
                }
            };
            // A parent's row error is a required error of this operator.
            if let Some(error) = page.parent_errors.first() {
                let row = selection.row(error.selected_ordinal()).ok_or_else(|| {
                    self.failure(CompiledTableFailureKind::ParentErrorOrdinal {
                        ordinal: error.selected_ordinal(),
                    })
                })?;
                return Err(self.failure(CompiledTableFailureKind::ParentData {
                    row,
                    cause: error.clone(),
                }));
            }
            for &parent in page.parent_ordinals.iter() {
                if completed[parent] {
                    return Err(
                        self.failure(CompiledTableFailureKind::RowAfterCompletion { parent })
                    );
                }
                counts[parent] = counts[parent].checked_add(1).ok_or_else(|| {
                    self.failure(CompiledTableFailureKind::OutputExtent {
                        prefix: counts[parent],
                        additional: 1,
                    })
                })?;
                produced.push(parent);
            }
            for &parent in page.completed_parents.iter() {
                if std::mem::replace(&mut completed[parent], true) {
                    return Err(
                        self.failure(CompiledTableFailureKind::DuplicateCompletion { parent })
                    );
                }
            }
            if !page.parent_ordinals.is_empty() {
                pages.push(page.columns);
            }
            if page.eof {
                break;
            }
        }
        if let Some(first) = completed.iter().position(|done| !done) {
            return Err(self.failure(CompiledTableFailureKind::IncompleteParents { first }));
        }
        cursor.finish(&self.control)?;
        drop(cursor);
        let mut columns = Vec::with_capacity(contract.result_types().len());
        for (index, result) in contract.result_types().iter().enumerate() {
            let column = if pages.is_empty() {
                new_empty_array(result.data_type())
            } else {
                let parts = pages
                    .iter()
                    .map(|page| page[index].as_ref())
                    .collect::<Vec<&dyn Array>>();
                concat(&parts).map_err(|error| {
                    self.failure(CompiledTableFailureKind::Arrow {
                        result: Some(index),
                        cause: error,
                    })
                })?
            };
            columns.push(column);
        }
        Ok(Produced {
            parents: produced,
            columns,
            counts,
        })
    }

    /// Freeze parent order once; no output chunks are materialized here.
    fn ordered_output(&self, input: Chunk, produced: Produced) -> ExecutionResult<OutputCursor> {
        let parents = input.len();
        // Rows of each parent: its produced rows, or one NULL-extended row for
        // a LEFT OUTER parent that completed without one.
        let mut starts = Vec::with_capacity(parents + 1);
        let mut total = 0usize;
        for &count in &produced.counts {
            starts.push(total);
            let rows = if count == 0 && self.left_outer {
                1
            } else {
                count
            };
            total = total.checked_add(rows).ok_or_else(|| {
                self.failure(CompiledTableFailureKind::OutputExtent {
                    prefix: total,
                    additional: rows,
                })
            })?;
        }
        starts.push(total);
        let mut parent_rows = vec![0u64; total];
        let mut result_rows: Vec<Option<u64>> = vec![None; total];
        let mut next = starts.clone();
        for (row, &parent) in produced.parents.iter().enumerate() {
            let at = next[parent];
            next[parent] += 1;
            parent_rows[at] = parent as u64;
            result_rows[at] = Some(row as u64);
        }
        for parent in 0..parents {
            if next[parent] < starts[parent + 1] {
                // The NULL-extended row of an empty LEFT OUTER parent.
                parent_rows[next[parent]] = parent as u64;
                next[parent] += 1;
            }
        }
        Ok(OutputCursor {
            input,
            columns: produced.columns,
            parent_rows,
            result_rows,
            offset: 0,
        })
    }

    fn output_page(&self, cursor: &OutputCursor, rows: usize) -> ExecutionResult<Chunk> {
        let end = cursor.offset + rows;
        let outer = UInt64Array::from(cursor.parent_rows[cursor.offset..end].to_vec());
        let results = UInt64Array::from(cursor.result_rows[cursor.offset..end].to_vec());
        let output = self.prepared_output()?;
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_typed_construction(output)?;
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns {
                channels: self.producers.len(),
            },
        )?;
        let mut columns = Vec::with_capacity(self.producers.len());
        for producer in self.producers.iter() {
            let column = match producer {
                Producer::Outer(input) => {
                    take(cursor.input.columns()[*input].as_ref(), &outer, None)
                }
                Producer::Result(index) => take(cursor.columns[*index].as_ref(), &results, None),
            }
            .map_err(|error| {
                self.failure(CompiledTableFailureKind::Arrow {
                    result: None,
                    cause: error,
                })
            })?;
            columns.push(column);
        }
        // Arrow gather may reuse nested input backing. The output must retain
        // its original finite storage responsibility after this cursor exits.
        let storage = cursor
            .input
            .source_storage_lifetime_observed::<ConstructionFailure>(
                owner.storage_lifetime(),
                |layout| owner.request(layout),
            )?;
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ConstructionFailure,
            _,
        >(
            Arc::clone(output),
            columns,
            rows,
            storage,
            |step| owner.observe(0, &[], step),
        )?)
    }
}

impl OperatorLifecycle for CompiledTableFunctionProcessor {
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
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        self.batch_rows = Some(
            std::num::NonZeroUsize::new(state.chunk_size())
                .ok_or_else(|| self.failure(CompiledTableFailureKind::InvalidBatchRows))?,
        );
        Ok(())
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

impl InputEncodingPolicy for CompiledTableFunctionProcessor {}
impl TransformKernel for CompiledTableFunctionProcessor {
    fn apply(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        self.alive()?;
        let chunk = input.into_uncovered_chunk()?;
        self.prepared_output()?;
        if chunk.is_empty() {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        }
        if chunk.record_batch().schema().as_ref() != self.input_schema.as_ref() {
            return Err(self.failure(CompiledTableFailureKind::InputPort));
        }
        if self.input_layout.is_typed() {
            let schema = chunk.chunk_schema();
            if !schema.is_typed()
                || schema.slots().len() != self.input_layout.slots().len()
                || schema.slots().iter().enumerate().any(|(ordinal, slot)| {
                    self.input_layout.value_binding_at(ordinal) != Some(slot.typed_binding())
                })
            {
                return Err(self.failure(CompiledTableFailureKind::InputValueBindings));
            }
        }
        let chunk_rows = self
            .batch_rows
            .ok_or_else(|| self.failure(CompiledTableFailureKind::MissingBatchRows))?
            .get();
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        let result = self
            .invoke(chunk.record_batch(), chunk_rows)
            .and_then(|produced| self.ordered_output(chunk, produced));
        match result {
            Ok(cursor) => {
                let next = if cursor.parent_rows.is_empty() {
                    NextStep::NeedInput
                } else {
                    self.cursor = Some(cursor);
                    NextStep::Runnable
                };
                Ok(StepResult::new(None, next, rows))
            }
            Err(cause) => {
                self.failed = true;
                self.cursor = None;
                Err(cause)
            }
        }
    }
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.alive()?;
        let Some(cursor) = self.cursor.as_ref() else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let rows = self
            .batch_rows
            .expect("an installed cursor has a prepared batch size")
            .get()
            .min(cursor.parent_rows.len() - cursor.offset);
        let output = match self.output_page(cursor, rows) {
            Ok(output) => output,
            Err(cause) => {
                self.failed = true;
                self.cursor = None;
                return Err(cause);
            }
        };
        let cursor = self
            .cursor
            .as_mut()
            .expect("output cursor remains installed");
        cursor.offset += rows;
        let next = if cursor.offset == cursor.parent_rows.len() {
            self.cursor = None;
            NextStep::NeedInput
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
