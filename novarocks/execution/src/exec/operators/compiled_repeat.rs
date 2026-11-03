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

//! Compiled Repeat: grouping-set expansion of a compiled program.
//!
//! Repeat owns no expression root. Each input row is emitted once per frozen
//! grouping set, in set order: the set's omitted key channels become NULL and
//! the set's frozen GROUPING values are appended as constant Int64 columns.
//! The output is exactly the node's frozen layout, including the nullability
//! the compiler widened and the grouping channels' names; nothing is
//! re-derived from the incoming batch. This is array code only; the legacy
//! Repeat operator is not reused because it re-derives its output schema.

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, SchemaRef};
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};

use crate::exec::chunk::{Chunk, ChunkBatchProjection, ChunkSchemaRef};
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
};
use crate::runtime::task::ExecutionResult;

pub struct CompiledRepeatProcessorFactory {
    name: String,
    /// The frozen input port; channels are read by position.
    input: SchemaRef,
    input_layout: novarocks_local_program::StaticLayout,
    output_layout: novarocks_local_program::StaticLayout,
    /// Per grouping set, the input ordinals replaced by NULL.
    null_ordinals: Arc<[Vec<usize>]>,
    /// Per grouping set, the value of every GROUPING output in order.
    grouping_values: Arc<[Vec<i64>]>,
}

impl CompiledRepeatProcessorFactory {
    pub(crate) fn try_new(program: &LocalProgram, node: ProgramNodeId) -> ExecutionResult<Self> {
        let nodes = program.graph().nodes();
        let at = node.index();
        let graph_node = nodes.get(at).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled Repeat node {at} is absent"),
            )
        })?;
        let ProgramNodeKind::Repeat {
            input,
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled local node {at} is not a Repeat"),
            ));
        };
        let input_layout = nodes
            .get(input.index())
            .ok_or_else(|| {
                program_binding_failure(
                    PlanBindingKind::MissingAssignment,
                    format!("compiled Repeat at local node {at} has no input"),
                )
            })?
            .output_layout();
        let output_layout = graph_node.output_layout();
        let invalid = |what: &str| {
            program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled Repeat at local node {at} {what}"),
            )
        };
        let width = input_layout.slots().len();
        if *repeat_times == 0
            || null_slot_ids.len() != *repeat_times
            || grouping_list.len() != grouping_slot_ids.len()
            || grouping_list.iter().any(|row| row.len() != *repeat_times)
        {
            return Err(invalid("has inconsistent grouping-set counts"));
        }
        if output_layout.slots().len() != width + grouping_slot_ids.len()
            || output_layout.slots()[..width] != *input_layout.slots()
            || output_layout.slots()[width..] != grouping_slot_ids[..]
        {
            return Err(invalid(
                "output channels are not its input followed by its grouping outputs",
            ));
        }
        let input_fields = input_layout.schema().fields();
        let output_fields = output_layout.schema().fields();
        for (ordinal, (input_field, output_field)) in
            input_fields.iter().zip(output_fields.iter()).enumerate()
        {
            if let (Some(input), Some(output)) = (
                input_layout.value_binding_at(ordinal),
                output_layout.value_binding_at(ordinal),
            ) {
                if (input.value_type().nullable() && !output.value_type().nullable())
                    || !input
                        .borrowed()
                        .same_physical_binding_ignoring_root_nullability_observed(
                            output.borrowed(),
                            || Ok::<(), std::convert::Infallible>(()),
                        )
                        .unwrap_or_else(|never| match never {})
                {
                    return Err(invalid(
                        "changes a passthrough channel's complete value binding",
                    ));
                }
            } else if input_layout.is_typed() || output_layout.is_typed() {
                return Err(invalid("is missing a passthrough value binding"));
            }
            if input_field.data_type() != output_field.data_type() {
                return Err(invalid("changes a passthrough channel type"));
            }
        }
        if output_fields[width..]
            .iter()
            .any(|field| field.data_type() != &DataType::Int64)
        {
            return Err(invalid("has a non-Int64 GROUPING output"));
        }
        let mut null_ordinals = Vec::with_capacity(*repeat_times);
        for slots in null_slot_ids {
            let mut ordinals = Vec::with_capacity(slots.len());
            for slot in slots {
                let ordinal = input_layout
                    .slots()
                    .iter()
                    .position(|candidate| candidate == slot)
                    .ok_or_else(|| invalid(&format!("nulls slot {slot} outside its input")))?;
                if !output_fields[ordinal].is_nullable() {
                    return Err(invalid(&format!("nulls non-nullable output slot {slot}")));
                }
                ordinals.push(ordinal);
            }
            null_ordinals.push(ordinals);
        }
        let grouping_values = (0..*repeat_times)
            .map(|set| grouping_list.iter().map(|row| row[set]).collect())
            .collect::<Vec<Vec<i64>>>();
        Ok(Self {
            name: format!("COMPILED_REPEAT (node={at})"),
            input: input_layout.schema().clone(),
            input_layout: input_layout.clone(),
            output_layout: output_layout.clone(),
            null_ordinals: Arc::from(null_ordinals),
            grouping_values: Arc::from(grouping_values),
        })
    }
}

impl OperatorFactory for CompiledRepeatProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(CompiledRepeatProcessor {
            name: self.name.clone(),
            input_schema: Arc::clone(&self.input),
            input_layout: self.input_layout.clone(),
            output_layout: self.output_layout.clone(),
            output: None,
            construction: None,
            null_ordinals: Arc::clone(&self.null_ordinals),
            grouping_values: Arc::clone(&self.grouping_values),
            input: None,
            next_set: 0,
        }))
    }
}

struct CompiledRepeatProcessor {
    name: String,
    input_schema: SchemaRef,
    input_layout: novarocks_local_program::StaticLayout,
    output_layout: novarocks_local_program::StaticLayout,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    null_ordinals: Arc<[Vec<usize>]>,
    grouping_values: Arc<[Vec<i64>]>,
    input: Option<ChunkBatchProjection>,
    next_set: usize,
}

impl CompiledRepeatProcessor {
    fn prepared_output(&self) -> ExecutionResult<&ChunkSchemaRef> {
        self.output.as_ref().ok_or_else(|| {
            crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into()
        })
    }
    fn expand(&self, input: &Chunk, set: usize) -> ExecutionResult<Chunk> {
        let factory = self
            .construction
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        super::repeat_processor::repeat_output_columns_owned(
            input,
            self.prepared_output()?,
            self.null_ordinals[set]
                .iter()
                .copied()
                .map(Ok::<_, crate::runtime::task::ExecutionFailure>),
            self.grouping_values[set]
                .iter()
                .copied()
                .map(Ok::<_, crate::runtime::task::ExecutionFailure>),
            factory.as_ref(),
            |value, rows| Arc::new(Int64Array::from_value(value, rows)),
        )
    }
}

impl OperatorLifecycle for CompiledRepeatProcessor {
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
        use crate::exec::pipeline::operator::ConstructionFailure;
        if self.output.is_some() {
            return Ok(());
        }
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        self.output = Some(factory.create_compiled_chunk_schema(&self.output_layout)?);
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.input = None;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.input = None;
        Ok(())
    }
    fn cancel(&mut self) {
        self.input = None;
    }
}
impl InputEncodingPolicy for CompiledRepeatProcessor {}
impl TransformKernel for CompiledRepeatProcessor {
    fn apply(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        self.prepared_output()?;
        if chunk.is_empty() {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        }
        if chunk.record_batch().schema().as_ref() != self.input_schema.as_ref() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "compiled Repeat input differs from its frozen input port",
            ));
        }
        let factory = self
            .construction
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        super::repeat_processor::validate_repeat_input(
            &chunk,
            self.input_schema.as_ref(),
            self.input_layout.slots(),
            |ordinal| self.input_layout.value_binding_at(ordinal),
            factory.as_ref(),
        )?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        self.input = Some(chunk.into_batch_projection());
        self.next_set = 0;
        Ok(StepResult::new(None, NextStep::Runnable, rows))
    }
    fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        let Some(input) = self.input.as_ref() else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        let source = input.clone().into_chunk();
        let output = self.expand(&source, self.next_set)?;
        self.next_set += 1;
        let next = if self.next_set == self.grouping_values.len() {
            self.input = None;
            NextStep::NeedInput
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(output), next, 0))
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.prepared_output()?;
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
}
