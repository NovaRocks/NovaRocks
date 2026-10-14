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

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, new_null_array};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::RuntimeState;

pub struct CompiledRepeatProcessorFactory {
    name: String,
    /// The frozen input port; channels are read by position.
    input: SchemaRef,
    output: ChunkSchemaRef,
    /// Per grouping set, the input ordinals replaced by NULL.
    null_ordinals: Arc<[Vec<usize>]>,
    /// Per grouping set, the value of every GROUPING output in order.
    grouping_values: Arc<[Vec<i64>]>,
}

impl CompiledRepeatProcessorFactory {
    pub(crate) fn try_new(program: &LocalProgram, node: ProgramNodeId) -> Result<Self, String> {
        let nodes = program.graph().nodes();
        let at = node.index();
        let graph_node = nodes
            .get(at)
            .ok_or_else(|| format!("compiled Repeat node {at} is absent"))?;
        let ProgramNodeKind::Repeat {
            input,
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
        } = graph_node.kind()
        else {
            return Err(format!("compiled local node {at} is not a Repeat"));
        };
        let input_layout = nodes
            .get(input.index())
            .ok_or_else(|| format!("compiled Repeat at local node {at} has no input"))?
            .output_layout();
        let output_layout = graph_node.output_layout();
        let invalid = |what: &str| format!("compiled Repeat at local node {at} {what}");
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
        for (input_field, output_field) in input_fields.iter().zip(output_fields.iter()) {
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
            output: ChunkSchema::from_compiled_layout(output_layout)?,
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
        Box::new(CompiledRepeatProcessor {
            name: self.name.clone(),
            input_schema: Arc::clone(&self.input),
            output: Arc::clone(&self.output),
            null_ordinals: Arc::clone(&self.null_ordinals),
            grouping_values: Arc::clone(&self.grouping_values),
            input: None,
            next_set: 0,
            finishing: false,
            finished: false,
        })
    }
}

struct CompiledRepeatProcessor {
    name: String,
    input_schema: SchemaRef,
    output: ChunkSchemaRef,
    null_ordinals: Arc<[Vec<usize>]>,
    grouping_values: Arc<[Vec<i64>]>,
    input: Option<Chunk>,
    next_set: usize,
    finishing: bool,
    finished: bool,
}

impl CompiledRepeatProcessor {
    fn expand(&self, input: &RecordBatch, set: usize) -> ExecutionResult<Chunk> {
        let rows = input.num_rows();
        let fields = self.output.arrow_schema_ref();
        let mut columns: Vec<ArrayRef> = input.columns().to_vec();
        for &ordinal in &self.null_ordinals[set] {
            columns[ordinal] = new_null_array(fields.field(ordinal).data_type(), rows);
        }
        for &value in &self.grouping_values[set] {
            columns.push(Arc::new(Int64Array::from_value(value, rows)));
        }
        let batch = RecordBatch::try_new(fields, columns)
            .map_err(|error| ExecutionFailure::from(format!("compiled Repeat output: {error}")))?;
        Ok(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.output),
        )?)
    }
}

impl Operator for CompiledRepeatProcessor {
    fn name(&self) -> &str {
        &self.name
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for CompiledRepeatProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.input.is_none()
    }
    fn has_output(&self) -> bool {
        self.input.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.input.is_some() {
            return Err("compiled Repeat received input while expanding".into());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        if chunk.batch.schema().as_ref() != self.input_schema.as_ref() {
            return Err("compiled Repeat input differs from its frozen input port".into());
        }
        self.input = Some(chunk);
        self.next_set = 0;
        Ok(())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let Some(input) = self.input.as_ref() else {
            if self.finishing {
                self.finished = true;
            }
            return Ok(None);
        };
        let output = self.expand(&input.batch, self.next_set)?;
        self.next_set += 1;
        if self.next_set == self.grouping_values.len() {
            self.input = None;
            if self.finishing {
                self.finished = true;
            }
        }
        Ok(Some(output))
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.input.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}
