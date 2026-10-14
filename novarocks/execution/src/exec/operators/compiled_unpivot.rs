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

//! Compiled Unpivot: every input row becomes one output row per frozen value
//! mapping.
//!
//! Output channels follow the node's frozen layout exactly: passthrough
//! channels copy their input channel, the value channel takes the mapping's
//! input channel, and each literal channel takes the mapping's constant. A
//! scalar constant is a compiled `UnpivotConstant` root, evaluated by this
//! driver's own instance once per input batch over every input row; Int32List
//! and Utf8Map constants are frozen values materialized with the output
//! channel's exact Arrow type. Expansion is mapping-major (the relational
//! contract promises no order), and every output batch honors the frozen row
//! and byte budgets through the same budget search as the legacy Unpivot.
//!
//! Reused legacy pieces are array code only: segment flattening, owned part
//! concatenation and the budget search. The legacy ExprArena constant
//! evaluation is not used.
//!
//! The expansion itself -- per-channel producers over one input batch and its
//! evaluated constants, emitted under the frozen budgets -- is the reusable
//! `UnpivotExpansion` core; the writer's grouped Unpivot runs it once per
//! target with that target's own mappings.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, UnpivotConstant,
};
use novarocks_types::SlotId;

use super::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use super::unpivot_processor::{concat_owned, flattened_segments, next_bounded_output};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// The frozen source of one literal channel for one mapping.
#[derive(Clone)]
pub(crate) enum LiteralSource {
    /// Index of an evaluated scalar constant column.
    Root(usize),
    /// A frozen Int32 list, materialized with the channel's list item field.
    Int32List(Arc<[i32]>),
    /// A frozen Utf8 map, materialized with the channel's entries field.
    Utf8Map(Arc<[(Arc<str>, Arc<str>)]>),
}

/// The producer of one output channel, in output order.
#[derive(Clone)]
pub(crate) enum Producer {
    Passthrough {
        input: usize,
    },
    /// The input ordinal of each mapping's value.
    Value {
        inputs: Vec<usize>,
    },
    /// Each mapping's constant.
    Literal {
        sources: Vec<LiteralSource>,
    },
}

pub struct CompiledUnpivotProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    /// The frozen input port; channels are read by position.
    input: SchemaRef,
    sites: Vec<ProgramExpressionRootSite>,
    producers: Arc<[Producer]>,
    mappings: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    output: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
}

pub(crate) fn list_item_field(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(item) if item.data_type() == &DataType::Int32 => Some(item),
        _ => None,
    }
}

pub(crate) fn map_entries_field(data_type: &DataType) -> Option<(&FieldRef, bool)> {
    let DataType::Map(entries, sorted) = data_type else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    (fields.len() == 2
        && fields
            .iter()
            .all(|field| field.data_type() == &DataType::Utf8))
    .then_some((entries, *sorted))
}

impl CompiledUnpivotProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let at = node.index();
        let nodes = program.graph().nodes();
        let graph_node = nodes
            .get(at)
            .ok_or_else(|| format!("compiled Unpivot node {at} is absent"))?;
        let ProgramNodeKind::Unpivot {
            input,
            passthrough_columns,
            value_output_slot_id,
            literal_output_slot_ids,
            value_mappings,
            max_output_rows,
            max_output_bytes,
        } = graph_node.kind()
        else {
            return Err(format!("compiled local node {at} is not an Unpivot"));
        };
        let invalid = |what: String| format!("compiled Unpivot at local node {at} {what}");
        if value_mappings.is_empty() || *max_output_rows == 0 || *max_output_bytes == 0 {
            return Err(invalid(
                "needs a mapping and nonzero output budgets".to_string(),
            ));
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
        let output_ordinal = |slot: SlotId| {
            layout
                .slots()
                .iter()
                .position(|candidate| *candidate == slot)
                .ok_or_else(|| invalid(format!("writes slot {slot} outside its output")))
        };
        let input_type = |ordinal: usize| input_layout.schema().field(ordinal).data_type();
        let output_type = |ordinal: usize| layout.schema().field(ordinal).data_type();
        let mut producers: Vec<Option<Producer>> = vec![None; layout.slots().len()];
        let mut assign = |ordinal: usize, producer: Producer| {
            if producers[ordinal].replace(producer).is_some() {
                return Err(invalid(format!(
                    "output channel {ordinal} has two producers"
                )));
            }
            Ok(())
        };
        for column in passthrough_columns {
            let source = input_ordinal(column.input_slot_id)?;
            let target = output_ordinal(column.output_slot_id)?;
            if input_type(source) != output_type(target) {
                return Err(invalid(format!(
                    "passthrough slot {} changes type",
                    column.input_slot_id
                )));
            }
            assign(target, Producer::Passthrough { input: source })?;
        }
        let value = output_ordinal(*value_output_slot_id)?;
        let mut value_inputs = Vec::with_capacity(value_mappings.len());
        for mapping in value_mappings {
            let source = input_ordinal(mapping.input_value_slot_id)?;
            if input_type(source) != output_type(value) {
                return Err(invalid(format!(
                    "value slot {} differs from the value channel type",
                    mapping.input_value_slot_id
                )));
            }
            value_inputs.push(source);
        }
        assign(
            value,
            Producer::Value {
                inputs: value_inputs,
            },
        )?;
        let mut sites = Vec::new();
        for (literal, slot) in literal_output_slot_ids.iter().enumerate() {
            let target = output_ordinal(*slot)?;
            let mut sources = Vec::with_capacity(value_mappings.len());
            for (mapping_ordinal, mapping) in value_mappings.iter().enumerate() {
                let constant = mapping.constants.get(literal).ok_or_else(|| {
                    invalid(format!("mapping {mapping_ordinal} lacks literal {literal}"))
                })?;
                let source = match constant {
                    UnpivotConstant::Scalar { .. } => {
                        sites.push(ProgramExpressionRootSite::Node {
                            node,
                            role: ProgramNodeExpressionRole::UnpivotConstant {
                                mapping: u32::try_from(mapping_ordinal)
                                    .map_err(|_| invalid("has too many mappings".to_string()))?,
                                constant: u32::try_from(literal)
                                    .map_err(|_| invalid("has too many literals".to_string()))?,
                            },
                        });
                        LiteralSource::Root(sites.len() - 1)
                    }
                    UnpivotConstant::Int32List(values) => {
                        if list_item_field(output_type(target)).is_none() {
                            return Err(invalid(format!(
                                "Int32List constant targets {:?}",
                                output_type(target)
                            )));
                        }
                        LiteralSource::Int32List(Arc::from(values.as_slice()))
                    }
                    UnpivotConstant::Utf8Map(entries) => {
                        if map_entries_field(output_type(target)).is_none() {
                            return Err(invalid(format!(
                                "Utf8Map constant targets {:?}",
                                output_type(target)
                            )));
                        }
                        LiteralSource::Utf8Map(Arc::from(entries.as_slice()))
                    }
                };
                sources.push(source);
            }
            assign(target, Producer::Literal { sources })?;
        }
        if value_mappings
            .iter()
            .any(|mapping| mapping.constants.len() != literal_output_slot_ids.len())
        {
            return Err(invalid(
                "has a mapping of another literal width".to_string(),
            ));
        }
        let producers = producers
            .into_iter()
            .enumerate()
            .map(|(ordinal, producer)| {
                producer.ok_or_else(|| invalid(format!("output channel {ordinal} has no producer")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output = ChunkSchema::from_compiled_layout(layout)?;
        let input = input_layout.schema().clone();
        let (max_output_rows, max_output_bytes) = (*max_output_rows, *max_output_bytes);
        let mappings = value_mappings.len();
        Ok(Self {
            name: format!("COMPILED_UNPIVOT (node={at})"),
            program,
            input,
            sites,
            producers: Arc::from(producers),
            mappings,
            max_output_rows,
            max_output_bytes,
            output,
            error,
        })
    }
}

impl OperatorFactory for CompiledUnpivotProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledUnpivotProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            input_schema: Arc::clone(&self.input),
            sites: self.sites.clone(),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            expansion: UnpivotExpansion::new(
                Arc::clone(&self.producers),
                Arc::clone(&self.output),
                self.mappings,
                self.max_output_rows,
                self.max_output_bytes,
            ),
            mem_tracker: None,
            finishing: false,
            finished: false,
        })
    }
}

/// One input batch and its evaluated scalar constant columns.
struct Expanding {
    chunk: Chunk,
    constants: Vec<ArrayRef>,
}

/// The reusable Unpivot expansion: each input row becomes one output row per
/// mapping, mapping-major, every output batch within the frozen row and byte
/// budgets. It owns at most one input batch at a time.
pub(crate) struct UnpivotExpansion {
    producers: Arc<[Producer]>,
    mappings: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    output: ChunkSchemaRef,
    input: Option<Expanding>,
    cursor: usize,
    hint: Option<usize>,
}

impl UnpivotExpansion {
    pub(crate) fn new(
        producers: Arc<[Producer]>,
        output: ChunkSchemaRef,
        mappings: usize,
        max_output_rows: usize,
        max_output_bytes: usize,
    ) -> Self {
        Self {
            producers,
            mappings,
            max_output_rows,
            max_output_bytes,
            output,
            input: None,
            cursor: 0,
            hint: None,
        }
    }

    /// Whether an input batch is still being expanded.
    pub(crate) fn is_expanding(&self) -> bool {
        self.input.is_some()
    }

    /// Start expanding one input batch with its evaluated scalar constant
    /// columns, one per `LiteralSource::Root` index.
    pub(crate) fn start(&mut self, chunk: Chunk, constants: Vec<ArrayRef>) -> ExecutionResult<()> {
        if self.input.is_some() {
            return Err("compiled Unpivot received input while expanding".into());
        }
        // A scalar constant must already carry its literal channel's type.
        for producer in self.producers.iter().enumerate() {
            let (ordinal, Producer::Literal { sources }) = producer else {
                continue;
            };
            let expected = self.output.arrow_schema_ref();
            let expected = expected.field(ordinal).data_type();
            for source in sources {
                if let LiteralSource::Root(root) = source {
                    let constant = constants.get(*root).ok_or_else(|| {
                        ExecutionFailure::from("compiled Unpivot constant is absent")
                    })?;
                    if constant.data_type() != expected {
                        return Err(ExecutionFailure::from(format!(
                            "compiled Unpivot constant has type {:?}, not {expected:?}",
                            constant.data_type()
                        )));
                    }
                }
            }
        }
        self.input = Some(Expanding { chunk, constants });
        self.cursor = 0;
        Ok(())
    }

    /// The next bounded output batch of the current input, if any.
    pub(crate) fn next(
        &mut self,
        mem_tracker: Option<&Arc<MemTracker>>,
    ) -> ExecutionResult<Option<Chunk>> {
        let Some(input) = self.input.as_ref() else {
            return Ok(None);
        };
        let total =
            input.chunk.len().checked_mul(self.mappings).ok_or(
                "ResourceExhausted: unpivot expanded row count exceeds addressable memory",
            )?;
        let (producers, output, cursor) = (&self.producers, &self.output, self.cursor);
        let (chunk, rows) = next_bounded_output(
            total - cursor,
            self.max_output_rows,
            self.max_output_bytes,
            &mut self.hint,
            mem_tracker,
            |len| build_output(producers, output, input, cursor, len),
        )?;
        self.cursor += rows;
        if self.cursor == total {
            self.input = None;
            self.cursor = 0;
        }
        Ok(Some(chunk))
    }
}

struct CompiledUnpivotProcessor {
    name: String,
    program: Arc<LocalProgram>,
    input_schema: SchemaRef,
    sites: Vec<ProgramExpressionRootSite>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    expansion: UnpivotExpansion,
    mem_tracker: Option<Arc<MemTracker>>,
    finishing: bool,
    finished: bool,
}

fn int32_list(item: &FieldRef, values: &[i32], copies: usize) -> Result<ArrayRef, String> {
    let mut flat = Vec::with_capacity(values.len().saturating_mul(copies));
    for _ in 0..copies {
        flat.extend_from_slice(values);
    }
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(values.len(), copies));
    ListArray::try_new(
        Arc::clone(item),
        offsets,
        Arc::new(Int32Array::from(flat)),
        None,
    )
    .map(|array| Arc::new(array) as ArrayRef)
    .map_err(|error| format!("compiled Unpivot Int32List constant: {error}"))
}

fn utf8_map(
    entries_field: &FieldRef,
    sorted: bool,
    entries: &[(Arc<str>, Arc<str>)],
    copies: usize,
) -> Result<ArrayRef, String> {
    let DataType::Struct(fields) = entries_field.data_type() else {
        return Err("compiled Unpivot Utf8Map entries are not a struct".to_string());
    };
    let total = entries.len().saturating_mul(copies);
    let mut keys = Vec::with_capacity(total);
    let mut values = Vec::with_capacity(total);
    for _ in 0..copies {
        for (key, value) in entries {
            keys.push(key.as_ref());
            values.push(value.as_ref());
        }
    }
    let (keys, values) = (StringArray::from(keys), StringArray::from(values));
    let entries_array = StructArray::try_new(
        fields.clone(),
        vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
        None,
    )
    .map_err(|error| format!("compiled Unpivot Utf8Map entries: {error}"))?;
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(entries.len(), copies));
    MapArray::try_new(
        Arc::clone(entries_field),
        offsets,
        entries_array,
        None,
        sorted,
    )
    .map(|array| Arc::new(array) as ArrayRef)
    .map_err(|error| format!("compiled Unpivot Utf8Map constant: {error}"))
}

/// Materialize expanded rows `[start, start + len)` of one input batch.
fn build_output(
    producers: &[Producer],
    output: &ChunkSchemaRef,
    input: &Expanding,
    start: usize,
    len: usize,
) -> Result<Chunk, String> {
    let segments = flattened_segments(start, len, input.chunk.len());
    let schema = output.arrow_schema_ref();
    let mut columns = Vec::with_capacity(producers.len());
    for (ordinal, producer) in producers.iter().enumerate() {
        let field = schema.field(ordinal);
        let mut parts = Vec::with_capacity(segments.len());
        for segment in &segments {
            let (offset, rows) = (segment.input_offset, segment.len);
            parts.push(match producer {
                Producer::Passthrough { input: source } => {
                    input.chunk.batch.column(*source).slice(offset, rows)
                }
                Producer::Value { inputs } => input
                    .chunk
                    .batch
                    .column(inputs[segment.mapping_index])
                    .slice(offset, rows),
                Producer::Literal { sources } => match &sources[segment.mapping_index] {
                    LiteralSource::Root(root) => input.constants[*root].slice(offset, rows),
                    LiteralSource::Int32List(values) => {
                        let item = list_item_field(field.data_type())
                            .ok_or("compiled Unpivot list channel changed type")?;
                        int32_list(item, values, rows)?
                    }
                    LiteralSource::Utf8Map(entries) => {
                        let (entries_field, sorted) = map_entries_field(field.data_type())
                            .ok_or("compiled Unpivot map channel changed type")?;
                        utf8_map(entries_field, sorted, entries, rows)?
                    }
                },
            });
        }
        let slot = output.slot_ids()[ordinal];
        columns.push(concat_owned(parts, slot)?);
    }
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|error| format!("compiled Unpivot output: {error}"))?;
    Chunk::try_new_with_chunk_schema(batch, Arc::clone(output))
}

impl Operator for CompiledUnpivotProcessor {
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.mem_tracker = Some(tracker);
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

impl ProcessorOperator for CompiledUnpivotProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && !self.expansion.is_expanding()
    }
    fn has_output(&self) -> bool {
        self.expansion.is_expanding()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.expansion.is_expanding() {
            return Err("compiled Unpivot received input while expanding".into());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        if chunk.batch.schema().as_ref() != self.input_schema.as_ref() {
            return Err("compiled Unpivot input differs from its frozen input port".into());
        }
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let mut constants = Vec::with_capacity(self.sites.len());
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            constants.push(evaluate_all(instance, *site, &chunk.batch, &self.control)?);
        }
        self.expansion.start(chunk, constants)
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        if !self.expansion.is_expanding() {
            if self.finishing {
                self.finished = true;
            }
            return Ok(None);
        }
        let chunk = self.expansion.next(self.mem_tracker.as_ref())?;
        if !self.expansion.is_expanding() && self.finishing {
            self.finished = true;
        }
        Ok(chunk)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if !self.expansion.is_expanding() {
            self.finished = true;
        }
        Ok(())
    }
}
