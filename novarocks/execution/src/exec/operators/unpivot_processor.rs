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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, make_array};
use arrow::ipc::writer::StreamWriter;
use arrow_data::transform::MutableArrayData;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::unpivot::{UnpivotPassthroughColumn, UnpivotValueMapping};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;

pub struct UnpivotProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    passthrough_columns: Vec<UnpivotPassthroughColumn>,
    value_output_slot_id: SlotId,
    literal_output_slot_ids: Vec<SlotId>,
    value_mappings: Vec<UnpivotValueMapping>,
    output_chunk_schema: ChunkSchemaRef,
    max_output_rows: usize,
    max_output_bytes: usize,
}

impl UnpivotProcessorFactory {
    #[expect(
        clippy::too_many_arguments,
        reason = "The typed unpivot operator keeps independently validated column roles explicit."
    )]
    pub fn new(
        node_id: i32,
        arena: Arc<ExprArena>,
        passthrough_columns: Vec<UnpivotPassthroughColumn>,
        value_output_slot_id: SlotId,
        literal_output_slot_ids: Vec<SlotId>,
        value_mappings: Vec<UnpivotValueMapping>,
        output_chunk_schema: ChunkSchemaRef,
        max_output_rows: usize,
        max_output_bytes: usize,
    ) -> Result<Self, String> {
        validate_static_contract(
            &arena,
            &passthrough_columns,
            value_output_slot_id,
            &literal_output_slot_ids,
            &value_mappings,
            &output_chunk_schema,
            max_output_rows,
            max_output_bytes,
        )?;
        Ok(Self {
            name: if node_id >= 0 {
                format!("UNPIVOT (id={node_id})")
            } else {
                "UNPIVOT".to_string()
            },
            arena,
            passthrough_columns,
            value_output_slot_id,
            literal_output_slot_ids,
            value_mappings,
            output_chunk_schema,
            max_output_rows,
            max_output_bytes,
        })
    }
}

impl OperatorFactory for UnpivotProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(UnpivotProcessorOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            passthrough_columns: self.passthrough_columns.clone(),
            value_output_slot_id: self.value_output_slot_id,
            literal_output_slot_ids: self.literal_output_slot_ids.clone(),
            value_mappings: self.value_mappings.clone(),
            output_chunk_schema: Arc::clone(&self.output_chunk_schema),
            max_output_rows: self.max_output_rows,
            max_output_bytes: self.max_output_bytes,
            input: None,
            cursor: 0,
            output_rows_hint: None,
            finishing: false,
            finished: false,
        })
    }
}

struct UnpivotProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    passthrough_columns: Vec<UnpivotPassthroughColumn>,
    value_output_slot_id: SlotId,
    literal_output_slot_ids: Vec<SlotId>,
    value_mappings: Vec<UnpivotValueMapping>,
    output_chunk_schema: ChunkSchemaRef,
    max_output_rows: usize,
    max_output_bytes: usize,
    input: Option<Chunk>,
    cursor: usize,
    output_rows_hint: Option<usize>,
    finishing: bool,
    finished: bool,
}

impl Operator for UnpivotProcessorOperator {
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

impl ProcessorOperator for UnpivotProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.input.is_none()
    }

    fn has_output(&self) -> bool {
        self.input.is_some() && !self.finished
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.need_input() {
            return Err("unpivot received input while its input buffer is full".to_string());
        }
        validate_input_contract(
            &chunk,
            &self.passthrough_columns,
            self.value_output_slot_id,
            &self.value_mappings,
            &self.output_chunk_schema,
        )?;
        self.cursor = 0;
        if !chunk.is_empty() {
            self.input = Some(chunk);
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let Some(input) = self.input.as_ref() else {
            if self.finishing {
                self.finished = true;
            }
            return Ok(None);
        };
        let total_rows = input
            .len()
            .checked_mul(self.value_mappings.len())
            .ok_or_else(|| {
                "ResourceExhausted: unpivot expanded row count exceeds addressable memory"
                    .to_string()
            })?;
        let remaining = total_rows - self.cursor;
        let row_limit = remaining.min(self.max_output_rows);
        let had_size_hint = self.output_rows_hint.is_some();
        let mut candidate_rows = if self.max_output_bytes == usize::MAX {
            row_limit
        } else {
            self.output_rows_hint.unwrap_or(1).min(row_limit)
        };
        let mut output = self.build_output(input, self.cursor, candidate_rows)?;
        let mut upper_bound = row_limit;
        let candidate_bytes = if self.max_output_bytes == usize::MAX {
            None
        } else {
            Some(output_size(&output)?)
        };
        if candidate_bytes.is_some_and(|bytes| bytes > self.max_output_bytes) {
            upper_bound = candidate_rows - 1;
            loop {
                if candidate_rows == 1 {
                    let actual_bytes = output_size(&output)?;
                    return Err(format!(
                        "ResourceExhausted: one unpivot output value requires {actual_bytes} bytes, limit is {}",
                        self.max_output_bytes
                    ));
                }
                candidate_rows = (candidate_rows / 2).max(1);
                output = self.build_output(input, self.cursor, candidate_rows)?;
                if output_fits_budget(&output, self.max_output_bytes)? {
                    break;
                }
            }
        } else if had_size_hint || candidate_rows == row_limit {
            // Reuse the preceding batch's exact fit, but allow one bounded
            // doubling probe when it now occupies at most half the budget.
            // This recovers from a hint reduced by an earlier large value
            // without repeating a logarithmic search for every output batch.
            upper_bound = candidate_rows;
            if candidate_rows < row_limit
                && candidate_bytes
                    .is_some_and(|bytes| bytes.saturating_mul(2) <= self.max_output_bytes)
            {
                let next_rows = candidate_rows.saturating_mul(2).min(row_limit);
                let next = self.build_output(input, self.cursor, next_rows)?;
                if output_fits_budget(&next, self.max_output_bytes)? {
                    candidate_rows = next_rows;
                    output = next;
                    upper_bound = candidate_rows;
                }
            }
        } else {
            // The first byte-bounded batch grows from one row so a small budget
            // never materializes the complete wide expansion merely to learn it
            // is too large.
            while candidate_rows < row_limit {
                let next_rows = candidate_rows.saturating_mul(2).min(row_limit);
                let next = self.build_output(input, self.cursor, next_rows)?;
                if output_fits_budget(&next, self.max_output_bytes)? {
                    candidate_rows = next_rows;
                    output = next;
                } else {
                    upper_bound = next_rows - 1;
                    break;
                }
            }
        }
        while candidate_rows < upper_bound {
            let middle = candidate_rows + (upper_bound - candidate_rows).div_ceil(2);
            let candidate = self.build_output(input, self.cursor, middle)?;
            if output_fits_budget(&candidate, self.max_output_bytes)? {
                candidate_rows = middle;
                output = candidate;
            } else {
                upper_bound = middle - 1;
            }
        }
        self.output_rows_hint = Some(candidate_rows);
        self.cursor += candidate_rows;
        if self.cursor == total_rows {
            self.input = None;
            self.cursor = 0;
            if self.finishing {
                self.finished = true;
            }
        }
        Ok(Some(output))
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        if self.input.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

fn output_fits_budget(chunk: &Chunk, max_output_bytes: usize) -> Result<bool, String> {
    if max_output_bytes == usize::MAX {
        return Ok(true);
    }
    Ok(output_size(chunk)? <= max_output_bytes)
}

fn output_size(chunk: &Chunk) -> Result<usize, String> {
    let retained_bytes = chunk.estimated_bytes();
    let logical_bytes = chunk.logical_bytes();
    let mut encoded = Vec::new();
    {
        let schema = chunk.batch.schema();
        let mut writer = StreamWriter::try_new(&mut encoded, &schema)
            .map_err(|error| format!("unpivot output size encoding failed: {error}"))?;
        writer
            .write(&chunk.batch)
            .map_err(|error| format!("unpivot output size encoding failed: {error}"))?;
        writer
            .finish()
            .map_err(|error| format!("unpivot output size encoding failed: {error}"))?;
    }
    Ok(retained_bytes.max(logical_bytes).max(encoded.len()))
}

#[derive(Clone, Copy)]
struct Segment {
    mapping_index: usize,
    input_offset: usize,
    len: usize,
}

impl UnpivotProcessorOperator {
    fn build_output(&self, input: &Chunk, start: usize, len: usize) -> Result<Chunk, String> {
        let segments = flattened_segments(start, len, input.len());
        let passthrough_by_output = self
            .passthrough_columns
            .iter()
            .map(|mapping| (mapping.output_slot_id, mapping.input_slot_id))
            .collect::<HashMap<_, _>>();
        let literal_index_by_output = self
            .literal_output_slot_ids
            .iter()
            .copied()
            .enumerate()
            .map(|(index, slot)| (slot, index))
            .collect::<HashMap<_, _>>();
        let mut columns = Vec::with_capacity(self.output_chunk_schema.slots().len());

        for output_slot in self.output_chunk_schema.slots() {
            let output_slot_id = output_slot.slot_id();
            let parts = if let Some(input_slot_id) = passthrough_by_output.get(&output_slot_id) {
                segment_input_parts(input, *input_slot_id, &segments)?
            } else if output_slot_id == self.value_output_slot_id {
                let mut parts = Vec::with_capacity(segments.len());
                for segment in &segments {
                    let mapping = &self.value_mappings[segment.mapping_index];
                    parts.push(
                        input
                            .column_by_slot_id(mapping.input_value_slot_id)?
                            .slice(segment.input_offset, segment.len),
                    );
                }
                parts
            } else if let Some(literal_index) = literal_index_by_output.get(&output_slot_id) {
                let mut parts = Vec::with_capacity(segments.len());
                for segment in &segments {
                    let mapping = &self.value_mappings[segment.mapping_index];
                    let input_slice = input.slice(segment.input_offset, segment.len);
                    parts.push(
                        self.arena
                            .eval(mapping.literal_exprs[*literal_index], &input_slice)
                            .map_err(|error| {
                                format!(
                                    "unpivot mapping {} literal {} evaluation failed: {error}",
                                    segment.mapping_index, literal_index
                                )
                            })?,
                    );
                }
                parts
            } else {
                return Err(format!(
                    "unpivot output slot {output_slot_id} has no producer"
                ));
            };
            columns.push(concat_owned(parts, output_slot_id)?);
        }

        Chunk::try_new_with_columns(Arc::clone(&self.output_chunk_schema), columns)
            .map_err(|error| format!("unpivot output chunk is invalid: {error}"))
    }
}

fn flattened_segments(start: usize, len: usize, input_rows: usize) -> Vec<Segment> {
    // Mapping-major traversal is an implementation detail. The relational
    // Unpivot contract deliberately exposes no output ordering guarantee.
    let mut segments = Vec::new();
    let mut cursor = start;
    let end = start + len;
    while cursor < end {
        let mapping_index = cursor / input_rows;
        let input_offset = cursor % input_rows;
        let segment_len = (input_rows - input_offset).min(end - cursor);
        segments.push(Segment {
            mapping_index,
            input_offset,
            len: segment_len,
        });
        cursor += segment_len;
    }
    segments
}

fn segment_input_parts(
    input: &Chunk,
    input_slot_id: SlotId,
    segments: &[Segment],
) -> Result<Vec<ArrayRef>, String> {
    let input_array = input.column_by_slot_id(input_slot_id)?;
    Ok(segments
        .iter()
        .map(|segment| input_array.slice(segment.input_offset, segment.len))
        .collect())
}

fn concat_owned(parts: Vec<ArrayRef>, output_slot_id: SlotId) -> Result<ArrayRef, String> {
    let capacity = parts.iter().try_fold(0_usize, |total, part| {
        total.checked_add(part.len()).ok_or_else(|| {
            "ResourceExhausted: unpivot output column length exceeds addressable memory".to_string()
        })
    })?;
    let data = parts.iter().map(|part| part.to_data()).collect::<Vec<_>>();
    let refs = data.iter().collect::<Vec<_>>();
    let mut output = MutableArrayData::new(refs, true, capacity);
    for (index, part) in parts.iter().enumerate() {
        output.extend(index, 0, part.len());
    }
    let output = make_array(output.freeze());
    if output.len() != capacity {
        return Err(format!(
            "unpivot output slot {output_slot_id} materialization produced {} rows, expected {capacity}",
            output.len()
        ));
    }
    Ok(output)
}

#[expect(
    clippy::too_many_arguments,
    reason = "The typed unpivot contract keeps independently validated column roles explicit."
)]
fn validate_static_contract(
    arena: &ExprArena,
    passthrough_columns: &[UnpivotPassthroughColumn],
    value_output_slot_id: SlotId,
    literal_output_slot_ids: &[SlotId],
    value_mappings: &[UnpivotValueMapping],
    output_chunk_schema: &ChunkSchemaRef,
    max_output_rows: usize,
    max_output_bytes: usize,
) -> Result<(), String> {
    if max_output_rows == 0 || max_output_bytes == 0 {
        return Err("unpivot output row and byte budgets must be greater than zero".to_string());
    }
    if value_mappings.is_empty() {
        return Err("unpivot requires at least one value mapping".to_string());
    }
    let mut output_roles = HashSet::new();
    for mapping in passthrough_columns {
        if !output_roles.insert(mapping.output_slot_id) {
            return Err(format!(
                "unpivot output slot {} has multiple producers",
                mapping.output_slot_id
            ));
        }
    }
    if !output_roles.insert(value_output_slot_id) {
        return Err(format!(
            "unpivot value output slot {value_output_slot_id} has multiple producers"
        ));
    }
    for slot_id in literal_output_slot_ids {
        if !output_roles.insert(*slot_id) {
            return Err(format!(
                "unpivot literal output slot {slot_id} has multiple producers"
            ));
        }
    }
    let schema_outputs = output_chunk_schema
        .slot_ids()
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if output_roles != schema_outputs {
        return Err(format!(
            "unpivot output roles {output_roles:?} do not match output schema {schema_outputs:?}"
        ));
    }
    for (mapping_index, mapping) in value_mappings.iter().enumerate() {
        if mapping.literal_exprs.len() != literal_output_slot_ids.len() {
            return Err(format!(
                "unpivot mapping {mapping_index} literal count mismatch: expected {}, got {}",
                literal_output_slot_ids.len(),
                mapping.literal_exprs.len()
            ));
        }
        for (literal_index, expr_id) in mapping.literal_exprs.iter().enumerate() {
            if !matches!(arena.node(*expr_id), Some(ExprNode::Literal(_))) {
                return Err(format!(
                    "unpivot mapping {mapping_index} literal {literal_index} is not a literal expression"
                ));
            }
            let output_slot = output_chunk_schema
                .slot(literal_output_slot_ids[literal_index])
                .expect("validated output slot membership");
            let expr_type = arena.data_type(*expr_id).ok_or_else(|| {
                format!(
                    "unpivot mapping {mapping_index} literal {literal_index} has no declared type"
                )
            })?;
            if expr_type != output_slot.data_type() {
                return Err(format!(
                    "unpivot mapping {mapping_index} literal {literal_index} type mismatch: expression {expr_type:?}, output {:?}",
                    output_slot.data_type()
                ));
            }
        }
    }
    Ok(())
}

fn validate_input_contract(
    input: &Chunk,
    passthrough_columns: &[UnpivotPassthroughColumn],
    value_output_slot_id: SlotId,
    value_mappings: &[UnpivotValueMapping],
    output_chunk_schema: &ChunkSchemaRef,
) -> Result<(), String> {
    for mapping in passthrough_columns {
        let input_slot = input
            .chunk_schema()
            .slot(mapping.input_slot_id)
            .ok_or_else(|| {
                format!(
                    "unpivot passthrough input slot {} is missing",
                    mapping.input_slot_id
                )
            })?;
        let output_slot = output_chunk_schema
            .slot(mapping.output_slot_id)
            .expect("validated output slot membership");
        if input_slot.data_type() != output_slot.data_type()
            || input_slot.nullable() != output_slot.nullable()
        {
            return Err(format!(
                "unpivot passthrough slot shape drift: input {} is {:?} nullable={}, output {} is {:?} nullable={}",
                mapping.input_slot_id,
                input_slot.data_type(),
                input_slot.nullable(),
                mapping.output_slot_id,
                output_slot.data_type(),
                output_slot.nullable()
            ));
        }
    }
    let value_output = output_chunk_schema
        .slot(value_output_slot_id)
        .expect("validated value output slot membership");
    let mut nullable = false;
    for (mapping_index, mapping) in value_mappings.iter().enumerate() {
        let input_slot = input
            .chunk_schema()
            .slot(mapping.input_value_slot_id)
            .ok_or_else(|| {
                format!(
                    "unpivot mapping {mapping_index} input value slot {} is missing",
                    mapping.input_value_slot_id
                )
            })?;
        if input_slot.data_type() != value_output.data_type() {
            return Err(format!(
                "unpivot mapping {mapping_index} value type drift: input {:?}, output {:?}",
                input_slot.data_type(),
                value_output.data_type()
            ));
        }
        nullable |= input_slot.nullable();
    }
    if nullable != value_output.nullable() {
        return Err(format!(
            "unpivot value output nullability drift: expected {nullable}, got {}",
            value_output.nullable()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use arrow::array::{Array, BinaryArray, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn schema(slots: Vec<(u32, &str, DataType, bool)>) -> ChunkSchemaRef {
        let slots = slots
            .into_iter()
            .map(|(id, name, data_type, nullable)| {
                ChunkSlotSchema::from_field(
                    SlotId::new(id),
                    &Field::new(name, data_type, nullable),
                    None,
                )
                .unwrap()
            })
            .collect();
        Arc::new(ChunkSchema::try_new(slots).unwrap())
    }

    fn input_chunk() -> Chunk {
        let schema = schema(vec![
            (1, "group", DataType::Utf8, false),
            (2, "v1", DataType::Int32, true),
            (3, "v2", DataType::Int32, false),
        ]);
        Chunk::try_new_with_columns(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![Some(10), None])),
                Arc::new(Int32Array::from(vec![20, 30])),
            ],
        )
        .unwrap()
    }

    fn byte_budget_input_chunk() -> Chunk {
        let groups = (0..128)
            .map(|index| format!("group-{index:03}-{}", "x".repeat(128)))
            .collect::<Vec<_>>();
        Chunk::try_new_with_columns(
            schema(vec![
                (1, "group", DataType::Utf8, false),
                (2, "v1", DataType::Int32, true),
                (3, "v2", DataType::Int32, false),
            ]),
            vec![
                Arc::new(StringArray::from(groups)),
                Arc::new(Int32Array::from((0..128).map(Some).collect::<Vec<_>>())),
                Arc::new(Int32Array::from_iter_values(128..256)),
            ],
        )
        .unwrap()
    }

    fn factory(max_rows: usize, max_bytes: usize) -> UnpivotProcessorFactory {
        let mut arena = ExprArena::default();
        let first = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("first".to_string())),
            DataType::Utf8,
        );
        let second = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("second".to_string())),
            DataType::Utf8,
        );
        UnpivotProcessorFactory::new(
            9,
            Arc::new(arena),
            vec![UnpivotPassthroughColumn {
                input_slot_id: SlotId::new(1),
                output_slot_id: SlotId::new(11),
            }],
            SlotId::new(13),
            vec![SlotId::new(12)],
            vec![
                UnpivotValueMapping {
                    input_value_slot_id: SlotId::new(2),
                    literal_exprs: vec![first],
                },
                UnpivotValueMapping {
                    input_value_slot_id: SlotId::new(3),
                    literal_exprs: vec![second],
                },
            ],
            schema(vec![
                (11, "group", DataType::Utf8, false),
                (12, "label", DataType::Utf8, false),
                (13, "value", DataType::Int32, true),
            ]),
            max_rows,
            max_bytes,
        )
        .unwrap()
    }

    #[test]
    fn unpivot_expands_typed_values_and_splits_by_row_budget() {
        let state = RuntimeState::default();
        let mut operator = factory(2, usize::MAX).create(1, 0);
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, input_chunk()).unwrap();

        let first = processor.pull_chunk(&state).unwrap().unwrap();
        let second = processor.pull_chunk(&state).unwrap().unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert!(!processor.has_output());
        let mut rows = Vec::new();
        for chunk in [&first, &second] {
            let groups = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let labels = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = chunk
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..chunk.len() {
                rows.push((
                    groups.value(row).to_string(),
                    labels.value(row).to_string(),
                    (!values.is_null(row)).then(|| values.value(row)),
                ));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("a".to_string(), "first".to_string(), Some(10)),
                ("a".to_string(), "second".to_string(), Some(20)),
                ("b".to_string(), "first".to_string(), None),
                ("b".to_string(), "second".to_string(), Some(30)),
            ]
        );
    }

    #[test]
    fn empty_input_produces_no_batch() {
        let state = RuntimeState::default();
        let mut operator = factory(2, usize::MAX).create(1, 0);
        let processor = operator.as_processor_mut().unwrap();
        let input = input_chunk();
        let empty = Chunk::try_new_with_chunk_schema(
            RecordBatch::new_empty(Arc::new(Schema::new(
                input
                    .chunk_schema()
                    .slots()
                    .iter()
                    .map(|slot| Arc::new(slot.field().clone()))
                    .collect::<Vec<_>>(),
            ))),
            input.chunk_schema_ref(),
        )
        .unwrap();
        processor.push_chunk(&state, empty).unwrap();
        assert!(!processor.has_output());
        assert!(processor.pull_chunk(&state).unwrap().is_none());
    }

    #[test]
    fn single_value_over_byte_budget_is_resource_exhausted() {
        let state = RuntimeState::default();
        let mut operator = factory(100, 1).create(1, 0);
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, input_chunk()).unwrap();
        let error = processor.pull_chunk(&state).unwrap_err();
        assert!(error.contains("ResourceExhausted"), "{error}");
    }

    #[test]
    fn splits_by_actual_output_byte_budget() {
        let state = RuntimeState::default();
        let mut one_row_operator = factory(1, usize::MAX).create(1, 0);
        let one_row_processor = one_row_operator.as_processor_mut().unwrap();
        one_row_processor
            .push_chunk(&state, byte_budget_input_chunk())
            .unwrap();
        let one_row_size = output_size(
            &one_row_processor
                .pull_chunk(&state)
                .unwrap()
                .expect("one output row"),
        )
        .unwrap();

        let mut full_operator = factory(256, usize::MAX).create(1, 0);
        let full_processor = full_operator.as_processor_mut().unwrap();
        full_processor
            .push_chunk(&state, byte_budget_input_chunk())
            .unwrap();
        let full_size = output_size(
            &full_processor
                .pull_chunk(&state)
                .unwrap()
                .expect("complete expansion"),
        )
        .unwrap();
        assert!(one_row_size < full_size);
        let byte_budget = one_row_size + (full_size - one_row_size) / 2;

        let mut bounded_operator = factory(256, byte_budget).create(1, 0);
        let bounded_processor = bounded_operator.as_processor_mut().unwrap();
        bounded_processor
            .push_chunk(&state, byte_budget_input_chunk())
            .unwrap();
        let mut rows = 0;
        let mut batches = 0;
        while bounded_processor.has_output() {
            let chunk = bounded_processor.pull_chunk(&state).unwrap().unwrap();
            assert!(output_size(&chunk).unwrap() <= byte_budget);
            rows += chunk.len();
            batches += 1;
        }
        assert_eq!(rows, 256);
        assert!(batches > 1);
    }

    #[test]
    fn wide_mapping_set_streams_without_materializing_the_full_expansion() {
        let state = RuntimeState::default();
        let mut arena = ExprArena::default();
        let label = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("value".to_string())),
            DataType::Utf8,
        );
        let mappings = (0..1_001)
            .map(|_| UnpivotValueMapping {
                input_value_slot_id: SlotId::new(2),
                literal_exprs: vec![label],
            })
            .collect::<Vec<_>>();
        let factory = UnpivotProcessorFactory::new(
            10,
            Arc::new(arena),
            vec![],
            SlotId::new(13),
            vec![SlotId::new(12)],
            mappings,
            schema(vec![
                (12, "label", DataType::Utf8, false),
                (13, "value", DataType::Int32, true),
            ]),
            17,
            usize::MAX,
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, input_chunk()).unwrap();

        let mut output_rows = 0;
        let mut output_batches = 0;
        while processor.has_output() {
            let chunk = processor.pull_chunk(&state).unwrap().unwrap();
            assert!(chunk.len() <= 17);
            output_rows += chunk.len();
            output_batches += 1;
        }
        assert_eq!(output_rows, 2_002);
        assert!(output_batches > 100);
    }

    #[test]
    fn supports_independent_passthrough_literal_and_value_types() {
        let state = RuntimeState::default();
        let mut arena = ExprArena::default();
        let label = arena.push_typed(ExprNode::Literal(LiteralValue::Int8(7)), DataType::Int8);
        let factory = UnpivotProcessorFactory::new(
            11,
            Arc::new(arena),
            vec![UnpivotPassthroughColumn {
                input_slot_id: SlotId::new(1),
                output_slot_id: SlotId::new(11),
            }],
            SlotId::new(13),
            vec![SlotId::new(12)],
            vec![UnpivotValueMapping {
                input_value_slot_id: SlotId::new(2),
                literal_exprs: vec![label],
            }],
            schema(vec![
                (11, "key", DataType::Int64, false),
                (12, "label", DataType::Int8, false),
                (13, "value", DataType::Binary, true),
            ]),
            128,
            usize::MAX,
        )
        .unwrap();
        let input = Chunk::try_new_with_columns(
            schema(vec![
                (1, "key", DataType::Int64, false),
                (2, "value", DataType::Binary, true),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![41, 42])),
                Arc::new(BinaryArray::from(vec![Some(&b"a"[..]), None])),
            ],
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, input).unwrap();
        let output = processor.pull_chunk(&state).unwrap().unwrap();

        assert_eq!(
            output
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[41, 42]
        );
        assert_eq!(
            output
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[7, 7]
        );
        assert!(output.batch.column(2).is_null(1));
    }

    #[test]
    fn preserves_all_null_value_columns() {
        let state = RuntimeState::default();
        let input = Chunk::try_new_with_columns(
            schema(vec![
                (1, "group", DataType::Utf8, false),
                (2, "v1", DataType::Int32, true),
                (3, "v2", DataType::Int32, true),
            ]),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![None::<i32>, None])),
                Arc::new(Int32Array::from(vec![None::<i32>, None])),
            ],
        )
        .unwrap();
        let mut operator = factory(128, usize::MAX).create(1, 0);
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, input).unwrap();
        let output = processor.pull_chunk(&state).unwrap().unwrap();

        assert_eq!(output.len(), 4);
        assert_eq!(output.batch.column(2).null_count(), 4);
    }

    #[test]
    fn byte_hint_recovers_after_a_large_leading_value() {
        let state = RuntimeState::default();
        let output_schema = schema(vec![(11, "value", DataType::Binary, false)]);
        let make_factory = |max_rows, max_bytes| {
            UnpivotProcessorFactory::new(
                12,
                Arc::new(ExprArena::default()),
                vec![],
                SlotId::new(11),
                vec![],
                vec![UnpivotValueMapping {
                    input_value_slot_id: SlotId::new(1),
                    literal_exprs: vec![],
                }],
                Arc::clone(&output_schema),
                max_rows,
                max_bytes,
            )
            .unwrap()
        };
        let make_input = || {
            let mut values = Vec::with_capacity(128);
            values.push(vec![1_u8; 100 * 1024]);
            values.push(vec![2_u8; 60 * 1024]);
            values.extend((2..128).map(|_| vec![3_u8]));
            Chunk::try_new_with_columns(
                schema(vec![(1, "value", DataType::Binary, false)]),
                vec![Arc::new(BinaryArray::from_iter_values(
                    values.iter().map(Vec::as_slice),
                ))],
            )
            .unwrap()
        };

        let mut measuring_operator = make_factory(1, usize::MAX).create(1, 0);
        let measuring_processor = measuring_operator.as_processor_mut().unwrap();
        measuring_processor
            .push_chunk(&state, make_input())
            .unwrap();
        let first_row = measuring_processor.pull_chunk(&state).unwrap().unwrap();
        let byte_budget = output_size(&first_row).unwrap();

        let mut bounded_operator = make_factory(128, byte_budget).create(1, 0);
        let bounded_processor = bounded_operator.as_processor_mut().unwrap();
        bounded_processor.push_chunk(&state, make_input()).unwrap();
        let mut batch_rows = Vec::new();
        while bounded_processor.has_output() {
            batch_rows.push(bounded_processor.pull_chunk(&state).unwrap().unwrap().len());
        }

        assert_eq!(batch_rows.iter().sum::<usize>(), 128);
        assert_eq!(batch_rows[0], 1);
        assert!(batch_rows[1..].iter().any(|rows| *rows > 1));
        assert!(batch_rows.len() < 16, "batch rows: {batch_rows:?}");
    }
}
