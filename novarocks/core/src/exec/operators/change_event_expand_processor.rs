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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, new_null_array};
use arrow::compute::{concat, filter};
use arrow::datatypes::DataType;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, cast_array_to_target};
use crate::exec::node::change_event_expand::ChangeEventRuntimeSpec;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;

/// Factory for runtime ChangeEventExpand processors.
pub struct ChangeEventExpandProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    events: Vec<ChangeEventRuntimeSpec>,
    output_chunk_schema: ChunkSchemaRef,
    output_slot_ids: Vec<SlotId>,
    effect_slot_id: SlotId,
}

impl ChangeEventExpandProcessorFactory {
    pub fn new(
        node_id: i32,
        arena: Arc<ExprArena>,
        events: Vec<ChangeEventRuntimeSpec>,
        output_chunk_schema: ChunkSchemaRef,
        output_slot_ids: Vec<SlotId>,
        effect_slot_id: SlotId,
    ) -> Result<Self, String> {
        validate_output_schema(&output_chunk_schema, &output_slot_ids, effect_slot_id)?;
        let name = if node_id >= 0 {
            format!("CHANGE_EVENT_EXPAND (id={node_id})")
        } else {
            "CHANGE_EVENT_EXPAND".to_string()
        };
        Ok(Self {
            name,
            arena,
            events,
            output_chunk_schema,
            output_slot_ids,
            effect_slot_id,
        })
    }
}

impl OperatorFactory for ChangeEventExpandProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(ChangeEventExpandProcessorOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            events: self.events.clone(),
            output_chunk_schema: Arc::clone(&self.output_chunk_schema),
            output_slot_ids: self.output_slot_ids.clone(),
            effect_slot_id: self.effect_slot_id,
            pending_output: None,
            finishing: false,
            finished: false,
        })
    }
}

struct ChangeEventExpandProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    events: Vec<ChangeEventRuntimeSpec>,
    output_chunk_schema: ChunkSchemaRef,
    output_slot_ids: Vec<SlotId>,
    effect_slot_id: SlotId,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for ChangeEventExpandProcessorOperator {
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

impl ProcessorOperator for ChangeEventExpandProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending_output.is_none()
    }

    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if self.pending_output.is_some() {
            return Err(
                "change event expand received input while output buffer is full".to_string(),
            );
        }
        self.pending_output = Some(self.process_one(&chunk)?);
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.finishing && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

impl ChangeEventExpandProcessorOperator {
    fn process_one(&self, chunk: &Chunk) -> Result<Chunk, String> {
        self.validate_output_schema()?;
        if chunk.is_empty() {
            return self.empty_output_chunk();
        }

        let masks = self
            .events
            .iter()
            .enumerate()
            .map(|(event_idx, event)| self.predicate_mask(event_idx, event, chunk))
            .collect::<Result<Vec<_>, _>>()?;

        let mut event_chunks = Vec::new();
        for (event_idx, (event, mask)) in self.events.iter().zip(masks.iter()).enumerate() {
            let selected_count = selected_row_count(mask);
            if selected_count == 0 {
                continue;
            }
            event_chunks.push(self.build_event_chunk(
                event_idx,
                event,
                mask,
                selected_count,
                chunk,
            )?);
        }

        self.concat_event_chunks(event_chunks)
    }

    fn validate_output_schema(&self) -> Result<(), String> {
        validate_output_schema(
            &self.output_chunk_schema,
            &self.output_slot_ids,
            self.effect_slot_id,
        )
    }

    fn predicate_mask(
        &self,
        event_idx: usize,
        event: &ChangeEventRuntimeSpec,
        chunk: &Chunk,
    ) -> Result<BooleanArray, String> {
        let Some(predicate) = event.predicate else {
            return Ok(BooleanArray::from(vec![true; chunk.len()]));
        };
        let array = self
            .arena
            .eval(predicate, chunk)
            .map_err(|err| format!("change event expand predicate {event_idx} failed: {err}"))?;
        let mask = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                format!(
                    "change event expand predicate {event_idx} must return boolean, got {:?}",
                    array.data_type()
                )
            })?;
        if mask.len() != chunk.len() {
            return Err(format!(
                "change event expand predicate {event_idx} length mismatch: mask={} input={}",
                mask.len(),
                chunk.len()
            ));
        }
        Ok(mask.clone())
    }

    fn build_event_chunk(
        &self,
        event_idx: usize,
        event: &ChangeEventRuntimeSpec,
        mask: &BooleanArray,
        selected_count: usize,
        chunk: &Chunk,
    ) -> Result<Chunk, String> {
        let assignments = self.assignment_arrays(event_idx, event, mask, selected_count, chunk)?;
        let mut columns = Vec::with_capacity(self.output_chunk_schema.slots().len());

        for slot_schema in self.output_chunk_schema.slots() {
            let slot_id = slot_schema.slot_id();
            let array = if slot_id == self.effect_slot_id {
                effect_value_array(event.effect as i8, slot_schema.data_type(), selected_count)?
            } else if let Some(array) = assignments.get(&slot_id) {
                array.clone()
            } else {
                new_null_array(slot_schema.data_type(), selected_count)
            };
            columns.push(array);
        }

        Chunk::try_new_with_columns(Arc::clone(&self.output_chunk_schema), columns).map_err(|err| {
            format!("change event expand build event {event_idx} chunk failed: {err}")
        })
    }

    fn assignment_arrays(
        &self,
        event_idx: usize,
        event: &ChangeEventRuntimeSpec,
        mask: &BooleanArray,
        selected_count: usize,
        chunk: &Chunk,
    ) -> Result<HashMap<SlotId, ArrayRef>, String> {
        let mut arrays = HashMap::with_capacity(event.assignments.len());
        for assignment in &event.assignments {
            let slot_id = assignment.output_slot_id;
            if slot_id == self.effect_slot_id {
                return Err(format!(
                    "change event expand event {event_idx} assignment targets generated route slot {}",
                    slot_id
                ));
            }
            let Some(slot_schema) = self.output_chunk_schema.slot(slot_id) else {
                return Err(format!(
                    "change event expand event {event_idx} assignment output slot {} is not in output schema",
                    slot_id
                ));
            };
            let array = match assignment.expr {
                Some(expr) => {
                    let array = self.arena.eval(expr, chunk).map_err(|err| {
                        format!(
                            "change event expand event {event_idx} assignment for slot {} failed: {err}",
                            slot_id
                        )
                    })?;
                    if array.len() != chunk.len() {
                        return Err(format!(
                            "change event expand event {event_idx} assignment for slot {} length mismatch: array={} input={}",
                            slot_id,
                            array.len(),
                            chunk.len()
                        ));
                    }
                    let filtered = filter_selected_rows(array, mask, selected_count)?;
                    cast_array_to_target(&filtered, slot_schema.data_type()).map_err(|err| {
                        format!(
                            "change event expand event {event_idx} assignment for slot {} cast to {:?} failed: {err}",
                            slot_id,
                            slot_schema.data_type()
                        )
                    })?
                }
                None => new_null_array(slot_schema.data_type(), selected_count),
            };
            if arrays.insert(slot_id, array).is_some() {
                return Err(format!(
                    "change event expand event {event_idx} has duplicate assignment for slot {}",
                    slot_id
                ));
            }
        }
        Ok(arrays)
    }

    fn concat_event_chunks(&self, event_chunks: Vec<Chunk>) -> Result<Chunk, String> {
        if event_chunks.is_empty() {
            return self.empty_output_chunk();
        }
        if event_chunks.len() == 1 {
            return Ok(event_chunks.into_iter().next().expect("event chunk"));
        }

        let mut columns = Vec::with_capacity(self.output_chunk_schema.slots().len());
        for column_idx in 0..self.output_chunk_schema.slots().len() {
            let parts = event_chunks
                .iter()
                .map(|chunk| chunk.batch.column(column_idx).as_ref())
                .collect::<Vec<&dyn Array>>();
            columns.push(concat(&parts).map_err(|err| {
                format!("change event expand concat output column {column_idx} failed: {err}")
            })?);
        }

        Chunk::try_new_with_columns(Arc::clone(&self.output_chunk_schema), columns)
            .map_err(|err| format!("change event expand build concatenated chunk failed: {err}"))
    }

    fn empty_output_chunk(&self) -> Result<Chunk, String> {
        let columns = self
            .output_chunk_schema
            .slots()
            .iter()
            .map(|slot| new_null_array(slot.data_type(), 0))
            .collect();
        Chunk::try_new_with_columns(Arc::clone(&self.output_chunk_schema), columns)
            .map_err(|err| format!("change event expand build empty output chunk failed: {err}"))
    }
}

fn selected_row_count(mask: &BooleanArray) -> usize {
    mask.iter()
        .filter(|value| matches!(value, Some(true)))
        .count()
}

fn filter_selected_rows(
    array: ArrayRef,
    mask: &BooleanArray,
    selected_count: usize,
) -> Result<ArrayRef, String> {
    if selected_count == array.len() {
        return Ok(array);
    }
    filter(array.as_ref(), mask).map_err(|err| format!("filter selected rows failed: {err}"))
}

fn effect_value_array(
    value: i8,
    target_type: &arrow::datatypes::DataType,
    len: usize,
) -> Result<ArrayRef, String> {
    let array = Arc::new(Int8Array::from_iter_values(std::iter::repeat_n(value, len))) as ArrayRef;
    cast_array_to_target(&array, target_type)
}

fn validate_output_schema(
    output_chunk_schema: &ChunkSchemaRef,
    output_slot_ids: &[SlotId],
    effect_slot_id: SlotId,
) -> Result<(), String> {
    if output_chunk_schema.slot_ids() != output_slot_ids {
        return Err(format!(
            "change event expand output_slot_ids {:?} do not match output schema slot order {:?}",
            output_slot_ids,
            output_chunk_schema.slot_ids()
        ));
    }
    let Some(effect_slot) = output_chunk_schema.slot(effect_slot_id) else {
        return Err(format!(
            "change event expand output schema is missing logical effect slot {}",
            effect_slot_id
        ));
    };
    if effect_slot.data_type() != &DataType::Int8 {
        return Err(format!(
            "change event expand logical effect slot {} must be Int8, got {:?}",
            effect_slot_id,
            effect_slot.data_type()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int8Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::ConnectorRowMutationEffect;

    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::node::change_event_expand::{
        ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
    };
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    const INPUT_SLOT: SlotId = SlotId::new(10);
    const EFFECT_SLOT: SlotId = SlotId::new(20);
    const VALUE_SLOT: SlotId = SlotId::new(21);

    fn schema(fields: Vec<(SlotId, Field)>) -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new(
                fields
                    .into_iter()
                    .map(|(slot_id, field)| {
                        ChunkSlotSchema::new_with_field(slot_id, field, None, None)
                    })
                    .collect(),
            )
            .expect("schema"),
        )
    }

    fn input() -> Chunk {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![42])) as ArrayRef],
        )
        .expect("input");
        Chunk::new_with_chunk_schema(
            batch,
            schema(vec![(
                INPUT_SLOT,
                Field::new("value", DataType::Int32, false),
            )]),
        )
    }

    fn output_schema() -> ChunkSchemaRef {
        schema(vec![
            (
                EFFECT_SLOT,
                Field::new("__row_mutation_effect", DataType::Int8, false),
            ),
            (VALUE_SLOT, Field::new("value", DataType::Int32, true)),
        ])
    }

    fn slot_expr(arena: &mut ExprArena) -> crate::exec::expr::ExprId {
        arena.push_typed(ExprNode::SlotId(INPUT_SLOT), DataType::Int32)
    }

    #[test]
    fn change_event_expand_materializes_logical_effects() {
        let mut arena = ExprArena::default();
        let value = slot_expr(&mut arena);
        let factory = ChangeEventExpandProcessorFactory::new(
            7,
            Arc::new(arena),
            vec![
                ChangeEventRuntimeSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Delete,
                    assignments: Vec::new(),
                },
                ChangeEventRuntimeSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Replace,
                    assignments: vec![ChangeEventRuntimeOutputExpr {
                        output_slot_id: VALUE_SLOT,
                        expr: Some(value),
                    }],
                },
            ],
            output_schema(),
            vec![EFFECT_SLOT, VALUE_SLOT],
            EFFECT_SLOT,
        )
        .expect("factory");
        let state = RuntimeState::default();
        let mut operator = factory.create(1, 0);
        let processor = operator.as_processor_mut().expect("processor");
        processor.push_chunk(&state, input()).expect("push");
        let output = processor.pull_chunk(&state).expect("pull").expect("output");

        let effect_array = output.column_by_slot_id(EFFECT_SLOT).expect("effect");
        let effects = effect_array
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("int8 effect");
        assert_eq!(effects.values(), &[1, 2]);
        let value_array = output.column_by_slot_id(VALUE_SLOT).expect("value");
        let values = value_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 value");
        assert!(values.is_null(0));
        assert_eq!(values.value(1), 42);
    }

    #[test]
    fn change_event_expand_skips_false_predicate_without_changing_schema() {
        let mut arena = ExprArena::default();
        let false_predicate = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(false)),
            DataType::Boolean,
        );
        let factory = ChangeEventExpandProcessorFactory::new(
            8,
            Arc::new(arena),
            vec![ChangeEventRuntimeSpec {
                predicate: Some(false_predicate),
                effect: ConnectorRowMutationEffect::Insert,
                assignments: Vec::new(),
            }],
            output_schema(),
            vec![EFFECT_SLOT, VALUE_SLOT],
            EFFECT_SLOT,
        )
        .expect("factory");
        let state = RuntimeState::default();
        let mut operator = factory.create(1, 0);
        let processor = operator.as_processor_mut().expect("processor");
        processor.push_chunk(&state, input()).expect("push");
        let output = processor
            .pull_chunk(&state)
            .expect("pull")
            .expect("empty output");
        assert_eq!(output.len(), 0);
        assert_eq!(output.chunk_schema().slot_ids(), &[EFFECT_SLOT, VALUE_SLOT]);
    }

    #[test]
    fn change_event_expand_rejects_non_int8_effect_slot() {
        let err = match ChangeEventExpandProcessorFactory::new(
            9,
            Arc::new(ExprArena::default()),
            Vec::new(),
            schema(vec![
                (
                    EFFECT_SLOT,
                    Field::new("__row_mutation_effect", DataType::Int32, false),
                ),
                (VALUE_SLOT, Field::new("value", DataType::Int32, true)),
            ]),
            vec![EFFECT_SLOT, VALUE_SLOT],
            EFFECT_SLOT,
        ) {
            Ok(_) => panic!("effect slot type must fail"),
            Err(err) => err,
        };
        assert!(
            err.contains("logical effect") && err.contains("Int8"),
            "{err}"
        );
    }
}
