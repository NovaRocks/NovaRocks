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

use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, new_null_array};
use arrow::compute::{concat, filter};
use arrow::datatypes::DataType;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{
    ExprArena, ExprEvaluation, ExprEvaluationContext, ExprEvaluationControl, cast_array_to_target,
};
use crate::exec::node::change_event_expand::ChangeEventRuntimeSpec;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner, Operator,
    ProcessorOperator,
};
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
            construction: None,
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
    construction: Option<Arc<dyn InputConstructionFactory>>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for ChangeEventExpandProcessorOperator {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        self.construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        if !self.output_chunk_schema.is_typed() {
            return Err(ConstructionFailure::InvalidSource(
                "change event output requires its complete typed declaration".into(),
            )
            .into());
        }
        Ok(())
    }
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

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.finished {
            return Ok(());
        }
        if self.pending_output.is_some() {
            return Err(
                "change event expand received input while output buffer is full"
                    .to_string()
                    .into(),
            );
        }
        let control = super::compiled_expression::RuntimeKernelControl::new(_state.error_state());
        ExprEvaluationControl::checkpoint(&control)?;
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_input_construction(&chunk)?;
        let evaluation = ExprEvaluationContext::new(&self.arena, owner.as_ref(), &control);
        self.pending_output = Some(self.process_one(&chunk, &evaluation, owner.as_ref())?);
        ExprEvaluationControl::checkpoint(&control)?;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let out = self.pending_output.take();
        if self.finishing && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

impl ChangeEventExpandProcessorOperator {
    fn process_one(
        &self,
        chunk: &Chunk,
        evaluation: &ExprEvaluationContext<'_>,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Chunk, ExecutionFailure> {
        self.validate_output_schema()?;
        if chunk.is_empty() {
            return Ok((self.empty_output_chunk(chunk, owner))?);
        }

        let masks = self
            .events
            .iter()
            .enumerate()
            .map(|(event_idx, event)| self.predicate_mask(event_idx, event, chunk, evaluation))
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
                evaluation,
                owner,
            )?);
        }

        Ok((self.concat_event_chunks(event_chunks, chunk, owner))?)
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
        evaluation: &ExprEvaluationContext<'_>,
    ) -> Result<BooleanArray, ExecutionFailure> {
        let Some(predicate) = event.predicate else {
            return Ok(BooleanArray::from(vec![true; chunk.len()]));
        };
        let array = ExprEvaluation::eval(evaluation, predicate, chunk).map_err(|error| {
            evaluation.annotate_error(error, |message| {
                format!("change event expand predicate {event_idx} failed: {message}")
            })
        })?;
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
            return Err((format!(
                "change event expand predicate {event_idx} length mismatch: mask={} input={}",
                mask.len(),
                chunk.len()
            ))
            .into());
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
        evaluation: &ExprEvaluationContext<'_>,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Chunk, ExecutionFailure> {
        let assignments =
            self.assignment_arrays(event_idx, event, mask, selected_count, chunk, evaluation)?;
        owner.request(std::alloc::Layout::array::<ArrayRef>(self.output_chunk_schema.slots().len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
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

        self.materialize_output(columns, selected_count, chunk, owner)
    }

    fn assignment_arrays(
        &self,
        event_idx: usize,
        event: &ChangeEventRuntimeSpec,
        mask: &BooleanArray,
        selected_count: usize,
        chunk: &Chunk,
        evaluation: &ExprEvaluationContext<'_>,
    ) -> Result<HashMap<SlotId, ArrayRef>, ExecutionFailure> {
        let mut arrays = HashMap::with_capacity(event.assignments.len());
        for assignment in &event.assignments {
            let slot_id = assignment.output_slot_id;
            if slot_id == self.effect_slot_id {
                return Err((format!(
                    "change event expand event {event_idx} assignment targets generated route slot {}",
                    slot_id
                )).into());
            }
            let Some(slot_schema) = self.output_chunk_schema.slot(slot_id) else {
                return Err((format!(
                    "change event expand event {event_idx} assignment output slot {} is not in output schema",
                    slot_id
                )).into());
            };
            let array = match assignment.expr {
                Some(expr) => {
                    let array = ExprEvaluation::eval(evaluation, expr, chunk)
                        .map_err(|error| evaluation.annotate_error(error, |message| format!(
                            "change event expand event {event_idx} assignment for slot {slot_id} failed: {message}"
                        )))?;
                    if array.len() != chunk.len() {
                        return Err((format!(
                            "change event expand event {event_idx} assignment for slot {} length mismatch: array={} input={}",
                            slot_id,
                            array.len(),
                            chunk.len()
                        )).into());
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
                return Err((format!(
                    "change event expand event {event_idx} has duplicate assignment for slot {}",
                    slot_id
                ))
                .into());
            }
        }
        Ok(arrays)
    }

    fn concat_event_chunks(
        &self,
        event_chunks: Vec<Chunk>,
        source: &Chunk,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Chunk, ExecutionFailure> {
        if event_chunks.is_empty() {
            return Ok((self.empty_output_chunk(source, owner))?);
        }
        if event_chunks.len() == 1 {
            return Ok(event_chunks.into_iter().next().expect("event chunk"));
        }

        owner.request(std::alloc::Layout::array::<ArrayRef>(self.output_chunk_schema.slots().len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut columns = Vec::with_capacity(self.output_chunk_schema.slots().len());
        for column_idx in 0..self.output_chunk_schema.slots().len() {
            let parts = event_chunks
                .iter()
                .map(|chunk| chunk.record_batch().column(column_idx).as_ref())
                .collect::<Vec<&dyn Array>>();
            columns.push(concat(&parts).map_err(|err| {
                format!("change event expand concat output column {column_idx} failed: {err}")
            })?);
        }

        let rows = event_chunks.iter().map(Chunk::len).sum();
        self.materialize_output(columns, rows, source, owner)
    }

    fn empty_output_chunk(
        &self,
        source: &Chunk,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Chunk, ExecutionFailure> {
        owner.request(std::alloc::Layout::array::<ArrayRef>(self.output_chunk_schema.slots().len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let columns = self
            .output_chunk_schema
            .slots()
            .iter()
            .map(|slot| new_null_array(slot.data_type(), 0))
            .collect();
        self.materialize_output(columns, 0, source, owner)
    }
    fn materialize_output(
        &self,
        columns: Vec<ArrayRef>,
        rows: usize,
        source: &Chunk,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Chunk> {
        let storage = source
            .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ExecutionFailure,
            _,
        >(
            self.output_chunk_schema.clone(),
            columns,
            rows,
            storage,
            |step| Ok(owner.observe(0, &[], step)?),
        )?)
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

    fn install_creator(operator: &mut dyn Operator, state: &RuntimeState) {
        let factory =
            crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                crate::runtime::mem_tracker::MemTracker::new_root(
                    "change event actual creator fixture",
                ),
                state.error_state(),
            )
            .unwrap();
        operator.set_construction_factory(factory);
        operator.prepare().unwrap();
    }

    fn schema(fields: Vec<(SlotId, Field)>) -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new_typed(
                fields
                    .into_iter()
                    .map(|(slot_id, field)| {
                        let ty = novarocks_type_contract::FunctionValueType::new(
                            field.data_type().clone(),
                            field.is_nullable(),
                        );
                        ChunkSlotSchema::try_new_typed(
                            slot_id,
                            Arc::new(field),
                            ty.typed_binding().clone(),
                            None,
                            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                        )
                        .unwrap()
                    })
                    .collect(),
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
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
        install_creator(operator.as_mut(), &state);
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
        install_creator(operator.as_mut(), &state);
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
    #[test]
    fn typed_change_event_predicate_and_assignment_use_the_installed_creator() {
        use crate::exec::expr::function::FunctionKind;
        use arrow::array::{Int64Builder, ListBuilder};
        let mut values = ListBuilder::new(Int64Builder::new());
        values.values().append_slice(&[1, 2]);
        values.append(true);
        values.values().append_value(3);
        values.append(true);
        let array = Arc::new(values.finish()) as ArrayRef;
        let input_schema = schema(vec![(
            INPUT_SLOT,
            Field::new("values", array.data_type().clone(), false),
        )]);
        let input = Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(input_schema.arrow_schema_ref(), vec![array.clone()]).unwrap(),
            input_schema,
        )
        .unwrap();
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(INPUT_SLOT), array.data_type().clone());
        let two = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let predicate = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Array("array_contains"),
                args: vec![source, two],
            },
            DataType::Boolean,
        );
        let position = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Array("array_position"),
                args: vec![source, two],
            },
            DataType::Int64,
        );
        let factory = ChangeEventExpandProcessorFactory::new(
            10,
            Arc::new(arena),
            vec![ChangeEventRuntimeSpec {
                predicate: Some(predicate),
                effect: ConnectorRowMutationEffect::Insert,
                assignments: vec![ChangeEventRuntimeOutputExpr {
                    output_slot_id: VALUE_SLOT,
                    expr: Some(position),
                }],
            }],
            output_schema(),
            vec![EFFECT_SLOT, VALUE_SLOT],
            EFFECT_SLOT,
        )
        .unwrap();
        let state = RuntimeState::default();
        let mut operator = factory.create(1, 0);
        install_creator(operator.as_mut(), &state);
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, input).unwrap();
        let output = processor.pull_chunk(&state).unwrap().unwrap();
        assert_eq!(output.len(), 1);
        let values = output.column_by_slot_id(VALUE_SLOT).unwrap();
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2)]
        );
        assert!(output.chunk_schema().is_typed());
        assert!(!output.construction_storage_lifetime().is_pure());
    }

    #[test]
    fn change_event_runtime_first_failure_preserves_payload_and_driver_context() {
        use crate::runtime::fragment::{ExecutionFailure, PipelineOperation};
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        let mut causes: Vec<ExecutionFailure> = vec![
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InstanceFailed,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("exact invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("exact internal")),
            KernelFailure::Operational(KernelDiagnostic::new("exact operation")),
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        causes.push(ExecutionFailure::from("cancelled").at_operator(43, PipelineOperation::Push));
        for original in causes {
            let mut arena = ExprArena::default();
            let source = slot_expr(&mut arena);
            let factory = ChangeEventExpandProcessorFactory::new(
                11,
                Arc::new(arena),
                vec![ChangeEventRuntimeSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: vec![ChangeEventRuntimeOutputExpr {
                        output_slot_id: VALUE_SLOT,
                        expr: Some(source),
                    }],
                }],
                output_schema(),
                vec![EFFECT_SLOT, VALUE_SLOT],
                EFFECT_SLOT,
            )
            .unwrap();
            let state = RuntimeState::default();
            let mut operator = factory.create(1, 0);
            install_creator(operator.as_mut(), &state);
            state.error_state().set_error(original.clone());
            state
                .error_state()
                .set_error("later cause cannot replace the first");
            let processor = operator.as_processor_mut().unwrap();
            assert_eq!(processor.push_chunk(&state, input()).unwrap_err(), original);
            assert!(!processor.has_output());
        }
    }
}
