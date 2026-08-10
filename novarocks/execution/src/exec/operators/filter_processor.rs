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
//! Expression filter processor for row-level predicate evaluation.
//!
//! Responsibilities:
//! - Evaluates boolean predicates over incoming chunks and applies selection masks.
//! - Produces filtered chunks while preserving schema and nullability semantics.
//!
//! Key exported interfaces:
//! - Types: `FilterProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_types::SlotId;

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for predicate processors that apply row-level filter masks to input chunks.
pub struct FilterProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    predicate: ExprId,
    encoding_policy: FilterEncodingPolicy,
}

impl FilterProcessorFactory {
    pub fn new(node_id: i32, arena: Arc<ExprArena>, predicate: ExprId) -> Self {
        let name = if node_id >= 0 {
            format!("FILTER (id={node_id})")
        } else {
            "FILTER".to_string()
        };
        let encoding_policy = FilterEncodingPolicy::from_predicate(arena.as_ref(), predicate);
        Self {
            name,
            arena,
            predicate,
            encoding_policy,
        }
    }
}

impl OperatorFactory for FilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(FilterProcessorOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            predicate: self.predicate,
            encoding_policy: self.encoding_policy.clone(),
            pending_output: None,
            finishing: false,
            finished: false,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FilterEncodingPolicy {
    predicate_slots: HashSet<SlotId>,
    unsupported_predicate_slots: HashSet<SlotId>,
}

impl FilterEncodingPolicy {
    pub(crate) fn from_predicate(arena: &ExprArena, predicate: ExprId) -> Self {
        let mut policy = Self::default();
        policy.analyze_predicate(arena, predicate);
        policy
    }

    pub(crate) fn accepts_encoded_column(&self, slot_id: SlotId, data_type: &DataType) -> bool {
        if !is_low_cardinality_string_dictionary(data_type) {
            return false;
        }
        !self.predicate_slots.contains(&slot_id)
            || !self.unsupported_predicate_slots.contains(&slot_id)
    }

    fn analyze_predicate(&mut self, arena: &ExprArena, expr: ExprId) -> bool {
        match arena.node(expr) {
            Some(ExprNode::And(left, right)) | Some(ExprNode::Or(left, right)) => {
                let left_supported = self.analyze_predicate(arena, *left);
                let right_supported = self.analyze_predicate(arena, *right);
                left_supported && right_supported
            }
            Some(ExprNode::Not(child)) => self.analyze_predicate(arena, *child),
            Some(ExprNode::Eq(left, right))
            | Some(ExprNode::Ne(left, right))
            | Some(ExprNode::Lt(left, right))
            | Some(ExprNode::Le(left, right))
            | Some(ExprNode::Gt(left, right))
            | Some(ExprNode::Ge(left, right)) => {
                self.analyze_binary_predicate(arena, *left, *right)
            }
            Some(ExprNode::In { child, values, .. }) => {
                self.analyze_in_predicate(arena, *child, values)
            }
            Some(ExprNode::IsNull(child)) | Some(ExprNode::IsNotNull(child)) => {
                self.analyze_null_predicate(arena, *child)
            }
            _ => {
                self.mark_slots_unsupported(arena, expr);
                false
            }
        }
    }

    fn analyze_binary_predicate(&mut self, arena: &ExprArena, left: ExprId, right: ExprId) -> bool {
        match (direct_slot(arena, left), direct_slot(arena, right)) {
            (Some(slot_id), None) if is_supported_literal_expr(arena, right) => {
                self.predicate_slots.insert(slot_id);
                true
            }
            (None, Some(slot_id)) if is_supported_literal_expr(arena, left) => {
                self.predicate_slots.insert(slot_id);
                true
            }
            _ => {
                self.mark_slots_unsupported(arena, left);
                self.mark_slots_unsupported(arena, right);
                false
            }
        }
    }

    fn analyze_in_predicate(
        &mut self,
        arena: &ExprArena,
        child: ExprId,
        values: &[ExprId],
    ) -> bool {
        let Some(slot_id) = direct_slot(arena, child) else {
            self.mark_slots_unsupported(arena, child);
            for value in values {
                self.mark_slots_unsupported(arena, *value);
            }
            return false;
        };
        if values.iter().any(|value| expr_contains_slot(arena, *value)) {
            self.mark_slots_unsupported(arena, child);
            for value in values {
                self.mark_slots_unsupported(arena, *value);
            }
            return false;
        }
        self.predicate_slots.insert(slot_id);
        true
    }

    fn analyze_null_predicate(&mut self, arena: &ExprArena, child: ExprId) -> bool {
        let Some(slot_id) = direct_slot(arena, child) else {
            self.mark_slots_unsupported(arena, child);
            return false;
        };
        self.predicate_slots.insert(slot_id);
        true
    }

    fn mark_slots_unsupported(&mut self, arena: &ExprArena, expr: ExprId) {
        collect_slot_ids(arena, expr, &mut self.predicate_slots);
        collect_slot_ids(arena, expr, &mut self.unsupported_predicate_slots);
    }
}

fn is_supported_literal_expr(arena: &ExprArena, expr: ExprId) -> bool {
    matches!(arena.node(expr), Some(ExprNode::Literal(_)))
}

fn is_low_cardinality_string_dictionary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(key_type, value_type)
            if key_type.as_ref() == &DataType::Int32
                && matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
    )
}

fn direct_slot(arena: &ExprArena, expr: ExprId) -> Option<SlotId> {
    match arena.node(expr) {
        Some(ExprNode::SlotId(slot_id)) => Some(*slot_id),
        _ => None,
    }
}

fn expr_contains_slot(arena: &ExprArena, expr: ExprId) -> bool {
    let mut slots = HashSet::new();
    collect_slot_ids(arena, expr, &mut slots);
    !slots.is_empty()
}

fn collect_slot_ids(arena: &ExprArena, expr: ExprId, out: &mut HashSet<SlotId>) {
    let Some(node) = arena.node(expr) else {
        return;
    };
    match node {
        ExprNode::Literal(_) => {}
        ExprNode::SlotId(slot_id) => {
            out.insert(*slot_id);
        }
        ExprNode::ArrayExpr { elements } => {
            for child in elements {
                collect_slot_ids(arena, *child, out);
            }
        }
        ExprNode::StructExpr { fields } => {
            for child in fields {
                collect_slot_ids(arena, *child, out);
            }
        }
        ExprNode::LambdaFunction {
            body,
            common_sub_exprs,
            ..
        } => {
            collect_slot_ids(arena, *body, out);
            for (_, expr_id) in common_sub_exprs {
                collect_slot_ids(arena, *expr_id, out);
            }
        }
        ExprNode::DictDecode { child, .. }
        | ExprNode::Cast(child)
        | ExprNode::CastTime(child)
        | ExprNode::CastTimeFromDatetime(child)
        | ExprNode::Not(child)
        | ExprNode::IsNull(child)
        | ExprNode::IsNotNull(child)
        | ExprNode::Clone(child) => collect_slot_ids(arena, *child, out),
        ExprNode::Add(left, right)
        | ExprNode::Sub(left, right)
        | ExprNode::Mul(left, right)
        | ExprNode::Div(left, right)
        | ExprNode::Mod(left, right)
        | ExprNode::Eq(left, right)
        | ExprNode::EqForNull(left, right)
        | ExprNode::Ne(left, right)
        | ExprNode::Lt(left, right)
        | ExprNode::Le(left, right)
        | ExprNode::Gt(left, right)
        | ExprNode::Ge(left, right)
        | ExprNode::And(left, right)
        | ExprNode::Or(left, right) => {
            collect_slot_ids(arena, *left, out);
            collect_slot_ids(arena, *right, out);
        }
        ExprNode::In { child, values, .. } => {
            collect_slot_ids(arena, *child, out);
            for value in values {
                collect_slot_ids(arena, *value, out);
            }
        }
        ExprNode::Case { children, .. } => {
            for child in children {
                collect_slot_ids(arena, *child, out);
            }
        }
        ExprNode::FunctionCall { args, .. } => {
            for arg in args {
                collect_slot_ids(arena, *arg, out);
            }
        }
    }
}

struct FilterProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    predicate: ExprId,
    encoding_policy: FilterEncodingPolicy,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for FilterProcessorOperator {
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

impl ProcessorOperator for FilterProcessorOperator {
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
            return Err("filter received input while output buffer is full".to_string());
        }
        if chunk.is_empty() {
            self.pending_output = Some(Chunk::default());
            return Ok(());
        }

        // Vectorized implementation: use eval to compute predicate on entire chunk
        let predicate_array = self
            .arena
            .eval(self.predicate, &chunk)
            .map_err(|e| e.to_string())?;

        // Downcast to BooleanArray
        let filter_mask = predicate_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "Filter predicate must return boolean array".to_string())?;

        // Use Arrow filter kernel to filter the RecordBatch
        let filtered_batch = filter_record_batch(&chunk.batch, filter_mask)
            .map_err(|e| format!("Filter failed: {}", e))?;

        self.pending_output = Some(Chunk::new_like(filtered_batch, &chunk));
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if out.is_some() && self.finishing && self.pending_output.is_none() {
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

    fn accepts_encoded_column(&self, slot_id: SlotId, data_type: &DataType) -> bool {
        self.encoding_policy
            .accepts_encoded_column(slot_id, data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn dict_utf8_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    }

    fn dict_utf8(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(values.into_iter().collect::<DictionaryArray<Int32Type>>())
    }

    fn utf8_literal(arena: &mut ExprArena, value: &str) -> ExprId {
        arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8(value.to_string())),
            DataType::Utf8,
        )
    }

    fn filter_test_chunk() -> Chunk {
        let status = dict_utf8(vec![Some("PAID"), Some("PENDING"), None, Some("PAID")]);
        let channel = dict_utf8(vec![Some("web"), Some("retail"), Some("ops"), Some("web")]);
        let amount = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;
        let slots = vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("status", DataType::Utf8, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(2),
                Field::new("channel", DataType::Utf8, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(3),
                Field::new("amount", DataType::Int32, false),
                None,
                None,
            ),
        ];
        let chunk_schema = Arc::new(ChunkSchema::try_new(slots).expect("chunk schema"));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("status", status.data_type().clone(), true),
                Field::new("channel", channel.data_type().clone(), true),
                Field::new("amount", DataType::Int32, false),
            ])),
            vec![status, channel, amount],
        )
        .expect("record batch");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    fn assert_int32_utf8_dictionary(column: &ArrayRef) {
        assert!(matches!(
            column.data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32
                    && value_type.as_ref() == &DataType::Utf8
        ));
    }

    #[test]
    fn filter_policy_accepts_passthrough_and_supported_predicate_dict_slots() {
        let mut arena = ExprArena::default();
        let status = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let state = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Utf8);
        let paid = utf8_literal(&mut arena, "PAID");
        let status_is_paid = arena.push_typed(ExprNode::Eq(status, paid), DataType::Boolean);
        let state_not_null = arena.push_typed(ExprNode::IsNotNull(state), DataType::Boolean);
        let predicate = arena.push_typed(
            ExprNode::And(status_is_paid, state_not_null),
            DataType::Boolean,
        );

        let policy = FilterEncodingPolicy::from_predicate(&arena, predicate);
        let dict = dict_utf8_type();

        assert!(policy.accepts_encoded_column(SlotId::new(1), &dict));
        assert!(policy.accepts_encoded_column(SlotId::new(2), &dict));
        assert!(policy.accepts_encoded_column(SlotId::new(3), &dict));
        assert!(!policy.accepts_encoded_column(SlotId::new(1), &DataType::Int32));
    }

    #[test]
    fn filter_policy_hydrates_slots_used_inside_complex_expressions() {
        let mut arena = ExprArena::default();
        let status = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let cast_status = arena.push_typed(ExprNode::Cast(status), DataType::Utf8);
        let paid = utf8_literal(&mut arena, "PAID");
        let predicate = arena.push_typed(ExprNode::Eq(cast_status, paid), DataType::Boolean);

        let policy = FilterEncodingPolicy::from_predicate(&arena, predicate);
        let dict = dict_utf8_type();

        assert!(!policy.accepts_encoded_column(SlotId::new(1), &dict));
        assert!(policy.accepts_encoded_column(SlotId::new(2), &dict));
    }

    #[test]
    fn filter_policy_rejects_cast_literal_comparison_side() {
        let mut arena = ExprArena::default();
        let status = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let paid = utf8_literal(&mut arena, "PAID");
        let cast_paid = arena.push_typed(ExprNode::Cast(paid), DataType::Utf8);
        let predicate = arena.push_typed(ExprNode::Eq(status, cast_paid), DataType::Boolean);

        let policy = FilterEncodingPolicy::from_predicate(&arena, predicate);
        let dict = dict_utf8_type();

        assert!(!policy.accepts_encoded_column(SlotId::new(1), &dict));
        assert!(policy.accepts_encoded_column(SlotId::new(2), &dict));
    }

    #[test]
    fn filter_policy_accepts_in_and_null_predicates_only_when_child_is_direct_slot() {
        let mut arena = ExprArena::default();
        let status = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let paid = utf8_literal(&mut arena, "PAID");
        let pending = utf8_literal(&mut arena, "PENDING");
        let in_pred = arena.push_typed(
            ExprNode::In {
                child: status,
                values: vec![paid, pending],
                is_not_in: false,
            },
            DataType::Boolean,
        );
        let policy = FilterEncodingPolicy::from_predicate(&arena, in_pred);
        assert!(policy.accepts_encoded_column(SlotId::new(1), &dict_utf8_type()));

        let mut complex_arena = ExprArena::default();
        let status = complex_arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let cast_status = complex_arena.push_typed(ExprNode::Cast(status), DataType::Utf8);
        let null_pred = complex_arena.push_typed(ExprNode::IsNull(cast_status), DataType::Boolean);
        let complex = FilterEncodingPolicy::from_predicate(&complex_arena, null_pred);
        assert!(!complex.accepts_encoded_column(SlotId::new(1), &dict_utf8_type()));
    }

    #[test]
    fn filter_processor_preserves_dictionary_columns_for_supported_predicates() {
        let mut arena = ExprArena::default();
        let status = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let paid = utf8_literal(&mut arena, "PAID");
        let predicate = arena.push_typed(ExprNode::Eq(status, paid), DataType::Boolean);
        let arena = Arc::new(arena);
        let factory = FilterProcessorFactory::new(7, Arc::clone(&arena), predicate);
        let mut op = factory.create(1, 0);
        let processor = op.as_processor_mut().expect("processor");
        let dict_type = dict_utf8_type();

        assert!(processor.accepts_encoded_column(SlotId::new(1), &dict_type));
        assert!(processor.accepts_encoded_column(SlotId::new(2), &dict_type));
        assert!(!processor.accepts_encoded_column(SlotId::new(3), &DataType::Int32));

        let state = RuntimeState::default();
        processor
            .push_chunk(&state, filter_test_chunk())
            .expect("push");
        let output = processor.pull_chunk(&state).expect("pull").expect("output");

        assert_eq!(output.len(), 2);
        assert_int32_utf8_dictionary(&output.columns()[0]);
        assert_int32_utf8_dictionary(&output.columns()[1]);

        let status = output.columns()[0]
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("status dictionary");
        let values = status
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("status values");
        for row in 0..status.len() {
            let key = status.keys().value(row) as usize;
            assert_eq!(values.value(key), "PAID");
        }
    }
}
