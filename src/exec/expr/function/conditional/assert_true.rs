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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};

const DEFAULT_FALSE_MESSAGE: &str = "assert_true failed due to false value";
const NULL_MESSAGE: &str = "assert_true failed due to null value";

pub fn eval_assert_true(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let condition = arena.eval(args[0], chunk)?;
    let condition = cast_condition_to_boolean(&condition)?;
    let condition = condition
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "assert_true: failed to downcast condition to BooleanArray".to_string())?;

    if condition.null_count() > 0 {
        return Err(NULL_MESSAGE.to_string());
    }

    let error_message = resolve_error_message(arena, args, chunk)?;
    for row in 0..condition.len() {
        if !condition.value(row) {
            return Err(error_message.clone());
        }
    }

    Ok(Arc::new(BooleanArray::from(vec![true; condition.len()])) as ArrayRef)
}

fn cast_condition_to_boolean(condition: &ArrayRef) -> Result<ArrayRef, String> {
    if condition.data_type() == &DataType::Boolean {
        return Ok(condition.clone());
    }

    cast(condition.as_ref(), &DataType::Boolean)
        .map_err(|e| format!("assert_true: failed to cast condition to BOOLEAN: {e}"))
}

fn resolve_error_message(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<String, String> {
    if args.len() < 2 {
        return Ok(DEFAULT_FALSE_MESSAGE.to_string());
    }

    if let Some(node) = arena.node(args[1]) {
        match node {
            ExprNode::Literal(LiteralValue::Utf8(v)) => return Ok(v.clone()),
            ExprNode::Literal(LiteralValue::Null) => return Ok(DEFAULT_FALSE_MESSAGE.to_string()),
            _ => {}
        }
    }

    let mut message_array = arena.eval(args[1], chunk)?;
    if message_array.data_type() != &DataType::Utf8 {
        if matches!(message_array.data_type(), DataType::Null) {
            return Ok(DEFAULT_FALSE_MESSAGE.to_string());
        }
        message_array = cast(message_array.as_ref(), &DataType::Utf8)
            .map_err(|e| format!("assert_true: failed to cast message to VARCHAR: {e}"))?;
    }

    let message_array = message_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "assert_true: failed to downcast message to StringArray".to_string())?;
    for row in 0..message_array.len() {
        if !message_array.is_null(row) {
            return Ok(message_array.value(row).to_string());
        }
    }

    Ok(DEFAULT_FALSE_MESSAGE.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn bool_chunk(values: Vec<Option<bool>>) -> Chunk {
        let field = field_with_slot_id(Field::new("cond", DataType::Boolean, true), SlotId::new(1));
        let schema = Arc::new(Schema::new(vec![field]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(BooleanArray::from(values))]).unwrap();
        Chunk::new(batch)
    }

    #[test]
    fn assert_true_returns_true_column() {
        let chunk = bool_chunk(vec![Some(true), Some(true), Some(true)]);
        let mut arena = ExprArena::default();
        let condition = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);

        let result = eval_assert_true(&arena, &[condition], &chunk).unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result.len(), 3);
        assert!(result.value(0));
        assert!(result.value(1));
        assert!(result.value(2));
    }

    #[test]
    fn assert_true_reports_false() {
        let chunk = bool_chunk(vec![Some(true), Some(false)]);
        let mut arena = ExprArena::default();
        let condition = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);

        let err = eval_assert_true(&arena, &[condition], &chunk).unwrap_err();
        assert_eq!(err, DEFAULT_FALSE_MESSAGE);
    }

    #[test]
    fn assert_true_reports_null() {
        let chunk = bool_chunk(vec![Some(true), None]);
        let mut arena = ExprArena::default();
        let condition = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);

        let err = eval_assert_true(&arena, &[condition], &chunk).unwrap_err();
        assert_eq!(err, NULL_MESSAGE);
    }

    #[test]
    fn assert_true_uses_custom_message() {
        let chunk = bool_chunk(vec![Some(false)]);
        let mut arena = ExprArena::default();
        let condition = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);
        let message = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("custom assert".to_string())),
            DataType::Utf8,
        );

        let err = eval_assert_true(&arena, &[condition, message], &chunk).unwrap_err();
        assert_eq!(err, "custom assert");
    }
}
