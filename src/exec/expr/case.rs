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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, BooleanArray, new_null_array};
use arrow::compute::kernels::cast;
use arrow::compute::kernels::cmp::eq;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;

// CASE expression for Arrow arrays
pub fn eval_case(
    arena: &ExprArena,
    has_case_expr: bool,
    has_else_expr: bool,
    children: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let len = chunk.len();
    let mut idx = 0usize;

    // If has_case_expr, first child is the case expression to compare
    let case_expr = if has_case_expr {
        if children.is_empty() {
            return Err("CASE_EXPR: missing case expression".to_string());
        }
        let expr = children[idx];
        idx += 1;
        Some(expr)
    } else {
        None
    };

    // Evaluate case_expr once if present
    let case_array = if let Some(expr) = case_expr {
        Some(arena.eval(expr, chunk)?)
    } else {
        None
    };

    let result_type = resolve_case_result_type(arena, children, idx, has_else_expr, chunk)?;
    if matches!(result_type, DataType::Null) {
        return Ok(new_null_array(&result_type, len));
    }

    let mut result = if has_else_expr && !children.is_empty() {
        let else_expr = children[children.len() - 1];
        let else_array = arena.eval(else_expr, chunk)?;
        if else_array.data_type() != &result_type {
            cast(else_array.as_ref(), &result_type).map_err(|e| e.to_string())?
        } else {
            else_array
        }
    } else {
        new_null_array(&result_type, len)
    };

    let end = if has_else_expr && !children.is_empty() {
        children.len() - 1
    } else {
        children.len()
    };

    // Evaluate WHEN/THEN pairs from right to left so the first WHEN clause keeps precedence.
    // CASE semantics are "first match wins", while zip(condition, value, current_result) naturally
    // applies "last match wins" when evaluated from left to right.
    let mut pair_end = end;
    while pair_end > idx + 1 {
        let condition_expr = children[pair_end - 2];
        let value_expr = children[pair_end - 1];
        pair_end -= 2;

        let condition_bool = if let Some(ref case_arr) = case_array {
            let condition_array = arena.eval(condition_expr, chunk)?;
            eq(
                &case_arr.as_ref() as &dyn arrow::array::Datum,
                &condition_array.as_ref() as &dyn arrow::array::Datum,
            )
            .map_err(|e| e.to_string())?
        } else {
            let condition_array = arena.eval(condition_expr, chunk)?;
            condition_array_to_bool(&condition_array)?
        };

        let value_array = arena.eval(value_expr, chunk)?;
        let value_array = if value_array.data_type() != &result_type {
            cast(value_array.as_ref(), &result_type).map_err(|e| e.to_string())?
        } else {
            value_array
        };

        result = zip(
            &condition_bool,
            &value_array.as_ref() as &dyn arrow::array::Datum,
            &result.as_ref() as &dyn arrow::array::Datum,
        )
        .map_err(|e| e.to_string())?;
    }

    Ok(result)
}

fn resolve_case_result_type(
    arena: &ExprArena,
    children: &[ExprId],
    start: usize,
    has_else_expr: bool,
    chunk: &Chunk,
) -> Result<DataType, String> {
    let end = if has_else_expr && !children.is_empty() {
        children.len() - 1
    } else {
        children.len()
    };

    let mut value_idx = start;
    while value_idx + 1 < end {
        let value_expr = children[value_idx + 1];
        let array = arena.eval(value_expr, chunk)?;
        if !matches!(array.data_type(), DataType::Null) {
            return Ok(array.data_type().clone());
        }
        value_idx += 2;
    }

    if has_else_expr && !children.is_empty() {
        let else_expr = children[children.len() - 1];
        let array = arena.eval(else_expr, chunk)?;
        return Ok(array.data_type().clone());
    }

    Ok(DataType::Null)
}

fn condition_array_to_bool(array: &ArrayRef) -> Result<BooleanArray, String> {
    let len = array.len();
    match array.data_type() {
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "failed to downcast condition to BooleanArray".to_string())
            .cloned(),
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "failed to downcast condition to Int64Array".to_string())?;
            let values: Vec<Option<bool>> = (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) != 0)
                    }
                })
                .collect();
            Ok(BooleanArray::from_iter(values))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "failed to downcast condition to Float64Array".to_string())?;
            let values: Vec<Option<bool>> = (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) != 0.0)
                    }
                })
                .collect();
            Ok(BooleanArray::from_iter(values))
        }
        _ => Err(format!(
            "case: condition must be boolean, int64, or float64, got {:?}",
            array.data_type()
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_i32_chunk(slot: SlotId, values: Vec<Option<i32>>) -> Chunk {
        let field = field_with_slot_id(Field::new("c0", DataType::Int32, true), slot);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap();
        Chunk::new(batch)
    }

    #[test]
    fn test_case_when_prefers_first_match() {
        let slot_id = SlotId(1);
        let chunk = make_i32_chunk(slot_id, vec![Some(95), Some(82), Some(60), None]);

        let mut arena = ExprArena::default();
        let score = arena.push_typed(ExprNode::SlotId(slot_id), DataType::Int32);
        let lit_90 = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(90)), DataType::Int32);
        let lit_80 = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(80)), DataType::Int32);
        let ge_90 = arena.push_typed(ExprNode::Ge(score, lit_90), DataType::Boolean);
        let ge_80 = arena.push_typed(ExprNode::Ge(score, lit_80), DataType::Boolean);

        let then_a = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("A".to_string())),
            DataType::Utf8,
        );
        let then_b = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("B".to_string())),
            DataType::Utf8,
        );
        let else_c = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("C".to_string())),
            DataType::Utf8,
        );

        let expr = arena.push_typed(
            ExprNode::Case {
                has_case_expr: false,
                has_else_expr: true,
                children: vec![ge_90, then_a, ge_80, then_b, else_c],
            },
            DataType::Utf8,
        );

        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "A");
        assert_eq!(out.value(1), "B");
        assert_eq!(out.value(2), "C");
        assert_eq!(out.value(3), "C");
    }

    #[test]
    fn test_case_expr_prefers_first_when_value() {
        let slot_id = SlotId(1);
        let chunk = make_i32_chunk(slot_id, vec![Some(1), Some(2), None]);

        let mut arena = ExprArena::default();
        let case_expr = arena.push_typed(ExprNode::SlotId(slot_id), DataType::Int32);
        let when_1a = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(1)), DataType::Int32);
        let then_first = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("first".to_string())),
            DataType::Utf8,
        );
        let when_1b = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(1)), DataType::Int32);
        let then_second = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("second".to_string())),
            DataType::Utf8,
        );
        let else_other = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("other".to_string())),
            DataType::Utf8,
        );

        let expr = arena.push_typed(
            ExprNode::Case {
                has_case_expr: true,
                has_else_expr: true,
                children: vec![
                    case_expr,
                    when_1a,
                    then_first,
                    when_1b,
                    then_second,
                    else_other,
                ],
            },
            DataType::Utf8,
        );

        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "first");
        assert_eq!(out.value(1), "other");
        assert_eq!(out.value(2), "other");
    }
}
