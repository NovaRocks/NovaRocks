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
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::{Array, ArrayRef, Int64Array, ListArray};
use serde_json::Value as JsonValue;
use std::sync::Arc;

fn try_eval_cast_literal_json_array_length(
    arena: &ExprArena,
    out_expr: ExprId,
    arg_expr: ExprId,
    num_rows: usize,
) -> Option<Result<ArrayRef, String>> {
    let ExprNode::Cast(child_expr) = arena.node(arg_expr)? else {
        return None;
    };
    let ExprNode::Literal(LiteralValue::Utf8(text)) = arena.node(*child_expr)? else {
        return None;
    };
    let target_type = arena.data_type(arg_expr)?;
    if !matches!(target_type, arrow::datatypes::DataType::List(_)) {
        return None;
    }

    let parsed: JsonValue = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            return Some(Err(format!(
                "array_length constant json array parse failed: {}",
                e
            )));
        }
    };
    let out = match parsed {
        JsonValue::Array(items) => {
            let length = items.len() as i64;
            Arc::new(Int64Array::from(vec![Some(length); num_rows])) as ArrayRef
        }
        JsonValue::Null => Arc::new(Int64Array::from(vec![None; num_rows])) as ArrayRef,
        _ => {
            return Some(Err(
                "array_length constant cast source must be a JSON array".to_string(),
            ));
        }
    };
    Some(super::common::cast_output(
        out,
        arena.data_type(out_expr),
        "array_length",
    ))
}

pub fn eval_array_length(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if let Some(result) = try_eval_cast_literal_json_array_length(arena, expr, args[0], chunk.len())
    {
        return result;
    }

    let arr = arena.eval(args[0], chunk)?;
    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_length expects ListArray, got {:?}", arr.data_type()))?;

    let mut out = Vec::with_capacity(list.len());
    let offsets = list.value_offsets();
    for row in 0..list.len() {
        if list.is_null(row) {
            out.push(None);
        } else {
            out.push(Some((offsets[row + 1] - offsets[row]) as i64));
        }
    }

    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "array_length")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_array_length_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Int32);
        let values =
            vec![arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64)];
        let arr = arena.push_typed(
            ExprNode::ArrayExpr { elements: values },
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        );

        let out = eval_array_function("array_length", &arena, expr, &[arr], &chunk).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(out.value(0), 1);
    }

    #[test]
    fn test_array_length_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Int64);
        let arr = typed_null(
            &mut arena,
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        );
        let out = eval_array_length(&arena, expr, &[arr], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(out.is_null(0));
    }

    #[test]
    fn test_array_length_const_cast_json_array() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Int64);
        let lit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("[\"a\", \"b\", null]".to_string())),
            DataType::Utf8,
        );
        let arr_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let cast_expr = arena.push_typed(ExprNode::Cast(lit), arr_type);

        let array = Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("dummy", DataType::Int64, false),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let chunk = Chunk::new(batch);

        let out = eval_array_length(&arena, expr, &[cast_expr], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.len(), 4);
        for i in 0..out.len() {
            assert_eq!(out.value(i), 3);
        }
    }
}
