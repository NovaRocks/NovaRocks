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
use arrow::array::{Array, ArrayRef, BooleanArray, ListArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

pub fn eval_all_match(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("all_match expects ListArray, got {:?}", arr.data_type()))?;

    let mut values = list.values().clone();
    if values.data_type() != &DataType::Boolean {
        if crate::common::largeint::is_largeint_data_type(values.data_type()) {
            let typed = crate::common::largeint::as_fixed_size_binary_array(
                &values,
                "all_match LARGEINT to BOOLEAN",
            )?;
            let mut out = Vec::with_capacity(typed.len());
            for idx in 0..typed.len() {
                if typed.is_null(idx) {
                    out.push(None);
                } else {
                    out.push(Some(crate::common::largeint::value_at(typed, idx)? != 0));
                }
            }
            values = Arc::new(BooleanArray::from(out)) as ArrayRef;
        } else {
            values = cast(&values, &DataType::Boolean)
                .map_err(|e| format!("all_match failed to cast element to BOOLEAN: {}", e))?;
        }
    }
    let values = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "all_match failed to downcast values to BooleanArray".to_string())?;

    let offsets = list.value_offsets();
    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            out.push(None);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut has_null = false;
        let mut all_true = true;
        for idx in start..end {
            if values.is_null(idx) {
                has_null = true;
                continue;
            }
            if !values.value(idx) {
                all_true = false;
                break;
            }
        }

        if !all_true {
            out.push(Some(false));
        } else if has_null {
            out.push(None);
        } else {
            out.push(Some(true));
        }
    }

    Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_all_match_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Boolean);
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));

        let t = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(true)),
            DataType::Boolean,
        );
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![t, t],
            },
            list_type,
        );

        let out = eval_array_function("all_match", &arena, expr, &[arr], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
    }

    #[test]
    fn test_all_match_false_and_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Boolean);
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));

        let t = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(true)),
            DataType::Boolean,
        );
        let f = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(false)),
            DataType::Boolean,
        );
        let null_elem = typed_null(&mut arena, DataType::Boolean);

        let arr_false = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![t, f, null_elem],
            },
            list_type.clone(),
        );
        let out_false =
            eval_array_function("all_match", &arena, expr, &[arr_false], &chunk).unwrap();
        let out_false = out_false.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!out_false.value(0));

        let expr2 = typed_null(&mut arena, DataType::Boolean);
        let arr_null = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![t, null_elem],
            },
            list_type,
        );
        let out_null =
            eval_array_function("all_match", &arena, expr2, &[arr_null], &chunk).unwrap();
        let out_null = out_null.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out_null.is_null(0));
    }
}
