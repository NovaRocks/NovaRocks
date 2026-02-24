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
use arrow::array::{Array, ArrayRef, ListArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

pub fn eval_array_join(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let list_arr = arena.eval(args[0], chunk)?;
    let sep_arr = arena.eval(args[1], chunk)?;
    let null_replace_arr = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };

    let list = list_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "array_join expects ListArray, got {:?}",
                list_arr.data_type()
            )
        })?;

    let mut list_values = list.values().clone();
    if list_values.data_type() != &DataType::Utf8 {
        list_values = cast(&list_values, &DataType::Utf8)
            .map_err(|e| format!("array_join failed to cast array elements to VARCHAR: {}", e))?;
    }
    let list_values = list_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "array_join failed to downcast list values to StringArray".to_string())?;

    let mut sep_cast = sep_arr.clone();
    if sep_cast.data_type() != &DataType::Utf8 {
        sep_cast = cast(&sep_cast, &DataType::Utf8)
            .map_err(|e| format!("array_join: invalid sep: {}", e))?;
    }
    let sep = sep_cast
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "array_join failed to downcast sep to StringArray".to_string())?;

    let null_replace_cast = if let Some(arr) = null_replace_arr {
        let mut casted = arr;
        if casted.data_type() != &DataType::Utf8 {
            casted = cast(&casted, &DataType::Utf8)
                .map_err(|e| format!("array_join: invalid null_replace: {}", e))?;
        }
        Some(casted)
    } else {
        None
    };
    let null_replace = null_replace_cast
        .as_ref()
        .map(|arr| {
            arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                "array_join failed to downcast null_replace to StringArray".to_string()
            })
        })
        .transpose()?;

    let list_offsets = list.value_offsets();
    let mut out = Vec::<Option<String>>::with_capacity(chunk.len());

    for row in 0..chunk.len() {
        let list_row = super::common::row_index(row, list.len());
        let sep_row = super::common::row_index(row, sep.len());
        let null_replace_row = null_replace.map(|arr| super::common::row_index(row, arr.len()));

        if list.is_null(list_row)
            || sep.is_null(sep_row)
            || (null_replace_row.is_some()
                && null_replace.unwrap().is_null(null_replace_row.unwrap()))
        {
            out.push(None);
            continue;
        }

        let start = list_offsets[list_row] as usize;
        let end = list_offsets[list_row + 1] as usize;
        let sep_val = sep.value(sep_row);
        let null_replace_val = null_replace_row.map(|idx| null_replace.unwrap().value(idx));

        let mut parts = Vec::<String>::new();
        for idx in start..end {
            if list_values.is_null(idx) {
                if let Some(v) = null_replace_val {
                    parts.push(v.to_string());
                }
            } else {
                parts.push(list_values.value(idx).to_string());
            }
        }
        out.push(Some(parts.join(sep_val)));
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};

    fn literal_string(arena: &mut ExprArena, v: &str) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Utf8(v.to_string())))
    }

    #[test]
    fn test_array_join_ignore_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, DataType::Utf8);

        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let null_elem = typed_null(&mut arena, DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v1, null_elem, v3],
            },
            list_type,
        );
        let sep = literal_string(&mut arena, "-");

        let out = eval_array_function("array_join", &arena, expr, &[arr, sep], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "1-3");
    }

    #[test]
    fn test_array_join_replace_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, DataType::Utf8);

        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let null_elem = typed_null(&mut arena, DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v1, null_elem, v3],
            },
            list_type,
        );
        let sep = literal_string(&mut arena, "-");
        let null_repl = literal_string(&mut arena, "X");

        let out = eval_array_function("array_join", &arena, expr, &[arr, sep, null_repl], &chunk)
            .unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "1-X-3");
    }
}
