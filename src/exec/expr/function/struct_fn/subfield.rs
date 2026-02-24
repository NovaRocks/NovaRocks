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
use arrow::array::{Array, ArrayRef, StringArray, StructArray, UInt32Array};
use arrow::compute::cast;
use arrow::compute::take;

pub fn eval_subfield(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let field_name_arr = arena.eval(args[1], chunk)?;
    let struct_arr = input
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| format!("subfield expects StructArray, got {:?}", input.data_type()))?;
    let field_name = parse_constant_field_name(field_name_arr.as_ref())?;

    let field_idx = struct_arr
        .fields()
        .iter()
        .position(|f| f.name() == field_name.as_str())
        .ok_or_else(|| format!("subfield field '{}' does not exist", field_name))?;
    let field_col = struct_arr.column(field_idx);

    let mut indices = Vec::with_capacity(struct_arr.len());
    for row in 0..struct_arr.len() {
        if struct_arr.is_null(row) {
            indices.push(None);
        } else {
            let idx = u32::try_from(row)
                .map_err(|_| "subfield index exceeds UInt32 range".to_string())?;
            indices.push(Some(idx));
        }
    }

    let indices = UInt32Array::from(indices);
    let out = take(field_col.as_ref(), &indices, None).map_err(|e| e.to_string())?;
    let Some(target_type) = arena.data_type(expr) else {
        return Ok(out);
    };
    if out.data_type() == target_type {
        return Ok(out);
    }
    cast(&out, target_type).map_err(|e| format!("subfield: failed to cast output: {}", e))
}

fn parse_constant_field_name(field_name_arr: &dyn Array) -> Result<String, String> {
    let arr = field_name_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "subfield field-name argument must be VARCHAR".to_string())?;
    if arr.is_empty() {
        return Err("subfield field-name argument is empty".to_string());
    }
    let first = if arr.is_null(0) {
        return Err("subfield field-name argument must be non-null".to_string());
    } else {
        arr.value(0)
    };
    for i in 1..arr.len() {
        if arr.is_null(i) || arr.value(i) != first {
            return Err("subfield field-name argument must be constant".to_string());
        }
    }
    Ok(first.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::struct_fn::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::sync::Arc;

    #[test]
    fn test_subfield_extracts_field() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let struct_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("a", DataType::Int64, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ]));
        let out_expr = typed_null(&mut arena, DataType::Utf8);

        let f0 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(7)), DataType::Int64);
        let f1 = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("x".to_string())),
            DataType::Utf8,
        );
        let struct_expr = arena.push_typed(
            ExprNode::StructExpr {
                fields: vec![f0, f1],
            },
            struct_type,
        );
        let field_name = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("b".to_string())),
            DataType::Utf8,
        );

        let out = eval_subfield(&arena, out_expr, &[struct_expr, field_name], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "x");
    }

    #[test]
    fn test_subfield_parent_null_propagates() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let struct_type = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
            "a",
            DataType::Int64,
            true,
        ))]));
        let out_expr = typed_null(&mut arena, DataType::Int64);
        let struct_expr = typed_null(&mut arena, struct_type);
        let field_name = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("a".to_string())),
            DataType::Utf8,
        );

        let out = eval_subfield(&arena, out_expr, &[struct_expr, field_name], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(out.is_null(0));
    }
}
