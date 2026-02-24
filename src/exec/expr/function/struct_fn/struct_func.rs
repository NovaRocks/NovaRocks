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
use arrow::array::{ArrayRef, StructArray, new_null_array};
use arrow::datatypes::DataType;
use std::sync::Arc;

fn eval_new_struct(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    let output_type = arena
        .data_type(expr)
        .cloned()
        .ok_or_else(|| format!("{} missing output type", fn_name))?;
    let struct_fields = match output_type {
        DataType::Struct(fields) => fields,
        other => {
            return Err(format!(
                "{} output type must be Struct, got {:?}",
                fn_name, other
            ));
        }
    };
    if struct_fields.len() != args.len() {
        return Err(format!(
            "{} field count mismatch: expected {}, got {}",
            fn_name,
            struct_fields.len(),
            args.len()
        ));
    }

    let num_rows = chunk.len();
    let mut arrays = Vec::with_capacity(args.len());
    for (idx, expr_id) in args.iter().enumerate() {
        let mut array = arena.eval(*expr_id, chunk)?;
        if array.len() != num_rows {
            return Err(format!(
                "{} field length mismatch at {}: expected {}, got {}",
                fn_name,
                idx,
                num_rows,
                array.len()
            ));
        }
        let expected_type = struct_fields[idx].data_type();
        if array.data_type() == &DataType::Null && expected_type != &DataType::Null {
            array = new_null_array(expected_type, num_rows);
        }
        if array.data_type() != expected_type {
            return Err(format!(
                "{} field type mismatch at {}: expected {:?}, got {:?}",
                fn_name,
                idx,
                expected_type,
                array.data_type()
            ));
        }
        arrays.push(array);
    }
    Ok(Arc::new(StructArray::new(struct_fields, arrays, None)) as ArrayRef)
}

pub fn eval_row(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_new_struct(arena, expr, args, chunk, "row")
}

pub fn eval_struct(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_new_struct(arena, expr, args, chunk, "struct")
}

pub fn eval_named_struct(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() < 2 || args.len() % 2 != 0 {
        return Err(format!(
            "named_struct expects an even number of arguments >= 2, got {}",
            args.len()
        ));
    }
    let values: Vec<ExprId> = args.iter().skip(1).step_by(2).copied().collect();
    eval_new_struct(arena, expr, &values, chunk, "named_struct")
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::struct_fn::eval_struct_function;
    use crate::exec::expr::function::struct_fn::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
    use arrow::array::{Int64Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::sync::Arc;

    fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
    }

    fn literal_string(arena: &mut ExprArena, v: &str) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Utf8(v.to_string())))
    }

    #[test]
    fn test_row() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let struct_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("c0", DataType::Int64, true)),
            Arc::new(Field::new("c1", DataType::Utf8, true)),
        ]));
        let expr = typed_null(&mut arena, struct_type);
        let a = literal_i64(&mut arena, 1);
        let b = literal_string(&mut arena, "x");
        let out = eval_struct_function("row", &arena, expr, &[a, b], &chunk).unwrap();
        let st = out.as_any().downcast_ref::<StructArray>().unwrap();
        let c0 = st.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c1 = st.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(c0.value(0), 1);
        assert_eq!(c1.value(0), "x");
    }

    #[test]
    fn test_struct_alias() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let struct_type = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
            "c0",
            DataType::Int64,
            true,
        ))]));
        let expr = typed_null(&mut arena, struct_type);
        let a = literal_i64(&mut arena, 7);
        let out = eval_struct_function("struct", &arena, expr, &[a], &chunk).unwrap();
        let st = out.as_any().downcast_ref::<StructArray>().unwrap();
        let c0 = st.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(c0.value(0), 7);
    }

    #[test]
    fn test_named_struct() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let struct_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("a", DataType::Int64, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ]));
        let expr = typed_null(&mut arena, struct_type);
        let name_a = literal_string(&mut arena, "a");
        let value_a = literal_i64(&mut arena, 42);
        let name_b = literal_string(&mut arena, "b");
        let value_b = literal_string(&mut arena, "ok");
        let out = eval_struct_function(
            "named_struct",
            &arena,
            expr,
            &[name_a, value_a, name_b, value_b],
            &chunk,
        )
        .unwrap();
        let st = out.as_any().downcast_ref::<StructArray>().unwrap();
        let c0 = st.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c1 = st.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(c0.value(0), 42);
        assert_eq!(c1.value(0), "ok");
    }

    #[test]
    fn test_named_struct_arg_validation() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let struct_type = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
            "a",
            DataType::Int64,
            true,
        ))]));
        let expr = typed_null(&mut arena, struct_type);
        let name_a = literal_string(&mut arena, "a");
        let value_a = literal_i64(&mut arena, 42);
        let extra = literal_string(&mut arena, "dangling");
        let err = super::eval_named_struct(&arena, expr, &[name_a, value_a, extra], &chunk)
            .expect_err("must fail");
        assert!(err.contains("even number"));
    }
}
