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
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray,
};
use std::sync::Arc;

fn avg_rows<T, F>(
    list: &ListArray,
    values: &T,
    chunk_len: usize,
    mut value_at: F,
) -> Vec<Option<f64>>
where
    T: Array,
    F: FnMut(&T, usize) -> f64,
{
    let offsets = list.value_offsets();
    let mut out = Vec::with_capacity(chunk_len);
    for row in 0..chunk_len {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            out.push(None);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut sum = 0.0_f64;
        let count = (end - start) as i64;
        let mut has_value = false;
        for idx in start..end {
            if values.is_null(idx) {
                continue;
            }
            has_value = true;
            sum += value_at(values, idx);
        }
        if count == 0 || !has_value {
            out.push(None);
        } else {
            out.push(Some(sum / count as f64));
        }
    }
    out
}

pub fn eval_array_avg(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_avg expects ListArray, got {:?}", arr.data_type()))?;
    let values = list.values();

    let out = match values.data_type() {
        arrow::datatypes::DataType::Boolean => {
            let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| if a.value(idx) { 1.0 } else { 0.0 },
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Int8 => {
            let values = values.as_any().downcast_ref::<Int8Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Int16 => {
            let values = values.as_any().downcast_ref::<Int16Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Int32 => {
            let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Int64 => {
            let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Float32 => {
            let values = values.as_any().downcast_ref::<Float32Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Float64 => {
            let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx),
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::Decimal128(_, scale) => {
            let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let factor = 10_f64.powi(i32::from(*scale));
            Arc::new(Float64Array::from(avg_rows(
                list,
                values,
                chunk.len(),
                |a, idx| a.value(idx) as f64 / factor,
            ))) as ArrayRef
        }
        arrow::datatypes::DataType::FixedSizeBinary(width)
            if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let values = crate::common::largeint::as_fixed_size_binary_array(
                &values,
                "array_avg LARGEINT values",
            )?;
            let mut converted = Vec::with_capacity(values.len());
            for idx in 0..values.len() {
                if values.is_null(idx) {
                    converted.push(None);
                } else {
                    let value = crate::common::largeint::value_at(values, idx)?;
                    converted.push(Some(value as f64));
                }
            }
            let converted = Float64Array::from(converted);
            Arc::new(Float64Array::from(avg_rows(
                list,
                &converted,
                chunk.len(),
                |a, idx| a.value(idx),
            ))) as ArrayRef
        }
        other => return Err(format!("array_avg unsupported element type: {:?}", other)),
    };

    super::common::cast_output(out, arena.data_type(expr), "array_avg")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_avg_int64() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Float64);
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v1, v3],
            },
            list_type,
        );

        let out = eval_array_function("array_avg", &arena, expr, &[arr], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((out.value(0) - 2.0).abs() < 1e-12);
    }

    #[test]
    fn test_array_avg_null_counts_in_denominator_and_empty() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Float64);
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let null_elem = typed_null(&mut arena, DataType::Int64);
        let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![null_elem, v2],
            },
            list_type.clone(),
        );

        let out = eval_array_function("array_avg", &arena, expr, &[arr], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((out.value(0) - 1.0).abs() < 1e-12);

        let expr2 = typed_null(&mut arena, DataType::Float64);
        let empty = arena.push_typed(ExprNode::ArrayExpr { elements: vec![] }, list_type);
        let out_empty = eval_array_function("array_avg", &arena, expr2, &[empty], &chunk).unwrap();
        let out_empty = out_empty.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(out_empty.is_null(0));
    }

    #[test]
    fn test_array_avg_all_null_returns_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Float64);
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let null_elem = typed_null(&mut arena, DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![null_elem, null_elem],
            },
            list_type,
        );
        let out = eval_array_function("array_avg", &arena, expr, &[arr], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(out.is_null(0));
    }
}
