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
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};
use std::sync::Arc;

enum IntArgArray<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl IntArgArray<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Int32(arr) => arr.len(),
            Self::Int64(arr) => arr.len(),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Int32(arr) => arr.is_null(row),
            Self::Int64(arr) => arr.is_null(row),
        }
    }

    fn value(&self, row: usize) -> i64 {
        match self {
            Self::Int32(arr) => arr.value(row) as i64,
            Self::Int64(arr) => arr.value(row),
        }
    }
}

fn downcast_int_arg_array<'a>(arr: &'a ArrayRef) -> Result<IntArgArray<'a>, String> {
    if let Some(v) = arr.as_any().downcast_ref::<Int32Array>() {
        return Ok(IntArgArray::Int32(v));
    }
    if let Some(v) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(IntArgArray::Int64(v));
    }
    Err("equiwidth_bucket expects BIGINT arguments".to_string())
}

fn const_i64_arg(arr: IntArgArray<'_>, name: &str) -> Result<i64, String> {
    if arr.len() == 0 {
        return Err(format!("equiwidth_bucket: {name} must be constant"));
    }
    if arr.is_null(0) {
        return Err(format!("equiwidth_bucket: {name} must be constant"));
    }
    let first = arr.value(0);
    for row in 1..arr.len() {
        if arr.is_null(row) || arr.value(row) != first {
            return Err(format!("equiwidth_bucket: {name} must be constant"));
        }
    }
    Ok(first)
}

pub fn eval_equiwidth_bucket(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let size_eval = arena.eval(args[0], chunk)?;
    let min_eval = arena.eval(args[1], chunk)?;
    let max_eval = arena.eval(args[2], chunk)?;
    let buckets_eval = arena.eval(args[3], chunk)?;

    let size_arr = downcast_int_arg_array(&size_eval)?;
    let min_arr = downcast_int_arg_array(&min_eval)?;
    let max_arr = downcast_int_arg_array(&max_eval)?;
    let buckets_arr = downcast_int_arg_array(&buckets_eval)?;

    let min = const_i64_arg(min_arr, "argument[min]")?;
    let max = const_i64_arg(max_arr, "argument[max]")?;
    let buckets = const_i64_arg(buckets_arr, "argument[buckets]")?;

    if min >= max {
        return Err("equiwidth_bucket requirement: min < max".to_string());
    }
    if buckets <= 0 {
        return Err("equiwidth_bucket requirement: buckets > 0".to_string());
    }

    let width = ((max - min) / buckets).max(1);
    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if size_arr.is_null(row) {
            out.push(None);
            continue;
        }
        let size = size_arr.value(row);
        if size < min {
            return Err("equiwidth_bucket requirement: size >= min".to_string());
        }
        if size > max {
            return Err("equiwidth_bucket requirement: size <= max".to_string());
        }
        out.push(Some((size - min) / width));
    }
    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr))
}

#[cfg(test)]
mod tests {
    use super::eval_equiwidth_bucket;
    use crate::exec::expr::function::math::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;

    fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
    }

    #[test]
    fn test_equiwidth_bucket_basic() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Int64);
        let value = literal_i64(&mut arena, 10);
        let min = literal_i64(&mut arena, 0);
        let max = literal_i64(&mut arena, 10);
        let buckets = literal_i64(&mut arena, 20);

        let out = eval_equiwidth_bucket(&arena, expr, &[value, min, max, buckets], &chunk_len_1())
            .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 10);
    }

    #[test]
    fn test_equiwidth_bucket_invalid_range() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Int64);
        let value = literal_i64(&mut arena, 1);
        let min = literal_i64(&mut arena, 2);
        let max = literal_i64(&mut arena, 2);
        let buckets = literal_i64(&mut arena, 1);
        let err = eval_equiwidth_bucket(&arena, expr, &[value, min, max, buckets], &chunk_len_1())
            .unwrap_err();
        assert!(err.contains("min < max"));
    }

    #[test]
    fn test_equiwidth_bucket_out_of_bounds() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Int64);
        let value = literal_i64(&mut arena, 11);
        let min = literal_i64(&mut arena, 0);
        let max = literal_i64(&mut arena, 10);
        let buckets = literal_i64(&mut arena, 20);
        let err = eval_equiwidth_bucket(&arena, expr, &[value, min, max, buckets], &chunk_len_1())
            .unwrap_err();
        assert!(err.contains("size <= max"));
    }
}
