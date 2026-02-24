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
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use std::sync::Arc;

pub fn eval_ilike(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;

    let left = left
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "ilike expects VARCHAR as first argument".to_string())?;
    let right = right
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "ilike expects VARCHAR as second argument".to_string())?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if left.is_null(row) || right.is_null(row) {
            out.push(None);
            continue;
        }

        let value = left.value(row).to_lowercase();
        let pattern = right.value(row).to_lowercase();
        out.push(Some(super::like::like_match(&value, &pattern)));
    }

    Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::eval_ilike;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::matching::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::BooleanArray;
    use arrow::datatypes::DataType;

    #[test]
    fn test_ilike_case_insensitive_match() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Boolean);
        let a = literal_string(&mut arena, "AbCd");
        let p = literal_string(&mut arena, "%bc_");

        let out = eval_ilike(&arena, expr, &[a, p], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
    }
}
