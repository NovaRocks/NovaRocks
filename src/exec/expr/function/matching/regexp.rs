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
use regex::Regex;
use std::sync::Arc;

pub fn eval_regexp(
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
        .ok_or_else(|| "regexp expects VARCHAR as first argument".to_string())?;
    let right = right
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp expects VARCHAR as second argument".to_string())?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if left.is_null(row) || right.is_null(row) {
            out.push(None);
            continue;
        }

        let pattern = right.value(row);
        let regex = Regex::new(pattern).map_err(|e| format!("regexp: invalid pattern: {}", e))?;
        out.push(Some(regex.is_match(left.value(row))));
    }

    Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::eval_regexp;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::matching::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::BooleanArray;
    use arrow::datatypes::DataType;

    #[test]
    fn test_regexp_partial_match() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Boolean);
        let a = literal_string(&mut arena, "abc123xyz");
        let p = literal_string(&mut arena, "\\d+");

        let out = eval_regexp(&arena, expr, &[a, p], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
    }
}
