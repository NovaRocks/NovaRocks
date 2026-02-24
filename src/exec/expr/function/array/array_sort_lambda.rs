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
use arrow::array::ArrayRef;

pub fn eval_array_sort_lambda(
    _arena: &ExprArena,
    _expr: ExprId,
    _args: &[ExprId],
    _chunk: &Chunk,
) -> Result<ArrayRef, String> {
    Err("array_sort_lambda should be lowered before evaluation".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_array_sort_lambda_returns_error() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());
        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let arr = arena.push_typed(ExprNode::ArrayExpr { elements: vec![v1] }, list_type);
        let lambda = typed_null(&mut arena, DataType::Utf8);

        let err = eval_array_function("array_sort_lambda", &arena, expr, &[arr, lambda], &chunk)
            .unwrap_err();
        assert!(err.contains("should be lowered"));
    }
}
