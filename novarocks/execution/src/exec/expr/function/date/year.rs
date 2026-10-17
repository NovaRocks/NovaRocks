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
use arrow::datatypes::DataType;
use novarocks_functions::{Selection, builtin::calendar_parts_shared::legacy_year_array};

// YEAR function for Arrow arrays
pub fn eval_year<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    year_expr_id: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    // Get the child expression (input date/timestamp)
    // For FunctionCall, the first argument is the input
    let child_expr_id = match arena.node(year_expr_id) {
        Some(crate::exec::expr::ExprNode::FunctionCall { args, .. }) => args
            .first()
            .copied()
            .ok_or_else(|| "year: missing argument".to_string())?,
        _ => return Err(("Year expression node type mismatch".to_string()).into()),
    };

    let array = arena.eval(child_expr_id, chunk)?;

    // Get expected return type from arena (for the Year expression itself)
    let expected_type = arena
        .data_type(year_expr_id)
        .cloned()
        .unwrap_or(DataType::Int64);

    Ok((legacy_year_array(&array, &expected_type, Selection::all(array.len())))?)
}
