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
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::datatypes::DataType;

use crate::exprs;

/// Lower CASE_EXPR expression to ExprNode::Case.
pub(crate) fn lower_case(
    node: &exprs::TExprNode,
    children: Vec<ExprId>,
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let case_expr = node
        .case_expr
        .as_ref()
        .ok_or_else(|| "CASE_EXPR missing case_expr payload".to_string())?;

    let has_case_expr = case_expr.has_case_expr;
    let mut has_else_expr = case_expr.has_else_expr;
    let inferred_has_else = if has_case_expr {
        children.len() % 2 == 0
    } else {
        children.len() % 2 == 1
    };
    if !has_else_expr && inferred_has_else {
        has_else_expr = true;
    }

    // Children structure:
    // If has_case_expr: [case_expr, condition1, value1, condition2, value2, ..., else_value?]
    // If !has_case_expr: [condition1, value1, condition2, value2, ..., else_value?]
    Ok(arena.push_typed(
        ExprNode::Case {
            has_case_expr,
            has_else_expr,
            children,
        },
        data_type,
    ))
}
