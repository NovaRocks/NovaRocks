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

pub(crate) fn lower_lambda_function(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let common_num_i32 = node.output_column.unwrap_or(0);
    if common_num_i32 < 0 {
        return Err("lambda common sub expr count is negative".to_string());
    }
    let common_num = usize::try_from(common_num_i32)
        .map_err(|_| "lambda common sub expr count overflow".to_string())?;

    if children.is_empty() {
        return Err("lambda function must have a body expression".to_string());
    }
    if common_num * 2 > children.len().saturating_sub(1) {
        return Err("lambda common sub expr layout is invalid".to_string());
    }

    let total = children.len();
    let args_end = total - 2 * common_num;
    if args_end < 1 {
        return Err("lambda function missing body".to_string());
    }

    let body = children[0];

    let mut arg_slots = Vec::new();
    for child in &children[1..args_end] {
        let slot_id = match arena.node(*child) {
            Some(ExprNode::SlotId(slot_id)) => *slot_id,
            _ => return Err("lambda arguments must be slot refs".to_string()),
        };
        arg_slots.push(slot_id);
    }

    let mut common_sub_exprs = Vec::with_capacity(common_num);
    for idx in 0..common_num {
        let slot_expr = children[args_end + idx];
        let slot_id = match arena.node(slot_expr) {
            Some(ExprNode::SlotId(slot_id)) => *slot_id,
            _ => return Err("lambda common sub expr slot ref must be slot".to_string()),
        };
        let expr_id = children[args_end + common_num + idx];
        common_sub_exprs.push((slot_id, expr_id));
    }

    let is_nondeterministic = node.is_nondeterministic.unwrap_or(false);

    Ok(arena.push_typed(
        ExprNode::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs,
            is_nondeterministic,
        },
        data_type,
    ))
}
