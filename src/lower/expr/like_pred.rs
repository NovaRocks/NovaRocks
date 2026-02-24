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
use crate::exec::expr::{ExprArena, ExprId, ExprNode, function::FunctionKind};
use arrow::datatypes::DataType;

use crate::exprs;

/// Lower LIKE_PRED expression to ExprNode::FunctionCall(FunctionKind::Like).
pub(crate) fn lower_like_pred(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let _pred = node
        .like_pred
        .as_ref()
        .ok_or_else(|| "LIKE_PRED missing like_pred payload".to_string())?;
    if children.len() != 2 {
        return Err(format!(
            "LIKE_PRED expected 2 children, got {}",
            children.len()
        ));
    }
    if !matches!(data_type, DataType::Boolean) {
        return Err(format!(
            "LIKE_PRED must return BOOLEAN type, got {:?}",
            data_type
        ));
    }
    Ok(arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Like,
            args: children.to_vec(),
        },
        data_type,
    ))
}
