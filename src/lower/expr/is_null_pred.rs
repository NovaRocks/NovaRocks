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

/// Lower IS_NULL_PRED expression to ExprNode::IsNull / ExprNode::IsNotNull.
pub(crate) fn lower_is_null_pred(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let pred = node
        .is_null_pred
        .as_ref()
        .ok_or_else(|| "IS_NULL_PRED missing is_null_pred payload".to_string())?;
    if children.len() != 1 {
        return Err(format!(
            "IS_NULL_PRED expected 1 child, got {}",
            children.len()
        ));
    }
    if !matches!(data_type, DataType::Boolean) {
        return Err(format!(
            "IS_NULL_PRED must return BOOLEAN type, got {:?}",
            data_type
        ));
    }

    let child = children[0];
    let node = if pred.is_not_null {
        ExprNode::IsNotNull(child)
    } else {
        ExprNode::IsNull(child)
    };
    Ok(arena.push_typed(node, data_type))
}
