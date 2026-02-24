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
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::datatypes::DataType;

use crate::exprs;

/// Lower INFO_FUNC expression to ExprNode::Literal.
pub(crate) fn lower_info_func(
    node: &exprs::TExprNode,
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let info = node
        .info_func
        .as_ref()
        .ok_or_else(|| "INFO_FUNC missing info_func payload".to_string())?;
    let id = if !info.str_value.is_empty() {
        arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8(info.str_value.clone())),
            data_type.clone(),
        )
    } else {
        arena.push_typed(
            ExprNode::Literal(LiteralValue::Int64(info.int_value)),
            data_type.clone(),
        )
    };
    Ok(id)
}
