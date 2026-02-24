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

pub(crate) fn lower_array_expr(
    children: Vec<ExprId>,
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    match data_type {
        DataType::List(_) => {
            Ok(arena.push_typed(ExprNode::ArrayExpr { elements: children }, data_type))
        }
        other => Err(format!("ARRAY_EXPR expects List type, got {:?}", other)),
    }
}
