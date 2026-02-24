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
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue, function::FunctionKind};
use arrow::datatypes::DataType;

use crate::exprs;

/// Lower SUBFIELD_EXPR to chained internal struct `subfield` function calls.
pub(crate) fn lower_subfield_expr(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    if children.len() != 1 {
        return Err(format!(
            "SUBFIELD_EXPR expected 1 child, got {}",
            children.len()
        ));
    }
    let subfield_names = node
        .used_subfield_names
        .as_ref()
        .ok_or_else(|| "SUBFIELD_EXPR missing used_subfield_names payload".to_string())?;
    if subfield_names.is_empty() {
        return Err("SUBFIELD_EXPR has empty used_subfield_names".to_string());
    }

    let mut current = children[0];
    for (i, field_name) in subfield_names.iter().enumerate() {
        let current_type = arena
            .data_type(current)
            .ok_or_else(|| "SUBFIELD_EXPR missing intermediate child type".to_string())?;
        let DataType::Struct(fields) = current_type else {
            return Err(format!(
                "SUBFIELD_EXPR expects STRUCT input at level {}, got {:?}",
                i, current_type
            ));
        };
        let field = fields
            .iter()
            .find(|f| f.name() == field_name)
            .ok_or_else(|| format!("SUBFIELD_EXPR field '{}' does not exist", field_name))?;

        let expected_type = if i + 1 == subfield_names.len() {
            if field.data_type() != &data_type {
                return Err(format!(
                    "SUBFIELD_EXPR return type mismatch: expected {:?}, got {:?}",
                    field.data_type(),
                    data_type
                ));
            }
            data_type.clone()
        } else {
            if !matches!(field.data_type(), DataType::Struct(_)) {
                return Err(format!(
                    "SUBFIELD_EXPR intermediate field '{}' is not STRUCT: {:?}",
                    field_name,
                    field.data_type()
                ));
            }
            field.data_type().clone()
        };

        let field_name_expr = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8(field_name.clone())),
            DataType::Utf8,
        );
        current = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::StructFn("subfield"),
                args: vec![current, field_name_expr],
            },
            expected_type,
        );
    }

    Ok(current)
}
