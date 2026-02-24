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
use crate::lower::expr::literals::{
    build_date_literal, build_decimal_literal, build_float_literal, build_int_literal,
    build_large_int_literal,
};

/// Lower literal expressions to ExprNode::Literal.
pub(crate) fn lower_literal(
    node: &exprs::TExprNode,
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    if matches!(data_type, DataType::LargeBinary)
        && node.node_type != exprs::TExprNodeType::NULL_LITERAL
    {
        return Err("VARIANT literal is not supported".to_string());
    }
    let id = match node.node_type {
        t if t == exprs::TExprNodeType::INT_LITERAL => {
            let v = node
                .int_literal
                .as_ref()
                .ok_or_else(|| "INT_LITERAL missing int_literal payload".to_string())?
                .value;
            let literal = build_int_literal(node, v)?;
            arena.push_typed(ExprNode::Literal(literal), data_type.clone())
        }
        t if t == exprs::TExprNodeType::DECIMAL_LITERAL => {
            let v = node
                .decimal_literal
                .as_ref()
                .ok_or_else(|| "DECIMAL_LITERAL missing decimal_literal payload".to_string())?
                .value
                .clone();
            let literal = build_decimal_literal(node, &v)?;
            arena.push_typed(ExprNode::Literal(literal), data_type.clone())
        }
        t if t == exprs::TExprNodeType::STRING_LITERAL => {
            let v = node
                .string_literal
                .as_ref()
                .ok_or_else(|| "STRING_LITERAL missing string_literal payload".to_string())?
                .value
                .clone();
            let literal = if matches!(data_type, DataType::Binary) {
                LiteralValue::Binary(v.into_bytes())
            } else {
                LiteralValue::Utf8(v)
            };
            arena.push_typed(ExprNode::Literal(literal), data_type.clone())
        }
        t if t == exprs::TExprNodeType::BINARY_LITERAL => {
            let v = node
                .binary_literal
                .as_ref()
                .ok_or_else(|| "BINARY_LITERAL missing binary_literal payload".to_string())?
                .value
                .clone();
            arena.push_typed(
                ExprNode::Literal(LiteralValue::Binary(v)),
                data_type.clone(),
            )
        }
        t if t == exprs::TExprNodeType::DATE_LITERAL => {
            let v = node
                .date_literal
                .as_ref()
                .ok_or_else(|| "DATE_LITERAL missing date_literal payload".to_string())?
                .value
                .clone();
            let literal = build_date_literal(node, &v)?;
            arena.push_typed(ExprNode::Literal(literal), data_type.clone())
        }
        t if t == exprs::TExprNodeType::BOOL_LITERAL => {
            let v = node
                .bool_literal
                .as_ref()
                .ok_or_else(|| "BOOL_LITERAL missing bool_literal payload".to_string())?
                .value;
            arena.push_typed(ExprNode::Literal(LiteralValue::Bool(v)), data_type.clone())
        }
        t if t == exprs::TExprNodeType::NULL_LITERAL => {
            arena.push_typed(ExprNode::Literal(LiteralValue::Null), data_type.clone())
        }
        t if t == exprs::TExprNodeType::FLOAT_LITERAL => {
            let v = node
                .float_literal
                .as_ref()
                .ok_or_else(|| "FLOAT_LITERAL missing float_literal payload".to_string())?
                .value;
            arena.push_typed(
                ExprNode::Literal(build_float_literal(node, v.0)),
                data_type.clone(),
            )
        }
        t if t == exprs::TExprNodeType::LARGE_INT_LITERAL => {
            let v = node
                .large_int_literal
                .as_ref()
                .ok_or_else(|| "LARGE_INT_LITERAL missing large_int_literal payload".to_string())?
                .value
                .clone();
            let literal = build_large_int_literal(node, &v)?;
            arena.push_typed(ExprNode::Literal(literal), data_type.clone())
        }
        _ => {
            return Err(format!(
                "lower_literal called for non-literal node type: {:?}",
                node.node_type
            ));
        }
    };
    Ok(id)
}
