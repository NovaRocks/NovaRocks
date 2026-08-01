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

//! Binary operation expression lowering.

use arrow::datatypes::DataType;

use super::lower_required_child;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks::protocol::FieldPath;
use novarocks_protocol::expr;

use super::NativeExpressionInputLayout;

pub(crate) fn lower_binary_op(
    binary: &expr::BinaryOpExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    let op = expr::BinaryOp::try_from(binary.op).map_err(|_| {
        super::NativeExpressionDecodeError::invalid_enum(
            path.clone().field("op"),
            format!("unknown BinaryOp {}", binary.op),
        )
    })?;
    let left = lower_required_child(
        &binary.left,
        path.clone().field("left"),
        arena,
        input_layout,
    )?;
    let right = lower_required_child(
        &binary.right,
        path.clone().field("right"),
        arena,
        input_layout,
    )?;
    let node = match op {
        expr::BinaryOp::Unspecified => {
            return Err(super::NativeExpressionDecodeError::invalid_enum(
                path.field("op"),
                "BinaryOp.op is unspecified",
            ));
        }
        expr::BinaryOp::Add => ExprNode::Add(left, right),
        expr::BinaryOp::Sub => ExprNode::Sub(left, right),
        expr::BinaryOp::Mul => ExprNode::Mul(left, right),
        expr::BinaryOp::Div => ExprNode::Div(left, right),
        expr::BinaryOp::Mod => ExprNode::Mod(left, right),
        expr::BinaryOp::Eq => ExprNode::Eq(left, right),
        expr::BinaryOp::Ne => ExprNode::Ne(left, right),
        expr::BinaryOp::Lt => ExprNode::Lt(left, right),
        expr::BinaryOp::Le => ExprNode::Le(left, right),
        expr::BinaryOp::Gt => ExprNode::Gt(left, right),
        expr::BinaryOp::Ge => ExprNode::Ge(left, right),
        expr::BinaryOp::EqForNull => ExprNode::EqForNull(left, right),
        expr::BinaryOp::And => ExprNode::And(left, right),
        expr::BinaryOp::Or => ExprNode::Or(left, right),
    };
    Ok(arena.push_typed(node, data_type))
}
