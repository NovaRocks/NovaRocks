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

//! Lambda expression lowering.

use super::{decode_type, lower_required_child};
use arrow::datatypes::DataType;
use novarocks::common::ids::SlotId;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks::protocol::FieldPath;
use novarocks_protocol::expr;

use super::NativeExpressionInputLayout;

pub(crate) fn lower_lambda(
    lambda: &expr::LambdaExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    let body = lower_required_child(
        &lambda.body,
        path.clone().field("body"),
        arena,
        input_layout,
    )?;
    let mut arg_slots = Vec::with_capacity(lambda.params.len());
    for (idx, param) in lambda.params.iter().enumerate() {
        let type_desc = param.r#type.as_ref().ok_or_else(|| {
            super::NativeExpressionDecodeError::missing(
                path.clone().field("params").index(idx).field("type"),
                "native Lambda parameter requires type",
            )
        })?;
        let _param_type = decode_type(type_desc).map_err(|error| {
            super::NativeExpressionDecodeError::invalid_value(
                path.clone().field("params").index(idx).field("type"),
                error,
            )
        })?;
        if param.slot_id <= 0 {
            return Err(super::NativeExpressionDecodeError::out_of_range(
                path.clone().field("params").index(idx).field("slot_id"),
                "Lambda parameter slot_id must be positive",
            ));
        }
        arg_slots.push(SlotId::try_from(param.slot_id).map_err(|error| {
            super::NativeExpressionDecodeError::out_of_range(
                path.clone().field("params").index(idx).field("slot_id"),
                error,
            )
        })?);
    }
    Ok(arena.push_typed(
        ExprNode::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs: Vec::new(),
            is_nondeterministic: false,
        },
        data_type,
    ))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{col, lower, lower_with_slots, scalar_expr, type_desc};
    use arrow::datatypes::{DataType, Field};
    use novarocks::common::ids::SlotId;
    use novarocks::exec::expr::{ExprNode, function::FunctionKind};
    use novarocks_protocol::expr;
    use std::sync::Arc;

    #[test]
    fn lowers_lambda_expr_to_lambda_function() {
        let lambda_slot = 1_900_000_000;
        let item_type = DataType::Int64;
        let array_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
        let lambda_param = scalar_expr(
            item_type.clone(),
            expr::expr::Kind::LambdaParamRef(expr::LambdaParamRef {
                slot_id: lambda_slot,
                name: Some("x".to_string()),
            }),
        );
        let body = scalar_expr(
            item_type.clone(),
            expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Add as i32,
                left: Some(Box::new(lambda_param)),
                right: Some(Box::new(col(7, item_type.clone()))),
            })),
        );
        let lambda = scalar_expr(
            item_type.clone(),
            expr::expr::Kind::Lambda(Box::new(expr::LambdaExpr {
                params: vec![expr::LambdaParam {
                    slot_id: lambda_slot,
                    name: Some("x".to_string()),
                    r#type: Some(type_desc(&item_type)),
                    nullable: true,
                }],
                body: Some(Box::new(body)),
            })),
        );
        let call = scalar_expr(
            array_type.clone(),
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                function_name: "array_map".to_string(),
                args: vec![lambda, col(1, array_type)],
                distinct: false,
            }),
        );

        let (arena, id) = lower_with_slots(&call, &[1, 7]);
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(id) else {
            panic!("expected array_map function call");
        };
        assert_eq!(*kind, FunctionKind::ArrayMap);
        assert_eq!(args.len(), 2);
        let Some(ExprNode::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs,
            is_nondeterministic,
        }) = arena.node(args[0])
        else {
            panic!("expected lowered lambda function");
        };
        assert_eq!(arg_slots, &[SlotId::new(lambda_slot as u32)]);
        assert!(common_sub_exprs.is_empty());
        assert!(!is_nondeterministic);
        let Some(ExprNode::Add(left, right)) = arena.node(*body) else {
            panic!("expected lambda body to keep captured-column add");
        };
        assert!(matches!(
            arena.node(*left),
            Some(ExprNode::SlotId(slot)) if *slot == SlotId::new(lambda_slot as u32)
        ));
        assert!(matches!(
            arena.node(*right),
            Some(ExprNode::SlotId(slot)) if *slot == SlotId::new(7)
        ));
    }

    #[test]
    fn lambda_expr_lowers_to_lambda_function() {
        let lambda = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::Lambda(Box::new(expr::LambdaExpr {
                params: vec![expr::LambdaParam {
                    slot_id: 3,
                    name: Some("x".to_string()),
                    r#type: Some(type_desc(&DataType::Int64)),
                    nullable: true,
                }],
                body: Some(Box::new(scalar_expr(
                    DataType::Int64,
                    expr::expr::Kind::LambdaParamRef(expr::LambdaParamRef {
                        slot_id: 3,
                        name: Some("x".to_string()),
                    }),
                ))),
            })),
        );

        let (arena, id) = lower(&lambda);
        let Some(ExprNode::LambdaFunction {
            arg_slots,
            common_sub_exprs,
            is_nondeterministic,
            ..
        }) = arena.node(id)
        else {
            panic!("expected LambdaFunction");
        };
        assert_eq!(arg_slots, &vec![SlotId::new(3)]);
        assert!(common_sub_exprs.is_empty());
        assert!(!is_nondeterministic);
    }
}
