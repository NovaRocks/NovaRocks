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
use crate::lower::type_lowering::{arrow_type_from_desc, arrow_type_from_primitive};
use crate::opcodes;
use crate::types;

fn common_decimal_compare_type(left: &DataType, right: &DataType) -> Result<DataType, String> {
    let (lp, ls, left_is_256) = match left {
        DataType::Decimal128(p, s) => (*p, *s, false),
        DataType::Decimal256(p, s) => (*p, *s, true),
        _ => {
            return Err(format!(
                "BINARY_PRED decimal child_type requires decimal children (left={:?}, right={:?})",
                left, right
            ));
        }
    };
    let (rp, rs, right_is_256) = match right {
        DataType::Decimal128(p, s) => (*p, *s, false),
        DataType::Decimal256(p, s) => (*p, *s, true),
        _ => {
            return Err(format!(
                "BINARY_PRED decimal child_type requires decimal children (left={:?}, right={:?})",
                left, right
            ));
        }
    };

    let target_scale: i8 = ls.max(rs);
    let lhs_int_digits: i16 = (lp as i16) - (ls as i16);
    let rhs_int_digits: i16 = (rp as i16) - (rs as i16);
    let int_digits: i16 = lhs_int_digits.max(rhs_int_digits).max(0);
    let target_precision: i16 = int_digits + (target_scale as i16);
    if target_precision <= 0 {
        return Err(format!(
            "BINARY_PRED invalid decimal precision (left={:?}, right={:?})",
            left, right
        ));
    }
    let target_precision_u8 = target_precision as u8;
    let need_decimal256 = left_is_256 || right_is_256 || target_precision > 38;
    if need_decimal256 {
        if target_precision > 76 {
            return Err(format!(
                "BINARY_PRED decimal precision overflow (left={:?}, right={:?}, target=Decimal256({}, {}))",
                left, right, target_precision, target_scale
            ));
        }
        return Ok(DataType::Decimal256(target_precision_u8, target_scale));
    }
    Ok(DataType::Decimal128(target_precision_u8, target_scale))
}

/// Lower BINARY_PRED expression to comparison ExprNode.
pub(crate) fn lower_binary_pred(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let opcode = node
        .opcode
        .ok_or_else(|| "BINARY_PRED missing opcode".to_string())?;
    if children.len() != 2 {
        return Err(format!(
            "BINARY_PRED expected 2 children, got {}",
            children.len()
        ));
    }
    let mut left = children[0];
    let mut right = children[1];

    // Align with StarRocks BE execution path: BINARY_PRED carries a child comparable type decided
    // during FE analysis (`child_type_desc` preferred, fallback to `child_type`). BE executes by
    // dispatching comparison kernels with that single logical type. We mirror this by inserting
    // explicit Cast nodes here when the plan provides that type.
    let compare_type = if let Some(desc) = node.child_type_desc.as_ref() {
        arrow_type_from_desc(desc).ok_or_else(|| {
            format!(
                "BINARY_PRED unsupported child_type_desc from FE plan: {:?}",
                desc
            )
        })?
    } else if let Some(primitive) = node.child_type {
        if let Some(compare_type) = arrow_type_from_primitive(primitive) {
            compare_type
        } else if primitive == types::TPrimitiveType::DECIMAL32
            || primitive == types::TPrimitiveType::DECIMAL64
            || primitive == types::TPrimitiveType::DECIMAL128
            || primitive == types::TPrimitiveType::DECIMAL256
            || primitive == types::TPrimitiveType::DECIMAL
            || primitive == types::TPrimitiveType::DECIMALV2
        {
            let left_type = arena
                .data_type(left)
                .ok_or_else(|| "BINARY_PRED left child type missing".to_string())?;
            let right_type = arena
                .data_type(right)
                .ok_or_else(|| "BINARY_PRED right child type missing".to_string())?;
            common_decimal_compare_type(left_type, right_type)?
        } else {
            return Err(format!(
                "BINARY_PRED unsupported child_type from FE plan: {:?}",
                primitive
            ));
        }
    } else {
        return Err("BINARY_PRED missing child_type_desc/child_type from FE plan".to_string());
    };

    if !matches!(compare_type, DataType::LargeBinary) {
        for child in [&mut left, &mut right] {
            let child_expr = *child;
            if let Some(child_type) = arena.data_type(child_expr) {
                if child_type != &compare_type {
                    let cast_expr =
                        arena.push_typed(ExprNode::Cast(child_expr), compare_type.clone());
                    *child = cast_expr;
                }
            }
        }
    }

    for child in [left, right] {
        if let Some(dt) = arena.data_type(child) {
            if matches!(dt, DataType::LargeBinary) {
                return Err("VARIANT is not supported in comparison predicates".to_string());
            }
        }
    }

    let id = match opcode {
        o if o == opcodes::TExprOpcode::EQ => {
            arena.push_typed(ExprNode::Eq(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::EQ_FOR_NULL => {
            arena.push_typed(ExprNode::EqForNull(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::NE => {
            arena.push_typed(ExprNode::Ne(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::LT => {
            arena.push_typed(ExprNode::Lt(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::LE => {
            arena.push_typed(ExprNode::Le(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::GT => {
            arena.push_typed(ExprNode::Gt(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::GE => {
            arena.push_typed(ExprNode::Ge(left, right), data_type.clone())
        }
        o if o == opcodes::TExprOpcode::EQ_FOR_NULL => {
            let left_is_null = arena.push_typed(ExprNode::IsNull(left), DataType::Boolean);
            let right_is_null = arena.push_typed(ExprNode::IsNull(right), DataType::Boolean);
            let both_null = arena.push_typed(
                ExprNode::And(left_is_null, right_is_null),
                DataType::Boolean,
            );

            let left_not_null = arena.push_typed(ExprNode::IsNotNull(left), DataType::Boolean);
            let right_not_null = arena.push_typed(ExprNode::IsNotNull(right), DataType::Boolean);
            let both_not_null = arena.push_typed(
                ExprNode::And(left_not_null, right_not_null),
                DataType::Boolean,
            );
            let eq_node = arena.push_typed(ExprNode::Eq(left, right), DataType::Boolean);
            let non_null_equal =
                arena.push_typed(ExprNode::And(both_not_null, eq_node), DataType::Boolean);

            arena.push_typed(ExprNode::Or(both_null, non_null_equal), data_type.clone())
        }
        _ => return Err(format!("unsupported BINARY_PRED opcode: {:?}", opcode)),
    };
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::expr::LiteralValue;
    fn create_dummy_type() -> types::TTypeDesc {
        types::TTypeDesc {
            types: Some(vec![types::TTypeNode {
                type_: types::TTypeNodeType::SCALAR,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn default_t_expr_node() -> exprs::TExprNode {
        exprs::TExprNode {
            node_type: exprs::TExprNodeType::INT_LITERAL,
            type_: create_dummy_type(),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    #[test]
    fn lower_binary_pred_casts_children_to_plan_child_type() {
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let right = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);

        let node = exprs::TExprNode {
            node_type: exprs::TExprNodeType::BINARY_PRED,
            opcode: Some(opcodes::TExprOpcode::EQ),
            num_children: 2,
            child_type: Some(types::TPrimitiveType::DOUBLE),
            ..default_t_expr_node()
        };

        let pred_id =
            lower_binary_pred(&node, &[left, right], &mut arena, DataType::Boolean).expect("lower");
        let pred = arena.node(pred_id).expect("predicate node");
        let (cast_left, cast_right) = match pred {
            ExprNode::Eq(l, r) => (*l, *r),
            other => panic!("unexpected predicate node: {other:?}"),
        };

        assert_eq!(arena.data_type(cast_left), Some(&DataType::Float64));
        assert_eq!(arena.data_type(cast_right), Some(&DataType::Float64));

        match arena.node(cast_left).expect("left cast") {
            ExprNode::Cast(id) => assert_eq!(*id, left),
            other => panic!("left child should be cast, got {other:?}"),
        }
        match arena.node(cast_right).expect("right cast") {
            ExprNode::Cast(id) => assert_eq!(*id, right),
            other => panic!("right child should be cast, got {other:?}"),
        }
    }

    #[test]
    fn lower_binary_pred_eq_for_null_maps_to_expr_node() {
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let right = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);

        let node = exprs::TExprNode {
            node_type: exprs::TExprNodeType::BINARY_PRED,
            opcode: Some(opcodes::TExprOpcode::EQ_FOR_NULL),
            num_children: 2,
            child_type: Some(types::TPrimitiveType::BIGINT),
            ..default_t_expr_node()
        };

        let pred_id =
            lower_binary_pred(&node, &[left, right], &mut arena, DataType::Boolean).expect("lower");
        match arena.node(pred_id).expect("predicate node") {
            ExprNode::EqForNull(l, r) => {
                assert_eq!(*l, left);
                assert_eq!(*r, right);
            }
            other => panic!("unexpected predicate node: {other:?}"),
        }
    }

    #[test]
    fn lower_binary_pred_rejects_missing_child_type_when_children_match() {
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let right = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);

        let node = exprs::TExprNode {
            node_type: exprs::TExprNodeType::BINARY_PRED,
            opcode: Some(opcodes::TExprOpcode::LE),
            num_children: 2,
            child_type: None,
            child_type_desc: None,
            ..default_t_expr_node()
        };

        let err = lower_binary_pred(&node, &[left, right], &mut arena, DataType::Boolean)
            .expect_err("missing child comparable type should fail");
        assert!(err.contains("missing child_type_desc/child_type"));
    }

    #[test]
    fn lower_binary_pred_rejects_missing_child_type_for_mixed_children() {
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let right = arena.push_typed(
            ExprNode::Literal(LiteralValue::Float64(1.0)),
            DataType::Float64,
        );

        let node = exprs::TExprNode {
            node_type: exprs::TExprNodeType::BINARY_PRED,
            opcode: Some(opcodes::TExprOpcode::EQ),
            num_children: 2,
            child_type: None,
            child_type_desc: None,
            ..default_t_expr_node()
        };

        let err = lower_binary_pred(&node, &[left, right], &mut arena, DataType::Boolean)
            .expect_err("missing child comparable type should fail");
        assert!(err.contains("missing child_type_desc/child_type"));
    }

    #[test]
    fn lower_binary_pred_decimal_child_type_uses_child_decimal_type() {
        let mut arena = ExprArena::default();
        let left = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 100,
                precision: 15,
                scale: 2,
            }),
            DataType::Decimal128(15, 2),
        );
        let right = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 200,
                precision: 15,
                scale: 2,
            }),
            DataType::Decimal128(15, 2),
        );

        let node = exprs::TExprNode {
            node_type: exprs::TExprNodeType::BINARY_PRED,
            opcode: Some(opcodes::TExprOpcode::LE),
            num_children: 2,
            child_type: Some(types::TPrimitiveType::DECIMAL64),
            child_type_desc: None,
            ..default_t_expr_node()
        };

        let pred_id =
            lower_binary_pred(&node, &[left, right], &mut arena, DataType::Boolean).expect("lower");
        match arena.node(pred_id).expect("predicate node") {
            ExprNode::Le(l, r) => {
                assert_eq!(*l, left);
                assert_eq!(*r, right);
            }
            other => panic!("unexpected predicate node: {other:?}"),
        }
    }

    #[test]
    fn lower_binary_pred_decimal_child_type_casts_to_common_precision() {
        let mut arena = ExprArena::default();
        let left = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 12345,
                precision: 7,
                scale: 2,
            }),
            DataType::Decimal128(7, 2),
        );
        let right = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 999,
                precision: 5,
                scale: 2,
            }),
            DataType::Decimal128(5, 2),
        );

        let node = exprs::TExprNode {
            node_type: exprs::TExprNodeType::BINARY_PRED,
            opcode: Some(opcodes::TExprOpcode::GT),
            num_children: 2,
            child_type: Some(types::TPrimitiveType::DECIMAL64),
            child_type_desc: None,
            ..default_t_expr_node()
        };

        let pred_id =
            lower_binary_pred(&node, &[left, right], &mut arena, DataType::Boolean).expect("lower");
        let (cast_left, cast_right) = match arena.node(pred_id).expect("predicate node") {
            ExprNode::Gt(l, r) => (*l, *r),
            other => panic!("unexpected predicate node: {other:?}"),
        };

        assert_eq!(
            arena.data_type(cast_left),
            Some(&DataType::Decimal128(7, 2))
        );
        assert_eq!(
            arena.data_type(cast_right),
            Some(&DataType::Decimal128(7, 2))
        );
        assert_eq!(cast_left, left);
        match arena.node(cast_right).expect("right child") {
            ExprNode::Cast(id) => assert_eq!(*id, right),
            other => panic!("right child should be cast, got {other:?}"),
        }
    }

    #[test]
    fn lower_binary_pred_supports_eq_for_null() {
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let right = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);

        let node = exprs::TExprNode {
            node_type: exprs::TExprNodeType::BINARY_PRED,
            opcode: Some(opcodes::TExprOpcode::EQ_FOR_NULL),
            num_children: 2,
            child_type: Some(types::TPrimitiveType::BIGINT),
            child_type_desc: None,
            ..default_t_expr_node()
        };

        let pred_id =
            lower_binary_pred(&node, &[left, right], &mut arena, DataType::Boolean).expect("lower");
        assert_eq!(arena.data_type(pred_id), Some(&DataType::Boolean));
    }
}
