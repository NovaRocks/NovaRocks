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

//! Predicate expression lowering.

use arrow::datatypes::DataType;

use super::{lower_expr_list, lower_required_child};
use novarocks::exec::expr::function::FunctionKind;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks::protocol::FieldPath;
use novarocks_protocol::expr;
use novarocks_types::comparison_common_type;

use super::NativeExpressionInputLayout;

pub(crate) fn lower_is_null(
    is_null: &expr::IsNullExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    let child = lower_required_child(&is_null.operand, path.field("operand"), arena, input_layout)?;
    let node = if is_null.negated {
        ExprNode::IsNotNull(child)
    } else {
        ExprNode::IsNull(child)
    };
    Ok(arena.push_typed(node, data_type))
}

pub(crate) fn lower_in_list(
    in_list: &expr::InListExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    let mut child = lower_required_child(
        &in_list.operand,
        path.clone().field("operand"),
        arena,
        input_layout,
    )?;
    let mut values = lower_expr_list(
        &in_list.list,
        path.clone().field("list"),
        arena,
        input_layout,
    )?;
    if let Some(compare_type) = in_list_comparison_type(arena, child, &values)
        .map_err(|error| super::NativeExpressionDecodeError::inconsistent(path.clone(), error))?
    {
        child = cast_to_type_if_needed(arena, child, &compare_type).map_err(|error| {
            super::NativeExpressionDecodeError::inconsistent(path.clone().field("operand"), error)
        })?;
        for value in &mut values {
            *value = cast_to_type_if_needed(arena, *value, &compare_type).map_err(|error| {
                super::NativeExpressionDecodeError::inconsistent(path.clone().field("list"), error)
            })?;
        }
    }
    Ok(arena.push_typed(
        ExprNode::In {
            child,
            values,
            is_not_in: in_list.negated,
        },
        data_type,
    ))
}

fn in_list_comparison_type(
    arena: &ExprArena,
    child: ExprId,
    values: &[ExprId],
) -> Result<Option<DataType>, String> {
    let mut compare_type = arena
        .data_type(child)
        .cloned()
        .ok_or_else(|| "IN list operand missing data type".to_string())?;
    let mut changed = false;

    for value in values {
        let value_type = arena
            .data_type(*value)
            .ok_or_else(|| "IN list value missing data type".to_string())?;
        if value_type == &compare_type {
            continue;
        }
        let common_type = if is_string_type(&compare_type) && is_numeric_type(value_type) {
            compare_type.clone()
        } else if let Some(common_type) = comparison_common_type(&compare_type, value_type)? {
            common_type
        } else {
            return Ok(None);
        };
        changed |= common_type != compare_type || value_type != &common_type;
        compare_type = common_type;
    }

    Ok(changed.then_some(compare_type))
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn cast_to_type_if_needed(
    arena: &mut ExprArena,
    expr: ExprId,
    target_type: &DataType,
) -> Result<ExprId, String> {
    let source_type = arena
        .data_type(expr)
        .ok_or_else(|| "expression missing data type for implicit cast".to_string())?;
    if source_type == target_type {
        return Ok(expr);
    }
    Ok(arena.push_typed(ExprNode::Cast(expr), target_type.clone()))
}

pub(crate) fn lower_between(
    between: &expr::BetweenExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    if !matches!(data_type, DataType::Boolean) {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            format!("Between must return Boolean, got {data_type:?}"),
        ));
    }
    let operand = lower_required_child(
        &between.operand,
        path.clone().field("operand"),
        arena,
        input_layout,
    )?;
    let low = lower_required_child(&between.low, path.clone().field("low"), arena, input_layout)?;
    let high = lower_required_child(&between.high, path.field("high"), arena, input_layout)?;
    if between.negated {
        let lt_low = arena.push_typed(ExprNode::Lt(operand, low), DataType::Boolean);
        let gt_high = arena.push_typed(ExprNode::Gt(operand, high), DataType::Boolean);
        Ok(arena.push_typed(ExprNode::Or(lt_low, gt_high), data_type))
    } else {
        let ge_low = arena.push_typed(ExprNode::Ge(operand, low), DataType::Boolean);
        let le_high = arena.push_typed(ExprNode::Le(operand, high), DataType::Boolean);
        let in_range = arena.push_typed(ExprNode::And(ge_low, le_high), DataType::Boolean);
        Ok(in_range)
    }
}

pub(crate) fn lower_like(
    like: &expr::LikeExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    let operand = lower_required_child(
        &like.operand,
        path.clone().field("operand"),
        arena,
        input_layout,
    )?;
    let pattern = lower_required_child(&like.pattern, path.field("pattern"), arena, input_layout)?;
    let like_id = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Like,
            args: vec![operand, pattern],
        },
        DataType::Boolean,
    );
    if like.negated {
        Ok(arena.push_typed(ExprNode::Not(like_id), data_type))
    } else {
        Ok(like_id)
    }
}

pub(crate) fn lower_is_truth(
    is_truth: &expr::IsTruthExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    if !matches!(data_type, DataType::Boolean) {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            format!("IsTruth must return Boolean, got {data_type:?}"),
        ));
    }
    let child = lower_required_child(
        &is_truth.operand,
        path.field("operand"),
        arena,
        input_layout,
    )?;
    if is_truth.value && !is_truth.negated {
        Ok(child)
    } else {
        Ok(arena.push_typed(ExprNode::Not(child), DataType::Boolean))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        col, int_lit, lower_err_with_slots, lower_with_slots, make_i64_chunk, scalar_expr,
    };
    use arrow::array::{Array, BooleanArray};
    use arrow::datatypes::DataType;
    use novarocks::common::ids::SlotId;
    use novarocks::exec::expr::{ExprNode, LiteralValue};
    use novarocks_protocol::expr;

    #[test]
    fn in_list_casts_numeric_candidates_to_string_operand_type() {
        let in_list = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::InList(Box::new(expr::InListExpr {
                operand: Some(Box::new(col(1, DataType::Utf8))),
                list: vec![int_lit(1)],
                negated: false,
            })),
        );

        let (arena, id) = lower_with_slots(&in_list, &[1]);
        let Some(ExprNode::In { child, values, .. }) = arena.node(id) else {
            panic!("expected IN node");
        };
        assert_eq!(arena.data_type(*child), Some(&DataType::Utf8));
        assert_eq!(values.len(), 1);
        assert_eq!(arena.data_type(values[0]), Some(&DataType::Utf8));
        let Some(ExprNode::Cast(inner)) = arena.node(values[0]) else {
            panic!("expected numeric candidate cast to Utf8");
        };
        assert!(matches!(
            arena.node(*inner),
            Some(ExprNode::Literal(LiteralValue::Int64(1)))
        ));
    }

    #[test]
    fn not_between_lowers_to_or_of_lt_and_gt() {
        let between = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::Between(Box::new(expr::BetweenExpr {
                operand: Some(Box::new(col(1, DataType::Int64))),
                low: Some(Box::new(int_lit(10))),
                high: Some(Box::new(int_lit(20))),
                negated: true,
            })),
        );

        let (arena, id) = lower_with_slots(&between, &[1]);
        let Some(ExprNode::Or(left, right)) = arena.node(id) else {
            panic!("expected NOT BETWEEN to lower as OR");
        };
        assert!(matches!(arena.node(*left), Some(ExprNode::Lt(_, _))));
        assert!(matches!(arena.node(*right), Some(ExprNode::Gt(_, _))));
    }

    #[test]
    fn between_requires_boolean_result_type() {
        for negated in [false, true] {
            let between = scalar_expr(
                DataType::Int64,
                expr::expr::Kind::Between(Box::new(expr::BetweenExpr {
                    operand: Some(Box::new(col(1, DataType::Int64))),
                    low: Some(Box::new(int_lit(10))),
                    high: Some(Box::new(int_lit(20))),
                    negated,
                })),
            );

            let err = lower_err_with_slots(&between, &[1]);
            assert!(err.contains("Between must return Boolean"), "{err}");
        }
    }

    #[test]
    fn numeric_is_false_uses_not_path() {
        let is_false = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::IsTruth(Box::new(expr::IsTruthExpr {
                operand: Some(Box::new(col(1, DataType::Int64))),
                value: false,
                negated: false,
            })),
        );

        let (arena, id) = lower_with_slots(&is_false, &[1]);
        let Some(ExprNode::Not(child)) = arena.node(id) else {
            panic!("expected numeric IS FALSE to lower through NOT");
        };
        assert!(matches!(
            arena.node(*child),
            Some(ExprNode::SlotId(SlotId(1)))
        ));

        let chunk = make_i64_chunk(SlotId::new(1), vec![Some(0), Some(1)]);
        let out = arena.eval(id, &chunk).expect("eval");
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(!out.value(1));
    }
}
