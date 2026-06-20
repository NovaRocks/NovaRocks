#![allow(dead_code)] // Shared by rule migrations as TypedExpr callers move to ScalarId.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, LiteralValue};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey};

pub(crate) fn column_id(arena: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(id) if *id != ColumnId::UNSET => Some(*id),
        _ => None,
    }
}

pub(crate) fn collect_column_ids_strict(
    arena: &ScalarArena,
    expr: ScalarId,
) -> Option<HashSet<ColumnId>> {
    let mut out = HashSet::new();
    collect_column_ids_strict_inner(arena, expr, &mut out)?;
    Some(out)
}

fn collect_column_ids_strict_inner(
    arena: &ScalarArena,
    expr: ScalarId,
    out: &mut HashSet<ColumnId>,
) -> Option<()> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(id) => {
            if *id == ColumnId::UNSET {
                return None;
            }
            out.insert(*id);
        }
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_column_ids_strict_inner(arena, *left, out)?;
            collect_column_ids_strict_inner(arena, *right, out)?;
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => collect_column_ids_strict_inner(arena, *child, out)?,
        ScalarNode::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            collect_column_ids_strict_inner(arena, *body, out)?;
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
            for item in order_by {
                collect_column_ids_strict_inner(arena, item.expr, out)?;
            }
        }
        ScalarNode::InList { child, list, .. } => {
            collect_column_ids_strict_inner(arena, *child, out)?;
            for item in list {
                collect_column_ids_strict_inner(arena, *item, out)?;
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_column_ids_strict_inner(arena, *child, out)?;
            collect_column_ids_strict_inner(arena, *low, out)?;
            collect_column_ids_strict_inner(arena, *high, out)?;
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_column_ids_strict_inner(arena, *child, out)?;
            collect_column_ids_strict_inner(arena, *pattern, out)?;
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_column_ids_strict_inner(arena, *operand, out)?;
            }
            for (when, then) in when_then {
                collect_column_ids_strict_inner(arena, *when, out)?;
                collect_column_ids_strict_inner(arena, *then, out)?;
            }
            if let Some(else_expr) = else_expr {
                collect_column_ids_strict_inner(arena, *else_expr, out)?;
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
            for expr in partition_by {
                collect_column_ids_strict_inner(arena, *expr, out)?;
            }
            for item in order_by {
                collect_column_ids_strict_inner(arena, item.expr, out)?;
            }
        }
    }
    Some(())
}

pub(crate) fn split_conjuncts(arena: &ScalarArena, expr: ScalarId, out: &mut Vec<ScalarId>) {
    match arena.node(expr) {
        ScalarNode::BinaryOp {
            op: BinOp::And,
            left,
            right,
        } => {
            split_conjuncts(arena, *left, out);
            split_conjuncts(arena, *right, out);
        }
        _ => out.push(expr),
    }
}

pub(crate) fn combine_conjuncts(
    arena: &mut ScalarArena,
    mut exprs: Vec<ScalarId>,
) -> Option<ScalarId> {
    let mut result = exprs.pop()?;
    while let Some(next) = exprs.pop() {
        let nullable = arena.nullable(next) || arena.nullable(result);
        result = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left: next,
                right: result,
            },
            DataType::Boolean,
            nullable,
        );
    }
    Some(result)
}

pub(crate) fn bool_literal(arena: &mut ScalarArena, value: bool) -> ScalarId {
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(value))),
        DataType::Boolean,
        false,
    )
}

pub(crate) fn int_literal(arena: &mut ScalarArena, value: i64) -> ScalarId {
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
        DataType::Int64,
        false,
    )
}

pub(crate) fn is_literal_count_arg(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(
        arena.node(expr),
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(_)))
            | ScalarNode::Literal(HashableLiteral(LiteralValue::Null))
    )
}

pub(crate) fn contains_aggregate(arena: &ScalarArena, expr: ScalarId) -> bool {
    match arena.node(expr) {
        ScalarNode::AggregateCall { .. } => true,
        ScalarNode::BinaryOp { left, right, .. } => {
            contains_aggregate(arena, *left) || contains_aggregate(arena, *right)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => contains_aggregate(arena, *child),
        ScalarNode::FunctionCall { args, .. } => {
            args.iter().any(|arg| contains_aggregate(arena, *arg))
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            contains_aggregate(arena, *body)
        }
        ScalarNode::InList { child, list, .. } => {
            contains_aggregate(arena, *child)
                || list.iter().any(|item| contains_aggregate(arena, *item))
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            contains_aggregate(arena, *child)
                || contains_aggregate(arena, *low)
                || contains_aggregate(arena, *high)
        }
        ScalarNode::Like { child, pattern, .. } => {
            contains_aggregate(arena, *child) || contains_aggregate(arena, *pattern)
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.is_some_and(|expr| contains_aggregate(arena, expr))
                || when_then.iter().any(|(when, then)| {
                    contains_aggregate(arena, *when) || contains_aggregate(arena, *then)
                })
                || else_expr.is_some_and(|expr| contains_aggregate(arena, expr))
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(|arg| contains_aggregate(arena, *arg))
                || partition_by
                    .iter()
                    .any(|expr| contains_aggregate(arena, *expr))
                || order_by
                    .iter()
                    .any(|item| contains_aggregate(arena, item.expr))
        }
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            false
        }
    }
}

pub(crate) fn sort_key_column_id(arena: &ScalarArena, key: &SortKey) -> Option<ColumnId> {
    column_id(arena, key.expr)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::sql::analysis::BinOp;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

    use super::*;

    fn col(arena: &mut ScalarArena, id: u32, nullable: bool) -> ScalarId {
        arena.intern(
            ScalarNode::ColumnRef(ColumnId(id)),
            DataType::Int64,
            nullable,
        )
    }

    #[test]
    fn strict_column_collection_rejects_unset_column_ref() {
        let mut arena = ScalarArena::new();
        let expr = arena.intern(
            ScalarNode::ColumnRef(ColumnId::UNSET),
            DataType::Int64,
            true,
        );

        assert_eq!(collect_column_ids_strict(&arena, expr), None);
    }

    #[test]
    fn split_and_combine_conjuncts_round_trip_column_refs() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1, true);
        let b = col(&mut arena, 2, false);
        let and = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left: a,
                right: b,
            },
            DataType::Boolean,
            false,
        );

        let mut parts = Vec::new();
        split_conjuncts(&arena, and, &mut parts);
        assert_eq!(parts, vec![a, b]);

        let rebuilt = combine_conjuncts(&mut arena, parts).unwrap();
        assert!(matches!(
            arena.node(rebuilt),
            ScalarNode::BinaryOp { op: BinOp::And, .. }
        ));
        assert_eq!(arena.data_type(rebuilt), &DataType::Boolean);
        assert!(arena.nullable(rebuilt));
    }
}
