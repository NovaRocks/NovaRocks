use std::collections::HashSet;

use crate::sql::ir::{BinOp, ExprKind, TypedExpr};
use crate::sql::plan::*;

/// Split an expression on AND into a flat list of conjuncts.
pub(super) fn split_and(expr: TypedExpr) -> Vec<TypedExpr> {
    let mut out = Vec::new();
    split_and_inner(expr, &mut out);
    out
}

fn split_and_inner(expr: TypedExpr, out: &mut Vec<TypedExpr>) {
    match expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            split_and_inner(*left, out);
            split_and_inner(*right, out);
        }
        _ => out.push(expr),
    }
}

/// Combine a list of conjuncts back into a single AND expression.
/// Panics if `exprs` is empty.
pub(super) fn combine_and(mut exprs: Vec<TypedExpr>) -> TypedExpr {
    assert!(!exprs.is_empty());
    let mut result = exprs.pop().unwrap();
    while let Some(left) = exprs.pop() {
        result = TypedExpr {
            data_type: arrow::datatypes::DataType::Boolean,
            nullable: left.nullable || result.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(result),
            },
        };
    }
    result
}

/// Collect all column names referenced in an expression (unqualified, lowercase).
pub(super) fn collect_column_refs(expr: &TypedExpr) -> Vec<&str> {
    let mut out = Vec::new();
    collect_column_refs_inner(expr, &mut out);
    out
}

fn collect_column_refs_inner<'a>(expr: &'a TypedExpr, out: &mut Vec<&'a str>) {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => out.push(column.as_str()),
        ExprKind::BinaryOp { left, right, .. } => {
            collect_column_refs_inner(left, out);
            collect_column_refs_inner(right, out);
        }
        ExprKind::UnaryOp { expr, .. } => collect_column_refs_inner(expr, out),
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_refs_inner(arg, out);
            }
        }
        ExprKind::AggregateCall { args, .. } => {
            for arg in args {
                collect_column_refs_inner(arg, out);
            }
        }
        ExprKind::Cast { expr, .. } => collect_column_refs_inner(expr, out),
        ExprKind::IsNull { expr, .. } => collect_column_refs_inner(expr, out),
        ExprKind::InList { expr, list, .. } => {
            collect_column_refs_inner(expr, out);
            for item in list {
                collect_column_refs_inner(item, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_column_refs_inner(expr, out);
            collect_column_refs_inner(low, out);
            collect_column_refs_inner(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_column_refs_inner(expr, out);
            collect_column_refs_inner(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_column_refs_inner(operand, out);
            }
            for (when, then) in when_then {
                collect_column_refs_inner(when, out);
                collect_column_refs_inner(then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_column_refs_inner(else_expr, out);
            }
        }
        ExprKind::IsTruthValue { expr, .. } => collect_column_refs_inner(expr, out),
        ExprKind::Nested(inner) => collect_column_refs_inner(inner, out),
        ExprKind::Literal(_) => {}
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_refs_inner(arg, out);
            }
            for pb in partition_by {
                collect_column_refs_inner(pb, out);
            }
            for ob in order_by {
                collect_column_refs_inner(&ob.expr, out);
            }
        }
    }
}

/// Merge a parent's needed columns with additional column names.
pub(super) fn merge_needed(parent: Option<&HashSet<String>>, extra: &[&str]) -> HashSet<String> {
    let mut result = parent.cloned().unwrap_or_default();
    for col in extra {
        result.insert(col.to_lowercase());
    }
    result
}

/// Wrap a plan in a Filter if there are remaining (un-pushed) predicates.
pub(super) fn wrap_remaining_filter(plan: LogicalPlan, remaining: Vec<TypedExpr>) -> LogicalPlan {
    if remaining.is_empty() {
        plan
    } else {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(plan),
            predicate: combine_and(remaining),
        })
    }
}
