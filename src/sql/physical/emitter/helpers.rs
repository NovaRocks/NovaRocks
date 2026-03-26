use crate::plan_nodes;
use crate::sql::ir::{self as query_ir, BinOp, ExprKind, TypedExpr};
use crate::sql::plan::AggregateCall;

/// Split a TypedExpr on AND into a flat list of conjuncts.
pub(super) fn split_and_conjuncts_typed(expr: &TypedExpr) -> Vec<&TypedExpr> {
    let mut result = Vec::new();
    collect_and_conjuncts_typed(expr, &mut result);
    result
}

pub(super) fn collect_and_conjuncts_typed<'a>(expr: &'a TypedExpr, out: &mut Vec<&'a TypedExpr>) {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_and_conjuncts_typed(left, out);
            collect_and_conjuncts_typed(right, out);
        }
        _ => {
            out.push(expr);
        }
    }
}

/// Display name for a TypedExpr (used as scope key for group_by columns).
/// Must be deterministic — same expression always produces the same name.
pub(crate) fn typed_expr_display_name(expr: &TypedExpr) -> String {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier: Some(_),
            column,
        } => column.clone(),
        ExprKind::ColumnRef {
            qualifier: None,
            column,
        } => column.clone(),
        ExprKind::Literal(lit) => format!("{:?}", lit),
        ExprKind::FunctionCall { name, args, .. } => {
            if args.is_empty() {
                format!("{}()", name)
            } else {
                let arg_names: Vec<String> = args.iter().map(typed_expr_display_name).collect();
                format!("{}({})", name, arg_names.join(", "))
            }
        }
        ExprKind::AggregateCall { name, .. } => name.clone(),
        ExprKind::Cast { expr: inner, target } => {
            format!("cast({} as {:?})", typed_expr_display_name(inner), target)
        }
        ExprKind::BinaryOp { left, op, right } => {
            format!(
                "({} {:?} {})",
                typed_expr_display_name(left),
                op,
                typed_expr_display_name(right)
            )
        }
        _ => format!("{:?}", expr.kind),
    }
}

/// Display name for an AggregateCall.
/// Build aggregate display name from components (used by expr_compiler for scope lookup).
pub(crate) fn agg_call_display_name_from_parts(
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
) -> String {
    if args.is_empty() {
        format!("{}(*)", name)
    } else {
        let arg_names: Vec<String> = args.iter().map(typed_expr_display_name).collect();
        if distinct {
            format!("{}(distinct {})", name, arg_names.join(", "))
        } else {
            format!("{}({})", name, arg_names.join(", "))
        }
    }
}

pub(super) fn agg_call_display_name(call: &AggregateCall) -> String {
    if call.args.is_empty() {
        format!("{}(*)", call.name)
    } else {
        let arg_names: Vec<String> = call.args.iter().map(typed_expr_display_name).collect();
        if call.distinct {
            format!("{}(distinct {})", call.name, arg_names.join(", "))
        } else {
            format!("{}({})", call.name, arg_names.join(", "))
        }
    }
}

/// Map JoinKind to TJoinOp.
pub(super) fn join_kind_to_op(kind: query_ir::JoinKind) -> plan_nodes::TJoinOp {
    match kind {
        query_ir::JoinKind::Inner => plan_nodes::TJoinOp::INNER_JOIN,
        query_ir::JoinKind::LeftOuter => plan_nodes::TJoinOp::LEFT_OUTER_JOIN,
        query_ir::JoinKind::RightOuter => plan_nodes::TJoinOp::RIGHT_OUTER_JOIN,
        query_ir::JoinKind::FullOuter => plan_nodes::TJoinOp::FULL_OUTER_JOIN,
        query_ir::JoinKind::Cross => plan_nodes::TJoinOp::CROSS_JOIN,
        query_ir::JoinKind::LeftSemi => plan_nodes::TJoinOp::LEFT_SEMI_JOIN,
        query_ir::JoinKind::RightSemi => plan_nodes::TJoinOp::RIGHT_SEMI_JOIN,
        query_ir::JoinKind::LeftAnti => plan_nodes::TJoinOp::LEFT_ANTI_JOIN,
        query_ir::JoinKind::RightAnti => plan_nodes::TJoinOp::RIGHT_ANTI_JOIN,
    }
}
