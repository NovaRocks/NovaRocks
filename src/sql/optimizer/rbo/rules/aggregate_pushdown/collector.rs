//! Aggregate pushdown collector — phase 1 of the rule.

use std::collections::HashMap;

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::{AggregateNode, LogicalPlan};

use super::context::{AggregatePushDownContext, PushPlan};

/// Examine the AggregateNode for entry-level rejections.
/// Returns Some(ctx) when the aggregate is a candidate to push;
/// returns None when an entry-level filter rejects it.
pub(crate) fn entry_safety_check(aggregate: &AggregateNode) -> Option<AggregatePushDownContext> {
    // Idempotency guard.
    if aggregate.already_pushed {
        return None;
    }
    // Empty group-by: partial collapses to a single row.
    if aggregate.group_by.is_empty() {
        return None;
    }
    // Per-call filters.
    for call in &aggregate.aggregates {
        // Distinct is SplitDistinctAgg's domain.
        if call.distinct {
            return None;
        }
        // Order-sensitive aggregate.
        if !call.order_by.is_empty() {
            return None;
        }
        // White-list check.
        let name = call.name.to_ascii_lowercase();
        if !matches!(name.as_str(), "sum" | "min" | "max" | "count") {
            return None;
        }
        // COUNT(*) has no args.
        if name == "count" && call.args.is_empty() {
            return None;
        }
        // Args must be bare ColumnRefs.
        for arg in &call.args {
            if !matches!(arg.kind, ExprKind::ColumnRef { .. }) {
                return None;
            }
            // Non-deterministic functions in args.
            if expr_uses_nondeterministic(arg) {
                return None;
            }
        }
    }

    Some(AggregatePushDownContext {
        original_groupby: aggregate.group_by.clone(),
        original_aggregates: aggregate.aggregates.clone(),
        required_columns: collect_required_columns(aggregate),
    })
}

fn collect_required_columns(aggregate: &AggregateNode) -> Vec<String> {
    let mut out = Vec::new();
    for gb in &aggregate.group_by {
        collect_column_refs_into(gb, &mut out);
    }
    for call in &aggregate.aggregates {
        for arg in &call.args {
            collect_column_refs_into(arg, &mut out);
        }
    }
    out.sort();
    out.dedup();
    out
}

fn collect_column_refs_into(expr: &TypedExpr, out: &mut Vec<String>) {
    if let ExprKind::ColumnRef { column, .. } = &expr.kind {
        out.push(column.clone());
    }
}

const NONDETERMINISTIC_FUNCTIONS: &[&str] = &[
    "rand",
    "random",
    "uuid",
    "now",
    "current_timestamp",
    "current_date",
];

fn expr_uses_nondeterministic(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            if NONDETERMINISTIC_FUNCTIONS
                .iter()
                .any(|n| n.eq_ignore_ascii_case(name))
            {
                return true;
            }
            args.iter().any(expr_uses_nondeterministic)
        }
        ExprKind::BinaryOp { left, right, .. } => {
            expr_uses_nondeterministic(left) || expr_uses_nondeterministic(right)
        }
        ExprKind::UnaryOp { expr: inner, .. } => expr_uses_nondeterministic(inner),
        _ => false,
    }
}

/// Top-level collector entry. To be wired in subsequent tasks (5-6).
#[allow(dead_code)]
pub(crate) fn collect_push_plan(
    aggregate: &AggregateNode,
    _table_stats: &HashMap<String, TableStatistics>,
) -> Option<PushPlan> {
    let _ctx = entry_safety_check(aggregate)?;
    // Traversal added in Tasks 5 and 6.
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn};
    use crate::sql::planner::plan::{AggregateCall, AggregateNode, LogicalPlan, ValuesNode};
    use arrow::datatypes::DataType;

    fn col_ref(name: &str, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn make_agg(
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        already_pushed: bool,
    ) -> AggregateNode {
        AggregateNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
            })),
            group_by,
            aggregates,
            output_columns: vec![],
            already_pushed,
        }
    }

    fn sum_call(col: &str) -> AggregateCall {
        AggregateCall {
            name: "sum".into(),
            args: vec![col_ref(col, DataType::Int64)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
        }
    }

    #[test]
    fn rejects_empty_groupby() {
        let agg = make_agg(vec![], vec![sum_call("v")], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_distinct_aggregate() {
        let mut call = sum_call("v");
        call.distinct = true;
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_order_sensitive_aggregate() {
        let mut call = sum_call("v");
        call.order_by.push(crate::sql::analysis::SortItem {
            expr: col_ref("v", DataType::Int64),
            asc: true,
            nulls_first: false,
        });
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_count_star() {
        let count_star = AggregateCall {
            name: "count".into(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![count_star], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_avg_function() {
        let avg = AggregateCall {
            name: "avg".into(),
            args: vec![col_ref("v", DataType::Int64)],
            distinct: false,
            result_type: DataType::Float64,
            order_by: vec![],
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![avg], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_aggregate_expr_not_columnref() {
        let mut call = sum_call("v");
        call.args[0] = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref("a", DataType::Int64)),
                op: crate::sql::analysis::BinOp::Add,
                right: Box::new(col_ref("b", DataType::Int64)),
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_nondeterministic_arg() {
        let mut call = sum_call("v");
        // Replace the arg with a non-ColumnRef expression that contains
        // a non-deterministic call. This should be rejected at the
        // "args must be bare ColumnRef" check before the
        // non-deterministic check fires — both serve as belt-and-suspenders.
        call.args[0] = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "rand".into(),
                args: vec![],
                distinct: false,
            },
            data_type: DataType::Float64,
            nullable: false,
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_already_pushed_aggregate() {
        let agg = make_agg(
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
            true,
        );
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn accepts_inner_join_candidate() {
        let agg = make_agg(
            vec![col_ref("k", DataType::Int64)],
            vec![sum_call("v")],
            false,
        );
        let ctx = entry_safety_check(&agg).expect("should pass entry checks");
        assert_eq!(ctx.original_groupby.len(), 1);
        assert_eq!(ctx.original_aggregates.len(), 1);
        assert!(ctx.required_columns.contains(&"k".to_string()));
        assert!(ctx.required_columns.contains(&"v".to_string()));
    }
}
