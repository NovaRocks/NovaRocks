//! Aggregate pushdown collector — phase 1 of the rule.

use std::collections::HashMap;

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::{AggregateNode, LogicalPlan};

use super::context::{AggregatePushDownContext, PushPlan, Side};

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

/// Top-level collector entry.
#[allow(dead_code)]
pub(crate) fn collect_push_plan(
    aggregate: &AggregateNode,
    _table_stats: &HashMap<String, TableStatistics>,
) -> Option<PushPlan> {
    let ctx = entry_safety_check(aggregate)?;
    let join = match aggregate.input.as_ref() {
        LogicalPlan::Join(j) => j,
        _ => return None,
    };
    split_at_join(join, ctx)
}

fn split_at_join(
    join: &crate::sql::planner::plan::JoinNode,
    ctx: AggregatePushDownContext,
) -> Option<PushPlan> {
    use crate::sql::analysis::JoinKind;

    // Step 1: join-shape filter.
    match join.join_type {
        JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter => {}
        _ => return None,
    }
    let cond = join.condition.as_ref()?;
    let equi_keys = extract_equi_key_pairs(cond);
    if equi_keys.is_empty() {
        return None;
    }

    // Step 2: per-side column visibility.
    let left_cols = collect_output_column_names(&join.left);
    let right_cols = collect_output_column_names(&join.right);

    let side = if ctx.required_columns.iter().all(|c| left_cols.contains(c)) {
        Side::Left
    } else if ctx.required_columns.iter().all(|c| right_cols.contains(c)) {
        Side::Right
    } else {
        return None;
    };

    // Step 3: outer-join amplifier rejection.
    match (join.join_type, side) {
        (JoinKind::RightOuter, Side::Left) => return None,
        (JoinKind::LeftOuter, Side::Right) => return None,
        _ => {}
    }

    // Step 4: chosen-side subtree MUST be a Scan in v1 (no nested joins,
    // no intermediate Filter/Project on the side).
    let side_subtree = match side {
        Side::Left => &join.left,
        Side::Right => &join.right,
    };
    if !matches!(side_subtree.as_ref(), LogicalPlan::Scan(_)) {
        return None;
    }
    let side_cols = match side {
        Side::Left => &left_cols,
        Side::Right => &right_cols,
    };

    // Step 5: partial group-by = original group-by cols on this side
    //         + side-bound equi-keys.
    let mut partial_groupby: Vec<TypedExpr> = ctx
        .original_groupby
        .iter()
        .filter(|gb| match &gb.kind {
            ExprKind::ColumnRef { column, .. } => side_cols.contains(column),
            _ => false,
        })
        .cloned()
        .collect();
    for (left_key, right_key) in &equi_keys {
        let candidate = match side {
            Side::Left => left_key,
            Side::Right => right_key,
        };
        let already = partial_groupby
            .iter()
            .any(|gb| match (&gb.kind, &candidate.kind) {
                (ExprKind::ColumnRef { column: a, .. }, ExprKind::ColumnRef { column: b, .. }) => {
                    a == b
                }
                _ => false,
            });
        if !already {
            partial_groupby.push(candidate.clone());
        }
    }

    Some(PushPlan {
        side,
        target_subtree: (**side_subtree).clone(),
        partial_groupby,
        partial_aggregates: ctx.original_aggregates,
    })
}

fn extract_equi_key_pairs(cond: &TypedExpr) -> Vec<(TypedExpr, TypedExpr)> {
    let mut out = Vec::new();
    walk_and_collect_equi(cond, &mut out);
    out
}

fn walk_and_collect_equi(expr: &TypedExpr, out: &mut Vec<(TypedExpr, TypedExpr)>) {
    use crate::sql::analysis::BinOp;
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if matches!(left.kind, ExprKind::ColumnRef { .. })
                && matches!(right.kind, ExprKind::ColumnRef { .. })
            {
                out.push(((**left).clone(), (**right).clone()));
            }
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            walk_and_collect_equi(left, out);
            walk_and_collect_equi(right, out);
        }
        _ => {}
    }
}

fn collect_output_column_names(plan: &LogicalPlan) -> Vec<String> {
    use crate::sql::planner::plan::*;
    match plan {
        LogicalPlan::Scan(s) => s.columns.iter().map(|c| c.name.clone()).collect(),
        LogicalPlan::Filter(f) => collect_output_column_names(&f.input),
        LogicalPlan::Project(p) => p.items.iter().map(|i| i.output_name.clone()).collect(),
        LogicalPlan::Join(j) => {
            let mut l = collect_output_column_names(&j.left);
            l.extend(collect_output_column_names(&j.right));
            l
        }
        LogicalPlan::Aggregate(a) => a.output_columns.iter().map(|c| c.name.clone()).collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::{AggregateCall, AggregateNode, LogicalPlan, ValuesNode};
    use arrow::datatypes::DataType;

    fn col_ref(name: &str, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
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

    use crate::sql::analysis::{JoinKind, ProjectItem};
    use crate::sql::catalog::{TableDef, ScanSource};
    use crate::sql::planner::plan::{FilterNode, JoinNode, ProjectNode, ScanNode};

    fn dummy_scan_with_cols(cols: &[(&str, DataType)]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
                source: ScanSource::ManagedLake,
            },
            alias: None,
            columns: cols
                .iter()
                .map(|(n, ty)| OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: (*n).into(),
                    data_type: ty.clone(),
                    nullable: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn rejects_when_input_is_scan_directly() {
        // No Join means no work to do — would just wrap the scan with an
        // identity partial that buys nothing. v1 rejects.
        let scan = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let agg = AggregateNode {
            input: Box::new(scan),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_when_input_is_filter_above_join() {
        // Filter intermediation between Aggregate and Join is an OPT-1
        // follow-up. v1 rejects.
        let scan_a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let scan_b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            join_type: JoinKind::Inner,
            condition: Some(col_ref("k", DataType::Boolean)),
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(join),
            predicate: col_ref("k", DataType::Boolean),
        });
        let agg = AggregateNode {
            input: Box::new(filter),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_when_input_is_project_above_join() {
        let scan_a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let scan_b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            join_type: JoinKind::Inner,
            condition: Some(col_ref("k", DataType::Boolean)),
        });
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(join),
            items: vec![ProjectItem {
                expr: col_ref("k", DataType::Int64),
                output_name: "k".into(),
            }],
        });
        let agg = AggregateNode {
            input: Box::new(project),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    use crate::sql::analysis::BinOp;

    fn eq(a: &str, b: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(a, DataType::Int64)),
                op: BinOp::Eq,
                right: Box::new(col_ref(b, DataType::Int64)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    #[test]
    fn pushes_sum_under_inner_join_to_left() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        let plan = collect_push_plan(&agg, &HashMap::new()).expect("should push to left");
        assert_eq!(plan.side, super::super::context::Side::Left);
        assert!(matches!(plan.target_subtree, LogicalPlan::Scan(_)));
    }

    #[test]
    fn rejects_outer_join_amplifier_side() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        // LEFT OUTER JOIN; aggregate on right (amplifier) — must reject.
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::LeftOuter,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn accepts_left_outer_when_agg_on_preserved_left() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::LeftOuter,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        let plan = collect_push_plan(&agg, &HashMap::new()).expect("push to preserved left");
        assert!(matches!(plan.target_subtree, LogicalPlan::Scan(_)));
    }

    #[test]
    fn rejects_cross_join() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("x", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::Cross,
            condition: None,
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_aggregate_columns_across_sides() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64), ("w", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
        });
        // sum(v) is on left; sum(w) is on right. Required = {k, v, w}.
        // Neither side covers all required cols → reject.
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v"), sum_call("w")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_semi_anti_join() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::LeftSemi,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_nested_join_on_target_side() {
        // v1 only handles direct-Scan sides. A nested join on the
        // chosen side must be rejected; multi-table is OPT-1 follow-up.
        let inner_join = LogicalPlan::Join(JoinNode {
            left: Box::new(dummy_scan_with_cols(&[
                ("k", DataType::Int64),
                ("v", DataType::Int64),
            ])),
            right: Box::new(dummy_scan_with_cols(&[("k", DataType::Int64)])),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
        });
        let outer_join = LogicalPlan::Join(JoinNode {
            left: Box::new(inner_join),
            right: Box::new(dummy_scan_with_cols(&[("k", DataType::Int64)])),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(outer_join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }
}
