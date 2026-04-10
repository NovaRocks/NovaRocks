use std::collections::HashSet;

use crate::sql::ir::{ExprKind, JoinKind, TypedExpr};
use crate::sql::plan::*;
use arrow::datatypes::DataType;

use super::expr_utils::{
    collect_column_refs, collect_output_columns, collect_qualified_column_refs,
    collect_qualified_output_columns, combine_and, split_and, wrap_remaining_filter,
};
use super::map_children;

/// Push Filter predicates as close to Scan nodes as possible.
///
/// Handles:
/// - Filter → Scan: push conjuncts into `ScanNode.predicates`
/// - Filter → Project: push through pass-through column projections
/// - Filter → Join: classify conjuncts by which side they reference and push
///   left-only / right-only predicates below the join
/// - Filter → Aggregate: push predicates that reference only GROUP BY columns
///   below the aggregate
pub(crate) fn push_down_predicates(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(node) => {
            let input = push_down_predicates(*node.input);
            push_filter_into(node.predicate, input)
        }
        // For SEMI/ANTI joins: push right-child-only predicates from the join
        // condition into the right child. This converts patterns like:
        //   SEMI JOIN (CROSS(store_sales, date_dim)) ON (corr AND ss_sold_date_sk = d_date_sk)
        // into:
        //   SEMI JOIN (INNER(store_sales, date_dim, ON ss_sold_date_sk = d_date_sk)) ON (corr)
        LogicalPlan::Join(join)
            if matches!(
                join.join_type,
                JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::RightSemi | JoinKind::RightAnti
            ) =>
        {
            let optimized = push_semi_condition_into_children(join);
            map_children(optimized, push_down_predicates)
        }
        // Recurse into INNER/CROSS joins normally.
        // OR factoring in join conditions is handled by the second pushdown
        // pass in the rewriter, which runs after join reorder.
        // Recurse into children for all other node types
        other => map_children(other, push_down_predicates),
    }
}

/// Try to push `predicate` into `input`.  Returns a (possibly simplified)
/// tree.
fn push_filter_into(predicate: TypedExpr, input: LogicalPlan) -> LogicalPlan {
    match input {
        LogicalPlan::Scan(mut scan) => {
            let conjuncts = split_and(predicate);
            let mut remaining = Vec::new();
            let scan_columns: HashSet<String> =
                scan.columns.iter().map(|c| c.name.to_lowercase()).collect();

            for conj in conjuncts {
                let refs = collect_column_refs(&conj);
                if refs
                    .iter()
                    .all(|r| scan_columns.contains(&r.to_lowercase()))
                {
                    scan.predicates.push(conj);
                } else {
                    remaining.push(conj);
                }
            }

            let plan = LogicalPlan::Scan(scan);
            wrap_remaining_filter(plan, remaining)
        }

        // Filter → Project: push predicates that don't reference computed
        // (non-column) projections through the project.
        LogicalPlan::Project(proj) => {
            let conjuncts = split_and(predicate);
            let mut pushable = Vec::new();
            let mut remaining = Vec::new();

            // A predicate can pass through a Project if every column it
            // references also exists in the child scope (i.e. it refers to
            // base columns, not aliases of computed expressions).
            let passthrough_columns: HashSet<String> = proj
                .items
                .iter()
                .filter_map(|item| {
                    if let ExprKind::ColumnRef { column, .. } = &item.expr.kind {
                        Some(column.to_lowercase())
                    } else {
                        None
                    }
                })
                .collect();

            for conj in conjuncts {
                let refs = collect_column_refs(&conj);
                if refs
                    .iter()
                    .all(|r| passthrough_columns.contains(&r.to_lowercase()))
                {
                    pushable.push(conj);
                } else {
                    remaining.push(conj);
                }
            }

            let new_input = if pushable.is_empty() {
                push_down_predicates(*proj.input)
            } else {
                let pushed = combine_and(pushable);
                push_filter_into(pushed, push_down_predicates(*proj.input))
            };

            let proj = LogicalPlan::Project(ProjectNode {
                input: Box::new(new_input),
                items: proj.items,
            });
            wrap_remaining_filter(proj, remaining)
        }

        // Filter → Join: classify each conjunct by which side of the join it
        // references and push single-side predicates below the join.
        LogicalPlan::Join(join) => push_predicates_through_join(predicate, join),

        // Filter → Aggregate: push predicates that reference only GROUP BY
        // columns (not aggregates) below the aggregate node.
        LogicalPlan::Aggregate(agg) => push_predicates_through_aggregate(predicate, agg),

        // Cannot push through other operators — keep the filter.
        other => LogicalPlan::Filter(FilterNode {
            input: Box::new(other),
            predicate,
        }),
    }
}

/// Push filter predicates through a Join node.
///
/// Conjuncts are classified into:
/// - **left-only**: references only left-side columns → pushed below left child
/// - **right-only**: references only right-side columns → pushed below right child
///   (only for INNER/CROSS joins; for outer/semi/anti joins these stay above)
/// - **both-sides**: references columns from both sides → merged into join condition
/// - **constant**: references no columns from either side → pushed to left child
///
/// Special handling for LEFT OUTER / LEFT SEMI / LEFT ANTI joins: when a
/// predicate's column names appear in both sides (due to subquery output
/// sharing names with the left child), but ALL columns are satisfiable by
/// the left side alone, the predicate is treated as left-only and pushed
/// to the left child instead of being merged into the join condition.
fn push_predicates_through_join(predicate: TypedExpr, join: JoinNode) -> LogicalPlan {
    let conjuncts = split_and(predicate);
    let left_cols = collect_output_columns(&join.left);
    let right_cols = collect_output_columns(&join.right);

    // Qualified output columns for precise self-join disambiguation.
    let left_qcols = collect_qualified_output_columns(&join.left);
    let right_qcols = collect_qualified_output_columns(&join.right);

    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut join_preds = Vec::new();
    let mut remaining = Vec::new();

    // For LEFT joins (OUTER/SEMI/ANTI), subquery rewrites can produce a right
    // child whose output columns share names with the left child. A predicate
    // that references only left-child columns may look like "both-sides" due to
    // this name overlap. Detect this and treat such predicates as left-only.
    let is_left_join_variant = matches!(
        join.join_type,
        JoinKind::LeftOuter | JoinKind::LeftSemi | JoinKind::LeftAnti
    );

    for conj in conjuncts {
        let refs = collect_column_refs(&conj);
        let in_left = refs.iter().any(|c| left_cols.contains(&c.to_lowercase()));
        let in_right = refs.iter().any(|c| right_cols.contains(&c.to_lowercase()));

        match (in_left, in_right) {
            (true, false) => left_preds.push(conj),
            (false, true) => {
                // For LEFT OUTER / LEFT SEMI / LEFT ANTI / FULL OUTER joins,
                // right-side predicates affect NULL preservation semantics and
                // must NOT be pushed below the join.
                // For RIGHT OUTER, left-side predicates have the same issue
                // (handled below), but right-side predicates are safe to push.
                match join.join_type {
                    JoinKind::Inner
                    | JoinKind::Cross
                    | JoinKind::RightOuter
                    | JoinKind::RightSemi
                    | JoinKind::RightAnti => {
                        right_preds.push(conj);
                    }
                    _ => remaining.push(conj),
                }
            }
            (true, true) => {
                // Bare-name matching says "both sides". Re-check with qualified
                // column references to handle self-joins (e.g. nation n1, nation n2)
                // where both sides share the same bare column names.
                let qrefs = collect_qualified_column_refs(&conj);
                let q_in_left = qrefs.iter().any(|r| left_qcols.contains(r));
                let q_in_right = qrefs.iter().any(|r| right_qcols.contains(r));

                // If ALL qualified refs are present in left (and not exclusively
                // right), treat as left-only. Vice versa for right-only.
                // Only if qualified refs genuinely span both sides treat as
                // "both-sides".
                let all_in_left = qrefs.iter().all(|r| left_qcols.contains(r));
                let all_in_right = qrefs.iter().all(|r| right_qcols.contains(r));

                // Guard against false "left-only" or "right-only" classification
                // when unqualified refs are ambiguous (present in both sides).
                // E.g., `d_week_seq = __sq_2.d_week_seq` has refs:
                //   (None, "d_week_seq")  — in both left & right qcols
                //   (Some("__sq_2"), "d_week_seq") — only in right qcols
                // `all_in_right` would be true, but the unqualified ref is
                // genuinely a left-side column. Treat as join predicate.
                let any_ambiguous_in_both = qrefs
                    .iter()
                    .any(|r| left_qcols.contains(r) && right_qcols.contains(r));

                if all_in_left && !all_in_right && !any_ambiguous_in_both {
                    // Qualified analysis shows left-only
                    left_preds.push(conj);
                } else if all_in_right && !all_in_left && !any_ambiguous_in_both {
                    // Qualified analysis shows right-only
                    match join.join_type {
                        JoinKind::Inner
                        | JoinKind::Cross
                        | JoinKind::RightOuter
                        | JoinKind::RightSemi
                        | JoinKind::RightAnti => {
                            right_preds.push(conj);
                        }
                        _ => remaining.push(conj),
                    }
                } else if q_in_left && q_in_right {
                    // Genuinely references both sides.
                    //
                    // For self-joins (left and right share the same bare column
                    // names), keep the predicate as a remaining filter above the
                    // join.  Merging it into the join condition can cause the
                    // execution layer to mis-resolve ambiguous column references
                    // when both sides originate from the same table.
                    let is_self_join_overlap = {
                        let bare_refs: Vec<String> =
                            refs.iter().map(|c| c.to_lowercase()).collect();
                        bare_refs
                            .iter()
                            .all(|c| left_cols.contains(c) && right_cols.contains(c))
                    };
                    if is_self_join_overlap
                        && q_in_left
                        && q_in_right
                        && !is_left_join_variant
                        && matches!(join.join_type, JoinKind::Cross | JoinKind::Inner)
                    {
                        // Shared column names but qualified refs confirm both sides
                        // are referenced (e.g., CTE.d_week_seq = date_dim.d_week_seq).
                        // Only push for CROSS/INNER joins to enable CROSS → INNER
                        // upgrade. Do NOT push for SEMI/ANTI/OUTER which have
                        // different scope semantics.
                        join_preds.push(conj);
                    } else if is_self_join_overlap {
                        remaining.push(conj);
                    } else if is_left_join_variant
                        && refs.iter().all(|c| left_cols.contains(&c.to_lowercase()))
                    {
                        left_preds.push(conj);
                    } else if is_left_join_variant {
                        remaining.push(conj);
                    } else {
                        // For OR predicates, try to extract common equi-join
                        // conditions shared by all OR branches. This handles:
                        //   (cd_demo_sk=ss_cdemo_sk AND ...) OR (cd_demo_sk=ss_cdemo_sk AND ...)
                        // → factor out cd_demo_sk=ss_cdemo_sk as join_pred,
                        //   keep remaining OR as other condition.
                        let (factored, or_remaining) =
                            factor_common_eq_from_or(&conj, &left_cols, &right_cols);
                        if !factored.is_empty() {
                            join_preds.extend(factored);
                            if let Some(rem) = or_remaining {
                                remaining.push(rem);
                            }
                        } else {
                            join_preds.push(conj);
                        }
                    }
                } else {
                    // Fallback: keep above the join as remaining filter.
                    // This handles cases where qualified matching cannot
                    // disambiguate (e.g. mixed qualified/unqualified refs).
                    remaining.push(conj);
                }
            }
            (false, false) => {
                // Constant predicates — push to left side
                left_preds.push(conj);
            }
        }
    }

    // For RIGHT OUTER joins, left-side predicates cannot be pushed below
    // (left side is the nullable side). Move them to remaining.
    if matches!(
        join.join_type,
        JoinKind::RightOuter | JoinKind::RightSemi | JoinKind::RightAnti
    ) {
        remaining.extend(left_preds.drain(..));
    }

    // For FULL OUTER joins, neither side can receive pushed predicates.
    if matches!(join.join_type, JoinKind::FullOuter) {
        remaining.extend(left_preds.drain(..));
        remaining.extend(right_preds.drain(..));
    }

    // Build the new left child, applying pushed predicates then recursing.
    let new_left = if left_preds.is_empty() {
        *join.left
    } else {
        let pushed = combine_and(left_preds);
        // Recursively push the new filter further down (through nested joins, etc.)
        push_filter_into(pushed, *join.left)
    };

    // Build the new right child.
    let new_right = if right_preds.is_empty() {
        *join.right
    } else {
        let pushed = combine_and(right_preds);
        push_filter_into(pushed, *join.right)
    };

    // Merge new join predicates with the existing join condition.
    let new_condition = merge_join_conditions(join.condition, join_preds);

    // When a CROSS JOIN gets join predicates extracted from the filter above,
    // upgrade it to INNER JOIN so the physical emitter can use hash join.
    let new_join_type = if join.join_type == JoinKind::Cross && new_condition.is_some() {
        JoinKind::Inner
    } else {
        join.join_type
    };

    let new_join = LogicalPlan::Join(JoinNode {
        left: Box::new(new_left),
        right: Box::new(new_right),
        join_type: new_join_type,
        condition: new_condition,
    });

    wrap_remaining_filter(new_join, remaining)
}

/// For SEMI/ANTI joins, push predicates from the join condition that only
/// reference the right child into the right child as a filter. This converts
/// CROSS JOINs inside SEMI right sides into proper INNER JOINs.
///
/// Example:
///   LEFT SEMI (store_sales CROSS date_dim) ON (corr AND ss_sold_date_sk = d_date_sk AND d_year = 2002)
/// becomes:
///   LEFT SEMI (store_sales INNER date_dim ON ss_sold_date_sk = d_date_sk WHERE d_year = 2002) ON (corr)
fn push_semi_condition_into_children(join: JoinNode) -> LogicalPlan {
    let Some(ref condition) = join.condition else {
        return LogicalPlan::Join(join);
    };

    let conjuncts = split_and(condition.clone());
    let right_cols = collect_output_columns(&join.right);
    let left_cols = collect_output_columns(&join.left);
    let right_qcols = collect_qualified_output_columns(&join.right);

    let mut keep_in_condition = Vec::new();
    let mut push_to_right = Vec::new();

    for conj in conjuncts {
        let refs = collect_column_refs(&conj);
        let qrefs = collect_qualified_column_refs(&conj);

        // Use qualified refs when available to handle self-joins
        // (e.g., catalog_sales cs1, catalog_sales cs2 — same bare names).
        // But also check bare refs to avoid pushing cross-side predicates
        // where one side is qualified and the other isn't.
        let is_right_only = if !qrefs.is_empty() {
            // All qualified refs must be in right's qualified columns,
            // AND all bare refs must be right-only (not also in left).
            let q_all_right = qrefs.iter().all(|r| right_qcols.contains(r));
            let bare_any_left = refs.iter().any(|c| left_cols.contains(&c.to_lowercase()));
            q_all_right && !bare_any_left
        } else if !refs.is_empty() {
            // Fallback: all bare refs in right but NOT all in left
            // (avoids pushing cross-side predicates for self-joins)
            let all_in_right = refs.iter().all(|c| right_cols.contains(&c.to_lowercase()));
            let any_in_left = refs.iter().any(|c| left_cols.contains(&c.to_lowercase()));
            all_in_right && !any_in_left
        } else {
            false
        };

        if is_right_only {
            push_to_right.push(conj);
        } else {
            keep_in_condition.push(conj);
        }
    }

    if push_to_right.is_empty() {
        return LogicalPlan::Join(join);
    }

    let new_condition = if keep_in_condition.is_empty() {
        None
    } else {
        Some(combine_and(keep_in_condition))
    };

    let pushed = combine_and(push_to_right);
    let new_right = push_filter_into(pushed, *join.right);

    LogicalPlan::Join(JoinNode {
        left: join.left,
        right: Box::new(new_right),
        join_type: join.join_type,
        condition: new_condition,
    })
}

/// Extract common equi-join conditions from all branches of an OR predicate.
/// Returns (extracted_join_preds, remaining_or_predicate).
///
/// For: `(A=B AND X) OR (A=B AND Y)` where A is from left and B from right,
/// extracts `A=B` as a join predicate and returns `X OR Y` as remaining.
fn factor_common_eq_from_or(
    expr: &TypedExpr,
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
) -> (Vec<TypedExpr>, Option<TypedExpr>) {
    // Split OR into branches
    let branches = split_or_branches(expr);
    if branches.len() < 2 {
        return (vec![], None);
    }

    // Collect AND conjuncts per branch
    let branch_conjuncts: Vec<Vec<&TypedExpr>> =
        branches.iter().map(|b| split_and_refs(b)).collect();

    // Find equi-join predicates (col=col where one side left, one right)
    // that appear in ALL branches
    let mut common_eqs: Vec<TypedExpr> = Vec::new();
    if let Some(first) = branch_conjuncts.first() {
        for candidate in first {
            if !is_cross_side_eq(candidate, left_cols, right_cols) {
                continue;
            }
            let in_all = branch_conjuncts[1..]
                .iter()
                .all(|conjs| conjs.iter().any(|c| expr_eq(c, candidate)));
            if in_all {
                common_eqs.push((*candidate).clone());
            }
        }
    }

    if common_eqs.is_empty() {
        return (vec![], None);
    }

    // Build remaining OR: remove common eqs from each branch
    let mut new_branches: Vec<TypedExpr> = Vec::new();
    for branch in &branch_conjuncts {
        let remaining: Vec<TypedExpr> = branch
            .iter()
            .filter(|c| !common_eqs.iter().any(|eq| expr_eq(c, eq)))
            .map(|c| (*c).clone())
            .collect();
        if remaining.is_empty() {
            // Branch was only the common eq → TRUE
            new_branches.push(TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::Literal(crate::sql::ir::LiteralValue::Bool(true)),
            });
        } else {
            new_branches.push(combine_and(remaining));
        }
    }

    let or_remaining = if new_branches.iter().all(|b| {
        matches!(
            b.kind,
            ExprKind::Literal(crate::sql::ir::LiteralValue::Bool(true))
        )
    }) {
        None // All branches were just the common eq
    } else {
        let mut result = new_branches.remove(0);
        for branch in new_branches {
            result = TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(result),
                    op: crate::sql::ir::BinOp::Or,
                    right: Box::new(branch),
                },
            };
        }
        Some(result)
    };

    (common_eqs, or_remaining)
}

/// Factor common equi-join conditions out of OR predicates for join reorder.
///
/// Extracts `col=col` equalities that appear in ALL OR branches so the
/// join graph can see them as independent binary predicates.
pub(super) fn factor_common_eq_from_or_for_reorder(
    expr: &TypedExpr,
) -> (Vec<TypedExpr>, Option<TypedExpr>) {
    let empty = HashSet::new();
    factor_common_eq_from_or_any_side(expr, &empty, &empty)
}

/// Like factor_common_eq_from_or but extracts ANY common col=col eq
/// (not just cross-side). Same-side eqs will be pushed to children.
fn factor_common_eq_from_or_any_side(
    expr: &TypedExpr,
    _left_cols: &HashSet<String>,
    _right_cols: &HashSet<String>,
) -> (Vec<TypedExpr>, Option<TypedExpr>) {
    let branches = split_or_branches(expr);
    if branches.len() < 2 {
        return (vec![], None);
    }
    let branch_conjuncts: Vec<Vec<&TypedExpr>> =
        branches.iter().map(|b| split_and_refs(b)).collect();

    // Find col=col eqs common to ALL branches
    let mut common_eqs: Vec<TypedExpr> = Vec::new();
    if let Some(first) = branch_conjuncts.first() {
        for candidate in first {
            if !is_any_eq(candidate) {
                continue;
            }
            let in_all = branch_conjuncts[1..]
                .iter()
                .all(|conjs| conjs.iter().any(|c| expr_eq(c, candidate)));
            if in_all {
                common_eqs.push((*candidate).clone());
            }
        }
    }
    if common_eqs.is_empty() {
        return (vec![], None);
    }

    let mut new_branches: Vec<TypedExpr> = Vec::new();
    for branch in &branch_conjuncts {
        let remaining: Vec<TypedExpr> = branch
            .iter()
            .filter(|c| !common_eqs.iter().any(|eq| expr_eq(c, eq)))
            .map(|c| (*c).clone())
            .collect();
        if remaining.is_empty() {
            new_branches.push(TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::Literal(crate::sql::ir::LiteralValue::Bool(true)),
            });
        } else {
            new_branches.push(combine_and(remaining));
        }
    }
    let or_rem = if new_branches.iter().all(|b| {
        matches!(
            b.kind,
            ExprKind::Literal(crate::sql::ir::LiteralValue::Bool(true))
        )
    }) {
        None
    } else {
        let mut r = new_branches.remove(0);
        for b in new_branches {
            r = TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(r),
                    op: crate::sql::ir::BinOp::Or,
                    right: Box::new(b),
                },
            };
        }
        Some(r)
    };
    (common_eqs, or_rem)
}

fn is_any_eq(expr: &TypedExpr) -> bool {
    matches!(&expr.kind, ExprKind::BinaryOp { left, op: crate::sql::ir::BinOp::Eq, right }
        if matches!(left.kind, ExprKind::ColumnRef { .. }) && matches!(right.kind, ExprKind::ColumnRef { .. }))
}

fn split_or_branches(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: crate::sql::ir::BinOp::Or,
            right,
        } => {
            let mut v = split_or_branches(left);
            v.extend(split_or_branches(right));
            v
        }
        ExprKind::Nested(inner) => split_or_branches(inner),
        _ => vec![expr],
    }
}

fn split_and_refs(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: crate::sql::ir::BinOp::And,
            right,
        } => {
            let mut v = split_and_refs(left);
            v.extend(split_and_refs(right));
            v
        }
        ExprKind::Nested(inner) => split_and_refs(inner),
        _ => vec![expr],
    }
}

fn is_cross_side_eq(
    expr: &TypedExpr,
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
) -> bool {
    if let ExprKind::BinaryOp {
        left,
        op: crate::sql::ir::BinOp::Eq,
        right,
    } = &expr.kind
    {
        let l_name = match &left.kind {
            ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
            _ => None,
        };
        let r_name = match &right.kind {
            ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
            _ => None,
        };
        match (l_name, r_name) {
            (Some(l), Some(r)) => {
                (left_cols.contains(&l) && right_cols.contains(&r))
                    || (left_cols.contains(&r) && right_cols.contains(&l))
            }
            _ => false,
        }
    } else {
        false
    }
}

fn expr_eq(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a.kind) == format!("{:?}", b.kind)
}

/// Merge new predicates into an existing (optional) join condition.
fn merge_join_conditions(
    existing: Option<TypedExpr>,
    new_preds: Vec<TypedExpr>,
) -> Option<TypedExpr> {
    let mut all = Vec::new();
    if let Some(cond) = existing {
        all.push(cond);
    }
    all.extend(new_preds);
    if all.is_empty() {
        None
    } else {
        Some(combine_and(all))
    }
}

/// Push filter predicates through an Aggregate node.
///
/// A predicate can be pushed below the aggregate if every column it references
/// is a GROUP BY column (i.e. a direct column reference in `group_by`).
/// Predicates that reference aggregate output names must remain above.
fn push_predicates_through_aggregate(predicate: TypedExpr, agg: AggregateNode) -> LogicalPlan {
    let conjuncts = split_and(predicate);

    // Collect the set of column names that come from GROUP BY expressions
    // and thus exist in the aggregate's input.
    let group_by_columns: HashSet<String> = agg
        .group_by
        .iter()
        .filter_map(|e| match &e.kind {
            ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
            _ => None,
        })
        .collect();

    let mut pushable = Vec::new();
    let mut remaining = Vec::new();

    for conj in conjuncts {
        let refs = collect_column_refs(&conj);
        if !refs.is_empty()
            && refs
                .iter()
                .all(|r| group_by_columns.contains(&r.to_lowercase()))
        {
            pushable.push(conj);
        } else {
            remaining.push(conj);
        }
    }

    let new_input = if pushable.is_empty() {
        *agg.input
    } else {
        let pushed = combine_and(pushable);
        // Recursively push the new filter further down.
        push_filter_into(pushed, *agg.input)
    };

    let agg_plan = LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(new_input),
        group_by: agg.group_by,
        aggregates: agg.aggregates,
        output_columns: agg.output_columns,
    });

    wrap_remaining_filter(agg_plan, remaining)
}
