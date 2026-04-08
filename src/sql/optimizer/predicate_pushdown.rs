use std::collections::HashSet;

use crate::sql::ir::{ExprKind, JoinKind, TypedExpr};
use crate::sql::plan::*;

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

                if all_in_left && !all_in_right {
                    // Qualified analysis shows left-only
                    left_preds.push(conj);
                } else if all_in_right && !all_in_left {
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
                    if is_self_join_overlap && q_in_left && q_in_right
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
                        join_preds.push(conj);
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
