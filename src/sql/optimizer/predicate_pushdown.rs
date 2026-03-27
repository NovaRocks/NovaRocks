use std::collections::HashSet;

use crate::sql::ir::{ExprKind, TypedExpr};
use crate::sql::plan::*;

use super::expr_utils::{collect_column_refs, combine_and, split_and, wrap_remaining_filter};
use super::map_children;

/// Push Filter predicates as close to Scan nodes as possible.
///
/// For a Filter → Scan chain the filter conjuncts that reference only the
/// scan's columns are moved into `ScanNode.predicates`.  Conjuncts that
/// cannot be pushed remain in a residual Filter node.
pub(super) fn push_down_predicates(plan: LogicalPlan) -> LogicalPlan {
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

        // Cannot push through other operators for now — keep the filter.
        other => LogicalPlan::Filter(FilterNode {
            input: Box::new(other),
            predicate,
        }),
    }
}
