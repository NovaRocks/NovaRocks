use std::collections::HashSet;

use crate::sql::plan::*;

use super::expr_utils::{collect_column_refs, merge_needed};

/// Walk the plan top-down, collecting which columns are actually needed, and
/// set `ScanNode.required_columns` accordingly.
pub(crate) fn prune_columns(plan: LogicalPlan) -> LogicalPlan {
    // Collect all columns required at the root level first, then recurse.
    prune_inner(plan, None)
}

/// `needed`: the set of column names required by the parent.
/// `None` means "all columns" (no restriction).
fn prune_inner(plan: LogicalPlan, needed: Option<&HashSet<String>>) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(mut scan) => {
            if let Some(needed) = needed {
                // Also include columns referenced by pushed-down predicates
                let mut required: HashSet<String> = needed.clone();
                for pred in &scan.predicates {
                    for col in collect_column_refs(pred) {
                        required.insert(col.to_lowercase());
                    }
                }
                let mut pruned: Vec<String> = scan
                    .columns
                    .iter()
                    .filter(|c| required.contains(&c.name.to_lowercase()))
                    .map(|c| c.name.clone())
                    .collect();
                // Ensure at least one column survives so the scan has a valid
                // output layout (needed for COUNT(*) and similar queries).
                if pruned.is_empty() && !scan.columns.is_empty() {
                    pruned.push(scan.columns[0].name.clone());
                }
                scan.required_columns = Some(pruned);
            }
            LogicalPlan::Scan(scan)
        }

        LogicalPlan::Filter(node) => {
            // The filter's predicate contributes required columns to the child.
            let pred_cols = collect_column_refs(&node.predicate);
            let child_needed = merge_needed(needed, &pred_cols);
            let input = prune_inner(*node.input, Some(&child_needed));
            LogicalPlan::Filter(FilterNode {
                input: Box::new(input),
                predicate: node.predicate,
            })
        }

        LogicalPlan::Project(node) => {
            // Collect columns referenced by projection expressions.
            let mut child_needed = HashSet::new();
            for item in &node.items {
                // If parent restricts needed columns, only include items
                // whose output name is in the needed set.
                let dominated =
                    needed.is_none() || needed.unwrap().contains(&item.output_name.to_lowercase());
                if dominated {
                    for col in collect_column_refs(&item.expr) {
                        child_needed.insert(col.to_lowercase());
                    }
                }
            }
            let input = prune_inner(*node.input, Some(&child_needed));
            LogicalPlan::Project(ProjectNode {
                input: Box::new(input),
                items: node.items,
            })
        }

        LogicalPlan::Aggregate(node) => {
            let mut child_needed = HashSet::new();
            for gb in &node.group_by {
                for col in collect_column_refs(gb) {
                    child_needed.insert(col.to_lowercase());
                }
            }
            for agg in &node.aggregates {
                for arg in &agg.args {
                    for col in collect_column_refs(arg) {
                        child_needed.insert(col.to_lowercase());
                    }
                }
            }
            let input = prune_inner(*node.input, Some(&child_needed));
            LogicalPlan::Aggregate(AggregateNode {
                input: Box::new(input),
                ..node
            })
        }

        LogicalPlan::Join(node) => {
            // Join needs all parent columns plus join condition columns.
            // If parent doesn't restrict (None), pass None to children.
            let child_needed = if let Some(needed) = needed {
                let mut combined = needed.clone();
                if let Some(ref cond) = node.condition {
                    for col in collect_column_refs(cond) {
                        combined.insert(col.to_lowercase());
                    }
                }
                Some(combined)
            } else {
                None
            };
            let left = prune_inner(*node.left, child_needed.as_ref());
            let right = prune_inner(*node.right, child_needed.as_ref());
            LogicalPlan::Join(JoinNode {
                left: Box::new(left),
                right: Box::new(right),
                join_type: node.join_type,
                condition: node.condition,
            })
        }

        LogicalPlan::Sort(node) => {
            // Sort needs all parent columns plus sort-key columns.
            // If parent doesn't restrict (None), pass None to child.
            let child_needed = if let Some(needed) = needed {
                let mut combined = needed.clone();
                for item in &node.items {
                    for col in collect_column_refs(&item.expr) {
                        combined.insert(col.to_lowercase());
                    }
                }
                Some(combined)
            } else {
                None
            };
            let input = prune_inner(*node.input, child_needed.as_ref());
            LogicalPlan::Sort(SortNode {
                input: Box::new(input),
                items: node.items,
            })
        }

        LogicalPlan::Limit(node) => {
            let input = prune_inner(*node.input, needed);
            LogicalPlan::Limit(LimitNode {
                input: Box::new(input),
                limit: node.limit,
                offset: node.offset,
            })
        }

        // Set operations: recurse into each child without column restriction
        // since all branches must produce the same schema.
        LogicalPlan::Union(node) => LogicalPlan::Union(UnionNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|i| prune_inner(i, None))
                .collect(),
            all: node.all,
        }),
        LogicalPlan::Intersect(node) => LogicalPlan::Intersect(IntersectNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|i| prune_inner(i, None))
                .collect(),
        }),
        LogicalPlan::Except(node) => LogicalPlan::Except(ExceptNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|i| prune_inner(i, None))
                .collect(),
        }),

        LogicalPlan::Values(node) => LogicalPlan::Values(node),
        LogicalPlan::GenerateSeries(node) => LogicalPlan::GenerateSeries(node),
        LogicalPlan::CTEConsume(node) => LogicalPlan::CTEConsume(node),

        LogicalPlan::Window(node) => {
            // Prune columns in the child, but don't restrict since the window
            // function itself needs columns from PARTITION BY / ORDER BY / args.
            let input = prune_inner(*node.input, None);
            LogicalPlan::Window(WindowNode {
                input: Box::new(input),
                ..node
            })
        }

        LogicalPlan::SubqueryAlias(node) => {
            // Don't propagate outer `needed` into subquery — the inner plan
            // has its own column namespace (aliases differ from base columns).
            // Passing `needed` through would incorrectly prune columns that
            // the inner SELECT references but the outer query doesn't.
            let input = prune_inner(*node.input, None);
            LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Box::new(input),
                alias: node.alias,
                output_columns: node.output_columns,
            })
        }

        LogicalPlan::Repeat(node) => {
            // Repeat needs all columns from input (rollup columns + others).
            let input = prune_inner(*node.input, None);
            LogicalPlan::Repeat(RepeatPlanNode {
                input: Box::new(input),
                repeat_column_ref_list: node.repeat_column_ref_list,
                grouping_ids: node.grouping_ids,
                all_rollup_columns: node.all_rollup_columns,
                grouping_fn_args: node.grouping_fn_args,
            })
        }
    }
}
