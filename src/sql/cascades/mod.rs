//! Cascades optimizer framework.

pub(crate) mod convert;
pub(crate) mod cost;
pub(crate) mod extract;
pub(crate) mod fragment_builder;
pub(crate) mod memo;
pub(crate) mod operator;
pub(crate) mod physical_plan;
pub(crate) mod property;
pub(crate) mod rewriter;
pub(crate) mod rule;
pub(crate) mod rules;
pub(crate) mod search;
pub(crate) mod stats;

pub(crate) use memo::{GroupId, MExprId, Memo};
pub(crate) use operator::Operator;
pub(crate) use physical_plan::PhysicalPlanNode;
pub(crate) use property::{ColumnRef, DistributionSpec, OrderingSpec, PhysicalPropertySet};

use std::collections::HashMap;

use memo::MExpr;
use rule::Rule;
use crate::sql::plan::LogicalPlan;
use crate::sql::statistics::TableStatistics;

/// Main entry point for the Cascades optimizer.
///
/// Takes a logical plan and table statistics, applies RBO rewrites,
/// converts to Memo, explores logical alternatives, generates physical
/// alternatives, runs top-down cost-based search with property enforcement,
/// and extracts the best physical plan.
pub(crate) fn optimize(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> Result<PhysicalPlanNode, String> {
    // 1. RBO rewrite (predicate pushdown + column pruning).
    let rewritten = rewriter::rewrite(plan);

    // 2. Convert to Memo.
    let mut memo = Memo::new();
    let root_group = convert::logical_plan_to_memo(&rewritten, &mut memo);

    // 3. Derive initial statistics.
    stats::derive_group_statistics(&mut memo, table_stats);

    // 4. Explore: apply transformation rules (logical -> logical).
    let transform_rules = rules::all_transformation_rules();
    explore(&mut memo, &transform_rules);

    // 5. Implement: apply implementation rules (logical -> physical).
    let impl_rules = rules::all_implementation_rules();
    implement(&mut memo, &impl_rules);

    // 6. Re-derive statistics for any newly created groups (e.g. from AggSplit).
    stats::derive_group_statistics(&mut memo, table_stats);

    // 7. Top-down search with property enforcement.
    let root_required = PhysicalPropertySet::gather();
    let mut ctx = search::SearchContext::new(table_stats.clone());
    ctx.optimize_group(&memo, root_group, &root_required)?;

    // 8. Extract best plan.
    extract::extract_best(&memo, root_group, &root_required, &ctx.winners)
}

/// Apply transformation rules to all groups in a fixed-point loop.
///
/// Iterates until no new expressions are added to any group.
fn explore(memo: &mut Memo, rules: &[Box<dyn Rule>]) {
    let mut changed = true;
    while changed {
        changed = false;
        for group_id in 0..memo.groups.len() {
            let exprs: Vec<MExpr> = memo.groups[group_id].logical_exprs.clone();
            for expr in &exprs {
                for rule in rules {
                    if rule.matches(&expr.op) {
                        let new_exprs = rule.apply(expr, memo);
                        for new_expr in new_exprs {
                            let already_exists = memo.groups[group_id]
                                .logical_exprs
                                .iter()
                                .any(|existing| op_equal(&existing.op, &new_expr.op));
                            if !already_exists {
                                let mexpr = MExpr {
                                    id: memo.next_expr_id(),
                                    op: new_expr.op,
                                    children: new_expr.children,
                                };
                                memo.add_expr_to_group(group_id, mexpr);
                                changed = true;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Apply implementation rules to all groups in a fixed-point loop.
///
/// Iterates logical expressions and adds physical alternatives.
fn implement(memo: &mut Memo, rules: &[Box<dyn Rule>]) {
    let mut changed = true;
    while changed {
        changed = false;
        for group_id in 0..memo.groups.len() {
            let exprs: Vec<MExpr> = memo.groups[group_id].logical_exprs.clone();
            for expr in &exprs {
                for rule in rules {
                    if rule.matches(&expr.op) {
                        let new_exprs = rule.apply(expr, memo);
                        for new_expr in new_exprs {
                            let already_exists = memo.groups[group_id]
                                .physical_exprs
                                .iter()
                                .any(|existing| op_equal(&existing.op, &new_expr.op));
                            if !already_exists {
                                let mexpr = MExpr {
                                    id: memo.next_expr_id(),
                                    op: new_expr.op,
                                    children: new_expr.children,
                                };
                                memo.add_expr_to_group(group_id, mexpr);
                                changed = true;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Shallow equality check for operators (structural comparison via Debug format).
///
/// This is conservative: two operators are equal only if their Debug
/// representations match exactly. False negatives are harmless (we just
/// keep a duplicate in the group).
fn op_equal(a: &Operator, b: &Operator) -> bool {
    format!("{:?}", a) == format!("{:?}", b)
}
