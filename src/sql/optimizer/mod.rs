//! Cascades optimizer framework.

pub(crate) mod convert;
pub(crate) mod cost;
pub(crate) mod cte_rewrite;
pub(crate) mod extract;
pub(crate) mod memo;
pub(crate) mod operator;
pub(crate) mod options;
pub(crate) mod physical_plan;
pub(crate) mod property;
pub(crate) mod rbo;
pub(crate) mod rule;
pub(crate) mod rules;
pub(crate) mod runtime_filter_planner;
pub(crate) mod search;
pub(crate) mod statistics;
pub(crate) mod stats;

pub(crate) use memo::Memo;
pub(crate) use operator::Operator;
pub(crate) use physical_plan::PhysicalPlanNode;
pub(crate) use property::PhysicalPropertySet;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::LogicalPlan;
use memo::MExpr;
use rule::Rule;

/// Wall-clock timeout for the entire optimization pipeline.
const OPTIMIZE_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of memo groups allowed during exploration.
/// Prevents exponential blowup from join associativity on large join graphs.
const EXPLORE_MAX_GROUPS: usize = 5000;

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
    let deadline = Instant::now() + OPTIMIZE_TIMEOUT;

    // 1. RBO four-pass pattern: push → reorder → push → prune.
    //    Matches the legacy pipeline exactly:
    //    - push_down_predicates
    //    - reorder_joins_cbo
    //    - push_down_predicates (again, catches post-reorder opportunities)
    //    - prune_columns (LAST — sees the final stable plan)
    //
    //    Column pruning MUST run after all pushdown + reorder passes are
    //    complete. Mixing PruneColumns with PushDownPredicate in a fixed-
    //    point loop causes the needed-column set to shrink across iterations
    //    (predicates get reshuffled between join conditions), incorrectly
    //    dropping join-key or select-list columns from scan required_columns.
    let options =
        options::OptimizerOptions::from_session(&options::current_session_optimizer_settings());
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        plan,
        &rbo::rules::predicate_pushdown_rbo_rules(),
        &options,
        deadline,
    )?;
    let rewritten = rbo::rules::join_reorder::reorder_joins_cbo(rewritten, table_stats);
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        rewritten,
        &rbo::rules::predicate_pushdown_rbo_rules(),
        &options,
        deadline,
    )?;
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        rewritten,
        &rbo::rules::column_pruning_rules(),
        &options,
        deadline,
    )?;

    // 4. CTE cleanup: intentional pre-Memo structural rewrite for CTE shape
    //    cleanup, not a second full logical optimization pass.
    let cte_ctx = cte_rewrite::collect_cte_counts(&rewritten);
    let rewritten = cte_rewrite::inline_single_use_ctes(rewritten, &cte_ctx);

    // 5. Convert to Memo.
    let mut memo = Memo::new();
    let root_group = convert::logical_plan_to_memo(&rewritten, &mut memo);

    // 6. Derive initial statistics.
    stats::derive_group_statistics(&mut memo, table_stats);

    check_deadline(deadline)?;

    // 7. Explore: apply transformation rules (logical -> logical).
    let transform_rules = rules::all_transformation_rules();
    explore(&mut memo, &transform_rules, &options, deadline)?;

    check_deadline(deadline)?;

    // 8. Implement: apply implementation rules (logical -> physical).
    let impl_rules = rules::all_implementation_rules();
    implement(&mut memo, &impl_rules, &options);

    // 9. Re-derive statistics for any newly created groups (e.g. from AggSplit).
    stats::derive_group_statistics(&mut memo, table_stats);

    check_deadline(deadline)?;

    // 10. Top-down search with property enforcement.
    let root_required = PhysicalPropertySet::gather();
    let mut ctx = search::SearchContext::new(table_stats.clone());
    ctx.optimize_group(&memo, root_group, &root_required)?;

    check_deadline(deadline)?;

    // 11. Extract best plan.
    extract::extract_best(&memo, root_group, &root_required, &ctx.winners)
}

/// True if `name` is the stable name of any rule that participates in
/// the standard `optimize()` rule pipelines (RBO predicate pushdown,
/// RBO column pruning, CBO transformations, CBO implementations).
///
/// Used by the server-side `SET disable_optimizer_rules` parser to
/// detect typos in rule names so they can be surfaced via `warn!`
/// without rejecting the SET statement.
pub(crate) fn is_known_rule_name(name: &str) -> bool {
    rules::all_transformation_rules()
        .iter()
        .any(|r| r.name() == name)
        || rules::all_implementation_rules()
            .iter()
            .any(|r| r.name() == name)
        || rbo::rules::predicate_pushdown_rbo_rules()
            .iter()
            .any(|r| r.name() == name)
        || rbo::rules::column_pruning_rules()
            .iter()
            .any(|r| r.name() == name)
}

fn check_deadline(deadline: Instant) -> Result<(), String> {
    if Instant::now() > deadline {
        return Err(format!(
            "optimizer timeout: exceeded {}s budget",
            OPTIMIZE_TIMEOUT.as_secs()
        ));
    }
    Ok(())
}

/// Apply transformation rules to all groups in a fixed-point loop.
///
/// Terminates when:
/// - No new expressions are added (fixed-point)
/// - Iteration limit reached
/// - Memo group count exceeds budget (join associativity explosion guard)
/// - Wall-clock deadline exceeded
const EXPLORE_MAX_ITERATIONS: usize = 16;

fn explore(
    memo: &mut Memo,
    rules: &[Box<dyn Rule>],
    options: &options::OptimizerOptions,
    deadline: Instant,
) -> Result<(), String> {
    for _round in 0..EXPLORE_MAX_ITERATIONS {
        if Instant::now() > deadline {
            return Err(format!(
                "optimizer timeout during exploration: exceeded {}s budget",
                OPTIMIZE_TIMEOUT.as_secs()
            ));
        }
        let mut changed = false;
        let num_groups = memo.groups.len();
        for group_id in 0..num_groups {
            if Instant::now() > deadline {
                return Err(format!(
                    "optimizer timeout: exceeded {}s budget",
                    OPTIMIZE_TIMEOUT.as_secs()
                ));
            }
            let exprs: Vec<MExpr> = memo.groups[group_id].logical_exprs.clone();
            for expr in &exprs {
                for rule in rules {
                    if !options.is_enabled(rule.name()) {
                        continue;
                    }
                    // Skip JoinAssociativity when the memo has grown large
                    // to prevent combinatorial explosion. RBO join reorder
                    // already handles join ordering for large join graphs.
                    if rule.name() == "JoinAssociativity" && memo.groups.len() > 200 {
                        continue;
                    }
                    if rule.matches(&expr.op) {
                        let new_exprs = rule.apply(expr, memo);
                        for new_expr in new_exprs {
                            // Dedup: compare operator AND children to avoid
                            // infinite JoinCommutativity A<->B oscillation.
                            let already_exists =
                                memo.groups[group_id].logical_exprs.iter().any(|existing| {
                                    existing.children == new_expr.children
                                        && op_equal(&existing.op, &new_expr.op)
                                });
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
            // Stop if memo grew too large (exponential join enumeration).
            if memo.groups.len() > EXPLORE_MAX_GROUPS {
                return Ok(());
            }
        }
        if !changed {
            break;
        }
    }
    Ok(())
}

/// Apply implementation rules to all groups.
///
/// Single pass — each logical expr gets physical alternatives once.
fn implement(memo: &mut Memo, rules: &[Box<dyn Rule>], options: &options::OptimizerOptions) {
    let mut changed = true;
    while changed {
        changed = false;
        let num_groups = memo.groups.len();
        for group_id in 0..num_groups {
            let exprs: Vec<MExpr> = memo.groups[group_id].logical_exprs.clone();
            for expr in &exprs {
                for rule in rules {
                    if !options.is_enabled(rule.name()) {
                        continue;
                    }
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

#[cfg(test)]
mod is_known_rule_name_tests {
    use super::*;

    #[test]
    fn is_known_rule_name_recognizes_real_rule() {
        // JoinCommutativity is a transformation rule that has been stable
        // for a while; if this assertion fails because the rule was renamed,
        // pick another known rule name from src/sql/optimizer/rules/.
        assert!(is_known_rule_name("JoinCommutativity"));
    }

    #[test]
    fn is_known_rule_name_rejects_typos() {
        assert!(!is_known_rule_name("TotallyNotARealRule"));
        assert!(!is_known_rule_name(""));
    }
}
