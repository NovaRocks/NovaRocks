//! Cascades optimizer framework.

pub(crate) mod cascades_rules;
pub(crate) mod convert;
pub(crate) mod cost;
pub(crate) mod cte_rewrite;
pub(crate) mod derive;
pub(crate) mod extract;
pub(crate) mod logical_props;
pub(crate) mod memo;
pub(crate) mod operator;
pub(crate) mod options;
pub(crate) mod physical_plan;
pub(crate) mod property;
pub(crate) mod rewrite;
pub(crate) mod rule;
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

use crate::sql::column_id::ColumnRefFactory;
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
/// Takes a logical plan and table statistics, applies query logical rewrites,
/// converts to Memo, explores logical alternatives, generates physical
/// alternatives, runs top-down cost-based search with property enforcement,
/// and extracts the best physical plan.
pub(crate) fn optimize(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
    factory: ColumnRefFactory,
) -> Result<PhysicalPlanNode, String> {
    let deadline = Instant::now() + OPTIMIZE_TIMEOUT;

    // 1. Query logical rewrite pipeline. The ordered stages preserve the
    //    legacy-safe sequence: pushdown → join reorder → pushdown →
    //    aggregate pushdown → column pruning.
    let session_settings = options::current_session_optimizer_settings();
    let options = options::OptimizerOptions::from_session(&session_settings);
    let mut rewrite_ctx =
        rewrite::context::RewriteContext::for_query(session_settings.disabled_rules.clone());
    rewrite_ctx.policy_mut().max_iterations = options.rewrite_max_iterations;
    rewrite_ctx.set_query_table_stats(table_stats.clone());
    rewrite_ctx.set_deadline(deadline);
    let rewritten =
        rewrite::registry::query_rewrite_pipeline(table_stats).rewrite(plan, &mut rewrite_ctx)?;

    // 4. CTE cleanup: intentional pre-Memo structural rewrite for CTE shape
    //    cleanup, not a second full logical optimization pass.
    let cte_ctx = cte_rewrite::collect_cte_counts(&rewritten);
    let rewritten = cte_rewrite::inline_single_use_ctes(rewritten, &cte_ctx);

    // 5. Convert to Memo.
    let mut memo = Memo::new();
    memo.factory = factory;
    let root_group = convert::logical_plan_to_memo(&rewritten, &mut memo);

    // 6. Derive initial statistics.
    stats::derive_group_statistics(&mut memo, table_stats);

    check_deadline(deadline)?;

    // 7. Explore: apply transformation rules (logical -> logical).
    let transform_rules = cascades_rules::all_transformation_rules();
    explore(&mut memo, &transform_rules, &options, deadline)?;

    check_deadline(deadline)?;

    // 8. Implement: apply implementation rules (logical -> physical).
    let impl_rules = cascades_rules::all_implementation_rules();
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
/// the standard `optimize()` rule pipelines (query logical rewrite,
/// CBO transformations, CBO implementations).
///
/// Used by the server-side `SET disable_optimizer_rules` parser to
/// detect typos in rule names so they can be surfaced via `warn!`
/// without rejecting the SET statement.
pub(crate) fn is_known_rule_name(name: &str) -> bool {
    cascades_rules::all_transformation_rules()
        .iter()
        .any(|r| r.name() == name)
        || cascades_rules::all_implementation_rules()
            .iter()
            .any(|r| r.name() == name)
        || rewrite::registry::is_known_rewrite_rule_name(name)
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
                    // to prevent combinatorial explosion. The query rewrite
                    // join reorder stage already handles large join graphs.
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
    fn optimize_accepts_migrated_query_rewrite_pipeline() {
        use std::collections::HashMap;

        use crate::sql::column_id::ColumnRefFactory;
        use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        });
        let factory = ColumnRefFactory::new();
        let physical = optimize(plan, &HashMap::new(), factory).expect("optimize values");
        let physical_debug = format!("{physical:?}");
        assert!(physical_debug.contains("PhysicalValues"));
    }

    #[test]
    fn is_known_rule_name_recognizes_real_rule() {
        // JoinCommutativity is a transformation rule that has been stable
        // for a while; if this assertion fails because the rule was renamed,
        // pick another known rule name from src/sql/optimizer/cascades_rules/.
        assert!(is_known_rule_name("JoinCommutativity"));
    }

    #[test]
    fn is_known_rule_name_rejects_typos() {
        assert!(!is_known_rule_name("TotallyNotARealRule"));
        assert!(!is_known_rule_name(""));
    }
}
