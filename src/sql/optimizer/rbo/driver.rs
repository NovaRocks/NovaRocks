//! Bottom-up tree rewriter that applies a list of `RewriteRule`s to a
//! `LogicalPlan` until a fixed point is reached.
//!
//! Iteration order:
//!   1. For each node, recursively rewrite children first.
//!   2. After children are stable, apply each enabled rule at this node
//!      and repeat at this node until no rule fires (node-level
//!      fixed-point).
//!   3. Repeat the whole bottom-up pass until no rule fires anywhere
//!      (tree-level fixed-point).
//!
//! The tree-level loop is bounded by `OptimizerOptions::rbo_max_iterations`
//! and by the optimize-call deadline.

use std::time::Instant;

use super::rule::RewriteRule;
use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::planner::plan::*;

/// Apply `rules` to `plan` until a fixed point is reached or a hard limit
/// is hit. Returns the rewritten plan on success, or an error string when
/// the optimize-call deadline is exceeded.
pub(crate) fn rewrite_to_fixed_point(
    mut plan: LogicalPlan,
    rules: &[Box<dyn RewriteRule>],
    options: &OptimizerOptions,
    deadline: Instant,
) -> Result<LogicalPlan, String> {
    for _round in 0..options.rbo_max_iterations {
        if Instant::now() > deadline {
            return Err(format!(
                "optimizer timeout during RBO: exceeded {}s budget",
                options.optimize_timeout.as_secs()
            ));
        }
        let (next, changed) = apply_rules_one_pass(plan, rules, options);
        plan = next;
        if !changed {
            break;
        }
    }
    Ok(plan)
}

fn apply_rules_one_pass(
    plan: LogicalPlan,
    rules: &[Box<dyn RewriteRule>],
    options: &OptimizerOptions,
) -> (LogicalPlan, bool) {
    // 1. Rewrite children first (bottom-up).
    let (plan, child_changed) = rewrite_children(plan, rules, options);

    // 2. Node-level fixed-point: apply enabled rules until none fires here.
    let mut current = plan;
    let mut local_changed = false;
    loop {
        let mut applied = false;
        for rule in rules {
            if !options.is_enabled(rule.name()) || !rule.matches(&current) {
                continue;
            }
            if let Some(next) = rule.apply(current.clone()) {
                current = next;
                local_changed = true;
                applied = true;
            }
        }
        if !applied {
            break;
        }
    }

    (current, child_changed || local_changed)
}

fn rewrite_children(
    plan: LogicalPlan,
    rules: &[Box<dyn RewriteRule>],
    options: &OptimizerOptions,
) -> (LogicalPlan, bool) {
    // For each variant with children, recurse on each child via
    // apply_rules_one_pass. OR-fold the per-child changed flags.
    // Leaf variants pass through unchanged.
    macro_rules! rec {
        ($child:expr) => {{
            let (out, ch) = apply_rules_one_pass(*$child, rules, options);
            (Box::new(out), ch)
        }};
    }

    match plan {
        // Leaves: no children.
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => (plan, false),

        // Single-input variants.
        LogicalPlan::Filter(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Filter(FilterNode {
                    input,
                    predicate: n.predicate,
                }),
                ch,
            )
        }
        LogicalPlan::Project(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Project(ProjectNode {
                    input,
                    items: n.items,
                }),
                ch,
            )
        }
        LogicalPlan::Aggregate(n) => {
            let (input, ch) = rec!(n.input);
            (LogicalPlan::Aggregate(AggregateNode { input, ..n }), ch)
        }
        LogicalPlan::Sort(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Sort(SortNode {
                    input,
                    items: n.items,
                }),
                ch,
            )
        }
        LogicalPlan::Limit(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Limit(LimitNode {
                    input,
                    limit: n.limit,
                    offset: n.offset,
                }),
                ch,
            )
        }
        LogicalPlan::Window(n) => {
            let (input, ch) = rec!(n.input);
            (LogicalPlan::Window(WindowNode { input, ..n }), ch)
        }
        LogicalPlan::TableFunction(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::TableFunction(TableFunctionNode { input, ..n }),
                ch,
            )
        }
        LogicalPlan::SubqueryAlias(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                    input,
                    alias: n.alias,
                    output_columns: n.output_columns,
                }),
                ch,
            )
        }
        LogicalPlan::Repeat(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::Repeat(RepeatPlanNode {
                    input,
                    repeat_column_ref_list: n.repeat_column_ref_list,
                    grouping_ids: n.grouping_ids,
                    all_rollup_columns: n.all_rollup_columns,
                    grouping_key_aliases: n.grouping_key_aliases,
                    grouping_fn_args: n.grouping_fn_args,
                }),
                ch,
            )
        }
        LogicalPlan::CTEProduce(n) => {
            let (input, ch) = rec!(n.input);
            (
                LogicalPlan::CTEProduce(CTEProduceNode {
                    cte_id: n.cte_id,
                    input,
                    output_columns: n.output_columns,
                }),
                ch,
            )
        }

        // Two-child variants.
        LogicalPlan::Join(n) => {
            let (left, lch) = rec!(n.left);
            let (right, rch) = rec!(n.right);
            (
                LogicalPlan::Join(JoinNode {
                    left,
                    right,
                    join_type: n.join_type,
                    condition: n.condition,
                }),
                lch || rch,
            )
        }
        LogicalPlan::CTEAnchor(n) => {
            let (produce, pch) = rec!(n.produce);
            let (consumer, cch) = rec!(n.consumer);
            (
                LogicalPlan::CTEAnchor(CTEAnchorNode {
                    cte_id: n.cte_id,
                    produce,
                    consumer,
                }),
                pch || cch,
            )
        }

        // N-child variants (set ops).
        LogicalPlan::Union(n) => {
            let mut changed = false;
            let inputs = n
                .inputs
                .into_iter()
                .map(|child| {
                    let (out, ch) = apply_rules_one_pass(child, rules, options);
                    if ch {
                        changed = true;
                    }
                    out
                })
                .collect();
            (
                LogicalPlan::Union(UnionNode { inputs, all: n.all }),
                changed,
            )
        }
        LogicalPlan::Intersect(n) => {
            let mut changed = false;
            let inputs = n
                .inputs
                .into_iter()
                .map(|child| {
                    let (out, ch) = apply_rules_one_pass(child, rules, options);
                    if ch {
                        changed = true;
                    }
                    out
                })
                .collect();
            (LogicalPlan::Intersect(IntersectNode { inputs }), changed)
        }
        LogicalPlan::Except(n) => {
            let mut changed = false;
            let inputs = n
                .inputs
                .into_iter()
                .map(|child| {
                    let (out, ch) = apply_rules_one_pass(child, rules, options);
                    if ch {
                        changed = true;
                    }
                    out
                })
                .collect();
            (LogicalPlan::Except(ExceptNode { inputs }), changed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::optimizer::rbo::rule::RewriteRule;
    use crate::sql::planner::plan::{FilterNode, ScanNode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn dummy_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn empty_rule_list_returns_input_unchanged_after_one_round() {
        let plan = dummy_scan();
        let rules: Vec<Box<dyn RewriteRule>> = vec![];
        let options = OptimizerOptions::default_settings();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let out = rewrite_to_fixed_point(plan.clone(), &rules, &options, deadline)
            .expect("should succeed");
        // Structurally identical (Scan with same fields).
        match (plan, out) {
            (LogicalPlan::Scan(a), LogicalPlan::Scan(b)) => assert_eq!(a.table.name, b.table.name),
            _ => panic!("expected Scan plan unchanged"),
        }
    }

    #[test]
    fn driver_terminates_when_rule_fires_finitely() {
        // Rule fires up to N times, then stops. Driver must terminate.
        struct FireNTimes(AtomicUsize, usize);
        impl RewriteRule for FireNTimes {
            fn name(&self) -> &'static str {
                "TestFireNTimes"
            }
            fn matches(&self, plan: &LogicalPlan) -> bool {
                matches!(plan, LogicalPlan::Scan(_))
            }
            fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
                let count = self.0.load(Ordering::Relaxed);
                if count >= self.1 {
                    None
                } else {
                    self.0.store(count + 1, Ordering::Relaxed);
                    Some(plan)
                }
            }
        }

        let rules: Vec<Box<dyn RewriteRule>> = vec![Box::new(FireNTimes(AtomicUsize::new(0), 3))];
        let options = OptimizerOptions::default_settings();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let out = rewrite_to_fixed_point(dummy_scan(), &rules, &options, deadline);
        assert!(out.is_ok(), "driver should terminate cleanly");
    }

    #[test]
    fn driver_recurses_through_filter_into_scan() {
        // A rule that targets Scan should be invoked at the Scan node even
        // when the root is Filter(Scan). Use an Arc<AtomicUsize> shared
        // between the test code and the rule so we can read back the visit
        // count after the rule has been boxed into the rules vec.
        use std::sync::Arc;

        struct OnScan(Arc<AtomicUsize>);
        impl RewriteRule for OnScan {
            fn name(&self) -> &'static str {
                "TestOnScan"
            }
            fn matches(&self, plan: &LogicalPlan) -> bool {
                matches!(plan, LogicalPlan::Scan(_))
            }
            fn apply(&self, _plan: LogicalPlan) -> Option<LogicalPlan> {
                self.0.fetch_add(1, Ordering::Relaxed);
                None // no change, but we counted the visit
            }
        }

        let counter = Arc::new(AtomicUsize::new(0));
        let rule = Box::new(OnScan(counter.clone()));
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(dummy_scan()),
            predicate: crate::sql::analysis::TypedExpr {
                kind: crate::sql::analysis::ExprKind::Literal(
                    crate::sql::analysis::LiteralValue::Bool(true),
                ),
                data_type: arrow::datatypes::DataType::Boolean,
                nullable: false,
            },
        });
        let rules: Vec<Box<dyn RewriteRule>> = vec![rule];
        let options = OptimizerOptions::default_settings();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let _ = rewrite_to_fixed_point(plan, &rules, &options, deadline).unwrap();
        assert!(
            counter.load(Ordering::Relaxed) >= 1,
            "rule should have visited the Scan child"
        );
    }

    #[test]
    fn driver_returns_error_on_deadline_exceeded() {
        // Create a rule that always reports a change so the driver loops
        // until it hits the deadline. Set deadline to "already past" so it
        // hits immediately on the first iteration boundary check.
        struct AlwaysChange;
        impl RewriteRule for AlwaysChange {
            fn name(&self) -> &'static str {
                "TestAlwaysChange"
            }
            fn matches(&self, plan: &LogicalPlan) -> bool {
                matches!(plan, LogicalPlan::Scan(_))
            }
            fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
                Some(plan)
            }
        }
        let rules: Vec<Box<dyn RewriteRule>> = vec![Box::new(AlwaysChange)];
        let options = OptimizerOptions::default_settings();
        let past_deadline = Instant::now() - std::time::Duration::from_secs(1);
        let result = rewrite_to_fixed_point(dummy_scan(), &rules, &options, past_deadline);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("timeout"));
    }
}
