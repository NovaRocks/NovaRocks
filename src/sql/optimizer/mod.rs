// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Cascades optimizer framework.

pub(crate) mod binder;
pub(crate) mod cascades_rules;
pub(crate) mod cost;
mod cse_pass;
pub(crate) mod cte_rewrite;
pub(crate) mod derive;
pub(crate) mod estimate;
pub(crate) mod extract;
pub(crate) mod logical_props;
pub(crate) mod memo;
pub(crate) mod memo_copy;
pub(crate) mod operator;
pub(crate) mod opt_expr;
pub(crate) mod options;
pub(crate) mod pattern;
pub(crate) mod physical_tree;
pub(crate) mod property;
pub(crate) mod rewrite;
pub(crate) mod rule;
pub(crate) mod scalar;
pub(crate) mod scalar_expr;
pub(crate) mod search;
pub(crate) mod statistics;
pub(crate) mod stats;
pub(crate) mod stats_input;
pub(crate) mod topn_proof;

pub(crate) use memo::Memo;
pub(crate) use operator::Operator;
pub(crate) use physical_tree::OptimizerPhysicalNode;
pub(crate) use property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::optimizer::stats_input::{OptimizerStatsInput, QueryStatsSnapshot};
use memo::MExpr;
use rule::Rule;

/// Wall-clock timeout for the entire optimization pipeline.
///
/// Raised from 10 s to 30 s after profiling the join-suite timeouts on
/// wide-table queries (`join_one_key` step 19 — a `LEFT SEMI` over a
/// 33-column 1.28M-row table — used the entire 10 s budget on
/// statistics-driven cost estimation in Cascades, and the same cost
/// model dominated every other affected join case). 30 s matches the
/// budget StarRocks FE and other comparable engines use, leaves room
/// for genuinely large plans, and is still small enough that a runaway
/// rule will surface rather than silently spin.
const OPTIMIZE_TIMEOUT: Duration = Duration::from_secs(30);

/// Main entry point for the Cascades optimizer.
///
/// Takes an optimizer-native logical tree and query-scoped statistics, applies query logical
/// rewrites, converts to Memo, explores logical alternatives, generates physical
/// alternatives, runs top-down cost-based search with property enforcement,
/// and extracts the best optimizer physical operator tree.
pub(crate) fn optimize(
    plan_expr: OptExpr,
    scalar_arena: ScalarArena,
    query_stats: &QueryStatsSnapshot,
    factory: ColumnRefFactory,
    dictionary_provider: Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>>,
    mv_candidates: Vec<cascades_rules::mv_rewrite::MvRewriteCandidate>,
) -> Result<OptimizerPhysicalNode, String> {
    validate_query_stats_bound(&plan_expr)?;
    let stats_input = OptimizerStatsInput::from_query_stats(query_stats);
    optimize_with_root_property(
        plan_expr,
        scalar_arena,
        stats_input,
        factory,
        dictionary_provider,
        mv_candidates,
        PhysicalPropertySet::gather(),
    )
}

pub(crate) fn optimize_with_root_distribution(
    plan_expr: OptExpr,
    scalar_arena: ScalarArena,
    query_stats: &QueryStatsSnapshot,
    factory: ColumnRefFactory,
    root_distribution: DistributionSpec,
) -> Result<OptimizerPhysicalNode, String> {
    validate_query_stats_bound(&plan_expr)?;
    let root_required = PhysicalPropertySet {
        distribution: root_distribution,
        ordering: OrderingSpec::Any,
    };
    let stats_input = OptimizerStatsInput::from_query_stats(query_stats);
    optimize_with_root_property(
        plan_expr,
        scalar_arena,
        stats_input,
        factory,
        None,
        Vec::new(),
        root_required,
    )
}

pub(crate) fn optimize_with_legacy_table_stats_for_migration(
    plan_expr: OptExpr,
    scalar_arena: ScalarArena,
    table_stats: &HashMap<String, TableStatistics>,
    factory: ColumnRefFactory,
    dictionary_provider: Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>>,
    mv_candidates: Vec<cascades_rules::mv_rewrite::MvRewriteCandidate>,
) -> Result<OptimizerPhysicalNode, String> {
    // Migration-only entry point for callers that still build unbound scan
    // expressions. The scan estimator ignores this name-keyed map; production
    // query planning must use `optimize` with a bound QueryStatsSnapshot.
    let stats_input = OptimizerStatsInput::from_legacy_table_stats_for_migration(table_stats);
    optimize_with_root_property(
        plan_expr,
        scalar_arena,
        stats_input,
        factory,
        dictionary_provider,
        mv_candidates,
        PhysicalPropertySet::gather(),
    )
}

pub(crate) fn optimize_with_root_distribution_and_legacy_table_stats_for_migration(
    plan_expr: OptExpr,
    scalar_arena: ScalarArena,
    table_stats: &HashMap<String, TableStatistics>,
    factory: ColumnRefFactory,
    root_distribution: DistributionSpec,
) -> Result<OptimizerPhysicalNode, String> {
    // Migration-only entry point; see `optimize_with_legacy_table_stats_for_migration`.
    let root_required = PhysicalPropertySet {
        distribution: root_distribution,
        ordering: OrderingSpec::Any,
    };
    let stats_input = OptimizerStatsInput::from_legacy_table_stats_for_migration(table_stats);
    optimize_with_root_property(
        plan_expr,
        scalar_arena,
        stats_input,
        factory,
        None,
        Vec::new(),
        root_required,
    )
}

fn optimize_with_root_property(
    plan_expr: OptExpr,
    scalar_arena: ScalarArena,
    stats_input: OptimizerStatsInput,
    factory: ColumnRefFactory,
    dictionary_provider: Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>>,
    mv_candidates: Vec<cascades_rules::mv_rewrite::MvRewriteCandidate>,
    root_required: PhysicalPropertySet,
) -> Result<OptimizerPhysicalNode, String> {
    let deadline = Instant::now() + OPTIMIZE_TIMEOUT;

    // Wrap factory in Rc<RefCell<...>> so it can be shared with RewriteContext
    // for the duration of the rewrite phase (needed for auto-fill column minting
    // in per-operator column-pruning rules). The Rc is unwrapped back to a plain
    // ColumnRefFactory before the Memo build step.
    let factory = Rc::new(RefCell::new(factory));

    // 1. Query logical rewrite pipeline. The ordered stages preserve the
    //    legacy-safe sequence: pushdown → join reorder → pushdown →
    //    variant path pushdown → aggregate pushdown → column pruning.
    let session_settings = options::current_session_optimizer_settings();
    let options = options::OptimizerOptions::from_session(&session_settings);
    let mut rewrite_ctx =
        rewrite::context::RewriteContext::for_query(session_settings.disabled_rules.clone());
    rewrite_ctx.policy_mut().max_iterations = options.rewrite_max_iterations;
    rewrite_ctx.set_query_stats_input(stats_input.clone());
    rewrite_ctx.set_deadline(deadline);
    if let Some(provider) = resolve_dictionary_provider(dictionary_provider) {
        rewrite_ctx.set_dictionary_provider(provider);
    }
    rewrite_ctx.set_column_ref_factory(Rc::clone(&factory));
    let arena = Rc::new(RefCell::new(scalar_arena));
    rewrite_ctx.set_scalar_arena(Rc::clone(&arena));
    let rewritten_expr =
        rewrite::registry::query_rewrite_pipeline().rewrite(plan_expr, &mut rewrite_ctx)?;

    // Non-disableable backstop: Apply must not survive the SubqueryRewrite
    // stage. The ApplyException rule reports this with rule attribution, but
    // a user-disabled rule must not let an Apply leak into memo conversion
    // (which panics by contract).
    if let Some(message) = rewrite::rules::subquery::find_residual_apply(&rewritten_expr) {
        return Err(message);
    }

    // 4. CTE cleanup: intentional pre-Memo structural rewrite for CTE shape
    //    cleanup, not a second full logical optimization pass.
    let cte_ctx = cte_rewrite::collect_cte_counts(&rewritten_expr);
    let rewritten_expr = {
        let mut scalar_arena = arena.borrow_mut();
        cte_rewrite::inline_single_use_ctes(rewritten_expr, &cte_ctx, &mut scalar_arena)?
    };

    // 5. Convert to Memo. Unwrap the factory from Rc<RefCell<...>> — rewrite
    //    is done so the only two references at this call site are the local
    //    `factory` binding and the clone stored in `rewrite_ctx`. Drop the
    //    context's reference first, then unwrap the local one.
    debug_assert_eq!(
        Rc::strong_count(&factory),
        2,
        "expected exactly 2 Rc references (factory + rewrite_ctx) before drop; \
         a rewrite rule stored an extra clone of the context — check column_ref_factory() call sites"
    );
    drop(rewrite_ctx);
    let factory = Rc::try_unwrap(factory)
        .expect(
            "ColumnRefFactory Rc must be uniquely owned after rewrite; \
             a rule cloned the context and did not drop the clone",
        )
        .into_inner();
    let mut memo = Memo::new();
    memo.factory = factory;
    memo.scalars = arena.borrow().clone();
    let root_group = memo_copy::opt_expr_to_memo(&rewritten_expr, &mut memo);

    // 6. Derive initial statistics.
    stats::derive_group_statistics(&mut memo, &stats_input);

    // 6b. In-memo multi-candidate join reorder (StarRocks-aligned, one-shot):
    //     inject alternative join orders into each reorderable inner/cross
    //     chain so the cost search chooses among them with distribution
    //     awareness. Gated by the "MultiJoinReorder" name so
    //     `SET disable_optimizer_rules='MultiJoinReorder'` turns the pass off
    //     entirely; the legacy RBO reorder was retired, so this is now the only
    //     join-reorder mechanism.
    if options.is_enabled("MultiJoinReorder") {
        cascades_rules::multi_join_reorder::run_multi_join_reorder(
            &mut memo,
            &options.reorder,
            &stats_input,
        );
    }

    check_deadline(deadline)?;

    // 7. Explore: apply transformation rules (logical -> logical). When the
    //    caller supplied usable MV candidates, append the MvRewrite rule so it
    //    can inject MV-scan alternatives alongside the other transformations.
    let mut transform_rules = cascades_rules::all_transformation_rules();
    if !mv_candidates.is_empty() {
        transform_rules.push(Box::new(
            cascades_rules::mv_rewrite::rule::MvRewriteRule::new(mv_candidates),
        ));
    }
    explore(&mut memo, &transform_rules, &options, deadline)?;

    check_deadline(deadline)?;

    // 8. Implement: apply implementation rules (logical -> physical).
    let impl_rules = cascades_rules::all_implementation_rules();
    implement(&mut memo, &impl_rules, &options);

    // 9. Re-derive statistics for any newly created groups (e.g. from AggSplit).
    stats::derive_group_statistics(&mut memo, &stats_input);

    check_deadline(deadline)?;

    // 10. Top-down search with property enforcement.
    let mut ctx = search::SearchContext::new(stats_input.clone(), options.cost_options.clone());
    ctx.optimize_group(&memo, root_group, &root_required)?;

    check_deadline(deadline)?;

    // 11. Extract best plan.
    let mut physical = extract::extract_best(&mut memo, root_group, &root_required, &ctx.winners)?;

    // 12. Common-subexpression elimination (materializes repeats as Project columns).
    cse_pass::rewrite(
        &mut physical,
        &mut memo.scalars,
        &mut memo.factory,
        &options,
    );
    physical_tree::attach_scalar_arena(&mut physical, Arc::new(memo.scalars.clone()));

    Ok(physical)
}

fn validate_query_stats_bound(expr: &OptExpr) -> Result<(), String> {
    match &expr.op {
        Operator::LogicalScan(scan) | Operator::PhysicalScan(scan) if scan.stats_ref.is_none() => {
            return Err(format!(
                "optimizer scan statistics are not bound for table {}",
                scan.table.name
            ));
        }
        _ => {}
    }
    for child in &expr.children {
        validate_query_stats_bound(child)?;
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn optimizer_rejects_unbound_scan_stats() {
    use arrow::datatypes::DataType;

    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::common::OutputColumn;
    use crate::sql::optimizer::operator::{Operator, ScanOp};

    let expr = OptExpr::leaf(Operator::LogicalScan(ScanOp {
        database: "db".to_string(),
        table: TableDef {
            name: "unbound_table".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        },
        alias: None,
        stats_ref: None,
        columns: vec![OutputColumn {
            column_id: crate::sql::column_id::ColumnId::new_for_test(1),
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }],
        predicates: vec![],
        required_columns: None,
        variant_columns: vec![],
        mv_rewritten_from: None,
    }));

    let err = validate_query_stats_bound(&expr).expect_err("unbound scan must be rejected");
    assert!(
        err.contains("optimizer scan statistics are not bound"),
        "unexpected error: {err}"
    );
}

/// Resolve which dictionary provider should be attached to the rewrite context
/// for this `optimize()` call.
///
/// Precedence: an explicit `parameter` (passed by the caller) wins over
/// any `with_dictionary_provider` TLS binding. The parameter wins even
/// when it is a provider that returns `None` for every column, so
/// production sites that pass `None` deliberately fall through to TLS
/// instead of overriding it with a silent no-op.
fn resolve_dictionary_provider(
    parameter: Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>>,
) -> Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>> {
    parameter.or_else(rewrite::context::current_dictionary_provider)
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
        || name == crate::sql::planner::runtime_filter_placement::RUNTIME_FILTER_RULE
        || name == cse_pass::CSE_RULE
        || name == cascades_rules::mv_rewrite::RULE_NAME
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
            for (expr_index, expr) in exprs.iter().enumerate() {
                for rule in rules {
                    if !options.is_enabled(rule.name()) {
                        continue;
                    }
                    // D2 (reorder/associativity mutual exclusion): skip
                    // JoinAssociativity on chains the in-memo reorder pass owns —
                    // it already injected multi-candidate orders for them, so
                    // re-associating here is redundant double-enumeration. The
                    // group-count throttle stays as a backstop for chains the
                    // pass did not own (bailed, or beyond its size cap).
                    if rule.name() == "JoinAssociativity"
                        && (memo.reorder_owned_groups.contains(&group_id)
                            || memo.groups.len() > 200)
                    {
                        continue;
                    }
                    if rule.matches(&expr.op) {
                        let pattern = rule.pattern();
                        // Root the binder on the SAME expr legacy used. The
                        // snapshot's positional index equals its index in the
                        // live `logical_exprs`: rules only ever APPEND to the
                        // tail during explore (`add_expr_to_group`), never
                        // reorder/remove, so existing indices are stable.
                        // (MExpr ids are not unique across snapshots, so we key
                        // on position, not id.)
                        let bindings = crate::sql::optimizer::binder::bind(
                            &pattern, memo, group_id, expr_index,
                        );
                        let bindings_slice: &[_] = if rule.first_match_only() {
                            &bindings[..bindings.len().min(1)]
                        } else {
                            &bindings
                        };
                        for binding in bindings_slice {
                            let new_exprs = rule.apply_bound(binding, memo);
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
            }
            // Stop if memo grew too large (exponential join enumeration). This
            // is a hard cap, not an error: explore returns early and the best
            // plan is extracted from whatever groups exist. Log it so the
            // truncation is observable instead of silent.
            if memo.groups.len() > options.cbo_max_groups {
                tracing::warn!(
                    groups = memo.groups.len(),
                    cap = options.cbo_max_groups,
                    "optimizer exploration truncated at memo group cap; some \
                     transformation rules may not have fired on this query"
                );
                return Ok(());
            }
        }
        if !changed {
            break;
        }
    }
    Ok(())
}

/// Apply implementation rules to all groups to a fixed point.
///
/// Some implementation rules allocate intermediate groups while lowering one
/// logical expression. The fixed-point loop visits those new groups, but each
/// logical expression is implemented by each rule at most once; otherwise rules
/// that allocate fresh child groups on every apply can keep the loop alive
/// indefinitely. Physical alternatives are deduplicated by both operator and
/// children so child-distinct alternatives remain visible to search.
fn implement(memo: &mut Memo, rules: &[Box<dyn Rule>], options: &options::OptimizerOptions) {
    let mut implemented_logical_rules = HashSet::new();
    let mut changed = true;
    while changed {
        changed = false;
        let num_groups = memo.groups.len();
        for group_id in 0..num_groups {
            let exprs: Vec<MExpr> = memo.groups[group_id].logical_exprs.clone();
            for (expr_index, expr) in exprs.iter().enumerate() {
                for rule in rules {
                    if !options.is_enabled(rule.name()) {
                        continue;
                    }
                    if should_skip_single_dedup_implementation(
                        rule.name(),
                        &expr.op,
                        memo,
                        group_id,
                    ) {
                        continue;
                    }
                    if rule.matches(&expr.op) {
                        let application_key = (
                            group_id,
                            expr.children.clone(),
                            format!("{:?}", expr.op),
                            rule.name().to_string(),
                        );
                        if !implemented_logical_rules.insert(application_key) {
                            continue;
                        }
                        let pattern = rule.pattern();
                        // Root the binder on the SAME expr legacy used. The
                        // snapshot's positional index equals its index in the
                        // live `logical_exprs`: implement only appends (physical
                        // alternatives and any newly-allocated groups go
                        // elsewhere/at the tail), so existing logical indices are
                        // stable. (MExpr ids are not unique, so we key on
                        // position, not id.)
                        let bindings = crate::sql::optimizer::binder::bind(
                            &pattern, memo, group_id, expr_index,
                        );
                        let bindings_slice: &[_] = if rule.first_match_only() {
                            &bindings[..bindings.len().min(1)]
                        } else {
                            &bindings
                        };
                        for binding in bindings_slice {
                            let new_exprs = rule.apply_bound(binding, memo);
                            for new_expr in new_exprs {
                                let already_exists =
                                    memo.groups[group_id].physical_exprs.iter().any(|existing| {
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
            }
        }
    }
}

fn should_skip_single_dedup_implementation(
    rule_name: &str,
    op: &Operator,
    memo: &Memo,
    group_id: usize,
) -> bool {
    rule_name == "AggToHashAgg"
        && is_single_pure_group_dedup(op)
        && memo.groups[group_id]
            .logical_exprs
            .iter()
            .any(|expr| is_split_global_aggregate(&expr.op))
}

fn is_single_pure_group_dedup(op: &Operator) -> bool {
    matches!(
        op,
        Operator::LogicalAggregate(agg)
            if agg.stage == operator::AggStage::Single
                && !agg.group_by.is_empty()
                && agg.aggregates.is_empty()
    )
}

fn is_split_global_aggregate(op: &Operator) -> bool {
    matches!(
        op,
        Operator::LogicalAggregate(agg)
            if agg.stage == operator::AggStage::Global && agg.is_split
    )
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::sql::optimizer::memo::{MExpr, Memo};
    use crate::sql::optimizer::operator::{
        AggMode, AggregateOutputLayout, LimitOp, LogicalAggregateOp, Operator, ValuesOp,
    };
    use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};

    #[test]
    fn optimize_accepts_migrated_query_rewrite_pipeline() {
        use std::collections::HashMap;

        use crate::sql::column_id::ColumnRefFactory;
        use crate::sql::planner::plan::{LogicalPlanKind, LogicalPlanNode, LogicalValuesNode};

        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        );
        let factory = ColumnRefFactory::new();
        let physical = optimize_logical(plan, &HashMap::new(), factory, None, Vec::new())
            .expect("optimize values");
        let physical_debug = format!("{physical:?}");
        assert!(physical_debug.contains("PhysicalValues"));
    }

    struct AllocatingLimitRule {
        apply_count: AtomicUsize,
    }

    impl Rule for AllocatingLimitRule {
        fn name(&self) -> &str {
            "AllocatingLimitRule"
        }

        fn rule_type(&self) -> RuleType {
            RuleType::Implementation
        }

        fn matches(&self, op: &Operator) -> bool {
            matches!(op, Operator::LogicalLimit(_))
        }

        fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
            let previous_count = self.apply_count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                previous_count, 0,
                "same logical expression should not be implemented twice by one rule"
            );
            let Operator::LogicalLimit(limit) = &expr.op else {
                return vec![];
            };
            let child_group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::PhysicalValues(ValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            vec![NewExpr {
                op: Operator::PhysicalLimit(LimitOp {
                    limit: limit.limit,
                    offset: limit.offset,
                }),
                children: vec![child_group],
            }]
        }
    }

    struct LimitToPhysicalWithOriginalChildren;

    impl Rule for LimitToPhysicalWithOriginalChildren {
        fn name(&self) -> &str {
            "LimitToPhysicalWithOriginalChildren"
        }

        fn rule_type(&self) -> RuleType {
            RuleType::Implementation
        }

        fn matches(&self, op: &Operator) -> bool {
            matches!(op, Operator::LogicalLimit(_))
        }

        fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
            let Operator::LogicalLimit(limit) = &expr.op else {
                return vec![];
            };
            vec![NewExpr {
                op: Operator::PhysicalLimit(LimitOp {
                    limit: limit.limit,
                    offset: limit.offset,
                }),
                children: expr.children.clone(),
            }]
        }
    }

    fn logical_values_group(memo: &mut Memo) -> usize {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        })
    }

    fn logical_limit(child: usize) -> MExpr {
        MExpr {
            id: 0,
            op: Operator::LogicalLimit(LimitOp {
                limit: Some(1),
                offset: Some(0),
            }),
            children: vec![child],
        }
    }

    fn pure_group_dedup_aggregate(memo: &mut Memo, child: usize) -> MExpr {
        let key_id = crate::sql::column_id::ColumnId::new_for_test(9001);
        let key = memo.scalars.intern(
            crate::sql::optimizer::scalar::ScalarNode::ColumnRef(key_id),
            arrow::datatypes::DataType::Int32,
            false,
        );
        let output = crate::sql::analysis::OutputColumn {
            column_id: key_id,
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
            is_internal: false,
        };
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![key],
                vec![],
                AggregateOutputLayout::new(vec![output.clone()], vec![]),
                vec![output],
            )),
            children: vec![child],
        }
    }

    fn hash_aggregate_modes(memo: &Memo, group_id: usize) -> Vec<AggMode> {
        memo.groups[group_id]
            .physical_exprs
            .iter()
            .filter_map(|expr| match &expr.op {
                Operator::PhysicalHashAggregate(op) => Some(op.mode),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn implement_applies_allocating_rule_once_per_logical_expression() {
        let mut memo = Memo::new();
        let child = logical_values_group(&mut memo);
        let root = memo.new_group(logical_limit(child));
        let rule: Box<dyn Rule> = Box::new(AllocatingLimitRule {
            apply_count: AtomicUsize::new(0),
        });
        let options = options::OptimizerOptions::default_settings();

        implement(&mut memo, &[rule], &options);

        assert_eq!(memo.groups[root].physical_exprs.len(), 1);
        assert_eq!(memo.groups.len(), 3);
    }

    #[test]
    fn implement_preserves_child_distinct_physical_alternatives() {
        let mut memo = Memo::new();
        let child_a = logical_values_group(&mut memo);
        let child_b = logical_values_group(&mut memo);
        let root = memo.new_group(logical_limit(child_a));
        memo.add_expr_to_group(root, logical_limit(child_b));
        let rule: Box<dyn Rule> = Box::new(LimitToPhysicalWithOriginalChildren);
        let options = options::OptimizerOptions::default_settings();

        implement(&mut memo, &[rule], &options);

        let physical_children: Vec<_> = memo.groups[root]
            .physical_exprs
            .iter()
            .filter_map(|expr| match expr.op {
                Operator::PhysicalLimit(_) => Some(expr.children.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(physical_children.len(), 2);
        assert!(physical_children.contains(&vec![child_a]));
        assert!(physical_children.contains(&vec![child_b]));
    }

    #[test]
    fn implement_skips_single_pure_dedup_only_when_split_alternative_exists() {
        let mut memo = Memo::new();
        let child = logical_values_group(&mut memo);
        let aggregate = pure_group_dedup_aggregate(&mut memo, child);
        let root = memo.new_group(aggregate);
        let options = options::OptimizerOptions::default_settings();

        explore(
            &mut memo,
            &[Box::new(
                cascades_rules::split_aggregate::SplitAggregateRule,
            )],
            &options,
            Instant::now() + Duration::from_secs(30),
        )
        .expect("split aggregate exploration");
        assert!(
            memo.groups[root]
                .logical_exprs
                .iter()
                .any(|expr| is_split_global_aggregate(&expr.op)),
            "split aggregate rule should add a global split alternative"
        );

        implement(
            &mut memo,
            &[Box::new(cascades_rules::implement::AggToHashAgg)],
            &options,
        );

        let modes = hash_aggregate_modes(&memo, root);
        assert!(
            !modes.contains(&AggMode::Single),
            "single pure dedup should be suppressed when split is available"
        );
        assert!(
            modes.contains(&AggMode::Global),
            "split global aggregate should remain implementable"
        );
    }

    #[test]
    fn implement_keeps_single_pure_dedup_when_split_rule_disabled() {
        let mut memo = Memo::new();
        let child = logical_values_group(&mut memo);
        let aggregate = pure_group_dedup_aggregate(&mut memo, child);
        let root = memo.new_group(aggregate);
        let mut options = options::OptimizerOptions::default_settings();
        options.disable("SplitAggregateRule");

        explore(
            &mut memo,
            &[Box::new(
                cascades_rules::split_aggregate::SplitAggregateRule,
            )],
            &options,
            Instant::now() + Duration::from_secs(30),
        )
        .expect("disabled split aggregate exploration");
        assert!(
            !memo.groups[root]
                .logical_exprs
                .iter()
                .any(|expr| is_split_global_aggregate(&expr.op)),
            "disabled SplitAggregateRule must not add a split alternative"
        );

        implement(
            &mut memo,
            &[Box::new(cascades_rules::implement::AggToHashAgg)],
            &options,
        );

        let modes = hash_aggregate_modes(&memo, root);
        assert!(
            modes.contains(&AggMode::Single),
            "single pure dedup remains the fallback when split is disabled"
        );
    }

    #[test]
    fn is_known_rule_name_recognizes_real_rule() {
        // JoinCommutativity is a transformation rule that has been stable
        // for a while; if this assertion fails because the rule was renamed,
        // pick another known rule name from src/sql/optimizer/cascades_rules/.
        assert!(is_known_rule_name("JoinCommutativity"));
    }

    #[test]
    fn is_known_rule_name_recognizes_split_aggregate_rule() {
        assert!(is_known_rule_name("SplitAggregateRule"));
    }

    #[test]
    fn is_known_rule_name_recognizes_pushdown_topn_to_preagg_rule() {
        assert!(is_known_rule_name("PushDownTopNToPreAgg"));
    }

    #[test]
    fn is_known_rule_name_recognizes_push_topn_through_join_rule() {
        assert!(is_known_rule_name("PushTopNThroughJoin"));
    }

    #[test]
    fn is_known_rule_name_recognizes_mv_rewrite() {
        assert!(is_known_rule_name("MvRewrite"));
    }

    #[test]
    fn is_known_rule_name_does_not_recognize_removed_push_topn_through_aggregate_rule() {
        assert!(!is_known_rule_name("PushTopNThroughAggregate"));
    }

    #[test]
    fn is_known_rule_name_rejects_typos() {
        assert!(!is_known_rule_name("TotallyNotARealRule"));
        assert!(!is_known_rule_name(""));
    }

    #[test]
    fn is_known_rule_name_recognizes_runtime_filter() {
        assert!(is_known_rule_name("RuntimeFilterPushDown"));
    }

    #[test]
    fn is_known_rule_name_recognizes_cse_rule() {
        assert!(is_known_rule_name("CommonSubexpressionReuse"));
    }

    // --- Item 4 (Important): provider precedence tests for optimize() ---

    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::engine::dictionary::model::{
        DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
    };
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::optimizer::rewrite::context::{
        QueryDictionaryProvider, RewriteContext, current_dictionary_provider,
        with_dictionary_provider,
    };
    use std::cell::RefCell;

    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarNode};
    use crate::sql::optimizer::stats_input::{
        BaseColumnStatistics, BaseTableStatistics, QueryStatsSnapshot, StatValue, StatsRef,
        StatsSource,
    };
    use crate::sql::planner::optimizer_bridge::plan::{
        logical_plan_to_opt_expr, opt_expr_to_logical_plan, try_logical_plan_to_opt_expr,
    };
    use crate::sql::planner::plan::{
        AggregateCall, LogicalAggregateNode, LogicalPlanKind, LogicalPlanNode, LogicalScanNode,
    };

    fn optimize_logical(
        plan: LogicalPlanNode,
        table_stats: &HashMap<String, TableStatistics>,
        factory: ColumnRefFactory,
        dictionary_provider: Option<Arc<dyn QueryDictionaryProvider>>,
        mv_candidates: Vec<cascades_rules::mv_rewrite::MvRewriteCandidate>,
    ) -> Result<OptimizerPhysicalNode, String> {
        let mut scalar_arena = ScalarArena::new();
        let plan_expr = try_logical_plan_to_opt_expr(&plan, &mut scalar_arena)?;
        optimize_with_legacy_table_stats_for_migration(
            plan_expr,
            scalar_arena,
            table_stats,
            factory,
            dictionary_provider,
            mv_candidates,
        )
    }

    fn optimize_logical_with_root_distribution(
        plan: LogicalPlanNode,
        table_stats: &HashMap<String, TableStatistics>,
        factory: ColumnRefFactory,
        root_distribution: DistributionSpec,
    ) -> Result<OptimizerPhysicalNode, String> {
        let mut scalar_arena = ScalarArena::new();
        let plan_expr = try_logical_plan_to_opt_expr(&plan, &mut scalar_arena)?;
        optimize_with_root_distribution_and_legacy_table_stats_for_migration(
            plan_expr,
            scalar_arena,
            table_stats,
            factory,
            root_distribution,
        )
    }

    fn iceberg_info(catalog: &str, ns: &str, tbl: &str) -> crate::sql::catalog::IcebergTableInfo {
        crate::sql::catalog::IcebergTableInfo {
            catalog: catalog.to_string(),
            namespace: ns.to_string(),
            table: tbl.to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 0,
            location: String::new(),
            schema: crate::sql::catalog::IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn iceberg_table(catalog: &str, ns: &str, tbl: &str, columns: &[&str]) -> TableDef {
        TableDef {
            name: tbl.to_string(),
            columns: columns
                .iter()
                .map(|name| ColumnDef {
                    name: name.to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg_info(catalog, ns, tbl),
                files: vec![],
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        }
    }

    fn int_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn scalar_col(
        arena: &mut ScalarArena,
        column: &OutputColumn,
    ) -> crate::sql::optimizer::scalar::ScalarId {
        arena.remember_source_column_display(column.column_id, None, column.name.clone());
        arena.intern(
            ScalarNode::ColumnRef(column.column_id),
            column.data_type.clone(),
            column.nullable,
        )
    }

    fn scalar_int(arena: &mut ScalarArena, value: i64) -> crate::sql::optimizer::scalar::ScalarId {
        arena.intern(
            ScalarNode::Literal(HashableLiteral(crate::sql::common::LiteralValue::Int(
                value,
            ))),
            DataType::Int64,
            false,
        )
    }

    fn scalar_cmp(
        arena: &mut ScalarArena,
        left: crate::sql::optimizer::scalar::ScalarId,
        op: crate::sql::common::BinOp,
        value: i64,
    ) -> crate::sql::optimizer::scalar::ScalarId {
        let right = scalar_int(arena, value);
        arena.intern(
            ScalarNode::BinaryOp { left, op, right },
            DataType::Boolean,
            true,
        )
    }

    fn scalar_cast_int64(
        arena: &mut ScalarArena,
        child: crate::sql::optimizer::scalar::ScalarId,
    ) -> crate::sql::optimizer::scalar::ScalarId {
        arena.intern(
            ScalarNode::Cast {
                child,
                target: DataType::Int64,
            },
            DataType::Int64,
            true,
        )
    }

    fn scalar_or(
        arena: &mut ScalarArena,
        left: crate::sql::optimizer::scalar::ScalarId,
        right: crate::sql::optimizer::scalar::ScalarId,
    ) -> crate::sql::optimizer::scalar::ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                left,
                op: crate::sql::common::BinOp::Or,
                right,
            },
            DataType::Boolean,
            true,
        )
    }

    fn narrow_base_stats(row_count: u64, columns: &[&str]) -> BaseTableStatistics {
        BaseTableStatistics {
            row_count: StatValue::known(
                row_count,
                crate::sql::optimizer::statistics::Confidence::Exact,
                StatsSource::TestFixture,
            ),
            columns: columns
                .iter()
                .map(|column| {
                    (
                        column.to_string(),
                        BaseColumnStatistics {
                            nulls_fraction: StatValue::known(
                                0.0,
                                crate::sql::optimizer::statistics::Confidence::Exact,
                                StatsSource::TestFixture,
                            ),
                            average_row_size: StatValue::known(
                                1.0,
                                crate::sql::optimizer::statistics::Confidence::Exact,
                                StatsSource::TestFixture,
                            ),
                            min_value: StatValue::known(
                                0.0,
                                crate::sql::optimizer::statistics::Confidence::Exact,
                                StatsSource::TestFixture,
                            ),
                            max_value: StatValue::known(
                                100.0,
                                crate::sql::optimizer::statistics::Confidence::Exact,
                                StatsSource::TestFixture,
                            ),
                            ndv: StatValue::missing(
                                crate::sql::optimizer::stats_input::StatsMissingReason::ColumnNotReported(
                                    column.to_string(),
                                ),
                            ),
                        },
                    )
                })
                .collect(),
            source: StatsSource::TestFixture,
        }
    }

    fn plan_contains_mv_scan(node: &OptimizerPhysicalNode, mv_name: &str) -> bool {
        match &node.op {
            Operator::PhysicalScan(scan) if scan.mv_rewritten_from.as_deref() == Some(mv_name) => {
                true
            }
            _ => node
                .children
                .iter()
                .any(|child| plan_contains_mv_scan(child, mv_name)),
        }
    }

    #[test]
    fn optimizer_selects_cheaper_exact_or_mv_candidate() {
        let base_a = int_col(1, "a");
        let base_b = int_col(2, "b");
        let base_v = int_col(3, "v");
        let base_table = iceberg_table("cat", "ns", "t", &["a", "b", "v"]);

        let mut query_scalars = ScalarArena::new();
        let query_a = scalar_col(&mut query_scalars, &base_a);
        let query_b = scalar_col(&mut query_scalars, &base_b);
        let query_predicate = {
            let query_a = scalar_cast_int64(&mut query_scalars, query_a);
            let query_b = scalar_cast_int64(&mut query_scalars, query_b);
            let left = scalar_cmp(
                &mut query_scalars,
                query_a,
                crate::sql::common::BinOp::Gt,
                10,
            );
            let right = scalar_cmp(
                &mut query_scalars,
                query_b,
                crate::sql::common::BinOp::Lt,
                3,
            );
            scalar_or(&mut query_scalars, left, right)
        };
        let scan = OptExpr::leaf(Operator::LogicalScan(
            crate::sql::optimizer::operator::ScanOp {
                database: "ns".to_string(),
                table: base_table.clone(),
                alias: None,
                stats_ref: Some(StatsRef::new(0)),
                columns: vec![base_a.clone(), base_b.clone(), base_v.clone()],
                predicates: vec![query_predicate],
                required_columns: Some(vec!["a".to_string(), "b".to_string()]),
                variant_columns: vec![],
                mv_rewritten_from: None,
            },
        ));
        let project_items = [base_a.clone(), base_b.clone()]
            .into_iter()
            .map(
                |column| crate::sql::optimizer::operator::ScalarProjectItem {
                    expr: scalar_col(&mut query_scalars, &column),
                    output_name: column.name.clone(),
                    output_column_id: column.column_id,
                    expr_display: None,
                },
            )
            .collect();
        let query = OptExpr::new(
            Operator::LogicalProject(crate::sql::optimizer::operator::ProjectOp {
                items: project_items,
                output_qualifier: None,
            }),
            vec![scan],
        );

        let mv_a = int_col(100, "a");
        let mv_b = int_col(101, "b");
        let mv_v = int_col(102, "v");
        let mut mv_scalars = ScalarArena::new();
        let mv_a_ref = scalar_col(&mut mv_scalars, &mv_a);
        let mv_b_ref = scalar_col(&mut mv_scalars, &mv_b);
        let mv_predicate = {
            let mv_a_ref = scalar_cast_int64(&mut mv_scalars, mv_a_ref);
            let mv_b_ref = scalar_cast_int64(&mut mv_scalars, mv_b_ref);
            let left = scalar_cmp(&mut mv_scalars, mv_a_ref, crate::sql::common::BinOp::Gt, 10);
            let right = scalar_cmp(&mut mv_scalars, mv_b_ref, crate::sql::common::BinOp::Lt, 3);
            scalar_or(&mut mv_scalars, left, right)
        };
        let mv_scan = OptExpr::leaf(Operator::LogicalScan(
            crate::sql::optimizer::operator::ScanOp {
                database: "ns".to_string(),
                table: iceberg_table("cat", "ns", "t", &["a", "b", "v"]),
                alias: None,
                stats_ref: None,
                columns: vec![mv_a.clone(), mv_b.clone(), mv_v.clone()],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            },
        ));
        let mv_expr = OptExpr::new(
            Operator::LogicalFilter(crate::sql::optimizer::operator::FilterOp {
                predicate: mv_predicate,
            }),
            vec![mv_scan],
        );
        let mv_desc = cascades_rules::mv_rewrite::descriptor::SpjgDescriptor::from_opt_expr(
            &mv_expr,
            &mut mv_scalars,
        )
        .expect("mv descriptor");
        let candidate = cascades_rules::mv_rewrite::MvRewriteCandidate {
            mv_name: "or_mv".to_string(),
            mv: mv_desc,
            mv_scalars,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "or_mv", &["a", "b", "v"]),
            target_stats_ref: StatsRef::new(1),
        };

        let mut stats = QueryStatsSnapshot::empty();
        stats.insert(
            StatsRef::new(0),
            "cat.ns.t",
            narrow_base_stats(2_400, &["a", "b", "v"]),
        );
        stats.insert(
            StatsRef::new(1),
            "cat.ns.or_mv",
            narrow_base_stats(1_680, &["a", "b", "v"]),
        );

        let physical = optimize(
            query,
            query_scalars,
            &stats,
            ColumnRefFactory::new(),
            None,
            vec![candidate],
        )
        .expect("optimize");

        assert!(
            plan_contains_mv_scan(&physical, "or_mv"),
            "optimizer should select the cheaper exact OR MV alternative, got {physical:#?}"
        );
    }

    struct AlwaysSomeProvider;
    impl QueryDictionaryProvider for AlwaysSomeProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            Ok(Some(DictionarySnapshot {
                dictionary_id: 1,
                owner: DictionaryOwner::StarRocksTable {
                    database: "db".to_string(),
                    table: "t".to_string(),
                    db_id: 1,
                    table_id: 2,
                },
                column_id: Some(10),
                column_name: column_name.to_string(),
                data_type: DataType::Utf8,
                version: 1,
                watermark: DictionaryWatermark::Iceberg {
                    snapshot_id: None,
                    schema_id: 0,
                },
                values: vec![DictionaryValue {
                    id: 1,
                    bytes: b"a".to_vec(),
                }],
                null_id: 0,
                state: DictionaryState::Active,
                order_preserving: true,
            }))
        }
    }

    struct AlwaysNoneProvider;
    impl QueryDictionaryProvider for AlwaysNoneProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            _column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            Ok(None)
        }
    }

    /// Build the minimal Aggregate(Scan) shape that the rewrite rule
    /// can act on: GROUP BY on the string column `s`.
    fn agg_over_string_scan() -> LogicalPlanNode {
        // Use deterministic non-UNSET column IDs so that intern_typed succeeds.
        let s_id = ColumnId::new_for_test(1);
        let cnt_id = ColumnId::new_for_test(2);
        let table = TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        let s_col = OutputColumn {
            column_id: s_id,
            name: "s".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            is_internal: false,
        };
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: table,
                alias: None,
                columns: vec![s_col.clone()],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let s_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: s_id,
                qualifier: None,
                column: "s".to_string(),
            },
            data_type: DataType::Utf8,
            nullable: false,
        };
        LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![s_ref],
                aggregates: vec![AggregateCall {
                    name: "count".to_string(),
                    args: vec![],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: cnt_id,
                }],
                output_columns: vec![
                    s_col,
                    OutputColumn {
                        column_id: cnt_id,
                        name: "cnt".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                already_pushed: false,
            }),
            vec![scan],
            None,
        )
    }

    /// Run the query rewrite pipeline against `agg_over_string_scan`
    /// using the same precedence rule that `optimize()` applies.
    fn rewrite_with(parameter: Option<Arc<dyn QueryDictionaryProvider>>) -> LogicalPlanNode {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        if let Some(provider) = resolve_dictionary_provider(parameter) {
            ctx.set_dictionary_provider(provider);
        }
        ctx.set_query_stats_input(
            crate::sql::optimizer::stats_input::OptimizerStatsInput::from_legacy_table_stats_for_migration(
                &HashMap::new(),
            ),
        );
        let pipeline = query_rewrite_pipeline();
        let mut scalars = ScalarArena::new();
        let opt_plan = logical_plan_to_opt_expr(&agg_over_string_scan(), &mut scalars);
        let arena_rc = Rc::new(RefCell::new(scalars));
        ctx.set_scalar_arena(arena_rc.clone());
        let opt_result = pipeline.rewrite(opt_plan, &mut ctx).unwrap();
        let arena = arena_rc.borrow();
        opt_expr_to_logical_plan(opt_result, &arena)
    }

    fn assert_no_native_dict_rewrite(rewritten: &LogicalPlanNode, context: &str) {
        let LogicalPlanKind::Aggregate(_) = &rewritten.kind else {
            panic!("{context}: expected aggregate root, got {rewritten:?}")
        };
        let LogicalPlanKind::Scan(_) = &rewritten.unary_input().kind else {
            panic!(
                "{context}: expected scan child, got {:?}",
                rewritten.unary_input()
            )
        };
    }

    #[test]
    fn optimize_with_none_provider_does_not_rewrite() {
        // Sanity: ensure TLS is unset for this thread.
        assert!(current_dictionary_provider().is_none());
        let rewritten = rewrite_with(None);
        assert_no_native_dict_rewrite(&rewritten, "no provider");
    }

    #[test]
    fn optimize_with_tls_provider_does_not_rewrite_after_legacy_removal() {
        let provider: Arc<dyn QueryDictionaryProvider> = Arc::new(AlwaysSomeProvider);
        let rewritten = with_dictionary_provider(provider, || rewrite_with(None));
        assert_no_native_dict_rewrite(&rewritten, "TLS provider");
    }

    #[test]
    fn optimize_parameter_provider_does_not_rewrite_after_legacy_removal() {
        let tls_provider: Arc<dyn QueryDictionaryProvider> = Arc::new(AlwaysSomeProvider);
        let param_provider: Arc<dyn QueryDictionaryProvider> = Arc::new(AlwaysNoneProvider);
        let rewritten =
            with_dictionary_provider(tls_provider, || rewrite_with(Some(param_provider.clone())));
        assert_no_native_dict_rewrite(&rewritten, "parameter provider");
    }

    #[test]
    fn optimize_implements_assert_one_row() {
        use std::collections::HashMap;

        use crate::sql::column_id::ColumnRefFactory;
        use crate::sql::planner::plan::{
            LogicalAssertOneRowNode, LogicalPlanKind, LogicalPlanNode, LogicalValuesNode,
        };

        let plan = LogicalPlanNode::new(
            LogicalPlanKind::AssertOneRow(LogicalAssertOneRowNode::global_at_most_one("select 1")),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Values(LogicalValuesNode {
                    rows: vec![],
                    columns: vec![],
                }),
                vec![],
                None,
            )],
            None,
        );
        let factory = ColumnRefFactory::new();
        let physical = optimize_logical(plan, &HashMap::new(), factory, None, Vec::new())
            .expect("optimize assert one row");
        let physical_debug = format!("{physical:?}");
        assert!(physical_debug.contains("PhysicalAssertOneRow"));
    }

    #[test]
    fn optimize_rejects_residual_apply_when_rule_disabled() {
        use std::collections::{HashMap, HashSet};

        use arrow::datatypes::DataType;

        use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
        use crate::sql::column_id::{ColumnId, ColumnRefFactory};
        use crate::sql::planner::plan::{
            ApplyKind, LogicalApplyNode, LogicalPlanKind, LogicalPlanNode, LogicalValuesNode,
        };

        let values = || {
            LogicalPlanNode::new(
                LogicalPlanKind::Values(LogicalValuesNode {
                    rows: vec![],
                    columns: vec![],
                }),
                vec![],
                None,
            )
        };
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(5),
                        qualifier: None,
                        column: "sq".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                },
                output_column: OutputColumn {
                    column_id: ColumnId(5),
                    name: "sq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: ColumnId(5),
                correlation_column_ids: vec![],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![values(), values()],
            None,
        );

        // Disable ALL rules that could eliminate the Apply: ApplyException
        // (the SubqueryRewrite guard) AND the M1b decorrelation rules
        // (ScalarApplyToJoin, PushDownApplyAggFilter, PushDownApplyFilter).
        // With all three disabled, the Apply survives the SubqueryRewrite stage
        // and the non-disableable optimize() backstop (find_residual_apply) fires.
        let settings = crate::sql::optimizer::options::SessionOptimizerSettings {
            disabled_rules: vec![
                "ApplyException".to_string(),
                "ScalarApplyToJoin".to_string(),
                "PushDownApplyAggFilter".to_string(),
                "PushDownApplyFilter".to_string(),
            ],
            ..Default::default()
        };
        let err = crate::sql::optimizer::options::with_session_optimizer_settings(settings, || {
            optimize_logical(
                plan,
                &HashMap::new(),
                ColumnRefFactory::new(),
                None,
                Vec::new(),
            )
        })
        .expect_err("backstop must reject the residual apply");
        assert!(
            err.contains("subquery decorrelation failed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn optimize_with_root_distribution_overrides_default_gather_root() {
        use crate::sql::analysis::ExprKind;
        use crate::sql::catalog::{CatalogProvider, ColumnDef, ScanSource, TableDef};
        use crate::sql::column_id::ColumnRefFactory;
        use crate::sql::optimizer::property::DistributionSpec;
        use crate::sql::planner::plan::LogicalPlanKind;

        struct MinimalCatalog;
        impl CatalogProvider for MinimalCatalog {
            fn get_table(&self, _db: &str, table: &str) -> Result<TableDef, String> {
                match table {
                    "t1" => Ok(TableDef {
                        name: table.to_string(),
                        columns: vec![ColumnDef {
                            name: "k1".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        }],
                        iceberg_row_lineage_metadata_columns: vec![],
                        source: ScanSource::StarRocks {
                            db_id: 0,
                            table_id: 0,
                        },
                    }),
                    other => Err(format!("table not found: {other}")),
                }
            }
        }

        let sql = "SELECT k1 FROM t1";
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let mut ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).expect("parse query");
        let stmt = ast.pop().expect("expected a statement");
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => panic!("expected a query"),
        };
        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &MinimalCatalog, "default").expect("analyze");
        let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)
            .expect("plan query");
        let hash_col = match &logical.kind {
            LogicalPlanKind::Project(project) => match &project.items[0].expr.kind {
                ExprKind::ColumnRef { column_id, .. } => *column_id,
                other => panic!("expected column projection, got {other:?}"),
            },
            other => panic!("expected project root, got {other:?}"),
        };

        let default_physical = optimize_logical(
            logical.clone(),
            &HashMap::new(),
            ColumnRefFactory::new(),
            None,
            Vec::new(),
        )
        .expect("default optimize");
        assert_root_distribution(&default_physical, &DistributionSpec::Gather);

        let root_distribution = DistributionSpec::shuffle_agg([hash_col]);
        let physical = optimize_logical_with_root_distribution(
            logical,
            &HashMap::new(),
            ColumnRefFactory::new(),
            root_distribution.clone(),
        )
        .expect("optimize with root distribution");
        assert_root_distribution(&physical, &root_distribution);
    }

    fn assert_root_distribution(
        physical: &crate::sql::optimizer::physical_tree::OptimizerPhysicalNode,
        expected: &crate::sql::optimizer::property::DistributionSpec,
    ) {
        match &physical.op {
            crate::sql::optimizer::operator::Operator::PhysicalDistribution(op) => {
                assert_eq!(&op.spec, expected);
            }
            other => panic!("expected root PhysicalDistribution, got {other:?}"),
        }
    }

    #[test]
    fn optimize_preserves_ranking_window_partition_topn_sort() {
        use crate::exec::node::sort::SortTopNType;
        use crate::sql::catalog::{CatalogProvider, ColumnDef, ScanSource, TableDef};
        use crate::sql::optimizer::operator::Operator;
        use crate::sql::optimizer::physical_tree::OptimizerPhysicalNode;

        struct RankingCatalog;
        impl CatalogProvider for RankingCatalog {
            fn get_table(&self, _db: &str, table: &str) -> Result<TableDef, String> {
                match table {
                    "rw_sales" => Ok(TableDef {
                        name: table.to_string(),
                        columns: vec![
                            ColumnDef {
                                name: "region".to_string(),
                                data_type: arrow::datatypes::DataType::Utf8,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                            ColumnDef {
                                name: "amount".to_string(),
                                data_type: arrow::datatypes::DataType::Int32,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                        ],
                        iceberg_row_lineage_metadata_columns: vec![],
                        source: ScanSource::StarRocks {
                            db_id: 0,
                            table_id: 0,
                        },
                    }),
                    other => Err(format!("table not found: {other}")),
                }
            }
        }

        fn has_rank_partition_topn_sort(plan: &OptimizerPhysicalNode) -> bool {
            if let Operator::PhysicalSort(sort) = &plan.op
                && sort.partition_limit == Some(2)
                && sort.topn_type == Some(SortTopNType::Rank)
            {
                return true;
            }
            plan.children.iter().any(has_rank_partition_topn_sort)
        }

        fn logical_has_rank_partition_topn_sort(
            plan: &crate::sql::planner::plan::LogicalPlanNode,
        ) -> bool {
            if let crate::sql::planner::plan::LogicalPlanKind::Sort(sort) = &plan.kind
                && sort.partition_limit == Some(2)
                && sort.topn_type == Some(SortTopNType::Rank)
            {
                return true;
            }
            plan.children
                .iter()
                .any(logical_has_rank_partition_topn_sort)
        }

        let sql = "
            SELECT *
            FROM (
                SELECT region, amount,
                       rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rk
                FROM rw_sales
            ) t
            WHERE rk <= 2
            ORDER BY region, amount DESC
        ";
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let mut ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).expect("parse query");
        let stmt = ast.pop().expect("expected a statement");
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => panic!("expected a query"),
        };
        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &RankingCatalog, "default").expect("analyze");
        let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)
            .expect("plan query");
        let mut scalars = crate::sql::optimizer::scalar::ScalarArena::new();
        let opt_plan = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            &logical,
            &mut scalars,
        )
        .expect("logical to opt expr");
        let mut rewrite_ctx =
            crate::sql::optimizer::rewrite::context::RewriteContext::for_query(Vec::new());
        rewrite_ctx.set_query_stats_input(
            crate::sql::optimizer::stats_input::OptimizerStatsInput::from_legacy_table_stats_for_migration(
                &HashMap::new(),
            ),
        );
        let arena = std::rc::Rc::new(std::cell::RefCell::new(scalars));
        rewrite_ctx.set_scalar_arena(std::rc::Rc::clone(&arena));
        let rewritten_expr = crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline()
            .rewrite(opt_plan, &mut rewrite_ctx)
            .expect("rewrite pipeline");
        let rewritten_logical =
            crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan(
                rewritten_expr,
                &arena.borrow(),
            );
        assert!(
            logical_has_rank_partition_topn_sort(&rewritten_logical),
            "expected query rewrite pipeline to set ranking partition-topn, trace: {:#?}, got: {rewritten_logical:#?}",
            rewrite_ctx.trace().events()
        );
        let physical = optimize_logical(logical, &HashMap::new(), factory, None, Vec::new())
            .expect("optimize ranking window");

        assert!(
            has_rank_partition_topn_sort(&physical),
            "expected RankingWindowPredicatePushdown to survive into the physical plan, got: {physical:#?}"
        );
    }

    /// End-to-end proof: the full analyze → plan_query → optimize chain in
    /// the Apply framework turns a scalar subquery into a `LogicalPlanKind::Apply`, which
    /// M1b's `SubqueryRewrite` decorrelation rules (PushDownApplyAggFilter +
    /// ScalarApplyToJoin) rewrite into a LEFT OUTER JOIN over a vector
    /// aggregate. The optimized physical plan contains a HashJoin (or NestLoop)
    /// and no residual Apply node or "subquery decorrelation failed" error.
    #[test]
    fn scalar_subquery_decorrelates_to_join() {
        use crate::sql::catalog::{CatalogProvider, ColumnDef, ScanSource, TableDef};
        use crate::sql::column_id::ColumnRefFactory;

        // Minimal catalog providing t1(k1, k2) and t2(k1, k2) — the same
        // shape the planner and analyzer test modules use.
        struct MinimalCatalog;
        impl CatalogProvider for MinimalCatalog {
            fn get_table(&self, _db: &str, table: &str) -> Result<TableDef, String> {
                match table {
                    "t1" | "t2" => Ok(TableDef {
                        name: table.to_string(),
                        columns: vec![
                            ColumnDef {
                                name: "k1".to_string(),
                                data_type: arrow::datatypes::DataType::Int64,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                            ColumnDef {
                                name: "k2".to_string(),
                                data_type: arrow::datatypes::DataType::Int64,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                        ],
                        iceberg_row_lineage_metadata_columns: vec![],
                        source: ScanSource::StarRocks {
                            db_id: 0,
                            table_id: 0,
                        },
                    }),
                    other => Err(format!("table not found: {other}")),
                }
            }
        }

        // A correlated WHERE-clause scalar subquery: aggregate inner with an
        // outer-column equality predicate. M1b decorrelates this shape into a
        // LEFT OUTER JOIN over a vector aggregate.
        let sql = "SELECT k1 FROM t1 WHERE k1 = (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1)";

        // parse → analyze
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let mut ast = sqlparser::parser::Parser::parse_sql(&dialect, sql)
            .map_err(|e| e.to_string())
            .expect("parse must succeed");
        let stmt = ast.pop().expect("expected a statement");
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => panic!("expected a query"),
        };
        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &MinimalCatalog, "default")
                .expect("analyze with apply framework must succeed");

        // plan_query: turns the ApplyScalarSpec into LogicalPlanKind::Apply.
        let plan = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)
            .expect("plan_query with apply framework must succeed");

        // optimize: M1b's decorrelation rules must rewrite the Apply to a
        // join; no ApplyException error, no residual Apply.
        let physical = optimize_logical(
            plan,
            &HashMap::new(),
            ColumnRefFactory::new(),
            None,
            Vec::new(),
        )
        .expect("optimize must succeed: M1b decorrelates correlated aggregate scalar subquery");

        let physical_debug = format!("{physical:?}");
        // The correlated aggregate scalar path becomes a LEFT OUTER JOIN (HashJoin or NestLoop).
        assert!(
            physical_debug.contains("HashJoin") || physical_debug.contains("NestLoop"),
            "expected a join in the decorrelated plan; got: {physical_debug}"
        );
        // No residual Apply must survive.
        assert!(
            !physical_debug.contains("Apply"),
            "residual Apply must not appear in the decorrelated plan; got: {physical_debug}"
        );
    }
}
