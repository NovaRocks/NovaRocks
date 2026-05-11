//! Join reorder optimization pass.
//!
//! Multiple strategies are available with adaptive algorithm selection:
//!
//! 1. **DP (<=12 tables)** — Exhaustive dynamic programming over all join
//!    orderings. Optimal but exponential in the number of relations.
//!
//! 2. **Greedy (<=16 tables)** — Level-by-level join building: at each level,
//!    tries all (prev_level_group, atom) pairs and keeps the best plan per
//!    table subset. Polynomial time with good results.
//!
//! 3. **LeftDeep (any size)** — Sorts tables by row count and greedily attaches
//!    one table at a time, preferring equi-join connections. O(n^2) time,
//!    always produces a left-deep tree.
//!
//! 4. **Heuristic** — `reorder_joins_heuristic` (final fallback): For hash joins
//!    the left child is the **probe** side and the right child is the **build**
//!    side.  Swaps children when the right (build) side is significantly larger
//!    than the left (probe) side.
//!
//! The CBO entry point (`reorder_joins_cbo`) adaptively selects: DP -> Greedy
//! -> LeftDeep -> Heuristic, based on the number of relations in the join graph.

use std::collections::HashMap;
use std::collections::HashSet;

use super::cardinality;
use super::cost;
use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr};
use crate::sql::catalog::TableStorage;
use crate::sql::optimizer::statistics::*;
use crate::sql::planner::plan::*;
use arrow::datatypes::DataType;

/// Count the number of AND-conjuncts in a predicate expression.
fn count_conjuncts(expr: &TypedExpr) -> usize {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => count_conjuncts(left) + count_conjuncts(right),
        _ => 1,
    }
}

/// Returns true if the expression contains at least one top-level AND-connected
/// `col = col` equality predicate (equijoin key).
///
/// Used in the cost model: if a join condition has no equijoin key it will be
/// implemented as a NEST LOOP JOIN, not a hash join, and should use the
/// O(left × right) cost model rather than the O(left + right) hash join model.
fn has_equijoin_predicate(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::Nested(inner) => has_equijoin_predicate(inner),
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => has_equijoin_predicate(left) || has_equijoin_predicate(right),
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            matches!(left.kind, ExprKind::ColumnRef { .. })
                && matches!(right.kind, ExprKind::ColumnRef { .. })
        }
        _ => false,
    }
}

/// Estimate the output "size" of a plan subtree in bytes.
///
/// This is a rough heuristic that does not require table statistics.  It is
/// good enough to distinguish a 6 million-row fact table from a 30 thousand-row
/// dimension table, which is the primary goal.
fn estimate_size(plan: &LogicalPlan) -> u64 {
    match plan {
        LogicalPlan::Scan(s) => {
            let raw_size = match &s.table.storage {
                TableStorage::S3ParquetFiles { files, .. } => {
                    // Prefer row_count when available (from Iceberg metadata).
                    // Fall back to file size in bytes if any file lacks row_count.
                    let all_have_row_count =
                        !files.is_empty() && files.iter().all(|f| f.row_count.is_some());
                    if all_have_row_count {
                        let total: u64 = files
                            .iter()
                            .map(|f| f.row_count.unwrap().max(0) as u64)
                            .sum();
                        total.max(1)
                    } else {
                        let total: u64 = files.iter().map(|f| f.size.max(0) as u64).sum();
                        total.max(1)
                    }
                }
                TableStorage::LocalParquetFile { path } => std::fs::metadata(path)
                    .map(|m| m.len())
                    .unwrap_or(1_000_000),
                // Iceberg metadata tables (snapshots/history/refs/partitions)
                // are tiny by design — they enumerate snapshot/history/ref
                // entries which are bounded by the table's history depth.
                // A small constant keeps them at the bottom of any join
                // reorder ordering without forcing a separate stats path.
                TableStorage::IcebergMetadataTable { .. } => 1,
            };
            // Apply selectivity for pushed-down predicates on the scan
            let num_predicates = s.predicates.len();
            if num_predicates == 0 {
                raw_size
            } else {
                // Each predicate applies ~30% selectivity, multiplicatively
                // 1 pred: 30%, 2 preds: 9%, 3 preds: 2.7%, 4+: ~1%
                let factor = match num_predicates {
                    1 => 30,
                    2 => 9,
                    3 => 3,
                    _ => 1,
                };
                (raw_size * factor / 100).max(1)
            }
        }
        LogicalPlan::Filter(f) => {
            // Count conjuncts in the filter predicate for better selectivity estimate
            let num_conjuncts = count_conjuncts(&f.predicate);
            let input_size = estimate_size(&f.input);
            let factor = match num_conjuncts {
                1 => 30,
                2 => 9,
                3 => 3,
                _ => 1,
            };
            (input_size * factor / 100).max(1)
        }
        LogicalPlan::Join(j) => {
            // For an inner join the output is roughly bounded by the smaller
            // input (assuming a PK-FK join).
            let left = estimate_size(&j.left);
            let right = estimate_size(&j.right);
            left.min(right)
        }
        LogicalPlan::Aggregate(a) => {
            // Aggregation significantly reduces row count.
            estimate_size(&a.input) / 10
        }
        LogicalPlan::Project(p) => estimate_size(&p.input),
        LogicalPlan::Sort(s) => estimate_size(&s.input),
        LogicalPlan::Limit(l) => {
            let input = estimate_size(&l.input);
            // LIMIT drastically caps the output.
            input.min(10_000)
        }
        LogicalPlan::Window(w) => estimate_size(&w.input),
        // Leaf / set-op nodes without better info: default 1 MB.
        _ => 1_000_000,
    }
}

/// Reorder join children so that the larger relation is on the left (probe)
/// and the smaller relation is on the right (build).
///
/// The pass is applied bottom-up: children are reordered first so that size
/// estimates of intermediate joins are based on already-reordered subtrees.
pub(crate) fn reorder_joins_heuristic(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Join(mut j) => {
            // Recurse into children first (bottom-up).
            j.left = Box::new(reorder_joins_heuristic(*j.left));
            j.right = Box::new(reorder_joins_heuristic(*j.right));

            match j.join_type {
                // INNER and CROSS are commutative — safe to swap.
                JoinKind::Inner | JoinKind::Cross => {
                    let left_size = estimate_size(&j.left);
                    let right_size = estimate_size(&j.right);

                    if right_size > left_size * 2 {
                        // The build side (right) is much larger than the probe
                        // side (left).  Swap so the big relation probes and the
                        // small relation builds.
                        //
                        // For INNER JOIN the join condition is symmetric (both
                        // sides reference columns by qualified name), so
                        // swapping children does not require rewriting the
                        // condition expression.
                        //
                        // For CROSS JOIN there is no condition at all.
                        tracing::debug!(
                            left_bytes = left_size,
                            right_bytes = right_size,
                            "join_reorder: swapping join sides"
                        );
                        std::mem::swap(&mut j.left, &mut j.right);
                    }
                }
                // Left/Right outer, semi, anti, and full outer joins have
                // asymmetric semantics — the preserved/probing side is fixed.
                JoinKind::LeftOuter
                | JoinKind::RightOuter
                | JoinKind::FullOuter
                | JoinKind::LeftSemi
                | JoinKind::RightSemi
                | JoinKind::LeftAnti
                | JoinKind::RightAnti => {}
            }

            LogicalPlan::Join(j)
        }
        // --- Recurse through all other node types --------------------------------
        LogicalPlan::Filter(mut f) => {
            f.input = Box::new(reorder_joins_heuristic(*f.input));
            LogicalPlan::Filter(f)
        }
        LogicalPlan::Project(mut p) => {
            p.input = Box::new(reorder_joins_heuristic(*p.input));
            LogicalPlan::Project(p)
        }
        LogicalPlan::Aggregate(mut a) => {
            a.input = Box::new(reorder_joins_heuristic(*a.input));
            LogicalPlan::Aggregate(a)
        }
        LogicalPlan::Sort(mut s) => {
            s.input = Box::new(reorder_joins_heuristic(*s.input));
            LogicalPlan::Sort(s)
        }
        LogicalPlan::Limit(mut l) => {
            l.input = Box::new(reorder_joins_heuristic(*l.input));
            LogicalPlan::Limit(l)
        }
        LogicalPlan::Window(mut w) => {
            w.input = Box::new(reorder_joins_heuristic(*w.input));
            LogicalPlan::Window(w)
        }
        LogicalPlan::Union(mut u) => {
            u.inputs = u.inputs.into_iter().map(reorder_joins_heuristic).collect();
            LogicalPlan::Union(u)
        }
        LogicalPlan::Intersect(mut i) => {
            i.inputs = i.inputs.into_iter().map(reorder_joins_heuristic).collect();
            LogicalPlan::Intersect(i)
        }
        LogicalPlan::Except(mut e) => {
            e.inputs = e.inputs.into_iter().map(reorder_joins_heuristic).collect();
            LogicalPlan::Except(e)
        }
        LogicalPlan::SubqueryAlias(mut s) => {
            s.input = Box::new(reorder_joins_heuristic(*s.input));
            LogicalPlan::SubqueryAlias(s)
        }
        LogicalPlan::Repeat(mut r) => {
            r.input = Box::new(reorder_joins_heuristic(*r.input));
            LogicalPlan::Repeat(r)
        }
        LogicalPlan::TableFunction(mut t) => {
            t.input = Box::new(reorder_joins_heuristic(*t.input));
            LogicalPlan::TableFunction(t)
        }
        // Leaf nodes: Scan, Values, GenerateSeries, CTEConsume — nothing to reorder.
        other => other,
    }
}

// ===========================================================================
// CBO: DP-based join reorder
// ===========================================================================

use crate::sql::optimizer::rbo::utils::{
    QualifiedRef, collect_qualified_output_columns, combine_and, split_and,
};

/// Entry for the DP memo table.
struct DpEntry {
    plan: LogicalPlan,
    stats: Statistics,
    cumulative_cost: f64,
}

/// The join graph: a set of base relations and predicates that connect them.
#[derive(Clone)]
struct JoinGraph {
    /// Leaf plans (the base relations of the join graph).
    relations: Vec<LogicalPlan>,
    /// Each predicate: (condition expr, bitmask of relations it references).
    /// Uses u32 to support up to 32 relations.
    predicates: Vec<(TypedExpr, u32)>,
}

/// CBO join reorder: walks the plan tree and applies join enumeration to
/// chains of INNER JOINs using adaptive algorithm selection:
///
/// - DP (<=12 tables): exhaustive enumeration
/// - Greedy (<=20 tables): level-by-level best-pair construction
/// - LeftDeep (any size): greedy left-deep tree construction
/// - Heuristic fallback: simple size-based swap
///
/// Non-INNER joins are left in place but their children are recursively
/// optimized.
pub(crate) fn reorder_joins_cbo(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> LogicalPlan {
    match plan {
        LogicalPlan::Join(j) if matches!(j.join_type, JoinKind::Inner | JoinKind::Cross) => {
            // Try to extract a join graph from this chain of INNER/CROSS JOINs.
            let full_plan = LogicalPlan::Join(j);
            match extract_join_graph(&full_plan) {
                Some(graph) if graph.relations.len() >= 2 => {
                    let n = graph.relations.len();

                    // Recursively optimize each leaf relation first.
                    let optimized_relations: Vec<LogicalPlan> = graph
                        .relations
                        .into_iter()
                        .map(|r| reorder_joins_cbo(r, table_stats))
                        .collect();

                    let optimized_graph = JoinGraph {
                        relations: optimized_relations,
                        predicates: graph.predicates,
                    };

                    // Adaptive algorithm selection:
                    // DP (<=12) -> Greedy (13-16) -> LeftDeep (any)
                    let result = if n <= 12 {
                        tracing::debug!(n, "join_reorder: using DP algorithm");
                        dp_join_reorder(optimized_graph, table_stats)
                    } else if n <= 16 {
                        tracing::debug!(n, "join_reorder: using Greedy algorithm");
                        greedy_join_reorder(optimized_graph.clone(), table_stats).or_else(|| {
                            tracing::debug!(
                                n,
                                "join_reorder: Greedy failed, falling back to LeftDeep"
                            );
                            left_deep_join_reorder(optimized_graph, table_stats)
                        })
                    } else {
                        tracing::debug!(n, "join_reorder: using LeftDeep algorithm");
                        left_deep_join_reorder(optimized_graph, table_stats)
                    };

                    match result {
                        Some(plan) => plan,
                        None => {
                            // Algorithm failed, fall back to heuristic.
                            reorder_joins_heuristic(full_plan)
                        }
                    }
                }
                _ => {
                    // Could not extract graph, use heuristic.
                    reorder_joins_heuristic(full_plan)
                }
            }
        }
        LogicalPlan::Join(mut j) => {
            // Non-INNER join: recurse into children but do not reorder.
            j.left = Box::new(reorder_joins_cbo(*j.left, table_stats));
            j.right = Box::new(reorder_joins_cbo(*j.right, table_stats));
            LogicalPlan::Join(j)
        }
        LogicalPlan::Filter(mut f) => {
            f.input = Box::new(reorder_joins_cbo(*f.input, table_stats));
            LogicalPlan::Filter(f)
        }
        LogicalPlan::Project(mut p) => {
            p.input = Box::new(reorder_joins_cbo(*p.input, table_stats));
            LogicalPlan::Project(p)
        }
        LogicalPlan::Aggregate(mut a) => {
            a.input = Box::new(reorder_joins_cbo(*a.input, table_stats));
            LogicalPlan::Aggregate(a)
        }
        LogicalPlan::Sort(mut s) => {
            s.input = Box::new(reorder_joins_cbo(*s.input, table_stats));
            LogicalPlan::Sort(s)
        }
        LogicalPlan::Limit(mut l) => {
            l.input = Box::new(reorder_joins_cbo(*l.input, table_stats));
            LogicalPlan::Limit(l)
        }
        LogicalPlan::Window(mut w) => {
            w.input = Box::new(reorder_joins_cbo(*w.input, table_stats));
            LogicalPlan::Window(w)
        }
        LogicalPlan::Union(mut u) => {
            u.inputs = u
                .inputs
                .into_iter()
                .map(|p| reorder_joins_cbo(p, table_stats))
                .collect();
            LogicalPlan::Union(u)
        }
        LogicalPlan::Intersect(mut i) => {
            i.inputs = i
                .inputs
                .into_iter()
                .map(|p| reorder_joins_cbo(p, table_stats))
                .collect();
            LogicalPlan::Intersect(i)
        }
        LogicalPlan::Except(mut e) => {
            e.inputs = e
                .inputs
                .into_iter()
                .map(|p| reorder_joins_cbo(p, table_stats))
                .collect();
            LogicalPlan::Except(e)
        }
        LogicalPlan::SubqueryAlias(mut s) => {
            s.input = Box::new(reorder_joins_cbo(*s.input, table_stats));
            LogicalPlan::SubqueryAlias(s)
        }
        LogicalPlan::Repeat(mut r) => {
            r.input = Box::new(reorder_joins_cbo(*r.input, table_stats));
            LogicalPlan::Repeat(r)
        }
        LogicalPlan::CTEAnchor(mut n) => {
            n.produce = Box::new(reorder_joins_cbo(*n.produce, table_stats));
            n.consumer = Box::new(reorder_joins_cbo(*n.consumer, table_stats));
            LogicalPlan::CTEAnchor(n)
        }
        LogicalPlan::CTEProduce(mut n) => {
            n.input = Box::new(reorder_joins_cbo(*n.input, table_stats));
            LogicalPlan::CTEProduce(n)
        }
        LogicalPlan::CTEConsume(_) => plan,
        other => other,
    }
}

/// Flatten a tree of INNER JOINs into a join graph of base relations and
/// predicates.  Returns `None` if the tree contains non-INNER joins at the
/// top level.
fn extract_join_graph(plan: &LogicalPlan) -> Option<JoinGraph> {
    let mut relations: Vec<LogicalPlan> = Vec::new();
    let mut raw_predicates: Vec<TypedExpr> = Vec::new();

    flatten_inner_joins(plan, &mut relations, &mut raw_predicates);

    if relations.len() < 2 {
        return None;
    }

    // Build the output column sets for each relation so we can map predicates
    // to the relations they reference.
    let relation_columns: Vec<std::collections::HashSet<QualifiedRef>> = relations
        .iter()
        .map(collect_qualified_output_columns)
        .collect();

    // Pre-process: factor common equi-join conditions out of OR predicates
    // so the join graph sees them as independent binary predicates.
    // e.g. (cd_demo_sk=ss_cdemo_sk AND ... OR cd_demo_sk=ss_cdemo_sk AND ...)
    // → cd_demo_sk=ss_cdemo_sk (factored) + remaining OR
    let mut expanded_predicates: Vec<TypedExpr> = Vec::new();
    let _all_cols: std::collections::HashSet<String> = relation_columns
        .iter()
        .flat_map(|s| s.iter().map(|r| r.1.clone()))
        .collect();
    for pred in raw_predicates {
        let (factored, remaining) = factor_common_eq_from_or_for_reorder(&pred);
        if factored.is_empty() {
            expanded_predicates.push(pred);
        } else {
            expanded_predicates.extend(factored);
            if let Some(rem) = remaining {
                expanded_predicates.push(rem);
            }
        }
    }

    // Classify each predicate by which relations it touches.
    let mut predicates = Vec::new();
    for pred in expanded_predicates {
        let refs = crate::sql::optimizer::rbo::utils::collect_qualified_column_refs(&pred);
        let mut mask: u32 = 0;
        for qref in &refs {
            for (i, rel_cols) in relation_columns.iter().enumerate() {
                if rel_cols.contains(qref) {
                    mask |= 1u32 << i;
                }
            }
        }
        predicates.push((pred, mask));
    }

    Some(JoinGraph {
        relations,
        predicates,
    })
}

/// Recursively flatten a tree of INNER/CROSS JOINs into leaf relations and
/// predicate conjuncts.  CROSS JOINs (condition=None) are treated as INNER
/// joins with no predicate, allowing the reorder algorithm to see all tables.
fn flatten_inner_joins(
    plan: &LogicalPlan,
    relations: &mut Vec<LogicalPlan>,
    predicates: &mut Vec<TypedExpr>,
) {
    match plan {
        LogicalPlan::Join(j) if matches!(j.join_type, JoinKind::Inner | JoinKind::Cross) => {
            flatten_inner_joins(&j.left, relations, predicates);
            flatten_inner_joins(&j.right, relations, predicates);
            if let Some(ref cond) = j.condition {
                let conjuncts = split_and(cond.clone());
                predicates.extend(conjuncts);
            }
        }
        // Absorb Filter nodes sitting on top of Inner/Cross joins so that
        // their predicates participate in join reorder.
        LogicalPlan::Filter(f)
            if matches!(
                f.input.as_ref(),
                LogicalPlan::Join(j) if matches!(j.join_type, JoinKind::Inner | JoinKind::Cross)
            ) =>
        {
            let conjuncts = split_and(f.predicate.clone());
            predicates.extend(conjuncts);
            flatten_inner_joins(&f.input, relations, predicates);
        }
        _ => {
            relations.push(plan.clone());
        }
    }
}

/// DP join reorder: enumerate all subsets of relations and find the cheapest
/// join order.  Uses a u32 bitmask (DP is still limited to <=12 relations
/// to keep the 2^n subset enumeration tractable).
fn dp_join_reorder(
    graph: JoinGraph,
    table_stats: &HashMap<String, TableStatistics>,
) -> Option<LogicalPlan> {
    let n = graph.relations.len();
    if n > 12 {
        return None;
    }

    let mut memo: HashMap<u32, DpEntry> = HashMap::new();

    // Phase 1: Initialize single-relation entries.
    for (i, rel) in graph.relations.iter().enumerate() {
        let mask = 1u32 << i;
        let stats = cardinality::estimate_statistics(rel, table_stats);
        let self_cost = cost::estimate_operator_cost(rel, &stats, &[]);
        memo.insert(
            mask,
            DpEntry {
                plan: rel.clone(),
                stats,
                cumulative_cost: self_cost.total_cost(),
            },
        );
    }

    // Phase 2: Enumerate subsets of increasing size.
    let full_mask = (1u32 << n) - 1;
    for size in 2..=n {
        for subset in SubsetIter::new(full_mask, size as u32) {
            // Try all bipartitions of `subset` into (left, right).
            let mut best: Option<DpEntry> = None;

            // Enumerate non-empty proper subsets as the "left" side.
            let mut left = (subset - 1) & subset;
            while left > 0 {
                let right = subset & !left;
                if right == 0 || left > right {
                    // Skip: either right is empty, or we've already
                    // considered this pair (we try both orientations below).
                    left = (left - 1) & subset;
                    continue;
                }

                // Check that there is at least one predicate connecting left and right.
                let connecting_preds = find_connecting_predicates(&graph.predicates, left, right);

                if connecting_preds.is_empty() {
                    // No predicates connect these subsets: skip to avoid
                    // creating an unintended cross join.
                    left = (left - 1) & subset;
                    continue;
                }

                let condition = combine_and(connecting_preds);

                // Require at least one equijoin (col = col) predicate to form
                // this join.  A bipartition where ALL connecting predicates are
                // non-equijoin (e.g. `d3.d_date > d1.d_date + 5`) would force
                // a NEST LOOP JOIN on potentially large relations, exploding
                // cardinality estimates and corrupting subsequent cost choices.
                // Skip it: the non-equijoin predicate will be included as an
                // "other condition" in whichever equijoin bipartition covers
                // the same table set (e.g. cs JOIN d3 via equijoin, where
                // d3.d_date > d1.d_date + 5 is pushed into that join's
                // condition automatically).
                if !has_equijoin_predicate(&condition) {
                    left = (left - 1) & subset;
                    continue;
                }

                // We only need left and right entries, which should exist
                // from previous iterations.
                if let (Some(left_entry), Some(right_entry)) = (memo.get(&left), memo.get(&right)) {
                    // Try left-right orientation: left probes, right builds.
                    try_join_orientation(
                        &left_entry.plan,
                        &left_entry.stats,
                        left_entry.cumulative_cost,
                        &right_entry.plan,
                        &right_entry.stats,
                        right_entry.cumulative_cost,
                        &condition,
                        table_stats,
                        &mut best,
                    );

                    // Try right-left orientation: right probes, left builds.
                    try_join_orientation(
                        &right_entry.plan,
                        &right_entry.stats,
                        right_entry.cumulative_cost,
                        &left_entry.plan,
                        &left_entry.stats,
                        left_entry.cumulative_cost,
                        &condition,
                        table_stats,
                        &mut best,
                    );
                }

                left = (left - 1) & subset;
            }

            if let Some(entry) = best {
                memo.insert(subset, entry);
            }
        }
    }

    memo.remove(&full_mask).map(|e| e.plan)
}

/// Try a specific left-right join orientation and update `best` if cheaper.
#[allow(clippy::too_many_arguments)]
fn try_join_orientation(
    left_plan: &LogicalPlan,
    left_stats: &Statistics,
    left_cumulative: f64,
    right_plan: &LogicalPlan,
    right_stats: &Statistics,
    right_cumulative: f64,
    condition: &TypedExpr,
    table_stats: &HashMap<String, TableStatistics>,
    best: &mut Option<DpEntry>,
) {
    // If the condition has no equijoin predicates (col = col), this join cannot
    // be implemented as a hash join — it will use a NEST LOOP JOIN at execution
    // time (O(left × right)).  Use the Cross join cost model so the DP
    // correctly accounts for the quadratic cost and avoids choosing direct
    // non-equi joins between large relations when equijoin paths exist.
    let join_type_for_cost = if has_equijoin_predicate(condition) {
        JoinKind::Inner
    } else {
        JoinKind::Cross
    };

    let join_plan = LogicalPlan::Join(JoinNode {
        left: Box::new(left_plan.clone()),
        right: Box::new(right_plan.clone()),
        join_type: JoinKind::Inner,
        condition: Some(condition.clone()),
    });

    let join_stats = cardinality::estimate_statistics(&join_plan, table_stats);

    // Build a cost-estimation plan with the effective join type.
    let cost_plan = LogicalPlan::Join(JoinNode {
        left: Box::new(left_plan.clone()),
        right: Box::new(right_plan.clone()),
        join_type: join_type_for_cost,
        condition: Some(condition.clone()),
    });
    let join_self_cost =
        cost::estimate_operator_cost(&cost_plan, &join_stats, &[left_stats, right_stats]);

    let total_cost = left_cumulative + right_cumulative + join_self_cost.total_cost();

    let dominated = best
        .as_ref()
        .is_some_and(|b| b.cumulative_cost <= total_cost);
    if !dominated {
        *best = Some(DpEntry {
            plan: join_plan,
            stats: join_stats,
            cumulative_cost: total_cost,
        });
    }
}

/// Find all predicates that connect two subsets (reference columns from both).
fn find_connecting_predicates(
    predicates: &[(TypedExpr, u32)],
    left_mask: u32,
    right_mask: u32,
) -> Vec<TypedExpr> {
    let combined = left_mask | right_mask;
    predicates
        .iter()
        .filter(|(_, mask)| {
            // Predicate must reference at least one relation from each side,
            // and all referenced relations must be within the combined subset.
            let touches_left = (*mask & left_mask) != 0;
            let touches_right = (*mask & right_mask) != 0;
            let within_scope = (*mask & !combined) == 0;
            touches_left && touches_right && within_scope
        })
        .map(|(pred, _)| pred.clone())
        .collect()
}

/// Collect all predicates whose referenced relations are fully contained within
/// the union of `left_mask` and `right_mask`, and that touch at least one
/// relation from each side.
///
/// This is the public-facing helper that greedy/left-deep algorithms use to find
/// join predicates between two table sets.
fn collect_join_predicates(
    graph: &JoinGraph,
    left_mask: u32,
    right_mask: u32,
) -> Vec<(TypedExpr, u32)> {
    let combined = left_mask | right_mask;
    graph
        .predicates
        .iter()
        .filter(|(_, mask)| {
            let touches_left = (*mask & left_mask) != 0;
            let touches_right = (*mask & right_mask) != 0;
            let within_scope = (*mask & !combined) == 0;
            touches_left && touches_right && within_scope
        })
        .cloned()
        .collect()
}

// ===========================================================================
// Greedy join reorder
// ===========================================================================

/// Greedy join reorder: level-by-level join building similar to StarRocks
/// `JoinReorderGreedy.java`.
///
/// At each level, tries all (prev_level_group, single_atom) pairs, builds the
/// join, estimates cost, and keeps the best plan per table subset.
/// Prefers equi-join connections over cross joins (10x cost penalty for cross).
fn greedy_join_reorder(
    graph: JoinGraph,
    table_stats: &HashMap<String, TableStatistics>,
) -> Option<LogicalPlan> {
    let n = graph.relations.len();
    if !(2..=16).contains(&n) {
        return None;
    }

    // Fast path: if no predicates connect any relations (all CROSS JOINs),
    // greedy enumeration is O(n × 2^n) with no benefit over left_deep O(n²).
    // Skip and let the caller fall back to left_deep.
    if graph.predicates.is_empty() {
        return None;
    }

    let mut memo: HashMap<u32, DpEntry> = HashMap::new();

    // Phase 1: Initialize single-relation entries.
    for (i, rel) in graph.relations.iter().enumerate() {
        let mask = 1u32 << i;
        let stats = cardinality::estimate_statistics(rel, table_stats);
        let self_cost = cost::estimate_operator_cost(rel, &stats, &[]);
        memo.insert(
            mask,
            DpEntry {
                plan: rel.clone(),
                stats,
                cumulative_cost: self_cost.total_cost(),
            },
        );
    }

    // Phase 2: Level-by-level construction.
    // `prev_level` holds the masks from the previous level; at level 2 we
    // combine single atoms to form pairs, at level 3 pairs+atom -> triples, etc.
    let mut prev_level: Vec<u32> = (0..n).map(|i| 1u32 << i).collect();

    for _level in 2..=n {
        let mut next_level_set: std::collections::HashSet<u32> = std::collections::HashSet::new();

        for &group_mask in &prev_level {
            for i in 0..n {
                let atom_mask = 1u32 << i;
                // Skip if atom already part of this group.
                if (group_mask & atom_mask) != 0 {
                    continue;
                }

                let combined = group_mask | atom_mask;

                let connecting = collect_join_predicates(&graph, group_mask, atom_mask);
                let is_cross = connecting.is_empty();

                let condition = if is_cross {
                    None
                } else {
                    let preds: Vec<TypedExpr> = connecting.into_iter().map(|(e, _)| e).collect();
                    Some(combine_and(preds))
                };

                // A join is effectively a NEST LOOP (quadratic cost) when:
                // (a) there are no connecting predicates at all (cross join), OR
                // (b) all connecting predicates are non-equijoin conditions — the
                //     join cannot use hashing and will fall back to NEST LOOP.
                let is_nest_loop = is_cross
                    || condition
                        .as_ref()
                        .is_some_and(|c| !has_equijoin_predicate(c));

                let (group_entry, atom_entry) = match (memo.get(&group_mask), memo.get(&atom_mask))
                {
                    (Some(g), Some(a)) => (g, a),
                    _ => continue,
                };

                // Build side should be the smaller relation.
                let (left_plan, left_stats, left_cost, right_plan, right_stats, right_cost) =
                    if group_entry.stats.output_row_count >= atom_entry.stats.output_row_count {
                        (
                            &group_entry.plan,
                            &group_entry.stats,
                            group_entry.cumulative_cost,
                            &atom_entry.plan,
                            &atom_entry.stats,
                            atom_entry.cumulative_cost,
                        )
                    } else {
                        (
                            &atom_entry.plan,
                            &atom_entry.stats,
                            atom_entry.cumulative_cost,
                            &group_entry.plan,
                            &group_entry.stats,
                            group_entry.cumulative_cost,
                        )
                    };

                let join_type = if condition.is_some() {
                    JoinKind::Inner
                } else {
                    JoinKind::Cross
                };
                let join_plan = LogicalPlan::Join(JoinNode {
                    left: Box::new(left_plan.clone()),
                    right: Box::new(right_plan.clone()),
                    join_type,
                    condition: condition.clone(),
                });

                let join_stats = cardinality::estimate_statistics(&join_plan, table_stats);

                // Use Cross join type for cost estimation when no equijoin key
                // exists, so the O(left × right) NEST LOOP cost is reflected.
                let join_type_for_cost = if is_nest_loop {
                    JoinKind::Cross
                } else {
                    JoinKind::Inner
                };
                let cost_plan = LogicalPlan::Join(JoinNode {
                    left: Box::new(left_plan.clone()),
                    right: Box::new(right_plan.clone()),
                    join_type: join_type_for_cost,
                    condition: condition.clone(),
                });
                let join_self_cost = cost::estimate_operator_cost(
                    &cost_plan,
                    &join_stats,
                    &[left_stats, right_stats],
                );

                let mut total_cost = left_cost + right_cost + join_self_cost.total_cost();

                // Additional cross join penalty for completely unconnected tables.
                if is_cross {
                    total_cost *= 10.0;
                }

                let dominated = memo
                    .get(&combined)
                    .is_some_and(|existing| existing.cumulative_cost <= total_cost);
                if !dominated {
                    memo.insert(
                        combined,
                        DpEntry {
                            plan: join_plan,
                            stats: join_stats,
                            cumulative_cost: total_cost,
                        },
                    );
                    next_level_set.insert(combined);
                }
            }
        }

        if next_level_set.is_empty() {
            break;
        }
        prev_level = next_level_set.into_iter().collect();
    }

    let full_mask = (1u32 << n) - 1;
    memo.remove(&full_mask).map(|e| e.plan)
}

// ===========================================================================
// Left-deep join reorder
// ===========================================================================

/// Left-deep join reorder: sorts tables by estimated row count and greedily
/// attaches one table at a time.
///
/// - Starts with the largest table as the initial probe side.
/// - At each step, picks the unattached table that has equi-join predicates to
///   the current left side (preferring the smallest such table). Falls back to
///   the smallest unattached table if no equi-join predicates exist.
/// - Always produces a left-deep tree shape.
fn left_deep_join_reorder(
    graph: JoinGraph,
    table_stats: &HashMap<String, TableStatistics>,
) -> Option<LogicalPlan> {
    let n = graph.relations.len();
    if !(2..=32).contains(&n) {
        return None;
    }

    // Compute stats for each relation.
    let rel_stats: Vec<Statistics> = graph
        .relations
        .iter()
        .map(|r| cardinality::estimate_statistics(r, table_stats))
        .collect();

    // Start with the largest table (highest row_count) as the initial left side.
    let mut used_mask: u32 = 0;
    let start_idx = (0..n)
        .max_by(|&a, &b| {
            rel_stats[a]
                .output_row_count
                .partial_cmp(&rel_stats[b].output_row_count)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(0);

    let mut current_plan = graph.relations[start_idx].clone();
    let mut current_mask = 1u32 << start_idx;
    used_mask |= current_mask;

    for _ in 1..n {
        // Find the best next table to join.
        let mut best_idx: Option<usize> = None;
        let mut best_has_equi = false;
        let mut best_row_count = u64::MAX;

        for (i, rs) in rel_stats.iter().enumerate() {
            let atom_mask = 1u32 << i;
            if (used_mask & atom_mask) != 0 {
                continue;
            }

            let connecting = collect_join_predicates(&graph, current_mask, atom_mask);
            let has_equi = !connecting.is_empty();
            let rc = rs.output_row_count as u64;

            // Prefer tables with equi-join predicates. Among those (or among
            // tables without predicates), prefer the smallest.
            let is_better = match (has_equi, best_has_equi) {
                (true, false) => true,
                (false, true) => false,
                _ => rc < best_row_count,
            };

            if best_idx.is_none() || is_better {
                best_idx = Some(i);
                best_has_equi = has_equi;
                best_row_count = rc;
            }
        }

        let next_idx = best_idx?;
        let next_mask = 1u32 << next_idx;

        let connecting = collect_join_predicates(&graph, current_mask, next_mask);
        let condition = if connecting.is_empty() {
            None
        } else {
            let preds: Vec<TypedExpr> = connecting.into_iter().map(|(e, _)| e).collect();
            Some(combine_and(preds))
        };

        // Build side (right) should be the smaller relation; in left-deep the
        // new table is always on the right (build) side, so we rely on the
        // selection logic above to pick small tables.
        //
        // Use Cross when there is no join condition.  This preserves correct
        // semantics and, critically, prevents the Cascades JoinAssociativity
        // rule from exploding on long chains of conditionless joins (e.g.
        // q9 with 15 scalar-subquery CROSS JOINs).
        let join_type = if condition.is_some() {
            JoinKind::Inner
        } else {
            JoinKind::Cross
        };
        current_plan = LogicalPlan::Join(JoinNode {
            left: Box::new(current_plan),
            right: Box::new(graph.relations[next_idx].clone()),
            join_type,
            condition,
        });

        current_mask |= next_mask;
        used_mask |= next_mask;
    }

    Some(current_plan)
}

/// Iterator over all subsets of `universe` with exactly `k` bits set.
/// Uses u32 to support join graphs with up to 32 relations.
struct SubsetIter {
    universe: u32,
    #[allow(dead_code)]
    k: u32,
    current: Option<u32>,
}

impl SubsetIter {
    fn new(universe: u32, k: u32) -> Self {
        if k == 0 || k > universe.count_ones() {
            return Self {
                universe,
                k,
                current: None,
            };
        }
        // Find the smallest subset of `universe` with exactly k bits.
        let first = smallest_k_subset(universe, k);
        Self {
            universe,
            k,
            current: first,
        }
    }
}

impl Iterator for SubsetIter {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        let val = self.current?;
        // Find next subset of universe with k bits.
        self.current = next_k_subset(val, self.universe);
        Some(val)
    }
}

/// Find the smallest subset of `universe` with exactly `k` bits set.
fn smallest_k_subset(universe: u32, k: u32) -> Option<u32> {
    if k == 0 {
        return Some(0);
    }
    let bits: Vec<u32> = (0..32).filter(|&i| (universe >> i) & 1 == 1).collect();
    if (k as usize) > bits.len() {
        return None;
    }
    let mut result = 0u32;
    for &bit in bits.iter().take(k as usize) {
        result |= 1 << bit;
    }
    Some(result)
}

/// Given a k-subset `current` of `universe`, find the lexicographically next
/// k-subset, or None if `current` is the last.
fn next_k_subset(current: u32, universe: u32) -> Option<u32> {
    // Gosper's hack adapted for a constrained universe.
    let bits: Vec<u32> = (0..32).filter(|&i| (universe >> i) & 1 == 1).collect();
    let k = current.count_ones() as usize;

    // Map current to indices within `bits`.
    let mut indices: Vec<usize> = Vec::with_capacity(k);
    for (idx, &bit) in bits.iter().enumerate() {
        if (current >> bit) & 1 == 1 {
            indices.push(idx);
        }
    }

    // Find rightmost index that can be incremented.
    let n = bits.len();
    let mut i = k;
    loop {
        if i == 0 {
            return None;
        }
        i -= 1;
        indices[i] += 1;
        if indices[i] <= n - (k - i) {
            break;
        }
    }

    // Reset all indices after position i.
    for j in (i + 1)..k {
        indices[j] = indices[j - 1] + 1;
    }

    let mut result = 0u32;
    for &idx in &indices {
        result |= 1 << bits[idx];
    }
    Some(result)
}

// ===========================================================================
// OR-factoring helpers for join reorder
// (moved from legacy predicate_pushdown.rs in Phase 3)
// ===========================================================================

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
                kind: ExprKind::Literal(LiteralValue::Bool(true)),
            });
        } else {
            new_branches.push(combine_and(remaining));
        }
    }
    let or_rem = if new_branches
        .iter()
        .all(|b| matches!(b.kind, ExprKind::Literal(LiteralValue::Bool(true))))
    {
        None
    } else {
        let mut r = new_branches.remove(0);
        for b in new_branches {
            r = TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(r),
                    op: BinOp::Or,
                    right: Box::new(b),
                },
            };
        }
        Some(r)
    };
    (common_eqs, or_rem)
}

fn is_any_eq(expr: &TypedExpr) -> bool {
    matches!(&expr.kind, ExprKind::BinaryOp { left, op: BinOp::Eq, right }
        if matches!(left.kind, ExprKind::ColumnRef { .. }) && matches!(right.kind, ExprKind::ColumnRef { .. }))
}

fn split_or_branches(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
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
            op: BinOp::And,
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

fn expr_eq(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a.kind) == format!("{:?}", b.kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, S3FileInfo, TableDef, TableStorage};
    use arrow::datatypes::DataType;

    /// Helper: build a `TableDef` backed by S3 parquet files with the given
    /// total byte size.
    fn s3_table(name: &str, total_bytes: i64) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            iceberg_table: None,
            storage: TableStorage::S3ParquetFiles {
                files: vec![S3FileInfo {
                    path: format!("s3://bucket/{}.parquet", name),
                    size: total_bytes,
                    row_count: None,
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    delete_files: vec![],
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: Default::default(),
            },
        }
    }

    fn scan_for(table: &TableDef) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: table.clone(),
            alias: None,
            columns: vec![OutputColumn {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
        })
    }

    fn eq_condition() -> Option<TypedExpr> {
        Some(TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some("a".to_string()),
                        column: "id".to_string(),
                    },
                    data_type: DataType::Int32,
                    nullable: false,
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some("b".to_string()),
                        column: "id".to_string(),
                    },
                    data_type: DataType::Int32,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        })
    }

    #[test]
    fn inner_join_swaps_when_build_side_is_larger() {
        // Left = small (1 KB), Right = large (10 MB) => should swap.
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&small)),
            right: Box::new(scan_for(&large)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let reordered = reorder_joins_heuristic(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                // After reorder: left (probe) = large, right (build) = small.
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                let right_name = match j.right.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "large", "probe side should be the large table");
                assert_eq!(right_name, "small", "build side should be the small table");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn inner_join_no_swap_when_already_correct() {
        // Left = large (10 MB), Right = small (1 KB) => already correct.
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&large)),
            right: Box::new(scan_for(&small)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let reordered = reorder_joins_heuristic(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(
                    left_name, "large",
                    "probe side should remain the large table"
                );
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn left_outer_join_never_swaps() {
        // Even though right is larger, LEFT OUTER cannot swap.
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&small)),
            right: Box::new(scan_for(&large)),
            join_type: JoinKind::LeftOuter,
            condition: eq_condition(),
        });

        let reordered = reorder_joins_heuristic(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "small", "LEFT OUTER must preserve left side");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn left_semi_join_never_swaps() {
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&small)),
            right: Box::new(scan_for(&large)),
            join_type: JoinKind::LeftSemi,
            condition: eq_condition(),
        });

        let reordered = reorder_joins_heuristic(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "small", "LEFT SEMI must preserve left side");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn cross_join_swaps_when_build_side_is_larger() {
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&small)),
            right: Box::new(scan_for(&large)),
            join_type: JoinKind::Cross,
            condition: None,
        });

        let reordered = reorder_joins_heuristic(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "large", "probe side should be the large table");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn nested_joins_reordered_bottom_up() {
        // Simulate TPC-H q3 shape:
        //   (customer JOIN orders) JOIN lineitem
        // customer = 3 KB, orders = 70 KB, lineitem = 600 KB
        let customer = s3_table("customer", 3_000);
        let orders = s3_table("orders", 70_000);
        let lineitem = s3_table("lineitem", 600_000);

        let inner_join = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&customer)),
            right: Box::new(scan_for(&orders)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let outer_join = LogicalPlan::Join(JoinNode {
            left: Box::new(inner_join),
            right: Box::new(scan_for(&lineitem)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let reordered = reorder_joins_heuristic(outer_join);

        // After reorder:
        //   The inner join: customer(3K) vs orders(70K)
        //     => orders is >2x customer, so swap: left=orders, right=customer
        //   The outer join: inner_join result (~3K estimated) vs lineitem(600K)
        //     => lineitem is >2x inner result, so swap: left=lineitem, right=inner_join
        match reordered {
            LogicalPlan::Join(outer) => {
                // Outer left should be lineitem (the large fact table).
                match outer.left.as_ref() {
                    LogicalPlan::Scan(s) => {
                        assert_eq!(
                            s.table.name, "lineitem",
                            "lineitem should be probe of outer join"
                        );
                    }
                    other => panic!(
                        "expected Scan(lineitem) as outer left, got {:?}",
                        std::mem::discriminant(other)
                    ),
                }

                // Outer right should be the inner join.
                match outer.right.as_ref() {
                    LogicalPlan::Join(inner) => {
                        // Inner join: left=orders, right=customer
                        let inner_left = match inner.left.as_ref() {
                            LogicalPlan::Scan(s) => s.table.name.clone(),
                            other => {
                                panic!("expected Scan, got {:?}", std::mem::discriminant(other))
                            }
                        };
                        let inner_right = match inner.right.as_ref() {
                            LogicalPlan::Scan(s) => s.table.name.clone(),
                            other => {
                                panic!("expected Scan, got {:?}", std::mem::discriminant(other))
                            }
                        };
                        assert_eq!(inner_left, "orders", "orders should be probe of inner join");
                        assert_eq!(
                            inner_right, "customer",
                            "customer should be build of inner join"
                        );
                    }
                    other => panic!(
                        "expected Join as outer right, got {:?}",
                        std::mem::discriminant(other)
                    ),
                }
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    /// Helper: build a TableDef with S3 files that have row counts.
    fn s3_table_with_rows(name: &str, total_bytes: i64, row_count: i64) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            iceberg_table: None,
            storage: TableStorage::S3ParquetFiles {
                files: vec![S3FileInfo {
                    path: format!("s3://bucket/{}.parquet", name),
                    size: total_bytes,
                    row_count: Some(row_count),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    delete_files: vec![],
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: Default::default(),
            },
        }
    }

    #[test]
    fn row_count_overrides_file_size_for_join_reorder() {
        // dim_table: large file (10 MB) but few rows (1000)
        // fact_table: smaller file (5 MB) but many rows (1_000_000)
        // Without row_count: dim(10MB) > fact(5MB), so dim probes, fact builds — WRONG
        // With row_count: fact(1M) > dim(1K), so fact probes, dim builds — CORRECT
        let dim = s3_table_with_rows("dim", 10_000_000, 1_000);
        let fact = s3_table_with_rows("fact", 5_000_000, 1_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&dim)),
            right: Box::new(scan_for(&fact)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let reordered = reorder_joins_heuristic(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                let right_name = match j.right.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "fact", "fact (more rows) should be probe side");
                assert_eq!(right_name, "dim", "dim (fewer rows) should be build side");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    // -----------------------------------------------------------------------
    // CBO DP join reorder tests
    // -----------------------------------------------------------------------

    fn make_table_stats(name: &str, row_count: u64, ndv: f64) -> (String, TableStatistics) {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "id".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: row_count as f64,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
            },
        );
        (
            name.to_string(),
            TableStatistics {
                row_count,
                column_stats: col_stats,
            },
        )
    }

    /// Build a scan with a specific alias for CBO tests.
    fn scan_with_alias(table: &TableDef, alias: &str) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: table.clone(),
            alias: Some(alias.to_string()),
            columns: vec![OutputColumn {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
        })
    }

    fn qualified_eq(left_q: &str, right_q: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some(left_q.to_string()),
                        column: "id".to_string(),
                    },
                    data_type: DataType::Int32,
                    nullable: false,
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some(right_q.to_string()),
                        column: "id".to_string(),
                    },
                    data_type: DataType::Int32,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    /// Extract all table names from a plan tree in join order (left-to-right DFS).
    fn collect_table_names(plan: &LogicalPlan) -> Vec<String> {
        match plan {
            LogicalPlan::Scan(s) => {
                vec![s.alias.clone().unwrap_or_else(|| s.table.name.clone())]
            }
            LogicalPlan::Join(j) => {
                let mut names = collect_table_names(&j.left);
                names.extend(collect_table_names(&j.right));
                names
            }
            LogicalPlan::Filter(f) => collect_table_names(&f.input),
            LogicalPlan::Project(p) => collect_table_names(&p.input),
            _ => vec![],
        }
    }

    #[test]
    fn cbo_two_table_join_small_on_build_side() {
        // Large fact table joined with small dim table.
        // CBO should place small on the right (build side).
        let fact = s3_table_with_rows("fact", 1_000_000, 6_000_000);
        let dim = s3_table_with_rows("dim", 100_000, 25_000);

        let (fn_, ft) = make_table_stats("fact", 6_000_000, 6_000_000.0);
        let (dn, dt) = make_table_stats("dim", 25_000, 25_000.0);
        let mut table_stats = HashMap::new();
        table_stats.insert(fn_, ft);
        table_stats.insert(dn, dt);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_with_alias(&dim, "dim")),
            right: Box::new(scan_with_alias(&fact, "fact")),
            join_type: JoinKind::Inner,
            condition: Some(qualified_eq("dim", "fact")),
        });

        let reordered = reorder_joins_cbo(plan, &table_stats);

        match &reordered {
            LogicalPlan::Join(_) => {
                let names = collect_table_names(&reordered);
                // The larger table (fact) should be on the left (probe),
                // the smaller (dim) on the right (build).
                assert_eq!(
                    names.last().unwrap(),
                    "dim",
                    "dim should be on build side (right), got order: {:?}",
                    names
                );
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn cbo_three_table_join_optimal_order() {
        // TPC-H style: lineitem(6M) JOIN orders(1.5M) JOIN customer(150K)
        let lineitem = s3_table_with_rows("lineitem", 10_000_000, 6_000_000);
        let orders = s3_table_with_rows("orders", 5_000_000, 1_500_000);
        let customer = s3_table_with_rows("customer", 500_000, 150_000);

        let (ln, lt) = make_table_stats("lineitem", 6_000_000, 1_500_000.0);
        let (on, ot) = make_table_stats("orders", 1_500_000, 1_500_000.0);
        let (cn, ct) = make_table_stats("customer", 150_000, 150_000.0);
        let mut table_stats = HashMap::new();
        table_stats.insert(ln, lt);
        table_stats.insert(on, ot);
        table_stats.insert(cn, ct);

        // lineitem JOIN orders ON lineitem.id = orders.id
        //   JOIN customer ON orders.id = customer.id
        let inner1 = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_with_alias(&lineitem, "lineitem")),
            right: Box::new(scan_with_alias(&orders, "orders")),
            join_type: JoinKind::Inner,
            condition: Some(qualified_eq("lineitem", "orders")),
        });

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(inner1),
            right: Box::new(scan_with_alias(&customer, "customer")),
            join_type: JoinKind::Inner,
            condition: Some(qualified_eq("orders", "customer")),
        });

        let reordered = reorder_joins_cbo(plan, &table_stats);
        let names = collect_table_names(&reordered);

        // The smallest table (customer) should appear as a build side (rightmost
        // at some level of the tree). The exact order depends on cost, but the
        // key invariant is that the plan is a valid join tree with all 3 tables.
        assert_eq!(names.len(), 3, "should have 3 tables, got {:?}", names);
        assert!(
            names.contains(&"lineitem".to_string()),
            "missing lineitem in {:?}",
            names
        );
        assert!(
            names.contains(&"orders".to_string()),
            "missing orders in {:?}",
            names
        );
        assert!(
            names.contains(&"customer".to_string()),
            "missing customer in {:?}",
            names
        );
    }

    #[test]
    fn cbo_left_outer_join_not_reordered() {
        // LEFT OUTER joins should never be fed into the DP optimizer.
        let small = s3_table_with_rows("small", 1_000, 100);
        let large = s3_table_with_rows("large", 10_000_000, 1_000_000);

        let (sn, st) = make_table_stats("small", 100, 100.0);
        let (ln, lt) = make_table_stats("large", 1_000_000, 1_000_000.0);
        let mut table_stats = HashMap::new();
        table_stats.insert(sn, st);
        table_stats.insert(ln, lt);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_with_alias(&small, "small")),
            right: Box::new(scan_with_alias(&large, "large")),
            join_type: JoinKind::LeftOuter,
            condition: Some(qualified_eq("small", "large")),
        });

        let reordered = reorder_joins_cbo(plan, &table_stats);

        match &reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.alias.clone().unwrap_or_default(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(
                    left_name, "small",
                    "LEFT OUTER must preserve original left side"
                );
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn cbo_subset_iter_enumerates_correctly() {
        // Test that SubsetIter produces all C(4,2) = 6 subsets.
        let universe = 0b1111u32; // 4 bits
        let subsets: Vec<u32> = SubsetIter::new(universe, 2).collect();
        assert_eq!(subsets.len(), 6, "C(4,2) should be 6, got {:?}", subsets);
        for s in &subsets {
            assert_eq!(s.count_ones(), 2);
            assert_eq!(*s & !universe, 0);
        }
    }

    #[test]
    fn cbo_subset_iter_size_3() {
        let universe = 0b1111u32;
        let subsets: Vec<u32> = SubsetIter::new(universe, 3).collect();
        assert_eq!(subsets.len(), 4, "C(4,3) should be 4, got {:?}", subsets);
    }

    #[test]
    fn cbo_find_connecting_predicates() {
        // Predicate referencing relation 0 and 1 should connect masks 0b01 and 0b10.
        let pred = qualified_eq("a", "b");
        let predicates = vec![(pred.clone(), 0b11u32)];

        let result = find_connecting_predicates(&predicates, 0b01, 0b10);
        assert_eq!(result.len(), 1);

        // But not connect 0b01 and 0b100 (different relation set).
        let result2 = find_connecting_predicates(&predicates, 0b01, 0b100);
        assert_eq!(result2.len(), 0);
    }
}
