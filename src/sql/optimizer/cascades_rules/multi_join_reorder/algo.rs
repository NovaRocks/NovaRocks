//! Join-order enumeration cores re-expressed over [`JoinTree`]/`GroupId`.
//!
//! Ported faithfully from the RBO `join_reorder/reorder.rs` mask cores, with
//! two changes: leaves are existing memo groups (`JoinTree::Leaf(GroupId)`)
//! instead of cloned `LogicalPlanNode`s, and per-candidate statistics are computed
//! from the cached child `Statistics` via the shared `estimate::cardinality`
//! kernel (no plan re-walk). Cost is an *enumeration-internal pruning proxy*
//! only; the authoritative cost is the memo search (Phase 5).

use arrow::datatypes::DataType;

use crate::sql::common::{BinOp, JoinKind};
use crate::sql::optimizer::estimate::cardinality::{JoinCardInput, estimate_join_cardinality};
use crate::sql::optimizer::estimate::join_condition::estimate_join_condition;
use crate::sql::optimizer::memo::JoinTree;
use crate::sql::optimizer::operator::LogicalJoinOp;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::statistics::{CostEstimate, Statistics};

use super::MultiJoinGraph;

/// Saturation ceiling for the enumeration-internal cost proxy. Keeps the DP
/// branch-and-bound comparator finite on cross-join chains (mirrors StarRocks
/// `JoinOrder.MAXIMUM_COST`).
const MAX_REORDER_COST: f64 = 1e300;
const REORDER_CROSS_CPU_FACTOR: f64 = 2.0;
const REORDER_CROSS_MEMORY_FACTOR: f64 = 200.0;
const REORDER_HASH_BUILD_FACTOR: f64 = 1.0;
const REORDER_OUTPUT_FACTOR: f64 = 1.0;
const REORDER_PREDICATE_COMPLEXITY_MAX: f64 = 16.0;

/// Caps controlling which algorithms run, mirroring StarRocks session vars.
/// Wired to `OptimizerOptions` in Phase 4; plain parameters here.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReorderCaps {
    pub(crate) enable_dp: bool,
    pub(crate) max_dp: usize,
    pub(crate) enable_greedy: bool,
    pub(crate) max_greedy: usize,
    pub(crate) topk: usize,
}

impl Default for ReorderCaps {
    fn default() -> Self {
        Self {
            enable_dp: true,
            max_dp: 10,
            enable_greedy: true,
            max_greedy: 16,
            topk: 10,
        }
    }
}

/// Enumerate candidate join orders for a flattened chain. LeftDeep always runs;
/// DP and Greedy-TopK run subject to caps. Returns deduplicated candidate trees
/// (by structural shape) for the caller to materialize and let the memo cost.
pub(crate) fn enumerate_orders(
    graph: &MultiJoinGraph,
    caps: ReorderCaps,
    arena: &mut ScalarArena,
) -> Vec<JoinTree> {
    let n = graph.atom_count();
    if n < 2 {
        return Vec::new();
    }

    let mut candidates: Vec<JoinTree> = Vec::new();
    if let Some(tree) = left_deep(graph, arena) {
        candidates.push(tree);
    }
    if caps.enable_dp && n <= caps.max_dp.min(MAX_MASK_ATOMS) {
        if let Some(tree) = dp(graph, arena) {
            candidates.push(tree);
        }
    }
    if caps.enable_greedy && n <= caps.max_greedy.min(MAX_MASK_ATOMS) {
        candidates.extend(greedy_topk(graph, caps.topk, arena));
    }

    dedup_trees(candidates)
}

/// `u32` relation masks cap the chain at 32 atoms.
const MAX_MASK_ATOMS: usize = 32;

/// Safety ceiling on the bushy DP's atom count. DP is O(3^n); 12 (≈531k
/// partition probes) is the largest chain we let the exhaustive DP build even if
/// a session raises `max_reorder_node_use_dp` higher. The default cap (10,
/// StarRocks `cbo_max_reorder_node_use_dp`) stays well under it.
const MAX_DP_ATOMS: usize = 12;

/// Per-level width cap for the bushy greedy: keep only this many cheapest masks
/// at each level so a dense join graph cannot blow the enumeration up. LeftDeep
/// always supplies a full-chain plan, so capping greedy never drops the only
/// full candidate.
const GREEDY_LEVEL_WIDTH: usize = 256;

// ---------------------------------------------------------------------------
// Per-candidate statistics and cost (cached-stats versions of the kernels)
// ---------------------------------------------------------------------------

/// Output statistics of joining two subtrees, from their cached child
/// statistics (mirrors `join_reorder/cardinality.rs::estimate_join`, but on
/// cached stats rather than a plan re-walk).
fn join_stats(
    arena: &ScalarArena,
    left: &Statistics,
    right: &Statistics,
    condition: Option<ScalarId>,
    kind: JoinKind,
) -> Statistics {
    let jc = estimate_join_condition(
        arena,
        condition,
        &left.column_statistics,
        &right.column_statistics,
    );
    let (output_rows, row_count_confidence) = estimate_join_cardinality(&JoinCardInput {
        left: (left.output_row_count, left.row_count_confidence),
        right: (right.output_row_count, right.row_count_confidence),
        kind,
        eq_key_ndvs: jc.eq_key_ndvs,
        non_equi_selectivity: jc.residual_selectivity,
    });
    let mut column_statistics = left.column_statistics.clone();
    column_statistics.extend(right.column_statistics.clone());
    Statistics {
        output_row_count: output_rows,
        row_count_confidence,
        column_statistics,
    }
}

fn finite_cost(v: f64) -> f64 {
    if v.is_finite() {
        v.min(MAX_REORDER_COST)
    } else {
        MAX_REORDER_COST
    }
}

/// Self-cost proxy of one join (mirrors `join_reorder/cost.rs::estimate_join_cost`
/// + `CostEstimate::total_cost`), saturated to stay finite.
fn join_self_cost(
    left: &Statistics,
    right: &Statistics,
    output: &Statistics,
    kind: JoinKind,
    predicate_complexity: f64,
) -> f64 {
    let est = match kind {
        JoinKind::Cross => CostEstimate {
            cpu_cost: finite_cost(
                left.compute_size() * right.output_row_count * REORDER_CROSS_CPU_FACTOR,
            ),
            memory_cost: finite_cost(right.compute_size() * REORDER_CROSS_MEMORY_FACTOR),
            network_cost: 0.0,
        },
        _ => {
            let right_rows = right.output_row_count.max(1.0);
            let probe_penalty = (right_rows / 100_000.0).ln().clamp(1.0, 12.0);
            let predicate_complexity = bounded_predicate_complexity(predicate_complexity);
            CostEstimate {
                cpu_cost: finite_cost(
                    (right.compute_size() * REORDER_HASH_BUILD_FACTOR
                        + left.compute_size() * probe_penalty)
                        * predicate_complexity
                        + output.compute_size() * REORDER_OUTPUT_FACTOR,
                ),
                memory_cost: finite_cost(right.compute_size()),
                network_cost: 0.0,
            }
        }
    };
    finite_cost(est.total_cost())
}

/// A built sub-plan: its order, output statistics, and cumulative cost proxy.
#[derive(Clone)]
struct Cell {
    tree: JoinTree,
    stats: Statistics,
    cost: f64,
}

/// Join two cells under the connecting condition (probe = left, build = right).
fn join_cells(
    left: &Cell,
    right: &Cell,
    condition: Option<ScalarId>,
    arena: &mut ScalarArena,
) -> Cell {
    let kind = if condition.is_some() {
        JoinKind::Inner
    } else {
        JoinKind::Cross
    };
    let stats = join_stats(arena, &left.stats, &right.stats, condition, kind);
    // Cost proxy uses Cross when there is no equi key (NestLoop), matching the
    // RBO cost-side join-type selection.
    let cost_kind = match &condition {
        Some(c) if has_equijoin_predicate(arena, *c) => JoinKind::Inner,
        _ => JoinKind::Cross,
    };
    let predicate_complexity = join_predicate_complexity(arena, condition);
    let self_cost = join_self_cost(
        &left.stats,
        &right.stats,
        &stats,
        cost_kind,
        predicate_complexity,
    );
    Cell {
        tree: JoinTree::Join {
            left: Box::new(left.tree.clone()),
            right: Box::new(right.tree.clone()),
            op: LogicalJoinOp {
                join_type: kind,
                condition,
            },
        },
        stats,
        cost: finite_cost(left.cost + right.cost + self_cost),
    }
}

fn atom_cell(graph: &MultiJoinGraph, i: usize) -> Cell {
    Cell {
        tree: JoinTree::Leaf(graph.atoms[i]),
        stats: graph.atom_stats[i].clone(),
        cost: 0.0,
    }
}

// ---------------------------------------------------------------------------
// LeftDeep
// ---------------------------------------------------------------------------

/// Left-deep greedy reorder: start from the largest atom, then repeatedly attach
/// the next atom preferring equi-join > non-equi > cross, and within a class the
/// smallest atom (build side). Always produces a left-deep tree.
fn left_deep(graph: &MultiJoinGraph, arena: &mut ScalarArena) -> Option<JoinTree> {
    let n = graph.atom_count();
    if !(2..=MAX_MASK_ATOMS).contains(&n) {
        return None;
    }

    let start = (0..n)
        .max_by(|&a, &b| {
            graph.atom_stats[a]
                .output_row_count
                .partial_cmp(&graph.atom_stats[b].output_row_count)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(0);

    let mut used: u32 = 1 << start;
    let mut current = atom_cell(graph, start);
    let mut current_mask: u32 = 1 << start;

    for _ in 1..n {
        let mut best: Option<(usize, u8, f64)> = None;
        for i in 0..n {
            let atom_mask = 1u32 << i;
            if used & atom_mask != 0 {
                continue;
            }
            let connecting = connecting_predicates(&graph.predicates, current_mask, atom_mask);
            let has_equi = connecting
                .iter()
                .any(|predicate| has_equijoin_predicate(arena, *predicate));
            let class = if has_equi {
                2u8
            } else if connecting.is_empty() {
                0
            } else {
                1
            };
            let rows = graph.atom_stats[i].output_row_count;
            let better = match &best {
                None => true,
                Some((_, best_class, best_rows)) => {
                    class > *best_class || (class == *best_class && rows < *best_rows)
                }
            };
            if better {
                best = Some((i, class, rows));
            }
        }

        let (next, _, _) = best?;
        let next_mask = 1u32 << next;
        let connecting = connecting_predicates(&graph.predicates, current_mask, next_mask);
        let condition = if connecting.is_empty() {
            None
        } else {
            Some(combine_and_scalar(arena, connecting))
        };
        current = join_cells(&current, &atom_cell(graph, next), condition, arena);
        current_mask |= next_mask;
        used |= next_mask;
    }

    Some(current.tree)
}

// ---------------------------------------------------------------------------
// DP (System-R style, exhaustive over subsets; bushy)
// ---------------------------------------------------------------------------

fn dp(graph: &MultiJoinGraph, arena: &mut ScalarArena) -> Option<JoinTree> {
    let n = graph.atom_count();
    if !(2..=MAX_DP_ATOMS).contains(&n) {
        return None;
    }
    let mut memo: std::collections::HashMap<u32, Cell> = std::collections::HashMap::new();
    for i in 0..n {
        memo.insert(1u32 << i, atom_cell(graph, i));
    }

    let full_mask = (1u32 << n) - 1;
    for size in 2..=n {
        for subset in SubsetIter::new(full_mask, size as u32) {
            let mut best: Option<Cell> = None;
            let mut left = (subset.wrapping_sub(1)) & subset;
            while left > 0 {
                let right = subset & !left;
                if right == 0 || left > right {
                    left = (left.wrapping_sub(1)) & subset;
                    continue;
                }
                if let Some(cell) = try_partition(&memo, &graph.predicates, left, right, arena) {
                    if best.as_ref().is_none_or(|b| cell.cost < b.cost) {
                        best = Some(cell);
                    }
                }
                left = (left.wrapping_sub(1)) & subset;
            }
            if let Some(cell) = best {
                memo.insert(subset, cell);
            }
        }
    }

    memo.remove(&full_mask).map(|c| c.tree)
}

/// Build the cheaper orientation of joining the `left`/`right` subsets, if they
/// have a connecting equi-join predicate.
fn try_partition(
    memo: &std::collections::HashMap<u32, Cell>,
    predicates: &[(ScalarId, u32)],
    left: u32,
    right: u32,
    arena: &mut ScalarArena,
) -> Option<Cell> {
    let connecting = connecting_predicates(predicates, left, right);
    if connecting.is_empty() {
        return None;
    }
    let condition = combine_and_scalar(arena, connecting);
    // Require an equi key to avoid materializing NestLoop joins during reorder.
    if !has_equijoin_predicate(arena, condition) {
        return None;
    }
    let left_cell = memo.get(&left)?;
    let right_cell = memo.get(&right)?;
    let a = join_cells(left_cell, right_cell, Some(condition.clone()), arena);
    let b = join_cells(right_cell, left_cell, Some(condition), arena);
    Some(if a.cost <= b.cost { a } else { b })
}

// ---------------------------------------------------------------------------
// Greedy (level-by-level; returns a bounded Top-K of full-join orders)
// ---------------------------------------------------------------------------

fn greedy_topk(graph: &MultiJoinGraph, k: usize, arena: &mut ScalarArena) -> Vec<JoinTree> {
    let n = graph.atom_count();
    if !(2..=MAX_MASK_ATOMS).contains(&n) || graph.predicates.is_empty() || k == 0 {
        return Vec::new();
    }
    let full_mask = (1u32 << n) - 1;

    // best[mask] = cheapest sub-plan found for that atom set so far.
    let mut best: std::collections::HashMap<u32, Cell> = std::collections::HashMap::new();
    // levels[L] = masks of popcount L kept as building blocks for higher levels.
    let mut levels: Vec<Vec<u32>> = vec![Vec::new(); n + 1];
    for i in 0..n {
        let m = 1u32 << i;
        best.insert(m, atom_cell(graph, i));
        levels[1].push(m);
    }

    // Bounded Top-K of full-mask plans, kept sorted ascending by cost.
    let mut full_topk: Vec<Cell> = Vec::new();

    for target in 2..=n {
        // Build target-level plans bushily: join a size-(target-r) sub-plan with
        // a disjoint, connected size-r sub-plan. r = 1 is the left-deep
        // extension; r >= 2 yields bushy trees. Keep the cheapest per mask.
        let mut produced: std::collections::HashMap<u32, Cell> = std::collections::HashMap::new();
        for r in 1..=(target / 2) {
            let l = target - r;
            for &lm in &levels[l] {
                for &rm in &levels[r] {
                    if lm & rm != 0 {
                        continue; // overlapping atom sets
                    }
                    if l == r && lm >= rm {
                        continue; // same-size symmetric pair handled once
                    }
                    let Some(cell) = try_partition(&best, &graph.predicates, lm, rm, arena) else {
                        continue;
                    };
                    let combined = lm | rm;
                    if combined == full_mask {
                        // Collect every full-chain candidate (bushy and left-deep)
                        // so the memo cost search can pick among them — not just
                        // the cheapest per mask.
                        insert_topk(&mut full_topk, cell, k);
                    } else if produced.get(&combined).is_none_or(|e| cell.cost < e.cost) {
                        produced.insert(combined, cell);
                    }
                }
            }
        }

        // Commit intermediate plans as building blocks for the next levels.
        let mut target_masks: Vec<u32> = Vec::with_capacity(produced.len());
        for (mask, cell) in produced {
            best.insert(mask, cell);
            target_masks.push(mask);
        }
        if target_masks.is_empty() {
            break; // no connected plan reaches this level
        }
        // Cap level width so a dense graph cannot blow up the enumeration.
        target_masks.sort_by(|&a, &b| {
            best[&a]
                .cost
                .partial_cmp(&best[&b].cost)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        target_masks.truncate(GREEDY_LEVEL_WIDTH);
        levels[target] = target_masks;
    }

    full_topk.into_iter().map(|c| c.tree).collect()
}

/// Insert `cell` into a cost-ascending bounded Top-K buffer.
fn insert_topk(buf: &mut Vec<Cell>, cell: Cell, k: usize) {
    let pos = buf
        .binary_search_by(|c| {
            c.cost
                .partial_cmp(&cell.cost)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or_else(|e| e);
    buf.insert(pos, cell);
    if buf.len() > k {
        buf.truncate(k);
    }
}

// ---------------------------------------------------------------------------
// Pure helpers (ported verbatim from reorder.rs — mask / ScalarId only)
// ---------------------------------------------------------------------------

/// Predicates connecting the two subsets: touch both, reference nothing outside
/// their union. Returns the predicate expressions (cloned).
fn connecting_predicates(
    predicates: &[(ScalarId, u32)],
    left_mask: u32,
    right_mask: u32,
) -> Vec<ScalarId> {
    let combined = left_mask | right_mask;
    predicates
        .iter()
        .filter(|(_, mask)| {
            (*mask & left_mask) != 0 && (*mask & right_mask) != 0 && (*mask & !combined) == 0
        })
        .map(|(pred, _)| *pred)
        .collect()
}

/// True if the predicate contains at least one `col = col` equi-join conjunct.
fn has_equijoin_predicate(arena: &ScalarArena, expr: ScalarId) -> bool {
    match arena.node(expr) {
        ScalarNode::Nested(inner) => has_equijoin_predicate(arena, *inner),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => has_equijoin_predicate(arena, *left) || has_equijoin_predicate(arena, *right),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            matches!(arena.node(*left), ScalarNode::ColumnRef(_))
                && matches!(arena.node(*right), ScalarNode::ColumnRef(_))
        }
        _ => false,
    }
}

fn join_predicate_complexity(arena: &ScalarArena, condition: Option<ScalarId>) -> f64 {
    condition
        .map(|expr| bounded_predicate_complexity(count_predicate_conjuncts(arena, expr) as f64))
        .unwrap_or(1.0)
}

fn count_predicate_conjuncts(arena: &ScalarArena, expr: ScalarId) -> usize {
    match arena.node(expr) {
        ScalarNode::Nested(inner) => count_predicate_conjuncts(arena, *inner),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => count_predicate_conjuncts(arena, *left)
            .saturating_add(count_predicate_conjuncts(arena, *right)),
        _ => 1,
    }
}

fn bounded_predicate_complexity(v: f64) -> f64 {
    if v.is_finite() {
        v.max(1.0).min(REORDER_PREDICATE_COMPLEXITY_MAX)
    } else {
        1.0
    }
}

fn combine_and_scalar(arena: &mut ScalarArena, mut exprs: Vec<ScalarId>) -> ScalarId {
    assert!(!exprs.is_empty());
    let mut result = exprs.pop().unwrap();
    while let Some(left) = exprs.pop() {
        let nullable = arena.nullable(left) || arena.nullable(result);
        result = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left,
                right: result,
            },
            DataType::Boolean,
            nullable,
        );
    }
    result
}

/// Deduplicate candidate trees by structural shape (debug form).
fn dedup_trees(trees: Vec<JoinTree>) -> Vec<JoinTree> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for tree in trees {
        if seen.insert(format!("{tree:?}")) {
            out.push(tree);
        }
    }
    out
}

/// Iterate all `k`-bit subsets of `universe` (Gosper's hack), ported verbatim.
struct SubsetIter {
    universe: u32,
    current: Option<u32>,
}

impl SubsetIter {
    fn new(universe: u32, k: u32) -> Self {
        Self {
            universe,
            current: smallest_k_subset(universe, k),
        }
    }
}

impl Iterator for SubsetIter {
    type Item = u32;
    fn next(&mut self) -> Option<u32> {
        let cur = self.current?;
        self.current = next_k_subset(cur, self.universe);
        Some(cur)
    }
}

fn smallest_k_subset(universe: u32, k: u32) -> Option<u32> {
    let bits = universe.count_ones();
    if k == 0 || k > bits {
        return None;
    }
    // The k lowest set bits of `universe`.
    let mut result = 0u32;
    let mut remaining = k;
    let mut u = universe;
    while remaining > 0 && u != 0 {
        let low = u & u.wrapping_neg();
        result |= low;
        u &= u - 1;
        remaining -= 1;
    }
    Some(result)
}

fn next_k_subset(current: u32, universe: u32) -> Option<u32> {
    // Walk submasks of `universe` ascending and return the next one with the
    // same popcount as `current`. `universe` has at most `MAX_MASK_ATOMS` bits
    // and SubsetIter is only used by DP (n <= MAX_DP_ATOMS), so this is cheap.
    let k = current.count_ones();
    let mut candidate = current;
    loop {
        candidate = next_submask(candidate, universe)?;
        if candidate.count_ones() == k {
            return Some(candidate);
        }
    }
}

/// Next submask of `universe` strictly greater than `current` (ascending),
/// or `None` when `current == universe`.
fn next_submask(current: u32, universe: u32) -> Option<u32> {
    if current == universe {
        return None;
    }
    // Add 1 within the universe's bit positions: ((current | ~universe) + 1) & universe.
    Some(((current | !universe).wrapping_add(1)) & universe)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::ScalarArena;

    use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence};
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use std::collections::HashMap;

    fn col_ref(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id + 1),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn eq(l: TypedExpr, r: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(l),
                op: BinOp::Eq,
                right: Box::new(r),
            },
            data_type: arrow::datatypes::DataType::Boolean,
            nullable: false,
        }
    }

    fn atom_stats(col_id: u32, rows: f64, ndv: f64) -> Statistics {
        let mut cs = HashMap::new();
        cs.insert(
            ColumnId::new_for_test(col_id + 1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: ndv,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Estimated,
            },
        );
        Statistics {
            output_row_count: rows,
            row_count_confidence: Confidence::Estimated,
            column_statistics: cs,
        }
    }

    #[test]
    fn join_self_cost_penalizes_cross_join_above_equi_join() {
        let left = atom_stats(0, 10_000.0, 10_000.0);
        let right = atom_stats(1, 10_000.0, 10_000.0);
        let output = atom_stats(2, 1_000.0, 1_000.0);

        let equi_cost = join_self_cost(&left, &right, &output, JoinKind::Inner, 1.0);
        let cross_cost = join_self_cost(&left, &right, &output, JoinKind::Cross, 1.0);

        assert!(cross_cost > equi_cost * 10.0);
    }

    #[test]
    fn join_self_cost_accounts_for_output_size() {
        let left = atom_stats(0, 10_000.0, 10_000.0);
        let right = atom_stats(1, 10_000.0, 10_000.0);
        let small_output = atom_stats(2, 100.0, 100.0);
        let large_output = atom_stats(3, 100_000.0, 100_000.0);

        assert!(
            join_self_cost(&left, &right, &large_output, JoinKind::Inner, 1.0)
                > join_self_cost(&left, &right, &small_output, JoinKind::Inner, 1.0)
        );
    }

    #[test]
    fn join_self_cost_accounts_for_predicate_complexity() {
        let left = atom_stats(0, 10_000.0, 10_000.0);
        let right = atom_stats(1, 10_000.0, 10_000.0);
        let output = atom_stats(2, 1_000.0, 1_000.0);
        let mut arena = ScalarArena::new();
        let single_key = pred(&mut arena, eq(col_ref(0), col_ref(1)));
        let second_key = pred(&mut arena, eq(col_ref(2), col_ref(3)));
        let complex_predicate = combine_and_scalar(&mut arena, vec![single_key, second_key]);

        let single_key_complexity = join_predicate_complexity(&arena, Some(single_key));
        let complex_predicate_complexity =
            join_predicate_complexity(&arena, Some(complex_predicate));

        assert!(complex_predicate_complexity > single_key_complexity);
        assert!(
            join_self_cost(
                &left,
                &right,
                &output,
                JoinKind::Inner,
                complex_predicate_complexity
            ) > join_self_cost(
                &left,
                &right,
                &output,
                JoinKind::Inner,
                single_key_complexity
            )
        );
    }

    fn pred(arena: &mut ScalarArena, expr: TypedExpr) -> ScalarId {
        intern_typed(arena, &expr)
    }

    /// Left-deep path chain over `n` atoms: c_i = c_{i+1} for i in 0..n-1.
    fn path_graph(n: usize, arena: &mut ScalarArena) -> MultiJoinGraph {
        let atoms: Vec<usize> = (0..n).map(|i| 100 + i).collect();
        let atom_stats: Vec<Statistics> = (0..n)
            .map(|i| atom_stats(i as u32, 10_000.0, 1_000.0))
            .collect();
        let predicates: Vec<(ScalarId, u32)> = (0..n.saturating_sub(1))
            .map(|i| {
                (
                    pred(arena, eq(col_ref(i as u32), col_ref(i as u32 + 1))),
                    (1u32 << i) | (1u32 << (i + 1)),
                )
            })
            .collect();
        MultiJoinGraph {
            atoms,
            atom_stats,
            predicates,
            chain_join_groups: vec![],
        }
    }

    /// Two equi-join pairs (0,1) and (2,3) bridged by 1=2, so the only way to
    /// join the two pairs without a cross join is the bushy (0⋈1)⋈(2⋈3).
    fn two_pairs_graph(arena: &mut ScalarArena) -> MultiJoinGraph {
        MultiJoinGraph {
            atoms: vec![100, 101, 102, 103],
            atom_stats: vec![
                atom_stats(0, 1_000_000.0, 1_000_000.0),
                atom_stats(1, 1_000_000.0, 1_000_000.0),
                atom_stats(2, 1_000_000.0, 1_000_000.0),
                atom_stats(3, 1_000_000.0, 1_000_000.0),
            ],
            predicates: vec![
                (pred(arena, eq(col_ref(0), col_ref(1))), 0b0011),
                (pred(arena, eq(col_ref(2), col_ref(3))), 0b1100),
                (pred(arena, eq(col_ref(1), col_ref(2))), 0b0110),
            ],
            chain_join_groups: vec![],
        }
    }

    fn is_bushy(tree: &JoinTree) -> bool {
        matches!(
            tree,
            JoinTree::Join { left, right, .. }
                if matches!(**left, JoinTree::Join { .. })
                    && matches!(**right, JoinTree::Join { .. })
        )
    }

    #[test]
    fn dp_covers_chains_up_to_ten_atoms() {
        // The DP internal ceiling must honor the configured max_dp (10), not the
        // old hardcoded 8. A 10-atom connected chain must produce a DP plan.
        let mut arena = ScalarArena::new();
        let graph10 = path_graph(10, &mut arena);
        assert!(
            dp(&graph10, &mut arena).is_some(),
            "DP must enumerate a 10-atom chain (was capped at 8)"
        );
        let graph9 = path_graph(9, &mut arena);
        assert!(
            dp(&graph9, &mut arena).is_some(),
            "DP must enumerate a 9-atom chain"
        );
        // Beyond the safety ceiling (12) DP bails (greedy/left-deep take over).
        let graph13 = path_graph(13, &mut arena);
        assert!(
            dp(&graph13, &mut arena).is_none(),
            "DP bails past the 12-atom ceiling"
        );
    }

    #[test]
    fn greedy_produces_bushy_orders() {
        // Greedy must enumerate bushy shapes (two join sub-trees), not only
        // left-deep spines, so it can find (0⋈1)⋈(2⋈3).
        let mut arena = ScalarArena::new();
        let graph = two_pairs_graph(&mut arena);
        let trees = greedy_topk(&graph, 10, &mut arena);
        assert!(
            trees.iter().any(is_bushy),
            "greedy must produce at least one bushy order; got {} trees",
            trees.len()
        );
    }

    /// Star schema: a big fact atom (0) equi-joined to two small dim atoms (1,2).
    fn star_graph(arena: &mut ScalarArena) -> MultiJoinGraph {
        MultiJoinGraph {
            atoms: vec![100, 101, 102],
            atom_stats: vec![
                atom_stats(0, 1_000_000.0, 1_000_000.0), // fact
                atom_stats(1, 100.0, 100.0),             // dim1
                atom_stats(2, 50.0, 50.0),               // dim2
            ],
            // fact.c0 = dim1.c1 (atoms 0,1) ; fact.c0b = dim2.c2 (atoms 0,2)
            predicates: vec![
                (pred(arena, eq(col_ref(0), col_ref(1))), 0b011),
                (pred(arena, eq(col_ref(0), col_ref(2))), 0b101),
            ],
            chain_join_groups: vec![],
        }
    }

    #[test]
    fn left_deep_starts_from_largest_and_prefers_equi() {
        let mut arena = ScalarArena::new();
        let graph = star_graph(&mut arena);
        let tree = left_deep(&graph, &mut arena).expect("left-deep over 3 atoms");
        // Left-deep shape: ((fact ⋈ dim) ⋈ dim). The deepest-left leaf is the
        // fact atom (100, the largest), reached by descending left children.
        let mut node = &tree;
        let mut depth = 0;
        loop {
            match node {
                JoinTree::Join { left, .. } => {
                    node = left;
                    depth += 1;
                }
                JoinTree::Leaf(g) => {
                    assert_eq!(*g, 100, "left-deep base must be the largest (fact) atom");
                    break;
                }
            }
        }
        assert_eq!(depth, 2, "3 atoms -> 2 joins -> left spine depth 2");
    }

    #[test]
    fn enumerate_orders_produces_candidates_and_dedups() {
        let mut arena = ScalarArena::new();
        let graph = star_graph(&mut arena);
        let trees = enumerate_orders(&graph, ReorderCaps::default(), &mut arena);
        assert!(!trees.is_empty(), "should enumerate at least one order");
        // All candidates must be 3-atom join trees (2 joins).
        for t in &trees {
            assert_eq!(count_leaves(t), 3);
        }
        // Dedup: no two identical shapes.
        let mut shapes: Vec<String> = trees.iter().map(|t| format!("{t:?}")).collect();
        shapes.sort();
        let n = shapes.len();
        shapes.dedup();
        assert_eq!(shapes.len(), n, "candidates must be deduplicated");
    }

    fn count_leaves(t: &JoinTree) -> usize {
        match t {
            JoinTree::Leaf(_) => 1,
            JoinTree::Join { left, right, .. } => count_leaves(left) + count_leaves(right),
        }
    }

    #[test]
    fn subset_iter_enumerates_k_subsets() {
        // universe = 0b111 (3 bits), k = 2 -> {011, 101, 110}.
        let subsets: Vec<u32> = SubsetIter::new(0b111, 2).collect();
        assert_eq!(subsets.len(), 3);
        for s in &subsets {
            assert_eq!(s.count_ones(), 2);
            assert_eq!(s & !0b111, 0, "subset within universe");
        }
        let mut sorted = subsets.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 3, "distinct 2-subsets");
    }
}
