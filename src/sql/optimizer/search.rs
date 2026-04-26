//! Top-down Cascades optimization with property enforcement.
//!
//! Implements the core Cascades search algorithm: for each (group, required_props)
//! pair, find the cheapest physical expression that satisfies the required
//! physical properties, recursively optimizing children and inserting enforcers
//! (PhysicalDistribution, PhysicalSort) when needed.

use std::collections::{HashMap, HashSet};

use super::cost::compute_cost;
use super::memo::{Cost, GroupId, Memo};
use super::operator::*;
use super::property::*;
use super::stats::derive_statistics;
use crate::sql::optimizer::statistics::TableStatistics;

// ---------------------------------------------------------------------------
// Broadcast row-count threshold
// ---------------------------------------------------------------------------

/// Hard limit: if the build side (right child) of a broadcast hash join
/// exceeds this many rows, skip the broadcast alternative entirely.
/// Aligned with StarRocks `SessionVariable.DEFAULT_BROADCAST_ROW_COUNT_LIMIT`.
const BROADCAST_ROW_COUNT_LIMIT: f64 = 500_000.0;

// ---------------------------------------------------------------------------
// Winner + Enforcer types
// ---------------------------------------------------------------------------

/// Records the best physical expression found for a (group, required_props) pair.
#[derive(Clone, Debug)]
pub(crate) struct Winner {
    #[allow(dead_code)]
    pub(crate) group_id: GroupId,
    /// Index into `group.physical_exprs`.
    pub(crate) expr_index: usize,
    pub(crate) cost: Cost,
    /// If present, the winner needs an enforcer on top of the physical expr.
    pub(crate) enforcer: Option<EnforcerInfo>,
}

/// Describes the enforcer node that must wrap the winner expression.
#[derive(Clone, Debug)]
pub(crate) struct EnforcerInfo {
    pub(crate) kind: EnforcerKind,
    /// The child properties that the enforcer's input was optimized for.
    pub(crate) child_props: PhysicalPropertySet,
}

/// The type of enforcer to insert.
#[derive(Clone, Debug)]
pub(crate) enum EnforcerKind {
    Distribution(DistributionSpec),
    Sort(OrderingSpec),
}

// ---------------------------------------------------------------------------
// SearchContext
// ---------------------------------------------------------------------------

/// Memoized search state for the top-down Cascades optimization.
pub(crate) struct SearchContext {
    /// (GroupId, PhysicalPropertySet) -> Winner
    pub(crate) winners: HashMap<(GroupId, PhysicalPropertySet), Winner>,
    pub(crate) table_stats: HashMap<String, TableStatistics>,
    /// Set of (GroupId, PhysicalPropertySet) pairs currently being computed.
    /// Used to break mutual enforcer cycles: when group A's enforcer path
    /// recurses back into the same (group, props) that is already on the
    /// call stack, we return INFINITY instead of recursing infinitely.
    in_progress: HashSet<(GroupId, PhysicalPropertySet)>,
}

impl SearchContext {
    pub(crate) fn new(table_stats: HashMap<String, TableStatistics>) -> Self {
        Self {
            winners: HashMap::new(),
            table_stats,
            in_progress: HashSet::new(),
        }
    }

    /// Top-down cost-based search for the cheapest plan in `group_id` that
    /// satisfies `required`.
    ///
    /// Returns the best cost found, or `f64::INFINITY` if no physical
    /// expression in the group can satisfy the requirement (even with
    /// enforcers).  This is not an error — some groups simply have no
    /// physical alternatives yet (e.g. unexpanded logical-only groups).
    pub(crate) fn optimize_group(
        &mut self,
        memo: &Memo,
        group_id: GroupId,
        required: &PhysicalPropertySet,
    ) -> Result<Cost, String> {
        let cache_key = (group_id, required.clone());

        // 1. Check winner cache.
        if let Some(winner) = self.winners.get(&cache_key) {
            return Ok(winner.cost);
        }

        // 2. Cycle guard: if this (group, props) is already being computed on
        //    the call stack, we have a mutual enforcer cycle.  Return INFINITY
        //    so the caller treats this path as infeasible and picks another.
        if self.in_progress.contains(&cache_key) {
            return Ok(f64::INFINITY);
        }
        self.in_progress.insert(cache_key.clone());

        let group = &memo.groups[group_id];
        let num_physical = group.physical_exprs.len();

        // Groups with no physical exprs: return infinity (not an error).
        if num_physical == 0 {
            self.in_progress.remove(&cache_key);
            return Ok(f64::INFINITY);
        }

        let mut best_cost = f64::INFINITY;
        let mut best_index: usize = 0;
        let mut best_enforcer: Option<EnforcerInfo> = None;

        for expr_idx in 0..num_physical {
            // We must re-borrow the group each iteration because
            // optimize_group may be called recursively (but Memo is &-shared).
            let expr = &memo.groups[group_id].physical_exprs[expr_idx];

            // --- Broadcast row-count threshold ---
            // If this is a broadcast hash join, check whether the build side
            // (right child) exceeds the hard row-count limit.  If so, skip
            // this alternative entirely to avoid broadcasting large tables.
            if let Operator::PhysicalHashJoin(ref j) = expr.op
                && matches!(j.distribution, JoinDistribution::Broadcast)
                && expr.children.get(1).is_some_and(|&build_group_id| {
                    let build_stats =
                        stats_for_group(&memo.groups[build_group_id], memo, &self.table_stats);
                    build_stats.output_row_count > BROADCAST_ROW_COUNT_LIMIT
                })
            {
                continue;
            }

            let provided = output_properties(&expr.op);

            if provided.satisfies(required) {
                // --- Direct satisfaction path ---
                let child_reqs = required_input_properties(&expr.op, required, expr.children.len());

                // Compute own cost.
                let own_stats = derive_statistics(expr, memo, &self.table_stats);
                let child_stats_vec: Vec<_> = expr
                    .children
                    .iter()
                    .map(|&cg| {
                        let child_group = &memo.groups[cg];
                        stats_for_group(child_group, memo, &self.table_stats)
                    })
                    .collect();
                let child_stats_refs: Vec<&_> = child_stats_vec.iter().collect();
                let own_cost = compute_cost(&expr.op, &own_stats, &child_stats_refs);

                // Recurse into children.
                let mut total = own_cost;
                let mut feasible = true;
                for (i, &child_group_id) in expr.children.iter().enumerate() {
                    let child_cost = self.optimize_group(memo, child_group_id, &child_reqs[i])?;
                    if child_cost.is_infinite() {
                        feasible = false;
                        break;
                    }
                    total += child_cost;
                }

                if feasible && total < best_cost {
                    best_cost = total;
                    best_index = expr_idx;
                    best_enforcer = None;
                }
            } else {
                // --- Enforcer path ---
                // The expr cannot directly satisfy `required`.  We optimize
                // the *same group* for the properties the expr naturally
                // provides, then add an enforcer on top.
                //
                // Important: use `provided` (not `required`) to break the
                // self-referencing loop. The winner cache prevents infinite
                // recursion: once we cache a result for (group_id, provided),
                // a recursive call for the same pair returns immediately.

                // Determine what kind of enforcer we need.
                let enforcers = needed_enforcers(required, &provided);
                if enforcers.is_empty() {
                    continue;
                }

                // Optimize the group for the natural provided properties.
                let child_cost = self.optimize_group(memo, group_id, &provided)?;
                if child_cost.is_infinite() {
                    continue;
                }

                // Compute the group statistics for enforcer cost estimation.
                let group_stats = stats_for_group(&memo.groups[group_id], memo, &self.table_stats);

                // Sum up enforcer costs.
                let mut enforcer_cost = 0.0;
                for enforcer in &enforcers {
                    enforcer_cost += estimate_enforcer_cost(enforcer, &group_stats);
                }

                let total = enforcer_cost + child_cost;
                if total < best_cost {
                    best_cost = total;
                    // The winner's expr_index refers to the best expr for `provided`,
                    // which was just cached. We record the enforcer so that extraction
                    // knows to wrap it.
                    //
                    // For the expr_index, we use the winner of the `provided` search
                    // (which is now in the cache).
                    let provided_key = (group_id, provided.clone());
                    if let Some(inner_winner) = self.winners.get(&provided_key) {
                        best_index = inner_winner.expr_index;
                    } else {
                        // The child_cost was not infinite and we just optimized it,
                        // so the winner must exist. If somehow it doesn't, skip.
                        continue;
                    }

                    // Use the first (most important) enforcer.  In practice we
                    // rarely need both distribution + sort enforcers simultaneously;
                    // when we do, the sort enforcer subsumes Gather distribution.
                    best_enforcer = Some(EnforcerInfo {
                        kind: enforcers.into_iter().next().unwrap(),
                        child_props: provided.clone(),
                    });
                }
            }
        }

        // Remove from in-progress before caching, so re-entrant calls after
        // this point (e.g. from a sibling in a parent loop) see the cache hit.
        self.in_progress.remove(&cache_key);

        // Cache the result even if best_cost is INFINITY (avoids recomputation).
        let winner = Winner {
            group_id,
            expr_index: best_index,
            cost: best_cost,
            enforcer: best_enforcer,
        };
        self.winners.insert(cache_key, winner);
        Ok(best_cost)
    }
}

// ---------------------------------------------------------------------------
// output_properties: what a physical operator naturally provides
// ---------------------------------------------------------------------------

/// Derive the physical properties that a physical operator naturally produces.
fn output_properties(op: &Operator) -> PhysicalPropertySet {
    match op {
        // Leaf: scan provides Any distribution, Any ordering.
        Operator::PhysicalScan(_) => PhysicalPropertySet::any(),

        // Values / GenerateSeries: single-node leaf, treat as Any.
        Operator::PhysicalValues(_) | Operator::PhysicalGenerateSeries(_) => {
            PhysicalPropertySet::any()
        }

        // CTE consume: leaf-like, Any.
        Operator::PhysicalCTEConsume(_) => PhysicalPropertySet::any(),

        // Filter, Project, Limit, SubqueryAlias, CTE Anchor, CTE Produce, Repeat:
        // currently modeled conservatively as Any-only structural operators.
        Operator::PhysicalFilter(_)
        | Operator::PhysicalProject(_)
        | Operator::PhysicalLimit(_)
        | Operator::PhysicalSubqueryAlias(_)
        | Operator::PhysicalCTEAnchor(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalRepeat(_) => PhysicalPropertySet::any(),

        // Window: passthrough child.
        Operator::PhysicalWindow(_) => PhysicalPropertySet::any(),

        // Hash join (Shuffle): output is Hash(left_eq_keys).
        Operator::PhysicalHashJoin(j) => match j.distribution {
            JoinDistribution::Shuffle => {
                let cols = eq_keys_to_column_refs(&j.eq_conditions, Side::Left);
                PhysicalPropertySet {
                    distribution: if cols.is_empty() {
                        DistributionSpec::Any
                    } else {
                        DistributionSpec::HashPartitioned(cols)
                    },
                    ordering: OrderingSpec::Any,
                }
            }
            JoinDistribution::Broadcast | JoinDistribution::Colocate => {
                // Broadcast/Colocate: output follows left child, approximate as Any.
                PhysicalPropertySet::any()
            }
        },

        // Nest-loop join: always Gather (both inputs are Gather).
        Operator::PhysicalNestLoopJoin(_) => PhysicalPropertySet::gather(),

        // Hash aggregate:
        //   - Single with group keys: Hash(group_keys)
        //   - Single without group keys: Gather
        //   - Local: Hash(group_keys)
        //   - Global: Hash(group_keys)
        Operator::PhysicalHashAggregate(a) => {
            let cols = typed_exprs_to_column_refs(&a.group_by);
            if cols.is_empty() {
                // Scalar aggregate -> result is a single row.
                PhysicalPropertySet::gather()
            } else {
                PhysicalPropertySet {
                    distribution: DistributionSpec::HashPartitioned(cols),
                    ordering: OrderingSpec::Any,
                }
            }
        }

        // Sort: Gather distribution + Ordered.
        Operator::PhysicalSort(s) => {
            let sort_keys: Vec<SortKey> = s
                .items
                .iter()
                .filter_map(|item| {
                    typed_expr_to_column_ref(&item.expr).map(|col| SortKey {
                        column: col,
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect();
            PhysicalPropertySet {
                distribution: DistributionSpec::Gather,
                ordering: if sort_keys.is_empty() {
                    OrderingSpec::Any
                } else {
                    OrderingSpec::Required(sort_keys)
                },
            }
        }

        // TopN provided properties depend on phase:
        //   - Partial: Any distribution (preserves child layout). Ordering = Required
        //     if sort keys present (each partial's output is sorted).
        //   - Final (split or not): Gather (final output serialized to one instance).
        Operator::PhysicalTopN(t) => {
            let sort_keys: Vec<SortKey> = t
                .items
                .iter()
                .filter_map(|item| {
                    typed_expr_to_column_ref(&item.expr).map(|col| SortKey {
                        column: col,
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect();
            let ordering = if sort_keys.is_empty() {
                OrderingSpec::Any
            } else {
                OrderingSpec::Required(sort_keys)
            };
            let distribution = match t.phase {
                TopNPhase::Partial => DistributionSpec::Any,
                TopNPhase::Final => DistributionSpec::Gather,
            };
            PhysicalPropertySet {
                distribution,
                ordering,
            }
        }

        // Distribution enforcer: outputs whatever its spec says.
        Operator::PhysicalDistribution(d) => PhysicalPropertySet {
            distribution: d.spec.clone(),
            ordering: OrderingSpec::Any,
        },

        // Union/Intersect/Except: Any (multi-child).
        Operator::PhysicalUnion(_)
        | Operator::PhysicalIntersect(_)
        | Operator::PhysicalExcept(_) => PhysicalPropertySet::any(),

        // Logical operators should not appear in the physical search.
        _ => PhysicalPropertySet::any(),
    }
}

// ---------------------------------------------------------------------------
// required_input_properties: what a physical operator needs from its children
// ---------------------------------------------------------------------------

/// Determine the required physical properties for each child of a physical operator.
pub(super) fn required_input_properties(
    op: &Operator,
    parent_required: &PhysicalPropertySet,
    num_children: usize,
) -> Vec<PhysicalPropertySet> {
    match op {
        // Leaf operators: no children.
        Operator::PhysicalScan(_)
        | Operator::PhysicalValues(_)
        | Operator::PhysicalGenerateSeries(_)
        | Operator::PhysicalCTEConsume(_) => vec![],

        // CTE anchor is structurally wired only for now. Do not imply real
        // property passthrough until the runtime path models it end-to-end.
        Operator::PhysicalCTEAnchor(_) => {
            vec![PhysicalPropertySet::any(), PhysicalPropertySet::any()]
        }

        // Shuffle join: both children hash-partitioned on eq keys.
        // Provide ALL eq column refs to each side — the fragment builder
        // resolves only those that exist in each child's scope. This
        // handles join reorder swapping the eq condition pair order.
        Operator::PhysicalHashJoin(j) => match j.distribution {
            JoinDistribution::Shuffle => {
                let all_cols: Vec<ColumnRef> = j
                    .eq_conditions
                    .iter()
                    .flat_map(|(l, r)| {
                        let mut v = Vec::new();
                        if let Some(c) = typed_expr_to_column_ref(l) {
                            v.push(c);
                        }
                        if let Some(c) = typed_expr_to_column_ref(r) {
                            v.push(c);
                        }
                        v
                    })
                    .collect();
                vec![
                    PhysicalPropertySet {
                        distribution: if all_cols.is_empty() {
                            DistributionSpec::Any
                        } else {
                            DistributionSpec::HashPartitioned(all_cols.clone())
                        },
                        ordering: OrderingSpec::Any,
                    },
                    PhysicalPropertySet {
                        distribution: if all_cols.is_empty() {
                            DistributionSpec::Any
                        } else {
                            DistributionSpec::HashPartitioned(all_cols)
                        },
                        ordering: OrderingSpec::Any,
                    },
                ]
            }
            JoinDistribution::Broadcast => {
                // Left: Any, Right: Gather (broadcast the right side).
                vec![PhysicalPropertySet::any(), PhysicalPropertySet::gather()]
            }
            JoinDistribution::Colocate => {
                // Both sides already co-located.
                vec![PhysicalPropertySet::any(), PhysicalPropertySet::any()]
            }
        },

        // Nest-loop join: both sides must be Gather.
        Operator::PhysicalNestLoopJoin(_) => {
            vec![PhysicalPropertySet::gather(), PhysicalPropertySet::gather()]
        }

        // Hash aggregate:
        //   Single: [Any] (or [Gather] if scalar agg with no group by)
        //   Local:  [Any]
        //   Global: [Hash(group_keys)]
        Operator::PhysicalHashAggregate(a) => match a.mode {
            AggMode::Single => {
                if a.group_by.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet::any()]
                }
            }
            AggMode::Local => vec![PhysicalPropertySet::any()],
            AggMode::Global => {
                let cols = typed_exprs_to_column_refs(&a.group_by);
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::HashPartitioned(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            // DISTINCT_GLOBAL receives shuffled-by-group_by input. Its own
            // group_by includes the distinct column, so the enforcer inserts a
            // Hash(group_by) exchange between LOCAL and DISTINCT_GLOBAL.
            AggMode::DistinctGlobal => {
                let cols = typed_exprs_to_column_refs(&a.group_by);
                if cols.is_empty() {
                    // Shouldn't happen — SplitDistinctAgg always adds the
                    // distinct column to group_by — but handle defensively.
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::HashPartitioned(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            // DISTINCT_LOCAL runs per-instance on DISTINCT_GLOBAL's output; no
            // exchange needed between them.
            AggMode::DistinctLocal => vec![PhysicalPropertySet::any()],
        },

        // Sort: child must be Gather.
        Operator::PhysicalSort(_) => vec![PhysicalPropertySet::gather()],

        // TopN child requirement depends on phase/is_split:
        //   - Partial: child is Any (don't force gather; we run per-instance).
        //   - Final + split=true: child is the PARTIAL with Any distribution; the
        //     fragment builder materializes the merging exchange, so no Gather
        //     enforcer between FINAL(split) and PARTIAL.
        //   - Final + !split (single-stage, today's behavior): child must be Gather.
        Operator::PhysicalTopN(t) => {
            let req = match (t.phase, t.is_split) {
                (TopNPhase::Partial, _) => PhysicalPropertySet::any(),
                (TopNPhase::Final, true) => PhysicalPropertySet::any(),
                (TopNPhase::Final, false) => PhysicalPropertySet::gather(),
            };
            vec![req]
        }

        // Filter, Project, Limit: passthrough parent requirement.
        Operator::PhysicalFilter(_) | Operator::PhysicalProject(_) | Operator::PhysicalLimit(_) => {
            vec![parent_required.clone()]
        }

        // SubqueryAlias, CTE Produce, Repeat: passthrough parent requirement.
        Operator::PhysicalSubqueryAlias(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalRepeat(_) => {
            vec![parent_required.clone()]
        }

        // Window: requires Hash(partition_keys) or Gather (if no partition).
        Operator::PhysicalWindow(w) => {
            // Collect partition-by columns from all window exprs.
            let mut partition_cols = Vec::new();
            for we in &w.window_exprs {
                for pbe in &we.partition_by {
                    if let Some(col) = typed_expr_to_column_ref(pbe)
                        && !partition_cols.contains(&col)
                    {
                        partition_cols.push(col);
                    }
                }
            }
            if partition_cols.is_empty() {
                vec![PhysicalPropertySet::gather()]
            } else {
                vec![PhysicalPropertySet {
                    distribution: DistributionSpec::HashPartitioned(partition_cols),
                    ordering: OrderingSpec::Any,
                }]
            }
        }

        // Distribution enforcer: no child requirements (it IS the enforcer).
        // In practice, distribution nodes have one child that was already
        // optimized for the child properties recorded in the enforcer info.
        Operator::PhysicalDistribution(_) => vec![PhysicalPropertySet::any()],

        // Union/Intersect/Except: each child gets Any.
        Operator::PhysicalUnion(_)
        | Operator::PhysicalIntersect(_)
        | Operator::PhysicalExcept(_) => vec![PhysicalPropertySet::any(); num_children],

        // Logical operators should not appear here.
        _ => vec![PhysicalPropertySet::any()],
    }
}

// ---------------------------------------------------------------------------
// Enforcer helpers
// ---------------------------------------------------------------------------

/// Determine what enforcers are needed to bridge `provided` -> `required`.
fn needed_enforcers(
    required: &PhysicalPropertySet,
    provided: &PhysicalPropertySet,
) -> Vec<EnforcerKind> {
    let mut enforcers = Vec::new();

    if !provided.distribution.satisfies(&required.distribution) {
        enforcers.push(EnforcerKind::Distribution(required.distribution.clone()));
    }

    if !provided.ordering.satisfies(&required.ordering) {
        enforcers.push(EnforcerKind::Sort(required.ordering.clone()));
    }

    enforcers
}

/// Network cost multiplier, matching `cost.rs`.
const NETWORK_COST: f64 = 1.5;

/// Estimate the cost of an enforcer given group statistics.
fn estimate_enforcer_cost(
    enforcer: &EnforcerKind,
    stats: &crate::sql::optimizer::statistics::Statistics,
) -> Cost {
    match enforcer {
        EnforcerKind::Distribution(_) => {
            // Distribution enforcer = network transfer.
            stats.compute_size() * NETWORK_COST
        }
        EnforcerKind::Sort(_) => {
            // Sort enforcer = n * log2(n).
            let n = stats.output_row_count.max(1.0);
            n * n.log2()
        }
    }
}

// ---------------------------------------------------------------------------
// Statistics helper
// ---------------------------------------------------------------------------

/// Get statistics for a group.  Prefers the first logical expr's derived stats
/// (which are stored in `logical_props`), falling back to deriving from the
/// first physical expr.
fn stats_for_group(
    group: &super::memo::Group,
    memo: &Memo,
    table_stats: &HashMap<String, TableStatistics>,
) -> crate::sql::optimizer::statistics::Statistics {
    // Try logical props first (set by derive_group_statistics).
    if let Some(ref lp) = group.logical_props {
        return crate::sql::optimizer::statistics::Statistics {
            output_row_count: lp.row_count,
            column_statistics: HashMap::new(),
        };
    }

    // Fall back to deriving from the first available expression.
    if let Some(expr) = group.logical_exprs.first() {
        return derive_statistics(expr, memo, table_stats);
    }
    if let Some(expr) = group.physical_exprs.first() {
        return derive_statistics(expr, memo, table_stats);
    }

    // Empty group — should not happen in practice.
    crate::sql::optimizer::statistics::Statistics {
        output_row_count: 1.0,
        column_statistics: HashMap::new(),
    }
}

// ---------------------------------------------------------------------------
// Column reference extraction helpers
// ---------------------------------------------------------------------------

/// Which side of a join equi-condition to extract columns from.
#[allow(dead_code)]
enum Side {
    Left,
    Right,
}

/// Extract `ColumnRef`s from the left or right side of equi-join conditions.
fn eq_keys_to_column_refs(
    eq_conditions: &[(
        crate::sql::analysis::TypedExpr,
        crate::sql::analysis::TypedExpr,
    )],
    side: Side,
) -> Vec<ColumnRef> {
    eq_conditions
        .iter()
        .filter_map(|(left, right)| {
            let expr = match side {
                Side::Left => left,
                Side::Right => right,
            };
            typed_expr_to_column_ref(expr)
        })
        .collect()
}

/// Try to extract a `ColumnRef` from a `TypedExpr`.
/// Only succeeds for direct column references.
fn typed_expr_to_column_ref(expr: &crate::sql::analysis::TypedExpr) -> Option<ColumnRef> {
    match &expr.kind {
        crate::sql::analysis::ExprKind::ColumnRef { qualifier, column } => Some(ColumnRef {
            qualifier: qualifier.clone(),
            column: column.clone(),
        }),
        _ => None,
    }
}

/// Extract `ColumnRef`s from a list of `TypedExpr`, skipping non-column-refs.
fn typed_exprs_to_column_refs(exprs: &[crate::sql::analysis::TypedExpr]) -> Vec<ColumnRef> {
    exprs.iter().filter_map(typed_expr_to_column_ref).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::memo::MExpr;

    /// Build a simple memo with a single PhysicalScan group.
    fn single_scan_memo() -> (Memo, GroupId) {
        let mut memo = Memo::new();
        let scan_op = Operator::PhysicalScan(PhysicalScanOp {
            database: "db".into(),
            table: crate::sql::catalog::TableDef {
                name: "t".into(),
                columns: vec![],
                storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
        });
        let expr = MExpr {
            id: 0,
            op: scan_op,
            children: vec![],
        };
        let gid = memo.new_group(expr);
        (memo, gid)
    }

    fn make_table_stats() -> HashMap<String, TableStatistics> {
        let mut ts = HashMap::new();
        ts.insert(
            "t".to_string(),
            TableStatistics {
                row_count: 1000,
                column_stats: HashMap::new(),
            },
        );
        ts
    }

    #[test]
    fn scan_satisfies_any() {
        let (memo, gid) = single_scan_memo();
        let mut ctx = SearchContext::new(make_table_stats());
        let cost = ctx
            .optimize_group(&memo, gid, &PhysicalPropertySet::any())
            .unwrap();
        assert!(cost.is_finite());
        assert!(ctx.winners.contains_key(&(gid, PhysicalPropertySet::any())));
    }

    #[test]
    fn scan_with_gather_uses_enforcer() {
        let (memo, gid) = single_scan_memo();
        let mut ctx = SearchContext::new(make_table_stats());
        let cost = ctx
            .optimize_group(&memo, gid, &PhysicalPropertySet::gather())
            .unwrap();
        assert!(cost.is_finite());
        let winner = ctx
            .winners
            .get(&(gid, PhysicalPropertySet::gather()))
            .unwrap();
        // Scan provides Any, Gather requires Gather -> needs enforcer.
        assert!(winner.enforcer.is_some());
    }

    #[test]
    fn empty_group_returns_infinity() {
        let mut memo = Memo::new();
        // Create a group with only a logical expr (no physical).
        let logical_op = Operator::LogicalScan(LogicalScanOp {
            database: "db".into(),
            table: crate::sql::catalog::TableDef {
                name: "t".into(),
                columns: vec![],
                storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
        });
        let expr = MExpr {
            id: 0,
            op: logical_op,
            children: vec![],
        };
        let gid = memo.new_group(expr);

        let mut ctx = SearchContext::new(make_table_stats());
        let cost = ctx
            .optimize_group(&memo, gid, &PhysicalPropertySet::any())
            .unwrap();
        assert!(cost.is_infinite());
    }

    #[test]
    fn winner_cache_prevents_recomputation() {
        let (memo, gid) = single_scan_memo();
        let mut ctx = SearchContext::new(make_table_stats());
        let cost1 = ctx
            .optimize_group(&memo, gid, &PhysicalPropertySet::any())
            .unwrap();
        let cost2 = ctx
            .optimize_group(&memo, gid, &PhysicalPropertySet::any())
            .unwrap();
        assert!((cost1 - cost2).abs() < f64::EPSILON);
    }

    #[test]
    fn output_properties_sort_has_gather_and_ordering() {
        use crate::sql::analysis::SortItem;

        let col_ref = crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                qualifier: None,
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let op = Operator::PhysicalSort(PhysicalSortOp {
            items: vec![SortItem {
                expr: col_ref,
                asc: true,
                nulls_first: false,
            }],
        });
        let props = output_properties(&op);
        assert_eq!(props.distribution, DistributionSpec::Gather);
        assert!(matches!(props.ordering, OrderingSpec::Required(_)));
    }

    #[test]
    fn output_properties_hash_agg_with_group_by() {
        let col_ref = crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                qualifier: Some("t".into()),
                column: "city".into(),
            },
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
        };
        let op = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![col_ref],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let props = output_properties(&op);
        match &props.distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].column, "city");
            }
            other => panic!("expected HashPartitioned, got {:?}", other),
        }
    }

    #[test]
    fn distinct_global_requires_hash_on_group_by() {
        use crate::sql::analysis::{ExprKind, TypedExpr};

        let col_g = TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: "g".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let col_x = TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: "x".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let op = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: vec![col_g, col_x],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let reqs = required_input_properties(&op, &PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        match &reqs[0].distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols.len(), 2, "Hash on both g and x");
            }
            other => panic!("expected HashPartitioned, got {:?}", other),
        }
    }

    #[test]
    fn distinct_local_requires_any() {
        let op = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctLocal,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let reqs = required_input_properties(&op, &PhysicalPropertySet::gather(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }

    #[test]
    fn required_input_shuffle_join() {
        use crate::sql::analysis::{ExprKind, TypedExpr};

        let left_key = TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: Some("a".into()),
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let right_key = TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: Some("b".into()),
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: crate::sql::analysis::JoinKind::Inner,
            eq_conditions: vec![(left_key, right_key)],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
        });
        let reqs = required_input_properties(&op, &PhysicalPropertySet::any(), 2);
        assert_eq!(reqs.len(), 2);

        // Design note (mirrors the production code path in this file): a
        // shuffle join's required_input_properties provides ALL eq column
        // refs (from both sides) to each child. Fragment builder resolves
        // only those that exist in each child's scope. This gives the
        // optimizer freedom when JoinCommutativity swaps children and the
        // eq_condition pair order becomes ambiguous. We therefore check
        // that both "a.id" and "b.id" appear on each side, regardless of
        // index order.
        for (side_label, req) in [("left", &reqs[0]), ("right", &reqs[1])] {
            match &req.distribution {
                DistributionSpec::HashPartitioned(cols) => {
                    assert_eq!(
                        cols.len(),
                        2,
                        "{} side should receive both eq column refs",
                        side_label
                    );
                    let qualifiers: std::collections::HashSet<&str> =
                        cols.iter().filter_map(|c| c.qualifier.as_deref()).collect();
                    assert!(
                        qualifiers.contains("a"),
                        "{} side missing a.id, got qualifiers {:?}",
                        side_label,
                        qualifiers
                    );
                    assert!(
                        qualifiers.contains("b"),
                        "{} side missing b.id, got qualifiers {:?}",
                        side_label,
                        qualifiers
                    );
                    for c in cols {
                        assert_eq!(c.column, "id");
                    }
                }
                other => panic!(
                    "expected HashPartitioned for {} side, got {:?}",
                    side_label, other
                ),
            }
        }
    }

    #[test]
    fn needed_enforcers_distribution_mismatch() {
        let required = PhysicalPropertySet::gather();
        let provided = PhysicalPropertySet::any();
        let enforcers = needed_enforcers(&required, &provided);
        assert_eq!(enforcers.len(), 1);
        assert!(matches!(
            enforcers[0],
            EnforcerKind::Distribution(DistributionSpec::Gather)
        ));
    }

    #[test]
    fn needed_enforcers_no_mismatch() {
        let required = PhysicalPropertySet::any();
        let provided = PhysicalPropertySet::gather();
        let enforcers = needed_enforcers(&required, &provided);
        assert!(enforcers.is_empty());
    }

    #[test]
    fn filter_passthrough_parent_required() {
        let op = Operator::PhysicalFilter(PhysicalFilterOp {
            predicate: crate::sql::analysis::TypedExpr {
                kind: crate::sql::analysis::ExprKind::Literal(
                    crate::sql::analysis::LiteralValue::Bool(true),
                ),
                data_type: arrow::datatypes::DataType::Boolean,
                nullable: false,
            },
        });
        let parent_req = PhysicalPropertySet::gather();
        let child_reqs = required_input_properties(&op, &parent_req, 1);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], parent_req);
    }

    #[test]
    fn cte_anchor_requires_any_for_both_children() {
        let op = Operator::PhysicalCTEAnchor(PhysicalCTEAnchorOp { cte_id: 7 });
        let parent_req = PhysicalPropertySet::gather();
        let child_reqs = required_input_properties(&op, &parent_req, 2);
        assert_eq!(child_reqs.len(), 2);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
        assert_eq!(child_reqs[1], PhysicalPropertySet::any());
    }
}

#[cfg(test)]
mod top_n_property_tests {
    use super::*;
    use crate::sql::optimizer::operator::{PhysicalTopNOp, TopNPhase};

    #[test]
    fn top_n_output_is_gather_when_sort_keys_resolve() {
        let op = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        });
        let out = output_properties(&op);
        // With no sort keys, ordering is Any but distribution should still be Gather
        // because TopN produces a globally-ordered single-partition output.
        assert!(matches!(out.distribution, DistributionSpec::Gather));
    }

    #[test]
    fn top_n_requires_gather_input() {
        let op = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        });
        let req = required_input_properties(&op, &PhysicalPropertySet::gather(), 1);
        assert_eq!(req.len(), 1);
        assert!(matches!(req[0].distribution, DistributionSpec::Gather));
    }

    #[test]
    fn top_n_partial_requires_any_and_provides_any() {
        let op = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Partial,
            is_split: false,
        });
        let out = output_properties(&op);
        assert!(matches!(out.distribution, DistributionSpec::Any));

        let reqs = required_input_properties(&op, &PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }

    #[test]
    fn top_n_final_split_requires_any_and_provides_gather() {
        let op = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: true,
        });
        let out = output_properties(&op);
        assert!(matches!(out.distribution, DistributionSpec::Gather));

        let reqs = required_input_properties(&op, &PhysicalPropertySet::gather(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }
}
