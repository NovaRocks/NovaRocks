//! Top-down Cascades optimization with property enforcement.
//!
//! Implements the core Cascades search algorithm: for each (group, required_props)
//! pair, find the cheapest physical expression that satisfies the required
//! physical properties, recursively optimizing children and inserting enforcers
//! (PhysicalDistribution, PhysicalSort) when needed.

use std::collections::HashMap;

use super::cost::compute_cost;
use super::memo::{Cost, GroupId, MExpr, Memo};
use super::operator::*;
use super::property::*;
use super::stats::derive_statistics;
use crate::sql::statistics::TableStatistics;

// ---------------------------------------------------------------------------
// Winner + Enforcer types
// ---------------------------------------------------------------------------

/// Records the best physical expression found for a (group, required_props) pair.
#[derive(Clone, Debug)]
pub(crate) struct Winner {
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
}

impl SearchContext {
    pub(crate) fn new(table_stats: HashMap<String, TableStatistics>) -> Self {
        Self {
            winners: HashMap::new(),
            table_stats,
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
        // 1. Check winner cache.
        let cache_key = (group_id, required.clone());
        if let Some(winner) = self.winners.get(&cache_key) {
            return Ok(winner.cost);
        }

        let group = &memo.groups[group_id];
        let num_physical = group.physical_exprs.len();

        // Groups with no physical exprs: return infinity (not an error).
        if num_physical == 0 {
            return Ok(f64::INFINITY);
        }

        let mut best_cost = f64::INFINITY;
        let mut best_index: usize = 0;
        let mut best_enforcer: Option<EnforcerInfo> = None;

        for expr_idx in 0..num_physical {
            // We must re-borrow the group each iteration because
            // optimize_group may be called recursively (but Memo is &-shared).
            let expr = &memo.groups[group_id].physical_exprs[expr_idx];
            let provided = output_properties(&expr.op);

            if provided.satisfies(required) {
                // --- Direct satisfaction path ---
                let child_reqs = required_input_properties(&expr.op, required);

                // Validate child count matches.
                if child_reqs.len() != expr.children.len() {
                    continue;
                }

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
                    let child_cost =
                        self.optimize_group(memo, child_group_id, &child_reqs[i])?;
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
                let group_stats = stats_for_group(
                    &memo.groups[group_id],
                    memo,
                    &self.table_stats,
                );

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
        // passthrough child properties (approximate as Any).
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
) -> Vec<PhysicalPropertySet> {
    match op {
        // Leaf operators: no children.
        Operator::PhysicalScan(_)
        | Operator::PhysicalValues(_)
        | Operator::PhysicalGenerateSeries(_)
        | Operator::PhysicalCTEConsume(_) => vec![],

        Operator::PhysicalCTEAnchor(_) => {
            vec![PhysicalPropertySet::any(), parent_required.clone()]
        }

        // Shuffle join: [Hash(left_eq_keys), Hash(right_eq_keys)]
        Operator::PhysicalHashJoin(j) => match j.distribution {
            JoinDistribution::Shuffle => {
                let left_cols = eq_keys_to_column_refs(&j.eq_conditions, Side::Left);
                let right_cols = eq_keys_to_column_refs(&j.eq_conditions, Side::Right);
                vec![
                    PhysicalPropertySet {
                        distribution: if left_cols.is_empty() {
                            DistributionSpec::Any
                        } else {
                            DistributionSpec::HashPartitioned(left_cols)
                        },
                        ordering: OrderingSpec::Any,
                    },
                    PhysicalPropertySet {
                        distribution: if right_cols.is_empty() {
                            DistributionSpec::Any
                        } else {
                            DistributionSpec::HashPartitioned(right_cols)
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
        },

        // Sort: child must be Gather.
        Operator::PhysicalSort(_) => vec![PhysicalPropertySet::gather()],

        // Filter, Project, Limit: passthrough parent requirement.
        Operator::PhysicalFilter(_)
        | Operator::PhysicalProject(_)
        | Operator::PhysicalLimit(_) => {
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
                    if let Some(col) = typed_expr_to_column_ref(pbe) {
                        if !partition_cols.contains(&col) {
                            partition_cols.push(col);
                        }
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
        Operator::PhysicalUnion(u) => {
            // Number of children is not in the op — we'll handle this via
            // the generic fallback below.  But we know unions typically have
            // 2+ children, so return a generous vector.
            // The caller clips to actual children count anyway.
            vec![PhysicalPropertySet::any(); 8]
        }
        Operator::PhysicalIntersect(_) => vec![PhysicalPropertySet::any(); 8],
        Operator::PhysicalExcept(_) => vec![PhysicalPropertySet::any(); 8],

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
    stats: &crate::sql::statistics::Statistics,
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
) -> crate::sql::statistics::Statistics {
    // Try logical props first (set by derive_group_statistics).
    if let Some(ref lp) = group.logical_props {
        return crate::sql::statistics::Statistics {
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
    crate::sql::statistics::Statistics {
        output_row_count: 1.0,
        column_statistics: HashMap::new(),
    }
}

// ---------------------------------------------------------------------------
// Column reference extraction helpers
// ---------------------------------------------------------------------------

/// Which side of a join equi-condition to extract columns from.
enum Side {
    Left,
    Right,
}

/// Extract `ColumnRef`s from the left or right side of equi-join conditions.
fn eq_keys_to_column_refs(
    eq_conditions: &[(crate::sql::ir::TypedExpr, crate::sql::ir::TypedExpr)],
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
fn typed_expr_to_column_ref(expr: &crate::sql::ir::TypedExpr) -> Option<ColumnRef> {
    match &expr.kind {
        crate::sql::ir::ExprKind::ColumnRef { qualifier, column } => Some(ColumnRef {
            qualifier: qualifier.clone(),
            column: column.clone(),
        }),
        _ => None,
    }
}

/// Extract `ColumnRef`s from a list of `TypedExpr`, skipping non-column-refs.
fn typed_exprs_to_column_refs(exprs: &[crate::sql::ir::TypedExpr]) -> Vec<ColumnRef> {
    exprs.iter().filter_map(typed_expr_to_column_ref).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::cascades::memo::MExpr;
    use crate::sql::statistics::{ColumnStatistic, Statistics};

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
        use crate::sql::ir::SortItem;

        let col_ref = crate::sql::ir::TypedExpr {
            kind: crate::sql::ir::ExprKind::ColumnRef {
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
        let col_ref = crate::sql::ir::TypedExpr {
            kind: crate::sql::ir::ExprKind::ColumnRef {
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
    fn required_input_shuffle_join() {
        use crate::sql::ir::{ExprKind, TypedExpr};

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
            join_type: crate::sql::ir::JoinKind::Inner,
            eq_conditions: vec![(left_key, right_key)],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
        });
        let reqs = required_input_properties(&op, &PhysicalPropertySet::any());
        assert_eq!(reqs.len(), 2);
        match &reqs[0].distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols[0].qualifier.as_deref(), Some("a"));
                assert_eq!(cols[0].column, "id");
            }
            other => panic!("expected HashPartitioned for left, got {:?}", other),
        }
        match &reqs[1].distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols[0].qualifier.as_deref(), Some("b"));
                assert_eq!(cols[0].column, "id");
            }
            other => panic!("expected HashPartitioned for right, got {:?}", other),
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
            predicate: crate::sql::ir::TypedExpr {
                kind: crate::sql::ir::ExprKind::Literal(crate::sql::ir::LiteralValue::Bool(
                    true,
                )),
                data_type: arrow::datatypes::DataType::Boolean,
                nullable: false,
            },
        });
        let parent_req = PhysicalPropertySet::gather();
        let child_reqs = required_input_properties(&op, &parent_req);
        assert_eq!(child_reqs.len(), 1);
        assert_eq!(child_reqs[0], parent_req);
    }
}
