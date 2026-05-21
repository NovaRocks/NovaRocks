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

pub(crate) use super::derive::EnforcerKind;

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
    /// The actual physical-property set this winner delivers. For an
    /// enforcer winner, this equals the required properties (because the
    /// enforcer was selected to bridge `provided -> required`). Otherwise
    /// it equals the natural output of the chosen physical expression.
    pub(crate) output: PhysicalPropertySet,
}

/// Describes the enforcer node that must wrap the winner expression.
#[derive(Clone, Debug)]
pub(crate) struct EnforcerInfo {
    pub(crate) kind: EnforcerKind,
    /// The child properties that the enforcer's input was optimized for.
    /// Kept for debugging / future cross-checks; extract.rs walks the
    /// underlying expr via `winner.expr_index` instead.
    #[allow(dead_code)]
    pub(crate) child_props: PhysicalPropertySet,
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
        let mut best_output = PhysicalPropertySet::any();

        for expr_idx in 0..num_physical {
            let expr = &memo.groups[group_id].physical_exprs[expr_idx];

            // Broadcast row-count threshold (unchanged from today).
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

            // 1. Determine each child's required properties (top-down — no child
            //    visibility yet).
            let child_reqs =
                super::derive::derive_required(&expr.op, required, expr.children.len());

            // 2. Compute own cost.
            let own_stats = derive_statistics(expr, memo, &self.table_stats);
            let child_stats_vec: Vec<_> = expr
                .children
                .iter()
                .map(|&cg| stats_for_group(&memo.groups[cg], memo, &self.table_stats))
                .collect();
            let child_stats_refs: Vec<&_> = child_stats_vec.iter().collect();
            let own_cost = compute_cost(&expr.op, &own_stats, &child_stats_refs);

            // 3. Optimize each child; collect its winner output.
            let mut total = own_cost;
            let mut child_outputs: Vec<PhysicalPropertySet> =
                Vec::with_capacity(expr.children.len());
            let mut feasible = true;
            for (i, &cg) in expr.children.iter().enumerate() {
                let child_cost = self.optimize_group(memo, cg, &child_reqs[i])?;
                if child_cost.is_infinite() {
                    feasible = false;
                    break;
                }
                total += child_cost;
                let cw = self
                    .winners
                    .get(&(cg, child_reqs[i].clone()))
                    .expect("child just optimized — winner must be in cache");
                child_outputs.push(cw.output.clone());
            }
            if !feasible {
                continue;
            }

            // 4. Derive this node's actual output from children winner outputs.
            let child_output_refs: Vec<&PhysicalPropertySet> = child_outputs.iter().collect();
            let provided = super::derive::derive_output(&expr.op, &child_output_refs);

            // 5. Bridge provided → required via enforcer if needed.
            let (actual_output, enforcer_info, candidate_cost) = if provided.satisfies(required) {
                (provided, None, total)
            } else {
                let enforcers = super::derive::needed_enforcers(required, &provided);
                if enforcers.is_empty() {
                    continue;
                }
                let group_stats = stats_for_group(&memo.groups[group_id], memo, &self.table_stats);
                let enforcer_cost: Cost = enforcers
                    .iter()
                    .map(|e| super::derive::estimate_enforcer_cost(e, &group_stats))
                    .sum();
                let kind = enforcers.into_iter().next().unwrap();
                (
                    required.clone(),
                    Some(EnforcerInfo {
                        kind,
                        child_props: provided,
                    }),
                    total + enforcer_cost,
                )
            };

            if candidate_cost < best_cost {
                best_cost = candidate_cost;
                best_index = expr_idx;
                best_enforcer = enforcer_info;
                best_output = actual_output;
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
            output: best_output,
        };
        self.winners.insert(cache_key, winner);
        Ok(best_cost)
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::MExpr;

    // Thin wrappers that delegate to the new per-op derive dispatcher.
    // Keeps the existing assertions short; the underlying logic lives in
    // `super::derive`.
    fn output_properties(op: &Operator) -> PhysicalPropertySet {
        super::super::derive::derive_output(op, &[])
    }

    fn required_input_properties(
        op: &Operator,
        parent_required: &PhysicalPropertySet,
        num_children: usize,
    ) -> Vec<PhysicalPropertySet> {
        super::super::derive::derive_required(op, parent_required, num_children)
    }

    fn needed_enforcers(
        required: &PhysicalPropertySet,
        provided: &PhysicalPropertySet,
    ) -> Vec<EnforcerKind> {
        super::super::derive::needed_enforcers(required, provided)
    }

    /// Build a simple memo with a single PhysicalScan group.
    fn single_scan_memo() -> (Memo, GroupId) {
        let mut memo = Memo::new();
        let scan_op = Operator::PhysicalScan(PhysicalScanOp {
            database: "db".into(),
            table: crate::sql::catalog::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
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
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
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
                column_id: ColumnId(1),
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
            analytic_partition_exprs: Vec::new(),
        });
        let props = output_properties(&op);
        assert_eq!(props.distribution, DistributionSpec::Gather);
        assert!(matches!(props.ordering, OrderingSpec::Required(_)));
    }

    #[test]
    fn output_properties_window_propagates_partition_distribution() {
        use crate::sql::analysis::{ExprKind, TypedExpr};
        use crate::sql::planner::plan::WindowExpr;

        let col_c0 = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(2),
                qualifier: None,
                column: "c0".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let window_expr = WindowExpr {
            name: "max".into(),
            args: vec![],
            partition_by: vec![col_c0.clone()],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "win".into(),
            result_type: arrow::datatypes::DataType::Int64,
        };
        let op = Operator::PhysicalWindow(PhysicalWindowOp {
            window_exprs: vec![window_expr],
            output_columns: vec![],
        });
        let props = output_properties(&op);
        match &props.distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0], ColumnId(2));
            }
            other => panic!("expected HashPartitioned([c0]), got {:?}", other),
        }
    }

    #[test]
    fn output_properties_window_without_partition_by_is_any() {
        use crate::sql::planner::plan::WindowExpr;

        let window_expr = WindowExpr {
            name: "row_number".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "win".into(),
            result_type: arrow::datatypes::DataType::Int64,
        };
        let op = Operator::PhysicalWindow(PhysicalWindowOp {
            window_exprs: vec![window_expr],
            output_columns: vec![],
        });
        let props = output_properties(&op);
        assert_eq!(props.distribution, DistributionSpec::Any);
    }

    #[test]
    fn output_properties_hash_agg_with_group_by() {
        let col_ref = crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                column_id: ColumnId(3),
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
                assert_eq!(cols[0], ColumnId(3));
            }
            other => panic!("expected HashPartitioned, got {:?}", other),
        }
    }

    #[test]
    fn distinct_global_requires_hash_on_group_by() {
        use crate::sql::analysis::{ExprKind, TypedExpr};

        let col_g = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(4),
                qualifier: None,
                column: "g".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let col_x = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(5),
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
                column_id: ColumnId(6),
                qualifier: Some("a".into()),
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let right_key = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(7),
                qualifier: Some("b".into()),
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: crate::sql::analysis::JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: left_key,
                right: right_key,
                null_safe: false,
            }],
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
                        "{} side should receive both eq column ids",
                        side_label
                    );
                    // Both sides should get ColumnId(6) (a.id) and ColumnId(7) (b.id).
                    let ids: std::collections::HashSet<ColumnId> = cols.iter().copied().collect();
                    assert!(
                        ids.contains(&ColumnId(6)),
                        "{} side missing ColumnId(6), got {:?}",
                        side_label,
                        ids
                    );
                    assert!(
                        ids.contains(&ColumnId(7)),
                        "{} side missing ColumnId(7), got {:?}",
                        side_label,
                        ids
                    );
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

    fn output_properties(op: &Operator) -> PhysicalPropertySet {
        super::super::derive::derive_output(op, &[])
    }

    fn required_input_properties(
        op: &Operator,
        parent_required: &PhysicalPropertySet,
        num_children: usize,
    ) -> Vec<PhysicalPropertySet> {
        super::super::derive::derive_required(op, parent_required, num_children)
    }

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
