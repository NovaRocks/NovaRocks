//! Top-down Cascades optimization with property enforcement.
//!
//! Implements the core Cascades search algorithm: for each (group, required_props)
//! pair, find the cheapest physical expression that satisfies the required
//! physical properties, recursively optimizing children and inserting enforcers
//! (PhysicalDistribution, PhysicalSort) when needed.

use std::collections::{HashMap, HashSet};

use super::cost::{CostOptions, compute_cost_with_properties};
use super::derive::PropertyAlternativeKind;
use super::memo::{Cost, GroupId, Memo};
use super::operator::*;
use super::property::*;
use super::stats::derive_statistics;
use crate::sql::optimizer::statistics::TableStatistics;

pub(crate) use super::derive::EnforcerKind;

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
    pub(crate) alt_kind: PropertyAlternativeKind,
    pub(crate) child_props: Vec<PhysicalPropertySet>,
    pub(crate) child_outputs: Vec<PhysicalPropertySet>,
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
        let mut best_alt_kind = PropertyAlternativeKind::Default;
        let mut best_child_props = Vec::new();
        let mut best_child_outputs = Vec::new();

        for expr_idx in 0..num_physical {
            let expr = &memo.groups[group_id].physical_exprs[expr_idx];

            // own_stats is the cardinality of THIS physical expr; it does not
            // depend on the property alternative, so derive it once per expr
            // instead of once per (expr, alt). Kept per-expr (not the group
            // cache) because same-group exprs can have different own_stats
            // (see physical_hash_aggregate_own_stats_are_per_expr_not_per_group).
            let own_stats = derive_statistics(expr, memo, &self.table_stats);

            let alternatives = super::derive::derive_required_alternatives(
                &expr.op,
                required,
                expr.children.len(),
            );
            let broadcast_is_only_alternative = !alternatives
                .iter()
                .any(|alt| alt.kind != PropertyAlternativeKind::BroadcastJoin);
            for alt in alternatives {
                let child_reqs = alt.child_props.clone();
                if child_reqs.len() != expr.children.len() {
                    continue;
                }

                if alt.kind == PropertyAlternativeKind::BroadcastJoin {
                    let required_for_correctness = matches!(
                        &expr.op,
                        Operator::PhysicalHashJoin(join)
                            if join.join_type == crate::sql::analysis::JoinKind::NullAwareLeftAnti
                    );
                    if let (Some(&probe_group_id), Some(&build_group_id)) =
                        (expr.children.first(), expr.children.get(1))
                    {
                        let probe_stats =
                            stats_for_group(&memo.groups[probe_group_id], memo, &self.table_stats);
                        let build_stats =
                            stats_for_group(&memo.groups[build_group_id], memo, &self.table_stats);
                        if !required_for_correctness
                            && !broadcast_is_only_alternative
                            && !super::cost::broadcast_gate_passes(
                                &probe_stats,
                                &build_stats,
                                &CostOptions::default(),
                            )
                        {
                            continue;
                        }
                    }
                }

                let child_stats_vec: Vec<_> = expr
                    .children
                    .iter()
                    .map(|&cg| stats_for_group(&memo.groups[cg], memo, &self.table_stats))
                    .collect();
                let child_stats_refs: Vec<&_> = child_stats_vec.iter().collect();

                let mut child_outputs: Vec<PhysicalPropertySet> =
                    Vec::with_capacity(expr.children.len());
                let mut child_cost_total = 0.0;
                let mut feasible = true;
                for (i, &cg) in expr.children.iter().enumerate() {
                    let child_cost = self.optimize_group(memo, cg, &child_reqs[i])?;
                    if child_cost.is_infinite() {
                        feasible = false;
                        break;
                    }
                    child_cost_total += child_cost;
                    let cw = self
                        .winners
                        .get(&(cg, child_reqs[i].clone()))
                        .expect("child just optimized; winner must be in cache");
                    child_outputs.push(cw.output.clone());
                }
                if !feasible {
                    continue;
                }

                let child_output_refs: Vec<&PhysicalPropertySet> = child_outputs.iter().collect();
                let own_cost = compute_cost_with_properties(
                    &expr.op,
                    &own_stats,
                    &child_stats_refs,
                    &child_output_refs,
                    &alt.kind,
                    &CostOptions::default(),
                );
                let total = own_cost + child_cost_total;
                let provided = super::derive::derive_output_for_alternative(
                    &expr.op,
                    &child_output_refs,
                    &alt.kind,
                );

                // Bridge provided → required via enforcer if needed.
                let (actual_output, enforcer_info, candidate_cost) = if provided.satisfies(required)
                {
                    (provided, None, total)
                } else {
                    let enforcers = super::derive::needed_enforcers(required, &provided);
                    if enforcers.is_empty() {
                        continue;
                    }
                    let group_stats =
                        stats_for_group(&memo.groups[group_id], memo, &self.table_stats);
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
                    best_alt_kind = alt.kind;
                    best_child_props = child_reqs;
                    best_child_outputs = child_outputs;
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
            output: best_output,
            alt_kind: best_alt_kind,
            child_props: best_child_props,
            child_outputs: best_child_outputs,
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
            row_count_confidence: lp.row_count_confidence,
            column_statistics: lp.column_statistics.clone(),
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
        row_count_confidence: crate::sql::optimizer::statistics::Confidence::Fallback,
        column_statistics: HashMap::new(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::MExpr;
    use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
    use arrow::datatypes::DataType;

    /// Build a simple memo with a single PhysicalScan group.
    pub(super) fn single_scan_memo() -> (Memo, GroupId) {
        let mut memo = Memo::new();
        let scan_op = Operator::PhysicalScan(PhysicalScanOp {
            database: "db".into(),
            table: crate::sql::catalog::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::sql::catalog::ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        });
        let expr = MExpr {
            id: 0,
            op: scan_op,
            children: vec![],
        };
        let gid = memo.new_group(expr);
        (memo, gid)
    }

    pub(super) fn make_table_stats() -> HashMap<String, TableStatistics> {
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

    fn test_col(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn minus_int(expr: TypedExpr, value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(expr),
                op: BinOp::Sub,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(value)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn make_two_table_inner_join_memo_for_test() -> (Memo, GroupId) {
        let mut memo = Memo::new();
        let left_group = memo.new_group(MExpr {
            id: 0,
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "left_t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::catalog::ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        });
        let right_group = memo.new_group(MExpr {
            id: 1,
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "right_t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::catalog::ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        });
        let root = memo.new_group(MExpr {
            id: 2,
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: test_col(1, "a_id"),
                    right: test_col(2, "b_id"),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Unknown,
            }),
            children: vec![left_group, right_group],
        });
        (memo, root)
    }

    fn inner_join_child_groups_for_test(memo: &Memo, root: GroupId) -> (GroupId, GroupId) {
        let root_expr = memo.groups[root]
            .physical_exprs
            .first()
            .expect("fixture root should have a physical expression");
        let [probe_group, build_group] = root_expr.children.as_slice() else {
            panic!("fixture join should have two children");
        };
        (*probe_group, *build_group)
    }

    fn set_group_logical_rows_for_test(
        memo: &mut Memo,
        group: GroupId,
        rows: f64,
        confidence: crate::sql::optimizer::statistics::Confidence,
    ) {
        memo.groups[group].logical_props = Some(crate::sql::optimizer::memo::LogicalProperties {
            output_columns: vec![],
            row_count: rows,
            row_count_confidence: confidence,
            column_statistics: HashMap::new(),
            equivalence_classes: Default::default(),
            unique_columns: vec![],
        });
    }

    fn make_large_build_inner_join_memo_for_test(
        probe_rows: f64,
        build_rows: f64,
        build_confidence: crate::sql::optimizer::statistics::Confidence,
    ) -> (Memo, GroupId, HashMap<String, TableStatistics>) {
        let (mut memo, root) = make_two_table_inner_join_memo_for_test();
        let (probe_group, build_group) = inner_join_child_groups_for_test(&memo, root);
        set_group_logical_rows_for_test(
            &mut memo,
            probe_group,
            probe_rows,
            crate::sql::optimizer::statistics::Confidence::Exact,
        );
        set_group_logical_rows_for_test(&mut memo, build_group, build_rows, build_confidence);
        (memo, root, HashMap::new())
    }

    fn make_expression_key_large_estimated_build_join_memo_for_test() -> (Memo, GroupId) {
        let (mut memo, root) = make_two_table_inner_join_memo_for_test();
        let (probe_group, build_group) = inner_join_child_groups_for_test(&memo, root);
        set_group_logical_rows_for_test(
            &mut memo,
            probe_group,
            3_543_657.0,
            crate::sql::optimizer::statistics::Confidence::Estimated,
        );
        set_group_logical_rows_for_test(
            &mut memo,
            build_group,
            648_000.0,
            crate::sql::optimizer::statistics::Confidence::Estimated,
        );

        let root_expr = memo.groups[root]
            .physical_exprs
            .first_mut()
            .expect("fixture root should have a physical expression");
        let Operator::PhysicalHashJoin(join) = &mut root_expr.op else {
            panic!("fixture root should be a hash join");
        };
        join.eq_conditions = vec![PhysicalHashJoinEqCondition {
            left: test_col(1, "a_id"),
            right: minus_int(test_col(2, "b_id"), 52),
            null_safe: false,
        }];
        (memo, root)
    }

    fn make_join_over_prepartitioned_children_for_test() -> (Memo, GroupId, GroupId, GroupId) {
        let mut memo = Memo::new();
        let left_scan = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(PhysicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let right_scan = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(PhysicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let left = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::shuffle_join([ColumnId(10)]),
            }),
            children: vec![left_scan],
        });
        let right = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::shuffle_join([ColumnId(20)]),
            }),
            children: vec![right_scan],
        });
        let root = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: crate::sql::analysis::JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: test_col(10, "c10"),
                    right: test_col(20, "c20"),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Unknown,
            }),
            children: vec![left, right],
        });
        (memo, root, left, right)
    }

    fn make_malformed_unknown_hash_join_memo_for_test() -> (Memo, GroupId) {
        let mut memo = Memo::new();
        let child_group = memo.new_group(MExpr {
            id: 0,
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "single_child".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::catalog::ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        });
        let root = memo.new_group(MExpr {
            id: 1,
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: test_col(1, "a_id"),
                    right: test_col(2, "b_id"),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Unknown,
            }),
            children: vec![child_group],
        });
        (memo, root)
    }

    fn find_hash_join_for_test(plan: &PhysicalPlanNode) -> Option<&PhysicalHashJoinOp> {
        if let Operator::PhysicalHashJoin(join) = &plan.op {
            return Some(join);
        }
        plan.children.iter().find_map(find_hash_join_for_test)
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
                source: crate::sql::catalog::ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
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
    fn winner_records_hash_join_alternative_and_child_properties() {
        let (memo, root) = make_two_table_inner_join_memo_for_test();
        let mut ctx = SearchContext::new(Default::default());
        let required = PhysicalPropertySet::gather();
        ctx.optimize_group(&memo, root, &required).expect("search");

        let winner = ctx
            .winners
            .get(&(root, required))
            .expect("root winner should be recorded");
        assert!(matches!(
            winner.alt_kind,
            super::super::derive::PropertyAlternativeKind::BroadcastJoin
                | super::super::derive::PropertyAlternativeKind::ShuffleJoin
        ));
        assert_eq!(winner.child_props.len(), 2);
        assert_eq!(winner.child_outputs.len(), 2);
        match winner.alt_kind {
            super::super::derive::PropertyAlternativeKind::BroadcastJoin => {
                assert_eq!(winner.child_props[0], PhysicalPropertySet::any());
                assert_eq!(winner.child_props[1], PhysicalPropertySet::broadcast());
            }
            super::super::derive::PropertyAlternativeKind::ShuffleJoin => {
                assert!(matches!(
                    winner.child_props[0].distribution,
                    DistributionSpec::HashPartitioned { .. }
                ));
                assert!(matches!(
                    winner.child_props[1].distribution,
                    DistributionSpec::HashPartitioned { .. }
                ));
            }
            super::super::derive::PropertyAlternativeKind::Default => {
                panic!("unknown hash join should choose a concrete property alternative")
            }
        }
    }

    #[test]
    fn unknown_hash_join_search_extracts_concrete_distribution() {
        let (memo, root) = make_two_table_inner_join_memo_for_test();
        let mut ctx = SearchContext::new(Default::default());
        let required = PhysicalPropertySet::gather();
        ctx.optimize_group(&memo, root, &required).expect("search");
        let plan =
            crate::sql::optimizer::extract::extract_best(&memo, root, &required, &ctx.winners)
                .expect("extract");
        let join = find_hash_join_for_test(&plan).expect("hash join");
        let winner = ctx
            .winners
            .get(&(root, required))
            .expect("root winner should be recorded");
        match winner.alt_kind {
            super::super::derive::PropertyAlternativeKind::BroadcastJoin => {
                assert_eq!(join.distribution, JoinDistribution::Broadcast);
            }
            super::super::derive::PropertyAlternativeKind::ShuffleJoin => {
                assert_eq!(join.distribution, JoinDistribution::Shuffle);
            }
            super::super::derive::PropertyAlternativeKind::Default => {
                assert!(!matches!(join.distribution, JoinDistribution::Unknown));
            }
        }
    }

    #[test]
    fn malformed_unknown_hash_join_is_infeasible_without_panic() {
        let (memo, root) = make_malformed_unknown_hash_join_memo_for_test();
        let mut ctx = SearchContext::new(Default::default());
        let required = PhysicalPropertySet::gather();

        let cost = ctx
            .optimize_group(&memo, root, &required)
            .expect("malformed unknown hash join should not panic");

        assert!(cost.is_infinite());
        let winner = ctx
            .winners
            .get(&(root, required))
            .expect("infeasible winner should still be cached");
        assert!(winner.cost.is_infinite());
    }

    #[test]
    fn search_rejects_broadcast_for_fallback_large_build() {
        let (memo, root, table_stats) = make_large_build_inner_join_memo_for_test(
            10_000_000.0,
            500_001.0,
            crate::sql::optimizer::statistics::Confidence::Fallback,
        );
        let mut ctx = SearchContext::new(table_stats);
        let required = PhysicalPropertySet::gather();

        ctx.optimize_group(&memo, root, &required).expect("search");
        let winner = ctx.winners.get(&(root, required.clone())).expect("winner");

        // The exact probe side is much larger than the fallback build side,
        // so broadcast would be cheaper than shuffle if the fallback
        // broadcast row gate did not reject builds above 500k rows.
        assert_eq!(
            winner.alt_kind,
            crate::sql::optimizer::derive::PropertyAlternativeKind::ShuffleJoin
        );
    }

    #[test]
    fn search_prefers_shuffle_when_exact_build_exceeds_probe_side() {
        let (memo, root, table_stats) = make_large_build_inner_join_memo_for_test(
            325_847.0,
            648_000.0,
            crate::sql::optimizer::statistics::Confidence::Exact,
        );
        let mut ctx = SearchContext::new(table_stats);
        let required = PhysicalPropertySet::gather();

        ctx.optimize_group(&memo, root, &required).expect("search");
        let winner = ctx.winners.get(&(root, required.clone())).expect("winner");

        assert_eq!(
            winner.alt_kind,
            crate::sql::optimizer::derive::PropertyAlternativeKind::ShuffleJoin
        );
    }

    #[test]
    fn search_rejects_broadcast_for_estimated_large_build() {
        let (memo, root, table_stats) = make_large_build_inner_join_memo_for_test(
            3_543_657.0,
            648_000.0,
            crate::sql::optimizer::statistics::Confidence::Estimated,
        );
        let mut ctx = SearchContext::new(table_stats);
        let required = PhysicalPropertySet::gather();

        ctx.optimize_group(&memo, root, &required).expect("search");
        let winner = ctx.winners.get(&(root, required.clone())).expect("winner");

        assert_eq!(
            winner.alt_kind,
            crate::sql::optimizer::derive::PropertyAlternativeKind::ShuffleJoin
        );
    }

    #[test]
    fn search_allows_broadcast_when_expression_key_has_no_shuffle_fallback() {
        let (memo, root) = make_expression_key_large_estimated_build_join_memo_for_test();
        let mut ctx = SearchContext::new(HashMap::new());
        let required = PhysicalPropertySet::gather();

        let cost = ctx.optimize_group(&memo, root, &required).expect("search");
        assert!(
            cost.is_finite(),
            "expression-key joins must keep a feasible broadcast fallback"
        );
        let winner = ctx.winners.get(&(root, required.clone())).expect("winner");

        assert_eq!(
            winner.alt_kind,
            crate::sql::optimizer::derive::PropertyAlternativeKind::BroadcastJoin
        );
    }

    #[test]
    fn search_reuses_child_shuffle_output_without_top_hash_enforcer() {
        let (memo, root, left, right) = make_join_over_prepartitioned_children_for_test();
        let mut ctx = SearchContext::new(Default::default());
        let required = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };

        ctx.optimize_group(&memo, root, &required).expect("search");
        let winner = ctx.winners.get(&(root, required.clone())).expect("winner");

        assert_eq!(
            winner.alt_kind,
            crate::sql::optimizer::derive::PropertyAlternativeKind::ShuffleJoin
        );
        let left_required = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let right_required = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        assert_eq!(
            winner.child_props,
            vec![left_required.clone(), right_required.clone()]
        );
        assert_eq!(
            winner.child_outputs,
            vec![left_required.clone(), right_required.clone()]
        );
        assert!(
            winner.enforcer.is_none(),
            "partitioned join output should satisfy parent directly"
        );

        let left_winner = ctx
            .winners
            .get(&(left, winner.child_props[0].clone()))
            .expect("left child winner");
        assert!(
            left_winner.enforcer.is_none(),
            "left child shuffle output should be reused directly"
        );
        assert_eq!(left_winner.output, winner.child_props[0]);

        let right_winner = ctx
            .winners
            .get(&(right, winner.child_props[1].clone()))
            .expect("right child winner");
        assert!(
            right_winner.enforcer.is_none(),
            "right child shuffle output should be reused directly"
        );
        assert_eq!(right_winner.output, winner.child_props[1]);
    }
}

#[cfg(test)]
mod cascaded_derivation_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::MExpr;
    use crate::sql::optimizer::operator::*;
    use arrow::datatypes::DataType;

    fn col(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan_op(table: &str) -> Operator {
        Operator::PhysicalScan(PhysicalScanOp {
            database: "db".into(),
            table: crate::sql::catalog::TableDef {
                name: table.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::sql::catalog::ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        })
    }

    fn table_stats_for_cascaded() -> HashMap<String, TableStatistics> {
        let mut ts = HashMap::new();
        ts.insert(
            "a".to_string(),
            TableStatistics {
                row_count: 10_000,
                column_stats: HashMap::new(),
            },
        );
        ts.insert(
            "b".to_string(),
            TableStatistics {
                row_count: 10_000,
                column_stats: HashMap::new(),
            },
        );
        // Small enough for the default broadcast gate.
        ts.insert(
            "small".to_string(),
            TableStatistics {
                row_count: 100,
                column_stats: HashMap::new(),
            },
        );
        ts
    }

    /// Build: Window[part=c10] -> BroadcastJoin(inner) -> (ShuffleJoin(a.c10 = b.c10), small)
    /// Returns (memo, root_group_id, broadcast_join_group_id).
    fn memo_window_over_broadcast_join() -> (Memo, GroupId, GroupId) {
        let mut memo = Memo::new();

        let g_a = memo.new_group(MExpr {
            id: 0,
            op: scan_op("a"),
            children: vec![],
        });
        let g_b = memo.new_group(MExpr {
            id: 1,
            op: scan_op("b"),
            children: vec![],
        });
        let g_small = memo.new_group(MExpr {
            id: 2,
            op: scan_op("small"),
            children: vec![],
        });

        let shuffle_join = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: col(10),
                right: col(10),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
        });
        let g_sj = memo.new_group(MExpr {
            id: 3,
            op: shuffle_join,
            children: vec![g_a, g_b],
        });

        let broadcast_join = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: col(10),
                right: col(10),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
        });
        let g_bj = memo.new_group(MExpr {
            id: 4,
            op: broadcast_join,
            children: vec![g_sj, g_small],
        });

        let window = Operator::PhysicalWindow(PhysicalWindowOp {
            window_exprs: vec![crate::sql::planner::plan::WindowExpr {
                name: "max".into(),
                args: vec![],
                partition_by: vec![col(10)],
                order_by: vec![],
                window_frame: None,
                ignore_nulls: false,
                distinct: false,
                output_name: "win".into(),
                output_column_id: crate::sql::column_id::ColumnId::UNSET,
                result_type: DataType::Int64,
            }],
            output_columns: vec![],
        });
        let g_w = memo.new_group(MExpr {
            id: 5,
            op: window,
            children: vec![g_bj],
        });

        (memo, g_w, g_bj)
    }

    #[test]
    fn winner_records_actual_output_for_scan() {
        let (memo, gid) = super::tests::single_scan_memo();
        let mut ctx = SearchContext::new(super::tests::make_table_stats());
        ctx.optimize_group(&memo, gid, &PhysicalPropertySet::any())
            .unwrap();
        let w = ctx.winners.get(&(gid, PhysicalPropertySet::any())).unwrap();
        assert_eq!(w.output, PhysicalPropertySet::any());
    }

    #[test]
    fn cascaded_output_through_broadcast_join_repartitions_after_join() {
        let (memo, root, g_bj) = memo_window_over_broadcast_join();
        let mut ctx = SearchContext::new(table_stats_for_cascaded());
        let cost = ctx
            .optimize_group(&memo, root, &PhysicalPropertySet::any())
            .unwrap();
        assert!(cost.is_finite(), "search must produce a feasible plan");

        // Window group's direct winner still has no top enforcer; the child
        // winner below is responsible for satisfying the partition requirement.
        let w = ctx
            .winners
            .get(&(root, PhysicalPropertySet::any()))
            .unwrap();
        assert!(
            w.enforcer.is_none(),
            "Window should receive an already-partitioned child. winner = {w:?}"
        );

        // Broadcast joins do not advertise inherited left distribution. When
        // a parent requires hash partitioning, the enforcer must sit above the
        // entire broadcast join, not below it on only the probe child.
        let window_expr = memo.groups[root]
            .physical_exprs
            .first()
            .expect("window physical expr");
        assert_eq!(window_expr.children.as_slice(), &[g_bj]);
        let bj_req = crate::sql::optimizer::derive::derive_required(
            &window_expr.op,
            &PhysicalPropertySet::any(),
            1,
        )
        .into_iter()
        .next()
        .expect("window child requirement");
        let bj_winner = ctx
            .winners
            .get(&(g_bj, bj_req))
            .expect("broadcast join child requirement should have a winner");
        assert!(
            matches!(
                bj_winner.enforcer.as_ref().map(|e| &e.kind),
                Some(EnforcerKind::Distribution(_))
            ),
            "Broadcast Join should repartition above the join. winner = {bj_winner:?}"
        );
    }
}
