//! Top-down Cascades optimization with property enforcement.
//!
//! Implements the core Cascades search algorithm: for each (group, required_props)
//! pair, find the cheapest physical expression that satisfies the required
//! physical properties, recursively optimizing children and inserting enforcers
//! (PhysicalDistribution, PhysicalSort) when needed.

use std::collections::{HashMap, HashSet};

use super::cost::{CostInput, CostOptions, compute_cost_estimate};
use super::derive::PropertyAlternativeKind;
use super::memo::{GroupId, Memo, TotalCost};
#[cfg(test)]
use super::operator::*;
use super::property::*;
use super::statistics::CostEstimate;
#[cfg(test)]
use super::statistics::MAX_FINITE_COST;
use crate::sql::optimizer::stats_input::OptimizerStatsInput;

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
    pub(crate) cost_estimate: CostEstimate,
    pub(crate) total_cost: TotalCost,
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

impl Winner {
    pub(crate) fn new(
        group_id: GroupId,
        expr_index: usize,
        cost_estimate: CostEstimate,
        cost_options: &CostOptions,
        enforcer: Option<EnforcerInfo>,
        output: PhysicalPropertySet,
        alt_kind: PropertyAlternativeKind,
        child_props: Vec<PhysicalPropertySet>,
        child_outputs: Vec<PhysicalPropertySet>,
    ) -> Self {
        let total_cost = cost_estimate.total_with_options(cost_options);
        Self {
            group_id,
            expr_index,
            cost_estimate,
            total_cost,
            enforcer,
            output,
            alt_kind,
            child_props,
            child_outputs,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_legacy_total(
        group_id: GroupId,
        expr_index: usize,
        total_cost: TotalCost,
        cost_options: &CostOptions,
        enforcer: Option<EnforcerInfo>,
        output: PhysicalPropertySet,
        alt_kind: PropertyAlternativeKind,
        child_props: Vec<PhysicalPropertySet>,
        child_outputs: Vec<PhysicalPropertySet>,
    ) -> Self {
        let cost_estimate = cost_estimate_for_total(total_cost, cost_options);
        let total_cost = if total_cost.is_finite() && total_cost > 0.0 {
            total_cost
        } else {
            0.0
        };
        // Compatibility bridge for legacy fixtures that still construct
        // winners from a scalar total.
        Self {
            group_id,
            expr_index,
            cost_estimate,
            total_cost,
            enforcer,
            output,
            alt_kind,
            child_props,
            child_outputs,
        }
    }

    pub(crate) fn infeasible(group_id: GroupId) -> Self {
        Self {
            group_id,
            expr_index: 0,
            cost_estimate: CostEstimate::default(),
            total_cost: f64::INFINITY,
            enforcer: None,
            output: PhysicalPropertySet::any(),
            alt_kind: PropertyAlternativeKind::Default,
            child_props: vec![],
            child_outputs: vec![],
        }
    }
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
    pub(crate) stats_input: OptimizerStatsInput,
    pub(crate) cost_options: CostOptions,
    /// Set of (GroupId, PhysicalPropertySet) pairs currently being computed.
    /// Used to break mutual enforcer cycles: when group A's enforcer path
    /// recurses back into the same (group, props) that is already on the
    /// call stack, we return INFINITY instead of recursing infinitely.
    in_progress: HashSet<(GroupId, PhysicalPropertySet)>,
}

impl SearchContext {
    pub(crate) fn new(stats_input: OptimizerStatsInput, cost_options: CostOptions) -> Self {
        Self {
            winners: HashMap::new(),
            stats_input,
            cost_options,
            in_progress: HashSet::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(stats_input: OptimizerStatsInput) -> Self {
        Self::new(stats_input, CostOptions::default())
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
    ) -> Result<TotalCost, String> {
        let cache_key = (group_id, required.clone());

        // 1. Check winner cache.
        if let Some(winner) = self.winners.get(&cache_key) {
            return Ok(winner.total_cost);
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
            self.winners.insert(cache_key, Winner::infeasible(group_id));
            return Ok(f64::INFINITY);
        }

        let mut best_cost = f64::INFINITY;
        let mut best_cost_estimate = CostEstimate::default();
        let mut best_index: usize = 0;
        let mut best_enforcer: Option<EnforcerInfo> = None;
        let mut best_output = PhysicalPropertySet::any();
        let mut best_alt_kind = PropertyAlternativeKind::Default;
        let mut best_child_props = Vec::new();
        let mut best_child_outputs = Vec::new();

        // own_stats is the operator's own output statistic. All physical
        // members of a group are logically equivalent, and the operators whose
        // cost reads own_stats (scan/filter/project/sort/distribution/union,
        // and NestLoopJoin via avg_row_size) get value-identical own_stats
        // across members; the operators whose own_stats can vary among members
        // (hash join reorder shapes, aggregate) are costed from child_stats and
        // do not read own_stats. So the single per-group collapsed statistic is
        // the correct, value-identical input — read it once per group.
        let own_stats = stats_for_group(&memo.groups[group_id], memo, &self.stats_input);

        for expr_idx in 0..num_physical {
            let expr = &memo.groups[group_id].physical_exprs[expr_idx];

            let alternatives = super::derive::derive_required_alternatives(
                &expr.op,
                &memo.scalars,
                required,
                expr.children.len(),
            );
            let feasibility_is_advisory =
                super::cost::feasibility_is_advisory_only(&expr.op, &alternatives);
            for alt in alternatives {
                let child_reqs = alt.child_props.clone();
                if child_reqs.len() != expr.children.len() {
                    continue;
                }

                if alt.kind == PropertyAlternativeKind::BroadcastJoin {
                    if let (Some(&probe_group_id), Some(&build_group_id)) =
                        (expr.children.first(), expr.children.get(1))
                    {
                        let probe_stats =
                            stats_for_group(&memo.groups[probe_group_id], memo, &self.stats_input);
                        let build_stats =
                            stats_for_group(&memo.groups[build_group_id], memo, &self.stats_input);
                        let feas = super::cost::broadcast_is_feasible(
                            &probe_stats,
                            &build_stats,
                            &self.cost_options,
                        );
                        if !feasibility_is_advisory && !feas.feasible {
                            continue;
                        }
                    }
                }

                let child_stats_vec: Vec<_> = expr
                    .children
                    .iter()
                    .map(|&cg| stats_for_group(&memo.groups[cg], memo, &self.stats_input))
                    .collect();
                let child_stats_refs: Vec<&_> = child_stats_vec.iter().collect();

                let mut child_outputs: Vec<PhysicalPropertySet> =
                    Vec::with_capacity(expr.children.len());
                let mut child_cost_estimate = CostEstimate::default();
                let mut feasible = true;
                for (i, &cg) in expr.children.iter().enumerate() {
                    let child_cost = self.optimize_group(memo, cg, &child_reqs[i])?;
                    if child_cost.is_infinite() {
                        feasible = false;
                        break;
                    }
                    let cw = self
                        .winners
                        .get(&(cg, child_reqs[i].clone()))
                        .expect("child just optimized; winner must be in cache");
                    child_cost_estimate = child_cost_estimate.add_sanitized(&cw.cost_estimate);
                    child_outputs.push(cw.output.clone());
                }
                if !feasible {
                    continue;
                }

                let child_output_refs: Vec<&PhysicalPropertySet> = child_outputs.iter().collect();
                let cost_input = CostInput {
                    op: &expr.op,
                    own_stats: &own_stats,
                    child_stats: &child_stats_refs,
                    child_outputs: &child_output_refs,
                    required_output: required,
                    alt_kind: &alt.kind,
                    scalars: Some(&memo.scalars),
                    options: &self.cost_options,
                };
                let operator_estimate = compute_cost_estimate(&cost_input).sanitized();
                let mut candidate_estimate = child_cost_estimate.add_sanitized(&operator_estimate);
                let provided = super::derive::derive_output_for_alternative(
                    &expr.op,
                    &memo.scalars,
                    &child_output_refs,
                    &alt.kind,
                );

                // Bridge provided → required via enforcer if needed.
                let (actual_output, enforcer_info) = if provided.satisfies(required) {
                    (provided, None)
                } else {
                    let enforcers = super::derive::needed_enforcers(required, &provided);
                    if enforcers.is_empty() {
                        continue;
                    }
                    let group_stats =
                        stats_for_group(&memo.groups[group_id], memo, &self.stats_input);
                    for enforcer in &enforcers {
                        let enforcer_estimate = super::derive::estimate_enforcer_cost_estimate(
                            enforcer,
                            &group_stats,
                            &self.cost_options,
                        )
                        .sanitized();
                        candidate_estimate = candidate_estimate.add_sanitized(&enforcer_estimate);
                    }
                    let kind = enforcers.into_iter().next().unwrap();
                    (
                        required.clone(),
                        Some(EnforcerInfo {
                            kind,
                            child_props: provided,
                        }),
                    )
                };
                let candidate_cost = candidate_estimate.total_with_options(&self.cost_options);

                if candidate_cost < best_cost {
                    best_cost = candidate_cost;
                    best_cost_estimate = candidate_estimate;
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
        let winner = if best_cost.is_infinite() {
            Winner::infeasible(group_id)
        } else {
            Winner::new(
                group_id,
                best_index,
                best_cost_estimate,
                &self.cost_options,
                best_enforcer,
                best_output,
                best_alt_kind,
                best_child_props,
                best_child_outputs,
            )
        };
        let total_cost = winner.total_cost;
        self.winners.insert(cache_key, winner);
        Ok(total_cost)
    }
}

#[cfg(test)]
pub(crate) fn cost_estimate_for_total(
    total_cost: TotalCost,
    cost_options: &CostOptions,
) -> CostEstimate {
    debug_assert!(total_cost.is_finite());
    let requested_total = if total_cost.is_finite() && total_cost > 0.0 {
        total_cost
    } else {
        0.0
    };
    if requested_total == 0.0 {
        return CostEstimate::default();
    }

    let mut closest_estimate: Option<(CostEstimate, f64)> = None;
    // Prefer a weight >= 1.0 so the synthetic dimension stays at or below the
    // scalar total and does not trip the per-dimension sanitizer cap.
    for require_weight_at_least_one in [true, false] {
        for (dimension_index, weight) in [
            cost_options.cpu_weight,
            cost_options.memory_weight,
            cost_options.network_weight,
        ]
        .into_iter()
        .enumerate()
        {
            if !weight.is_finite() || weight <= 0.0 || (require_weight_at_least_one && weight < 1.0)
            {
                continue;
            }

            let dimension_cost = requested_total / weight;
            if !dimension_cost.is_finite()
                || dimension_cost < 0.0
                || dimension_cost > MAX_FINITE_COST
            {
                continue;
            }

            let estimate = match dimension_index {
                0 => CostEstimate {
                    cpu_cost: dimension_cost,
                    memory_cost: 0.0,
                    network_cost: 0.0,
                },
                1 => CostEstimate {
                    cpu_cost: 0.0,
                    memory_cost: dimension_cost,
                    network_cost: 0.0,
                },
                2 => CostEstimate {
                    cpu_cost: 0.0,
                    memory_cost: 0.0,
                    network_cost: dimension_cost,
                },
                _ => unreachable!("fixed cost dimension list has three entries"),
            };

            let weighted_total = estimate.total_with_options(cost_options);
            let delta = (weighted_total - requested_total).abs();
            let tolerance = requested_total.abs().max(1.0) * 1.0e-12;
            if delta <= tolerance {
                return estimate;
            }
            match &closest_estimate {
                Some((_, closest_delta)) if *closest_delta <= delta => {}
                _ => closest_estimate = Some((estimate, delta)),
            }
        }
    }

    closest_estimate
        .map(|(estimate, _)| estimate)
        .unwrap_or_else(|| CostEstimate {
            cpu_cost: MAX_FINITE_COST,
            memory_cost: 0.0,
            network_cost: 0.0,
        })
}

// ---------------------------------------------------------------------------
// Statistics helper
// ---------------------------------------------------------------------------

/// Get a group's collapsed statistic: the lexicographic-argmax representative's
/// stats cached in `logical_props` (set by `derive_group_statistics`). If those
/// are absent (defensive — should not happen post-derive), re-pick via
/// `pick_group_representative`, the same collapse helper the derive path uses.
fn stats_for_group(
    group: &super::memo::Group,
    memo: &Memo,
    stats_input: &OptimizerStatsInput,
) -> crate::sql::optimizer::statistics::Statistics {
    // Try logical props first (set by derive_group_statistics). Once Site 1
    // (derive_group_statistics_for) is argmax-correct, this cache already holds
    // the lexicographic-argmax representative's stats.
    if let Some(ref lp) = group.logical_props {
        return crate::sql::optimizer::statistics::Statistics {
            output_row_count: lp.row_count,
            row_count_confidence: lp.row_count_confidence,
            column_statistics: lp.column_statistics.clone(),
        };
    }

    // Defensive fallback (should not happen in practice — logical_props is
    // populated by derive_group_statistics): re-pick the representative via the
    // same shared argmax helper Site 1 uses, so this path stays consistent
    // rather than re-deriving from first().
    crate::sql::optimizer::stats::pick_group_representative(memo, group.id, stats_input)
        .map(|(_, stats)| stats)
        .unwrap_or_else(|| {
            // Empty group — should not happen in practice.
            crate::sql::optimizer::statistics::Statistics {
                output_row_count: 1.0,
                row_count_confidence: crate::sql::optimizer::statistics::Confidence::Fallback,
                column_statistics: HashMap::new(),
            }
        })
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
    use crate::sql::optimizer::statistics::TableStatistics;
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use arrow::datatypes::DataType;

    /// Build a simple memo with a single PhysicalScan group.
    pub(super) fn single_scan_memo() -> (Memo, GroupId) {
        let mut memo = Memo::new();
        let scan_op = Operator::PhysicalScan(ScanOp {
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
            stats_ref: None,
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

    fn project_over_scan_memo_for_test() -> (Memo, GroupId, GroupId) {
        let (mut memo, scan_group) = single_scan_memo();
        let project_expr = intern_typed(&mut memo.scalars, &test_col(1, "c1"));
        let root = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    expr: project_expr,
                    output_name: "c1".to_string(),
                    output_column_id: ColumnId(1),
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            children: vec![scan_group],
        });
        set_group_logical_rows_for_test(
            &mut memo,
            scan_group,
            1_000.0,
            crate::sql::optimizer::statistics::Confidence::Exact,
        );
        set_group_logical_rows_for_test(
            &mut memo,
            root,
            1_000.0,
            crate::sql::optimizer::statistics::Confidence::Exact,
        );
        (memo, root, scan_group)
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

    pub(super) fn legacy_stats_input(
        table_stats: HashMap<String, TableStatistics>,
    ) -> OptimizerStatsInput {
        OptimizerStatsInput::from_legacy_table_stats_for_migration(&table_stats)
    }

    fn empty_stats_input() -> OptimizerStatsInput {
        legacy_stats_input(HashMap::new())
    }

    fn assert_cost_estimate_close(actual: &CostEstimate, expected: &CostEstimate) {
        assert_dimension_close("cpu", actual.cpu_cost, expected.cpu_cost);
        assert_dimension_close("memory", actual.memory_cost, expected.memory_cost);
        assert_dimension_close("network", actual.network_cost, expected.network_cost);
    }

    fn assert_dimension_close(label: &str, actual: f64, expected: f64) {
        let tolerance = expected.abs().max(1.0) * 1.0e-12;
        assert!(
            (actual - expected).abs() <= tolerance,
            "{label} cost mismatch: actual={actual}, expected={expected}, tolerance={tolerance}"
        );
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

    fn eq_cond(memo: &mut Memo, left: TypedExpr, right: TypedExpr) -> PhysicalHashJoinEqCondition {
        PhysicalHashJoinEqCondition {
            left: intern_typed(&mut memo.scalars, &left),
            right: intern_typed(&mut memo.scalars, &right),
            null_safe: false,
        }
    }

    fn make_two_table_inner_join_memo_for_test() -> (Memo, GroupId) {
        let mut memo = Memo::new();
        let left_group = memo.new_group(MExpr {
            id: 0,
            op: Operator::PhysicalScan(ScanOp {
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
                stats_ref: None,
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
            op: Operator::PhysicalScan(ScanOp {
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
                stats_ref: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        });
        let eq_condition = eq_cond(&mut memo, test_col(1, "a_id"), test_col(2, "b_id"));
        let root = memo.new_group(MExpr {
            id: 2,
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq_condition],
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

    fn set_group_logical_stats_for_test(
        memo: &mut Memo,
        group: GroupId,
        rows: f64,
        row_width: f64,
        confidence: crate::sql::optimizer::statistics::Confidence,
    ) {
        set_group_logical_rows_for_test(memo, group, rows, confidence);
        let props = memo.groups[group]
            .logical_props
            .as_mut()
            .expect("logical props should have been installed");
        let mut column = crate::sql::optimizer::statistics::ColumnStatistic::unknown();
        column.average_row_size = row_width;
        column.confidence = confidence;
        props.column_statistics.insert(ColumnId(10_000), column);
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

        let eq_condition = eq_cond(
            &mut memo,
            test_col(1, "a_id"),
            minus_int(test_col(2, "b_id"), 52),
        );
        let root_expr = memo.groups[root]
            .physical_exprs
            .first_mut()
            .expect("fixture root should have a physical expression");
        let Operator::PhysicalHashJoin(join) = &mut root_expr.op else {
            panic!("fixture root should be a hash join");
        };
        join.eq_conditions = vec![eq_condition];
        (memo, root)
    }

    fn make_join_over_prepartitioned_children_for_test() -> (Memo, GroupId, GroupId, GroupId) {
        let mut memo = Memo::new();
        let left_scan = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let right_scan = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
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
        let eq_condition = eq_cond(&mut memo, test_col(10, "c10"), test_col(20, "c20"));
        let root = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: crate::sql::analysis::JoinKind::Inner,
                eq_conditions: vec![eq_condition],
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
            op: Operator::PhysicalScan(ScanOp {
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
                stats_ref: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        });
        let eq_condition = eq_cond(&mut memo, test_col(1, "a_id"), test_col(2, "b_id"));
        let root = memo.new_group(MExpr {
            id: 1,
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq_condition],
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
        let mut ctx = SearchContext::new_for_test(legacy_stats_input(make_table_stats()));
        let cost = ctx
            .optimize_group(&memo, gid, &PhysicalPropertySet::any())
            .unwrap();
        assert!(cost.is_finite());
        assert!(ctx.winners.contains_key(&(gid, PhysicalPropertySet::any())));
    }

    #[test]
    fn scan_with_gather_uses_enforcer() {
        let (memo, gid) = single_scan_memo();
        let mut ctx = SearchContext::new_for_test(legacy_stats_input(make_table_stats()));
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
    fn search_records_cumulative_cost_estimate_for_scan_with_gather_enforcer() {
        let (memo, gid) = single_scan_memo();
        let mut ctx = SearchContext::new_for_test(legacy_stats_input(make_table_stats()));
        let required = PhysicalPropertySet::gather();

        ctx.optimize_group(&memo, gid, &required).expect("search");

        let winner = ctx.winners.get(&(gid, required.clone())).expect("winner");
        let expr = memo.groups[gid]
            .physical_exprs
            .get(winner.expr_index)
            .expect("winner expression");
        let own_stats = stats_for_group(&memo.groups[gid], &memo, &ctx.stats_input);
        let child_outputs: Vec<&PhysicalPropertySet> = Vec::new();
        let child_stats: Vec<&crate::sql::optimizer::statistics::Statistics> = Vec::new();
        let scan_input = CostInput {
            op: &expr.op,
            own_stats: &own_stats,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &winner.alt_kind,
            scalars: Some(&memo.scalars),
            options: &ctx.cost_options,
        };
        let scan_estimate = super::super::cost::compute_cost_estimate(&scan_input).sanitized();
        let enforcer = winner
            .enforcer
            .as_ref()
            .expect("gather requirement should require distribution enforcer");
        let enforcer_estimate = super::super::derive::estimate_enforcer_cost_estimate(
            &enforcer.kind,
            &own_stats,
            &ctx.cost_options,
        )
        .sanitized();
        let expected = scan_estimate.add_sanitized(&enforcer_estimate);

        assert_eq!(winner.cost_estimate.cpu_cost, expected.cpu_cost);
        assert_eq!(winner.cost_estimate.memory_cost, expected.memory_cost);
        assert_eq!(winner.cost_estimate.network_cost, expected.network_cost);
        assert!(winner.cost_estimate.network_cost > 0.0);
        assert_eq!(
            winner.total_cost,
            winner.cost_estimate.total_with_options(&ctx.cost_options)
        );
    }

    #[test]
    fn search_returned_total_matches_winner_flattened_cost_for_finite_scan_path() {
        let (memo, gid) = single_scan_memo();
        let mut ctx = SearchContext::new_for_test(legacy_stats_input(make_table_stats()));
        let required = PhysicalPropertySet::any();

        let returned_total = ctx.optimize_group(&memo, gid, &required).expect("search");

        let winner = ctx.winners.get(&(gid, required)).expect("winner");
        assert!(returned_total.is_finite());
        assert_eq!(returned_total, winner.total_cost);
        assert_eq!(
            winner.total_cost,
            winner.cost_estimate.total_with_options(&ctx.cost_options)
        );
    }

    #[test]
    fn winner_total_cost_uses_context_weights() {
        let (memo, gid) = single_scan_memo();
        let options = CostOptions {
            cpu_weight: 9.0,
            memory_weight: 0.0,
            network_weight: 0.0,
            ..Default::default()
        };
        let mut ctx = SearchContext::new(legacy_stats_input(make_table_stats()), options.clone());
        let required = PhysicalPropertySet::any();

        ctx.optimize_group(&memo, gid, &required).expect("search");

        let winner = ctx.winners.get(&(gid, required)).expect("winner");
        assert!(winner.cost_estimate.cpu_cost > 0.0);
        assert_eq!(
            winner.total_cost,
            winner.cost_estimate.total_with_options(&options)
        );
        assert_ne!(
            winner.total_cost,
            winner
                .cost_estimate
                .total_with_options(&CostOptions::default())
        );
    }

    #[test]
    fn search_parent_cost_estimate_includes_child_winner_estimate() {
        let (memo, root, child) = project_over_scan_memo_for_test();
        let mut ctx = SearchContext::new_for_test(empty_stats_input());
        let required = PhysicalPropertySet::any();

        ctx.optimize_group(&memo, root, &required).expect("search");

        let parent_winner = ctx
            .winners
            .get(&(root, required.clone()))
            .expect("parent winner");
        assert_eq!(
            parent_winner.child_props,
            vec![PhysicalPropertySet::any()],
            "project should optimize its single child with Any properties"
        );
        assert!(
            parent_winner.enforcer.is_none(),
            "Any requirement should not add a parent enforcer"
        );

        let child_required = parent_winner.child_props[0].clone();
        let child_winner = ctx
            .winners
            .get(&(child, child_required))
            .expect("child winner");
        let expr = memo.groups[root]
            .physical_exprs
            .get(parent_winner.expr_index)
            .expect("parent winner expression");
        let own_stats = stats_for_group(&memo.groups[root], &memo, &ctx.stats_input);
        let child_stats = stats_for_group(&memo.groups[child], &memo, &ctx.stats_input);
        let child_stats_refs = vec![&child_stats];
        let child_output_refs = vec![&child_winner.output];
        let parent_input = CostInput {
            op: &expr.op,
            own_stats: &own_stats,
            child_stats: &child_stats_refs,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &parent_winner.alt_kind,
            scalars: Some(&memo.scalars),
            options: &ctx.cost_options,
        };
        let parent_self_estimate = compute_cost_estimate(&parent_input).sanitized();
        assert!(
            parent_self_estimate.cpu_cost > 0.0 || parent_self_estimate.memory_cost > 0.0,
            "fixture must have a non-zero parent self cost"
        );

        let expected = child_winner
            .cost_estimate
            .add_sanitized(&parent_self_estimate);
        assert_cost_estimate_close(&parent_winner.cost_estimate, &expected);
        assert!(
            parent_winner.cost_estimate.cpu_cost > child_winner.cost_estimate.cpu_cost,
            "parent CPU estimate should include child CPU plus project self CPU"
        );
    }

    #[test]
    fn empty_group_returns_infinity() {
        let mut memo = Memo::new();
        // Create a group with only a logical expr (no physical).
        let logical_op = Operator::LogicalScan(ScanOp {
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
            stats_ref: None,
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

        let mut ctx = SearchContext::new_for_test(legacy_stats_input(make_table_stats()));
        let cost = ctx
            .optimize_group(&memo, gid, &PhysicalPropertySet::any())
            .unwrap();
        assert!(cost.is_infinite());
    }

    #[test]
    fn winner_cache_prevents_recomputation() {
        let (memo, gid) = single_scan_memo();
        let mut ctx = SearchContext::new_for_test(legacy_stats_input(make_table_stats()));
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
        let (mut memo, root) = make_two_table_inner_join_memo_for_test();
        let mut ctx = SearchContext::new_for_test(empty_stats_input());
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
    fn search_uses_cost_estimate_total_for_winner_cost() {
        let options = CostOptions {
            cpu_weight: 1.0,
            memory_weight: 0.0,
            network_weight: 0.0,
            ..Default::default()
        };
        let estimate = crate::sql::optimizer::statistics::CostEstimate {
            cpu_cost: 10.0,
            memory_cost: 1000.0,
            network_cost: 1000.0,
        };

        assert_eq!(estimate.total_with_options(&options), 10.0);
    }

    #[test]
    fn winner_new_keeps_total_cost_in_sync_with_estimate() {
        let options = CostOptions {
            cpu_weight: 1.0,
            memory_weight: 2.0,
            network_weight: 3.0,
            ..Default::default()
        };
        let estimate = crate::sql::optimizer::statistics::CostEstimate {
            cpu_cost: 10.0,
            memory_cost: 20.0,
            network_cost: 30.0,
        };

        let winner = Winner::new(
            7,
            3,
            estimate.clone(),
            &options,
            None,
            PhysicalPropertySet::gather(),
            PropertyAlternativeKind::Default,
            vec![PhysicalPropertySet::any()],
            vec![PhysicalPropertySet::any()],
        );

        assert_eq!(winner.cost_estimate.cpu_cost, estimate.cpu_cost);
        assert_eq!(winner.cost_estimate.memory_cost, estimate.memory_cost);
        assert_eq!(winner.cost_estimate.network_cost, estimate.network_cost);
        assert_eq!(winner.total_cost, estimate.total_with_options(&options));
    }

    #[test]
    fn cost_estimate_for_total_preserves_large_finite_total() {
        let options = CostOptions::default();
        let best_cost = 6.0e299;
        let estimate = cost_estimate_for_total(best_cost, &options);

        let winner = Winner::new(
            7,
            3,
            estimate,
            &options,
            None,
            PhysicalPropertySet::gather(),
            PropertyAlternativeKind::Default,
            vec![PhysicalPropertySet::any()],
            vec![PhysicalPropertySet::any()],
        );

        let tolerance = best_cost * 1.0e-12;
        assert!(
            (winner.total_cost - best_cost).abs() <= tolerance,
            "winner total {} should preserve best cost {}",
            winner.total_cost,
            best_cost
        );
    }

    #[test]
    fn legacy_winner_preserves_total_above_dimension_cap() {
        let options = CostOptions::default();
        let total_cost = 1.6e300;

        let winner = Winner::from_legacy_total(
            7,
            3,
            total_cost,
            &options,
            None,
            PhysicalPropertySet::gather(),
            PropertyAlternativeKind::Default,
            vec![PhysicalPropertySet::any()],
            vec![PhysicalPropertySet::any()],
        );

        let tolerance = total_cost * 1.0e-12;
        assert!(
            (winner.total_cost - total_cost).abs() <= tolerance,
            "legacy winner total {} should preserve scalar total {}",
            winner.total_cost,
            total_cost
        );
        assert!(winner.cost_estimate.cpu_cost.is_finite());
        assert!(winner.cost_estimate.memory_cost.is_finite());
        assert!(winner.cost_estimate.network_cost.is_finite());
    }

    #[test]
    fn infeasible_winner_uses_total_cost_sentinel_without_infinite_dimensions() {
        let winner = Winner::infeasible(9);

        assert!(winner.total_cost.is_infinite());
        assert!(winner.cost_estimate.cpu_cost.is_finite());
        assert!(winner.cost_estimate.memory_cost.is_finite());
        assert!(winner.cost_estimate.network_cost.is_finite());
    }

    #[test]
    fn unknown_hash_join_search_extracts_concrete_distribution() {
        let (mut memo, root) = make_two_table_inner_join_memo_for_test();
        let mut ctx = SearchContext::new_for_test(empty_stats_input());
        let required = PhysicalPropertySet::gather();
        ctx.optimize_group(&memo, root, &required).expect("search");
        let plan =
            crate::sql::optimizer::extract::extract_best(&mut memo, root, &required, &ctx.winners)
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
        let mut ctx = SearchContext::new_for_test(empty_stats_input());
        let required = PhysicalPropertySet::gather();

        let cost = ctx
            .optimize_group(&memo, root, &required)
            .expect("malformed unknown hash join should not panic");

        assert!(cost.is_infinite());
        let winner = ctx
            .winners
            .get(&(root, required))
            .expect("infeasible winner should still be cached");
        assert!(winner.total_cost.is_infinite());
    }

    #[test]
    fn search_rejects_broadcast_for_fallback_large_build() {
        let (memo, root, table_stats) = make_large_build_inner_join_memo_for_test(
            10_000_000.0,
            500_001.0,
            crate::sql::optimizer::statistics::Confidence::Fallback,
        );
        let mut ctx = SearchContext::new_for_test(legacy_stats_input(table_stats));
        let required = PhysicalPropertySet::gather();

        ctx.optimize_group(&memo, root, &required).expect("search");
        let winner = ctx.winners.get(&(root, required.clone())).expect("winner");

        // Fallback stats without column statistics are uninformative, so the
        // feasibility predicate prunes broadcast even when it could be cheaper.
        assert_eq!(
            winner.alt_kind,
            crate::sql::optimizer::derive::PropertyAlternativeKind::ShuffleJoin
        );
    }

    #[test]
    fn search_broadcast_feasibility_uses_context_cost_options() {
        let (mut memo, root) = make_two_table_inner_join_memo_for_test();
        let (probe_group, build_group) = inner_join_child_groups_for_test(&memo, root);
        set_group_logical_stats_for_test(
            &mut memo,
            probe_group,
            10_000_000.0,
            8.0,
            crate::sql::optimizer::statistics::Confidence::Exact,
        );
        set_group_logical_stats_for_test(
            &mut memo,
            build_group,
            100_000.0,
            8.0,
            crate::sql::optimizer::statistics::Confidence::Exact,
        );

        let required = PhysicalPropertySet::gather();
        let mut low_budget_profile = crate::sql::optimizer::cost::ClusterResourceProfile::default();
        low_budget_profile.per_node_build_memory_budget_bytes = 1.0 * 1024.0 * 1024.0;
        let mut low_budget_options = CostOptions::default();
        low_budget_options.apply_profile(low_budget_profile);
        let mut low_budget_ctx = SearchContext::new(empty_stats_input(), low_budget_options);
        low_budget_ctx
            .optimize_group(&memo, root, &required)
            .expect("low-budget search");
        let low_budget_winner = low_budget_ctx
            .winners
            .get(&(root, required.clone()))
            .expect("low-budget winner");

        let mut high_budget_profile =
            crate::sql::optimizer::cost::ClusterResourceProfile::default();
        high_budget_profile.per_node_build_memory_budget_bytes = 8.0 * 1024.0 * 1024.0;
        let mut high_budget_options = CostOptions::default();
        high_budget_options.apply_profile(high_budget_profile);
        let mut high_budget_ctx = SearchContext::new(empty_stats_input(), high_budget_options);
        high_budget_ctx
            .optimize_group(&memo, root, &required)
            .expect("high-budget search");
        let high_budget_winner = high_budget_ctx
            .winners
            .get(&(root, required.clone()))
            .expect("high-budget winner");

        assert_eq!(
            low_budget_winner.alt_kind,
            crate::sql::optimizer::derive::PropertyAlternativeKind::ShuffleJoin
        );
        assert_eq!(
            high_budget_winner.alt_kind,
            crate::sql::optimizer::derive::PropertyAlternativeKind::BroadcastJoin
        );
        assert_ne!(low_budget_winner.alt_kind, high_budget_winner.alt_kind);
    }

    #[test]
    fn search_prunes_broadcast_when_hash_table_exceeds_node_budget() {
        let (mut memo, root) = make_two_table_inner_join_memo_for_test();
        let (probe_group, build_group) = inner_join_child_groups_for_test(&memo, root);
        set_group_logical_stats_for_test(
            &mut memo,
            probe_group,
            10_000_000.0,
            8.0,
            crate::sql::optimizer::statistics::Confidence::Exact,
        );
        set_group_logical_stats_for_test(
            &mut memo,
            build_group,
            100_000.0,
            8.0,
            crate::sql::optimizer::statistics::Confidence::Exact,
        );

        let mut profile = crate::sql::optimizer::cost::ClusterResourceProfile::default();
        profile.per_node_build_memory_budget_bytes = 1.0 * 1024.0 * 1024.0;
        let mut options = CostOptions::default();
        options.apply_profile(profile);
        let mut ctx = SearchContext::new(empty_stats_input(), options);
        let required = PhysicalPropertySet::gather();

        ctx.optimize_group(&memo, root, &required).expect("search");
        let winner = ctx.winners.get(&(root, required.clone())).expect("winner");

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
        let mut ctx = SearchContext::new_for_test(legacy_stats_input(table_stats));
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
        let mut ctx = SearchContext::new_for_test(legacy_stats_input(table_stats));
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
        let mut ctx = SearchContext::new_for_test(empty_stats_input());
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
        let mut ctx = SearchContext::new_for_test(empty_stats_input());
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

    // -------------------------------------------------------------------------
    // G5 A2 precondition: deterministic equal-cost tie-break
    // -------------------------------------------------------------------------

    /// Pin the existing strict-less-than tie-break in `optimize_group`.
    ///
    /// Two structurally-identical `PhysicalScan` expressions are placed in the
    /// same Memo group at indices 0 and 1.  Because they scan the same table
    /// with the same statistics the optimizer assigns them identical costs.
    /// The loop in `SearchContext::optimize_group` uses strict `<`, so the
    /// best index is only updated when a *strictly* cheaper candidate is
    /// found; on a tie the first (lowest-index) alternative wins.
    ///
    /// This test characterises that existing deterministic behaviour so that
    /// any future regression (e.g. accidental `<=`) is caught.
    #[test]
    fn equal_cost_winner_is_lowest_index_alternative() {
        // Build a group with a single PhysicalScan (index 0).
        let (mut memo, gid) = single_scan_memo();

        // Add a second, identical PhysicalScan to the *same* group (index 1).
        // It is byte-for-byte the same operator so its computed cost will be
        // equal to the first one.
        let second_scan = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalScan(ScanOp {
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
                stats_ref: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        };
        memo.add_expr_to_group(gid, second_scan);

        // Confirm the fixture: the group must have exactly two physical exprs.
        assert_eq!(
            memo.groups[gid].physical_exprs.len(),
            2,
            "fixture should have exactly two physical exprs"
        );

        // Run the search.
        let mut ctx = SearchContext::new_for_test(legacy_stats_input(make_table_stats()));
        let required = PhysicalPropertySet::any();
        let cost = ctx
            .optimize_group(&memo, gid, &required)
            .expect("optimize_group should succeed");
        assert!(
            cost.is_finite(),
            "group with two scans must have a finite cost"
        );

        // The winner must be index 0: on equal cost the first-inserted
        // (lowest-index) physical expression wins because the comparison is
        // strict `<` (not `<=`).
        let winner = ctx
            .winners
            .get(&(gid, required))
            .expect("winner must be recorded");
        assert_eq!(
            winner.expr_index, 0,
            "on equal cost the lowest-index (first-inserted) physical expression \
             must win; got expr_index={} instead",
            winner.expr_index,
        );
    }
}

#[cfg(test)]
mod cascaded_derivation_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::MExpr;
    use crate::sql::optimizer::statistics::TableStatistics;
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use crate::sql::planner::optimizer_bridge::scalar::intern_window_exprs;
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

    fn eq_cond(memo: &mut Memo, left: TypedExpr, right: TypedExpr) -> PhysicalHashJoinEqCondition {
        PhysicalHashJoinEqCondition {
            left: intern_typed(&mut memo.scalars, &left),
            right: intern_typed(&mut memo.scalars, &right),
            null_safe: false,
        }
    }

    fn scan_op(table: &str) -> Operator {
        Operator::PhysicalScan(ScanOp {
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
            stats_ref: None,
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
        // Small enough for the default broadcast feasibility budgets.
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
            eq_conditions: vec![eq_cond(&mut memo, col(10), col(10))],
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
            eq_conditions: vec![eq_cond(&mut memo, col(10), col(10))],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
        });
        let g_bj = memo.new_group(MExpr {
            id: 4,
            op: broadcast_join,
            children: vec![g_sj, g_small],
        });

        let window_expr = crate::sql::planner::plan::WindowExpr {
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
        };
        let window = Operator::PhysicalWindow(WindowOp {
            window_exprs: intern_window_exprs(&mut memo.scalars, &[window_expr]),
            output_columns: vec![crate::sql::analysis::OutputColumn {
                column_id: ColumnId(1000),
                name: "win".into(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
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
        let mut ctx = SearchContext::new_for_test(super::tests::legacy_stats_input(
            super::tests::make_table_stats(),
        ));
        ctx.optimize_group(&memo, gid, &PhysicalPropertySet::any())
            .unwrap();
        let w = ctx.winners.get(&(gid, PhysicalPropertySet::any())).unwrap();
        assert_eq!(w.output, PhysicalPropertySet::any());
    }

    #[test]
    fn cascaded_output_through_broadcast_join_repartitions_after_join() {
        let (memo, root, g_bj) = memo_window_over_broadcast_join();
        let mut ctx = SearchContext::new_for_test(super::tests::legacy_stats_input(
            table_stats_for_cascaded(),
        ));
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
            &memo.scalars,
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
