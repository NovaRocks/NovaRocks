//! Cost model for physical operators in the Cascades optimizer.
//!
//! Provides a single `compute_cost` function that estimates the self-cost of
//! a physical operator (not including children).  The formulas are aligned with
//! StarRocks conventions and the existing `optimizer/cost.rs` model.

use super::memo::Cost;
use super::operator::{AggMode, JoinDistribution, Operator};
use super::property::PhysicalPropertySet;
use crate::sql::optimizer::derive::PropertyAlternativeKind;
use crate::sql::optimizer::statistics::Statistics;

/// Network transfer multiplier applied to data that crosses node boundaries.
/// Single source of truth: `derive` imports this constant.
pub(crate) const NETWORK_COST: f64 = 1.5;
/// Fixed startup cost for distribution/exchange operators and enforcers.
/// Exchange setup and sender synchronization are visible for tiny joins,
/// especially in debug builds, so a pure byte cost makes small shuffles look
/// unrealistically cheap. Single source of truth: `derive::estimate_enforcer_cost`
/// imports this constant (and waives it for ShuffleAgg pre-aggregation shuffles).
pub(crate) const DISTRIBUTION_STARTUP_COST: f64 = 16.0 * 1024.0 * 1024.0;

/// Penalty multiplier for cross joins (matches StarRocks `CROSS_JOIN_COST_PENALTY`).
const CROSS_JOIN_COST_PENALTY: f64 = 10.0;

/// Penalty multiplier for non-equi hash joins (has `other_condition`).
/// Matches StarRocks optimizer's execute-cost penalty coefficient.
const NON_EQUI_JOIN_COST_PENALTY: f64 = 2.0;

/// Penalty multiplier for nest-loop join execution cost.
/// NLJ is O(N*M) and should be heavily penalized relative to hash join.
const NEST_LOOP_COST_PENALTY: f64 = 100.0;

/// Estimate the self-cost of a single operator.
///
/// `own_stats`   — output statistics of the operator itself.
/// `child_stats` — output statistics of each child, in order
///                  (probe/left first, build/right second for joins).
///
/// Returns `0.0` for logical operators (they should never be costed).
pub(crate) fn compute_cost(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
) -> Cost {
    match op {
        // ------------------------------------------------------------------
        // Logical operators — not costed
        // ------------------------------------------------------------------
        Operator::LogicalScan(_)
        | Operator::LogicalFilter(_)
        | Operator::LogicalProject(_)
        | Operator::LogicalAggregate(_)
        | Operator::LogicalJoin(_)
        | Operator::LogicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::LogicalTopN(_)
        | Operator::LogicalWindow(_)
        | Operator::LogicalUnion(_)
        | Operator::LogicalIntersect(_)
        | Operator::LogicalExcept(_)
        | Operator::LogicalValues(_)
        | Operator::LogicalGenerateSeries(_)
        | Operator::LogicalTableFunction(_)
        | Operator::LogicalRepeat(_)
        | Operator::LogicalCTEAnchor(_)
        | Operator::LogicalCTEProduce(_)
        | Operator::LogicalCTEConsume(_)
        | Operator::LogicalDecode(_)
        | Operator::LogicalAggregateStateMerge(_)
        | Operator::LogicalAssertOneRow(_) => 0.0,

        // ------------------------------------------------------------------
        // Physical operators
        // ------------------------------------------------------------------
        Operator::PhysicalScan(_) => own_stats.compute_size(),

        Operator::PhysicalFilter(_) => own_stats.output_row_count * own_stats.avg_row_size() * 0.01,

        Operator::PhysicalProject(_) => own_stats.output_row_count * 0.01,

        Operator::PhysicalHashJoin(j) => {
            let probe_size = child_stats.first().map(|s| s.compute_size()).unwrap_or(0.0);
            let build_size = child_stats.get(1).map(|s| s.compute_size()).unwrap_or(0.0);

            let base_cost = match j.distribution {
                JoinDistribution::Shuffle => (build_size + probe_size) * NETWORK_COST + probe_size,
                JoinDistribution::Broadcast => build_size * NETWORK_COST + probe_size,
                JoinDistribution::Colocate => probe_size,
                JoinDistribution::Unknown => {
                    panic!("unknown join distribution should be resolved before costing")
                }
            };

            // Apply cross join penalty (StarRocks: getCrossJoinCostPenalty = 10).
            let cost_after_cross = if j.join_type == crate::sql::analysis::JoinKind::Cross {
                base_cost * CROSS_JOIN_COST_PENALTY
            } else {
                base_cost
            };

            // Apply non-equi join penalty: if the join has a residual
            // other_condition, hash probing is less efficient (StarRocks:
            // EXECUTE_COST_PENALTY = 100).
            if j.other_condition.is_some() {
                cost_after_cross * NON_EQUI_JOIN_COST_PENALTY
            } else {
                cost_after_cross
            }
        }

        Operator::PhysicalNestLoopJoin(_) => {
            let left_rows = child_stats
                .first()
                .map(|s| s.output_row_count)
                .unwrap_or(0.0);
            let right_rows = child_stats
                .get(1)
                .map(|s| s.output_row_count)
                .unwrap_or(0.0);
            let avg_row_size = own_stats.avg_row_size();
            left_rows * right_rows * avg_row_size * NEST_LOOP_COST_PENALTY
        }

        Operator::PhysicalHashAggregate(a) => {
            let input_size = child_stats.first().map(|s| s.compute_size()).unwrap_or(0.0);
            match a.mode {
                AggMode::Single => input_size,
                AggMode::Local => input_size * 0.5,
                AggMode::Global => input_size * 0.3,
                // DISTINCT multi-phase agg phases use the same reduction factor
                // as Global. This is a rough approximation — DistinctGlobal
                // typically processes more rows than Global (it groups by g+x,
                // not just g), so this may underestimate its cost.
                AggMode::DistinctGlobal | AggMode::DistinctLocal => input_size * 0.3,
            }
        }

        Operator::PhysicalSort(_) => {
            let n = own_stats.output_row_count.max(1.0);
            n * n.log2()
        }

        Operator::PhysicalTopN(t) => {
            // Physical model: TopN scans all input rows (size = child's output row count)
            // and maintains a heap of size k = min(input_rows, limit + offset).
            // Total cost: input_rows * log2(k).
            let input_rows = child_stats
                .first()
                .map(|s| s.output_row_count)
                .unwrap_or(own_stats.output_row_count)
                .max(1.0);
            let k = match (t.limit, t.offset) {
                (Some(l), Some(o)) => ((l as f64) + (o as f64)).min(input_rows).max(1.0),
                (Some(l), None) => (l as f64).min(input_rows).max(1.0),
                _ => input_rows,
            };
            // Guard against log2(1)=0 when limit=1: lower-bound the per-row work at 1.0.
            input_rows * k.log2().max(1.0)
        }

        Operator::PhysicalDistribution(_) => {
            DISTRIBUTION_STARTUP_COST + own_stats.compute_size() * NETWORK_COST
        }

        Operator::PhysicalLimit(_) => 0.01,

        Operator::PhysicalAssertOneRow(_) => 0.01,

        Operator::PhysicalCTEAnchor(_) => 0.0,

        // Window, Repeat, Union, Intersect, Except, Values, GenerateSeries,
        // CTE, Decode — lightweight default.
        Operator::PhysicalWindow(_)
        | Operator::PhysicalRepeat(_)
        | Operator::PhysicalUnion(_)
        | Operator::PhysicalIntersect(_)
        | Operator::PhysicalExcept(_)
        | Operator::PhysicalValues(_)
        | Operator::PhysicalGenerateSeries(_)
        | Operator::PhysicalTableFunction(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalCTEConsume(_)
        | Operator::PhysicalDecode(_)
        | Operator::PhysicalAggregateStateMerge(_) => own_stats.output_row_count * 0.01,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CostOptions {
    pub backend_factor: f64,
    pub broadcast_row_limit: f64,
    pub broadcast_byte_limit: f64,
    pub broadcast_right_table_scale_factor: f64,
    pub fallback_broadcast_row_limit: f64,
    pub network_cost: f64,
    pub memory_cost_weight: f64,
}

impl Default for CostOptions {
    fn default() -> Self {
        Self {
            backend_factor: 3.0,
            broadcast_row_limit: 15_000_000.0,
            broadcast_byte_limit: 512.0 * 1024.0 * 1024.0,
            broadcast_right_table_scale_factor: 10.0,
            fallback_broadcast_row_limit: 500_000.0,
            network_cost: NETWORK_COST,
            memory_cost_weight: 0.25,
        }
    }
}

pub(crate) fn broadcast_gate_passes(
    probe_stats: &Statistics,
    build_stats: &Statistics,
    options: &CostOptions,
) -> bool {
    let build_rows = build_stats.output_row_count;
    let build_bytes = build_stats.compute_size();
    let probe_bytes = probe_stats.compute_size();

    if build_bytes > options.broadcast_byte_limit {
        return false;
    }

    if build_stats.row_count_confidence != crate::sql::optimizer::statistics::Confidence::Exact
        && build_rows > options.fallback_broadcast_row_limit
    {
        return false;
    }

    let build_is_obviously_tiny = probe_bytes
        >= build_bytes * options.backend_factor * options.broadcast_right_table_scale_factor;
    if build_rows > options.broadcast_row_limit && !build_is_obviously_tiny {
        return false;
    }

    true
}

pub(crate) fn compute_cost_with_properties(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
    _child_outputs: &[&PhysicalPropertySet],
    alt_kind: &PropertyAlternativeKind,
    options: &CostOptions,
) -> Cost {
    match op {
        Operator::PhysicalHashJoin(j) => {
            let probe_stats = child_stats.first().copied();
            let build_stats = child_stats.get(1).copied();
            let probe_size = probe_stats.map(|s| s.compute_size()).unwrap_or(0.0);
            let build_size = build_stats.map(|s| s.compute_size()).unwrap_or(0.0);

            let base_cost = match alt_kind {
                PropertyAlternativeKind::BroadcastJoin => {
                    // The distribution enforcer cost models making the build
                    // child available to the join. The join self-cost still
                    // charges backend fanout and memory pressure during hash
                    // table materialization/probing.
                    probe_size
                        + build_size * options.network_cost * options.backend_factor
                        + build_size * options.memory_cost_weight * options.backend_factor
                }
                PropertyAlternativeKind::ShuffleJoin => {
                    probe_size + build_size / options.backend_factor.max(1.0)
                }
                PropertyAlternativeKind::Default => compute_cost(op, own_stats, child_stats),
            };

            let cost_after_cross = if j.join_type == crate::sql::analysis::JoinKind::Cross {
                base_cost * CROSS_JOIN_COST_PENALTY
            } else {
                base_cost
            };
            if j.other_condition.is_some() {
                cost_after_cross * NON_EQUI_JOIN_COST_PENALTY
            } else {
                cost_after_cross
            }
        }
        _ => compute_cost(op, own_stats, child_stats),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::JoinKind;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::*;
    use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec};
    use crate::sql::optimizer::statistics::ColumnStatistic;
    use std::collections::HashMap;

    fn stats(rows: f64, avg_size: f64) -> Statistics {
        let mut col = HashMap::new();
        col.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: avg_size,
                distinct_values_count: rows,
                ..Default::default()
            },
        );
        Statistics {
            output_row_count: rows,
            column_statistics: col,
            ..Default::default()
        }
    }

    #[test]
    fn scan_cost_equals_data_size() {
        let s = stats(1000.0, 100.0);
        let op = Operator::PhysicalScan(PhysicalScanOp {
            database: String::new(),
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
        let cost = compute_cost(&op, &s, &[]);
        assert!((cost - 100_000.0).abs() < 1.0);
    }

    #[test]
    fn shuffle_join_more_expensive_than_colocate() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);

        let shuffle = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
        });
        let colocate = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Colocate,
        });
        let cs = [&probe, &build];
        let c_shuffle = compute_cost(&shuffle, &own, &cs);
        let c_colocate = compute_cost(&colocate, &own, &cs);
        assert!(c_shuffle > c_colocate);
    }

    #[test]
    fn child_output_aware_shuffle_join_does_not_charge_network_exchange_twice() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        });
        let child_stats = [&probe, &build];
        let left_output = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(1)]),
            ordering: OrderingSpec::Any,
        };
        let right_output = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(2)]),
            ordering: OrderingSpec::Any,
        };
        let child_outputs = [&left_output, &right_output];

        let cost = compute_cost_with_properties(
            &op,
            &own,
            &child_stats,
            &child_outputs,
            &PropertyAlternativeKind::ShuffleJoin,
            &CostOptions::default(),
        );

        let probe_size = probe.compute_size();
        let build_size = build.compute_size();
        assert!(cost < (probe_size + build_size) * NETWORK_COST + probe_size);
        assert!(cost >= probe_size);
    }

    #[test]
    fn broadcast_gate_rejects_fallback_build_above_fallback_limit() {
        let mut build = stats(600_000.0, 100.0);
        build.row_count_confidence = crate::sql::optimizer::statistics::Confidence::Fallback;
        let probe = stats(700_000.0, 100.0);
        let options = CostOptions::default();

        assert!(!broadcast_gate_passes(&probe, &build, &options));
    }

    #[test]
    fn broadcast_gate_rejects_estimated_build_above_fallback_limit() {
        let mut build = stats(648_000.0, 100.0);
        build.row_count_confidence = crate::sql::optimizer::statistics::Confidence::Estimated;
        let probe = stats(3_543_657.0, 100.0);
        let options = CostOptions::default();

        assert!(!broadcast_gate_passes(&probe, &build, &options));
    }

    #[test]
    fn broadcast_join_alternative_charges_fanout_and_memory_pressure() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        });
        let child_stats = [&probe, &build];
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let child_output_refs = [&child_outputs[0], &child_outputs[1]];
        let options = CostOptions::default();

        let cost = compute_cost_with_properties(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &PropertyAlternativeKind::BroadcastJoin,
            &options,
        );

        let expected = probe.compute_size()
            + build.compute_size() * options.network_cost * options.backend_factor
            + build.compute_size() * options.memory_cost_weight * options.backend_factor;
        assert!((cost - expected).abs() < f64::EPSILON);
    }

    #[test]
    fn non_equi_hash_join_uses_optimizer_execute_cost_penalty() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: Some(crate::sql::analysis::TypedExpr {
                kind: crate::sql::analysis::ExprKind::Literal(
                    crate::sql::analysis::LiteralValue::Bool(true),
                ),
                data_type: arrow::datatypes::DataType::Boolean,
                nullable: false,
            }),
            distribution: JoinDistribution::Unknown,
        });
        let child_stats = [&probe, &build];
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let child_output_refs = [&child_outputs[0], &child_outputs[1]];
        let options = CostOptions::default();

        let cost = compute_cost_with_properties(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &PropertyAlternativeKind::BroadcastJoin,
            &options,
        );

        let base = probe.compute_size()
            + build.compute_size() * options.network_cost * options.backend_factor
            + build.compute_size() * options.memory_cost_weight * options.backend_factor;
        assert!((cost - base * 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn local_agg_cheaper_than_single() {
        let input = stats(100_000.0, 50.0);
        let own = stats(100.0, 50.0);

        let single = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let local = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });

        let cs = [&input];
        assert!(compute_cost(&single, &own, &cs) > compute_cost(&local, &own, &cs));
    }

    #[test]
    fn split_agg_total_cost_can_win_or_lose_after_exchange_cost() {
        use crate::sql::optimizer::property::DistributionSpec;

        let single = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let local = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let global = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let gather = Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::Gather,
        });

        let large_input = stats(1_000_000.0, 100.0);
        let reduced_rows = stats(100.0, 16.0);
        let final_rows = stats(100.0, 16.0);
        let single_large_cost = compute_cost(&single, &final_rows, &[&large_input]);
        let split_large_cost = compute_cost(&local, &reduced_rows, &[&large_input])
            + compute_cost(&gather, &reduced_rows, &[])
            + compute_cost(&global, &final_rows, &[&reduced_rows]);
        assert!(split_large_cost < single_large_cost);

        let small_input = stats(10.0, 8.0);
        let unreduced_rows = stats(10.0, 8.0);
        let single_small_cost = compute_cost(&single, &unreduced_rows, &[&small_input]);
        let split_small_cost = compute_cost(&local, &unreduced_rows, &[&small_input])
            + compute_cost(&gather, &unreduced_rows, &[])
            + compute_cost(&global, &unreduced_rows, &[&unreduced_rows]);
        assert!(single_small_cost < split_small_cost);
    }

    #[test]
    fn sort_cost_nlogn() {
        let s = stats(1024.0, 10.0);
        let op = Operator::PhysicalSort(PhysicalSortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
        });
        let cost = compute_cost(&op, &s, &[]);
        // 1024 * log2(1024) = 1024 * 10 = 10240
        assert!((cost - 10_240.0).abs() < 1.0);
    }

    #[test]
    fn logical_ops_have_zero_cost() {
        let s = stats(1000.0, 100.0);
        let op = Operator::LogicalScan(LogicalScanOp {
            database: String::new(),
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
        assert!((compute_cost(&op, &s, &[]) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn limit_is_nearly_free() {
        let s = stats(1_000_000.0, 100.0);
        let op = Operator::PhysicalLimit(PhysicalLimitOp {
            limit: Some(10),
            offset: None,
        });
        assert!(compute_cost(&op, &s, &[]) < 1.0);
    }

    #[test]
    fn distribution_has_network_multiplier() {
        let s = stats(1000.0, 100.0);
        let op = Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: crate::sql::optimizer::property::DistributionSpec::Any,
        });
        let cost = compute_cost(&op, &s, &[]);
        // 16 MiB + 1000 * 100 * 1.5 = 16_927_216
        let expected = DISTRIBUTION_STARTUP_COST + 150_000.0;
        assert!((cost - expected).abs() < 1.0);
    }

    #[test]
    fn top_n_cheaper_than_sort_for_small_limit() {
        // Input of 10M rows; TopN's own_stats is the limited output (k=100 rows),
        // while its child's output (the scan) has 10M rows.
        let input = stats(10_000_000.0, 50.0);
        let own = stats(100.0, 50.0);
        let sort = Operator::PhysicalSort(PhysicalSortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
        });
        let top_n = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        });
        let cost_sort = compute_cost(&sort, &input, &[]);
        let cost_top_n = compute_cost(&top_n, &own, &[&input]);
        // Expected ratio ~ log2(100)/log2(10M) ≈ 0.286.
        assert!(
            cost_top_n < cost_sort * 0.5,
            "expected TOP-N strictly cheaper than Sort; got top_n={} sort={}",
            cost_top_n,
            cost_sort
        );
    }

    #[test]
    fn top_n_falls_back_to_sort_cost_when_limit_exceeds_rows() {
        // When limit >> input rows, TopN's k clamps to input rows, and cost
        // equals Sort's cost (both are n * log2(n)).
        let input = stats(100.0, 10.0);
        let own = stats(100.0, 10.0); // unlimited output (limit exceeds input)
        let sort = Operator::PhysicalSort(PhysicalSortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
        });
        let top_n = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(10_000),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        });
        let cost_sort = compute_cost(&sort, &input, &[]);
        let cost_top_n = compute_cost(&top_n, &own, &[&input]);
        assert!((cost_top_n - cost_sort).abs() < 1.0);
    }

    #[test]
    fn top_n_with_offset_and_limit_sums_both() {
        // limit=50 + offset=50 => k=100. Same cost as limit=100, offset=None.
        let input = stats(10_000.0, 10.0);
        let own = stats(100.0, 10.0);
        let top_n = Operator::PhysicalTopN(PhysicalTopNOp {
            items: vec![],
            limit: Some(50),
            offset: Some(50),
            phase: TopNPhase::Final,
            is_split: false,
        });
        let cost = compute_cost(&top_n, &own, &[&input]);
        // input_rows=10_000, k=100, cost = 10_000 * log2(100) ≈ 66_438.56
        let expected = 10_000.0 * (100f64).log2();
        assert!(
            (cost - expected).abs() < 1.0,
            "got {}, expected {}",
            cost,
            expected
        );
    }
}
