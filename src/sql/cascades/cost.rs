//! Cost model for physical operators in the Cascades optimizer.
//!
//! Provides a single `compute_cost` function that estimates the self-cost of
//! a physical operator (not including children).  The formulas are aligned with
//! StarRocks conventions and the existing `optimizer/cost.rs` model.

use super::memo::Cost;
use super::operator::{AggMode, JoinDistribution, Operator};
use crate::sql::statistics::Statistics;

/// Network transfer multiplier applied to data that crosses node boundaries.
const NETWORK_COST: f64 = 1.5;

/// Penalty multiplier for cross joins (matches StarRocks `CROSS_JOIN_COST_PENALTY`).
const CROSS_JOIN_COST_PENALTY: f64 = 10.0;

/// Penalty multiplier for non-equi hash joins (has `other_condition`).
/// Matches StarRocks `HashJoinNode.EXECUTE_COST_PENALTY = 100`.
const NON_EQUI_JOIN_COST_PENALTY: f64 = 100.0;

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
        | Operator::LogicalWindow(_)
        | Operator::LogicalUnion(_)
        | Operator::LogicalIntersect(_)
        | Operator::LogicalExcept(_)
        | Operator::LogicalValues(_)
        | Operator::LogicalGenerateSeries(_)
        | Operator::LogicalSubqueryAlias(_)
        | Operator::LogicalRepeat(_)
        | Operator::LogicalCTEAnchor(_)
        | Operator::LogicalCTEProduce(_)
        | Operator::LogicalCTEConsume(_) => 0.0,

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
            };

            // Apply cross join penalty (StarRocks: getCrossJoinCostPenalty = 10).
            let cost_after_cross = if j.join_type == crate::sql::ir::JoinKind::Cross {
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
            }
        }

        Operator::PhysicalSort(_) => {
            let n = own_stats.output_row_count.max(1.0);
            n * n.log2()
        }

        Operator::PhysicalDistribution(_) => own_stats.compute_size() * NETWORK_COST,

        Operator::PhysicalLimit(_) => 0.01,

        Operator::PhysicalCTEAnchor(_) => 0.0,

        // Window, Repeat, Union, Intersect, Except, Values, GenerateSeries,
        // SubqueryAlias, CTE — lightweight default.
        Operator::PhysicalWindow(_)
        | Operator::PhysicalRepeat(_)
        | Operator::PhysicalUnion(_)
        | Operator::PhysicalIntersect(_)
        | Operator::PhysicalExcept(_)
        | Operator::PhysicalValues(_)
        | Operator::PhysicalGenerateSeries(_)
        | Operator::PhysicalSubqueryAlias(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalCTEConsume(_) => own_stats.output_row_count * 0.01,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::cascades::operator::*;
    use crate::sql::ir::JoinKind;
    use crate::sql::statistics::ColumnStatistic;
    use std::collections::HashMap;

    fn stats(rows: f64, avg_size: f64) -> Statistics {
        let mut col = HashMap::new();
        col.insert(
            "c".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: avg_size,
                distinct_values_count: rows,
            },
        );
        Statistics {
            output_row_count: rows,
            column_statistics: col,
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
                storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
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
    fn local_agg_cheaper_than_single() {
        let input = stats(100_000.0, 50.0);
        let own = stats(100.0, 50.0);

        let single = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
        });
        let local = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
        });

        let cs = [&input];
        assert!(compute_cost(&single, &own, &cs) > compute_cost(&local, &own, &cs));
    }

    #[test]
    fn sort_cost_nlogn() {
        let s = stats(1024.0, 10.0);
        let op = Operator::PhysicalSort(PhysicalSortOp { items: vec![] });
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
                storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
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
            spec: crate::sql::cascades::property::DistributionSpec::Any,
        });
        let cost = compute_cost(&op, &s, &[]);
        // 1000 * 100 * 1.5 = 150_000
        assert!((cost - 150_000.0).abs() < 1.0);
    }
}
