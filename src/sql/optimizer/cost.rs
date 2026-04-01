//! Cost model for the cost-based optimizer.
//!
//! Estimates a three-dimensional [`CostEstimate`] (cpu, memory, network) for
//! each logical operator.  The cost model is aligned with StarRocks conventions:
//! hash joins cost build + probe, sorts cost N*log(N), etc.

use crate::sql::ir::JoinKind;
use crate::sql::plan::*;
use crate::sql::statistics::*;

/// Estimate the self-cost of a single operator (not including children).
///
/// `own_stats` is the output statistics of the operator itself.
/// `child_stats` is the output statistics of each child, in order (left, right for joins).
pub(crate) fn estimate_operator_cost(
    plan: &LogicalPlan,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
) -> CostEstimate {
    match plan {
        LogicalPlan::Scan(_) => CostEstimate {
            cpu_cost: own_stats.compute_size(),
            memory_cost: 0.0,
            network_cost: 0.0,
        },
        LogicalPlan::Filter(_) | LogicalPlan::Project(_) => {
            let input_size = child_stats.first().map(|s| s.compute_size()).unwrap_or(0.0);
            CostEstimate {
                cpu_cost: input_size,
                memory_cost: 0.0,
                network_cost: 0.0,
            }
        }
        LogicalPlan::Aggregate(_) => {
            let input_size = child_stats.first().map(|s| s.compute_size()).unwrap_or(0.0);
            CostEstimate {
                cpu_cost: input_size,
                memory_cost: own_stats.compute_size(),
                network_cost: 0.0,
            }
        }
        LogicalPlan::Sort(_) => {
            let input = child_stats.first().copied().unwrap_or(own_stats);
            let rows = input.output_row_count.max(1.0);
            CostEstimate {
                cpu_cost: input.compute_size() * rows.log2().max(1.0),
                memory_cost: own_stats.compute_size(),
                network_cost: 0.0,
            }
        }
        LogicalPlan::Join(j) => {
            let left = child_stats.first().copied().unwrap_or(own_stats);
            let right = child_stats.get(1).copied().unwrap_or(own_stats);
            estimate_join_cost(j, left, right, own_stats)
        }
        LogicalPlan::Repeat(_) => CostEstimate {
            cpu_cost: own_stats.compute_size(),
            memory_cost: 0.0,
            network_cost: 0.0,
        },
        _ => CostEstimate::default(),
    }
}

/// Estimate the cost of a join operator.
fn estimate_join_cost(
    join: &JoinNode,
    left_stats: &Statistics,
    right_stats: &Statistics,
    output_stats: &Statistics,
) -> CostEstimate {
    match join.join_type {
        JoinKind::Cross => {
            // NestLoopJoin cost model.
            let left_size = left_stats.compute_size();
            let right_rows = right_stats.output_row_count;
            let right_size = right_stats.compute_size();
            CostEstimate {
                cpu_cost: left_size * right_rows * 2.0,
                memory_cost: right_size * 200.0,
                network_cost: 0.0,
            }
        }
        _ => {
            // HashJoin cost model.
            // Right side = build, left side = probe.
            let left_size = left_stats.compute_size();
            let right_size = right_stats.compute_size();
            let right_rows = right_stats.output_row_count.max(1.0);

            let build_cost = right_size;
            let probe_penalty = (right_rows / 100_000.0).ln().max(1.0).min(12.0);
            let probe_cost = left_size * probe_penalty;

            CostEstimate {
                cpu_cost: build_cost + probe_cost + output_stats.compute_size(),
                memory_cost: right_size,
                network_cost: 0.0,
            }
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ir::{JoinKind, OutputColumn};
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    fn simple_stats(rows: f64, avg_size: f64) -> Statistics {
        let mut col_stats = HashMap::new();
        col_stats.insert(
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
            column_statistics: col_stats,
        }
    }

    fn dummy_values() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }

    #[test]
    fn scan_cost_proportional_to_data_size() {
        let stats = simple_stats(1_000.0, 100.0);
        let plan = dummy_values(); // Type is checked in match, falls through to default.
        // Use Scan directly.
        let scan_plan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: crate::sql::catalog::TableDef {
                name: "t".to_string(),
                columns: vec![],
                storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/test.parquet"),
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
        });
        let cost = estimate_operator_cost(&scan_plan, &stats, &[]);
        // cpu = rows * avg_size = 1000 * 100 = 100_000
        assert!((cost.cpu_cost - 100_000.0).abs() < 1.0);
        assert!((cost.memory_cost - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn filter_cost_depends_on_input() {
        let input_stats = simple_stats(10_000.0, 50.0);
        let own_stats = simple_stats(1_000.0, 50.0);

        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(dummy_values()),
            predicate: crate::sql::ir::TypedExpr {
                kind: crate::sql::ir::ExprKind::Literal(crate::sql::ir::LiteralValue::Bool(true)),
                data_type: DataType::Boolean,
                nullable: false,
            },
        });

        let cost = estimate_operator_cost(&plan, &own_stats, &[&input_stats]);
        // cpu = input compute_size = 10000 * 50 = 500_000
        assert!((cost.cpu_cost - 500_000.0).abs() < 1.0);
    }

    #[test]
    fn aggregate_has_memory_cost() {
        let input_stats = simple_stats(100_000.0, 10.0);
        let own_stats = simple_stats(100.0, 10.0);

        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(dummy_values()),
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
        });

        let cost = estimate_operator_cost(&plan, &own_stats, &[&input_stats]);
        // memory = own output compute_size = 100 * 10 = 1000
        assert!((cost.memory_cost - 1_000.0).abs() < 1.0);
        // cpu = input compute_size = 100000 * 10 = 1_000_000
        assert!((cost.cpu_cost - 1_000_000.0).abs() < 1.0);
    }

    #[test]
    fn sort_cost_nlogn() {
        let input_stats = simple_stats(1_000.0, 10.0);
        let own_stats = simple_stats(1_000.0, 10.0);

        let plan = LogicalPlan::Sort(SortNode {
            input: Box::new(dummy_values()),
            items: vec![],
        });

        let cost = estimate_operator_cost(&plan, &own_stats, &[&input_stats]);
        // cpu = input_size * log2(rows) = 10000 * log2(1000) ~ 10000 * 9.97
        let expected_cpu = 10_000.0 * 1_000.0_f64.log2();
        assert!((cost.cpu_cost - expected_cpu).abs() < 1.0);
    }

    #[test]
    fn hash_join_cost_prefers_small_build_side() {
        let big = simple_stats(1_000_000.0, 100.0);
        let small = simple_stats(1_000.0, 100.0);
        let output = simple_stats(1_000.0, 200.0);

        let join = JoinNode {
            left: Box::new(dummy_values()),
            right: Box::new(dummy_values()),
            join_type: JoinKind::Inner,
            condition: None,
        };

        // Small on right (build side): lower cost.
        let cost_small_build = estimate_join_cost(&join, &big, &small, &output);
        // Big on right (build side): higher cost.
        let cost_big_build = estimate_join_cost(&join, &small, &big, &output);

        assert!(
            cost_small_build.total_cost() < cost_big_build.total_cost(),
            "small build ({}) should be cheaper than big build ({})",
            cost_small_build.total_cost(),
            cost_big_build.total_cost()
        );
    }

    #[test]
    fn cross_join_is_expensive() {
        let left = simple_stats(10_000.0, 100.0);
        let right = simple_stats(10_000.0, 100.0);
        let output = simple_stats(100_000_000.0, 200.0);

        let cross = JoinNode {
            left: Box::new(dummy_values()),
            right: Box::new(dummy_values()),
            join_type: JoinKind::Cross,
            condition: None,
        };

        let hash = JoinNode {
            left: Box::new(dummy_values()),
            right: Box::new(dummy_values()),
            join_type: JoinKind::Inner,
            condition: None,
        };

        let cross_cost = estimate_join_cost(&cross, &left, &right, &output);
        let hash_cost = estimate_join_cost(&hash, &left, &right, &output);

        assert!(
            cross_cost.total_cost() > hash_cost.total_cost(),
            "cross join ({}) should be more expensive than hash join ({})",
            cross_cost.total_cost(),
            hash_cost.total_cost()
        );
    }
}
