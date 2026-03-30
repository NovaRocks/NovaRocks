//! Statistics types for the cost-based optimizer.

use std::collections::HashMap;

/// Per-column statistics derived from Iceberg file metadata.
#[derive(Clone, Debug)]
pub struct ColumnStatistic {
    pub min_value: f64,
    pub max_value: f64,
    pub nulls_fraction: f64,
    pub average_row_size: f64,
    pub distinct_values_count: f64,
}

impl ColumnStatistic {
    pub fn unknown() -> Self {
        Self {
            min_value: f64::NEG_INFINITY,
            max_value: f64::INFINITY,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            distinct_values_count: 1.0,
        }
    }
}

/// Operator-level statistics propagated through the plan tree.
#[derive(Clone, Debug)]
pub struct Statistics {
    pub output_row_count: f64,
    pub column_statistics: HashMap<String, ColumnStatistic>,
}

impl Statistics {
    pub fn avg_row_size(&self) -> f64 {
        if self.column_statistics.is_empty() {
            8.0
        } else {
            self.column_statistics
                .values()
                .map(|c| c.average_row_size)
                .sum()
        }
    }

    pub fn compute_size(&self) -> f64 {
        self.output_row_count * self.avg_row_size()
    }
}

/// Three-dimensional cost estimate (aligned with StarRocks CostEstimate).
#[derive(Clone, Debug, Default)]
pub struct CostEstimate {
    pub cpu_cost: f64,
    pub memory_cost: f64,
    pub network_cost: f64,
}

impl CostEstimate {
    pub fn total_cost(&self) -> f64 {
        self.cpu_cost * 0.5 + self.memory_cost * 2.0 + self.network_cost * 1.5
    }

    pub fn add(&self, other: &CostEstimate) -> CostEstimate {
        CostEstimate {
            cpu_cost: self.cpu_cost + other.cpu_cost,
            memory_cost: self.memory_cost + other.memory_cost,
            network_cost: self.network_cost + other.network_cost,
        }
    }
}

/// Table-level statistics aggregated from file metadata.
#[derive(Clone, Debug)]
pub struct TableStatistics {
    pub row_count: u64,
    pub column_stats: HashMap<String, ColumnStatistic>,
}

/// Selectivity constants aligned with StarRocks StatisticsEstimateCoefficient.
pub const PREDICATE_UNKNOWN_FILTER: f64 = 0.25;
pub const IS_NULL_FILTER: f64 = 0.1;
pub const IN_PREDICATE_DEFAULT_FILTER: f64 = 0.5;
pub const UNKNOWN_GROUP_BY_CORRELATION: f64 = 0.75;
pub const ANTI_JOIN_SELECTIVITY: f64 = 0.4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_estimate_total() {
        let cost = CostEstimate {
            cpu_cost: 100.0,
            memory_cost: 50.0,
            network_cost: 0.0,
        };
        assert!((cost.total_cost() - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_estimate_add() {
        let a = CostEstimate {
            cpu_cost: 10.0,
            memory_cost: 20.0,
            network_cost: 5.0,
        };
        let b = CostEstimate {
            cpu_cost: 30.0,
            memory_cost: 10.0,
            network_cost: 15.0,
        };
        let c = a.add(&b);
        assert!((c.cpu_cost - 40.0).abs() < f64::EPSILON);
        assert!((c.memory_cost - 30.0).abs() < f64::EPSILON);
        assert!((c.network_cost - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn statistics_compute_size() {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "a".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 4.0,
                distinct_values_count: 50.0,
            },
        );
        col_stats.insert(
            "b".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1000.0,
                nulls_fraction: 0.1,
                average_row_size: 8.0,
                distinct_values_count: 200.0,
            },
        );
        let stats = Statistics {
            output_row_count: 1000.0,
            column_statistics: col_stats,
        };
        assert!((stats.compute_size() - 12000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn statistics_empty_columns_default_size() {
        let stats = Statistics {
            output_row_count: 100.0,
            column_statistics: HashMap::new(),
        };
        assert!((stats.avg_row_size() - 8.0).abs() < f64::EPSILON);
    }

    #[test]
    fn column_statistic_unknown() {
        let cs = ColumnStatistic::unknown();
        assert!(cs.min_value.is_infinite());
        assert_eq!(cs.distinct_values_count, 1.0);
    }
}
