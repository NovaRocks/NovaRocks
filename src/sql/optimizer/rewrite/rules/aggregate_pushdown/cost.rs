//! Aggregate pushdown cost gate — NDV bucketing + row-count threshold.

use std::collections::HashMap;

use crate::sql::analysis::ExprKind;
use crate::sql::optimizer::rewrite::rules::join_reorder::cardinality::estimate_statistics;
use crate::sql::optimizer::statistics::TableStatistics;

#[cfg(test)]
use crate::sql::analysis::TypedExpr;
#[cfg(test)]
use crate::sql::planner::plan::LogicalPlan;

use super::context::PushPlan;

#[allow(dead_code)]
const HIGH_REDUCTION_RATIO: f64 = 100.0;
const MIN_PARTIAL_BENEFIT_RATIO: f64 = 0.5;
const UNKNOWN_NDV_ROW_THRESHOLD: f64 = 10_000.0;

/// True iff pushing the partial aggregate is expected to reduce rows.
pub(crate) fn should_push(plan: &PushPlan, table_stats: &HashMap<String, TableStatistics>) -> bool {
    let stats = estimate_statistics(&plan.target_subtree, table_stats);
    let row_count = stats.output_row_count;
    if row_count <= 1.0 {
        // Trivially small subtree; partial buys nothing.
        return false;
    }

    let ndvs: Vec<Option<f64>> = plan
        .partial_groupby
        .iter()
        .map(|gb| match &gb.kind {
            ExprKind::ColumnRef { column, .. } => stats
                .column_statistics
                .get(column)
                .map(|cs| cs.distinct_values_count)
                .filter(|n| n.is_finite() && *n > 0.0),
            _ => None,
        })
        .collect();

    if ndvs.iter().any(|n| n.is_none()) {
        // Fallback: push only if the target is "big enough".
        return row_count > UNKNOWN_NDV_ROW_THRESHOLD;
    }

    let joint_ndv: f64 = ndvs.iter().flatten().product::<f64>().min(row_count);
    joint_ndv < row_count * MIN_PARTIAL_BENEFIT_RATIO
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::statistics::ColumnStatistic;
    use crate::sql::planner::plan::ScanNode;
    use arrow::datatypes::DataType;

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn scan_with_stats(
        table: &str,
        row_count: u64,
        col: &str,
        ndv: f64,
    ) -> (LogicalPlan, HashMap<String, TableStatistics>) {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: table.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: col.into(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        });
        let mut col_stats = HashMap::new();
        col_stats.insert(
            col.to_string(),
            ColumnStatistic {
                min_value: f64::NEG_INFINITY,
                max_value: f64::INFINITY,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
            },
        );
        let mut table_stats = HashMap::new();
        // estimate_scan keys by alias.unwrap_or(table.name).to_lowercase()
        table_stats.insert(
            table.to_lowercase(),
            TableStatistics {
                row_count,
                column_stats: col_stats,
            },
        );
        (scan, table_stats)
    }

    #[test]
    fn low_cardinality_pushes() {
        let (scan, stats) = scan_with_stats("t", 10_000, "k", 10.0);
        let plan = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![col_ref("k")],
            partial_aggregates: vec![],
        };
        assert!(should_push(&plan, &stats));
    }

    #[test]
    fn high_cardinality_rejects() {
        let (scan, stats) = scan_with_stats("t", 10_000, "k", 10_000.0);
        let plan = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![col_ref("k")],
            partial_aggregates: vec![],
        };
        assert!(!should_push(&plan, &stats));
    }

    #[test]
    fn unknown_ndv_pushes_above_threshold() {
        let (scan, stats) = scan_with_stats("t", 20_000, "k", f64::NAN);
        let plan = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![col_ref("k")],
            partial_aggregates: vec![],
        };
        assert!(should_push(&plan, &stats));
    }

    #[test]
    fn unknown_ndv_rejects_below_threshold() {
        let (scan, stats) = scan_with_stats("t", 500, "k", f64::NAN);
        let plan = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![col_ref("k")],
            partial_aggregates: vec![],
        };
        assert!(!should_push(&plan, &stats));
    }
}
