//! Aggregate pushdown cost gate — NDV bucketing + row-count threshold.

use std::collections::HashMap;

use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::statistics::{Confidence, TableStatistics};
use crate::sql::optimizer::stats::derive_opt_expr_statistics;

use super::context::PushPlan;

#[allow(dead_code)]
const HIGH_REDUCTION_RATIO: f64 = 100.0;
const MIN_PARTIAL_BENEFIT_RATIO: f64 = 0.5;
const UNKNOWN_NDV_ROW_THRESHOLD: f64 = 10_000.0;

/// True iff pushing the partial aggregate is expected to reduce rows.
pub(crate) fn should_push(
    plan: &PushPlan,
    arena: &ScalarArena,
    table_stats: &HashMap<String, TableStatistics>,
) -> bool {
    let stats = derive_opt_expr_statistics(&plan.target_subtree, arena, table_stats);
    let row_count = stats.output_row_count;
    if row_count <= 1.0 {
        // Trivially small subtree; partial buys nothing.
        return false;
    }

    let mut ndvs: Vec<Option<f64>> = plan
        .partial_groupby
        .iter()
        .map(|gb_id| ndv_for_group_expr(arena, *gb_id, &stats, row_count))
        .collect();
    ndvs.extend(
        plan.partial_extra_groupby
            .iter()
            .map(|gb_id| ndv_for_group_expr(arena, *gb_id, &stats, row_count)),
    );

    if ndvs.iter().any(|n| n.is_none()) {
        // Fallback: push only if the target is "big enough".
        return row_count >= UNKNOWN_NDV_ROW_THRESHOLD;
    }

    let joint_ndv: f64 = ndvs.iter().flatten().product::<f64>().min(row_count);
    joint_ndv < row_count * MIN_PARTIAL_BENEFIT_RATIO
}

fn ndv_for_group_expr(
    arena: &ScalarArena,
    expr: ScalarId,
    stats: &crate::sql::optimizer::statistics::Statistics,
    row_count: f64,
) -> Option<f64> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => stats.column_statistics.get(column_id).and_then(|cs| {
            let ndv = cs.distinct_values_count;
            if !ndv.is_finite() || ndv <= 0.0 {
                return None;
            }
            match cs.confidence {
                Confidence::Exact => Some(ndv),
                Confidence::Estimated if ndv < row_count * MIN_PARTIAL_BENEFIT_RATIO => Some(ndv),
                Confidence::Estimated | Confidence::Fallback => None,
            }
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{Operator, ScanOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::scalar::{ScalarArena, intern_typed};
    use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence};
    use arrow::datatypes::DataType;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "k" => ColumnId::new_for_test(1),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col_ref_typed(name: &str) -> crate::sql::analysis::TypedExpr {
        crate::sql::analysis::TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
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
        arena: &mut ScalarArena,
    ) -> (OptExpr, HashMap<String, TableStatistics>) {
        let scan = OptExpr::leaf(Operator::LogicalScan(ScanOp {
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
                column_id: test_col_id(col),
                name: col.into(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }));
        let _ = arena; // arena not needed for scan construction but kept for consistency
        let mut col_stats = HashMap::new();
        let confidence = if ndv.is_finite() {
            Confidence::Exact
        } else {
            Confidence::Fallback
        };
        col_stats.insert(
            col.to_string(),
            ColumnStatistic {
                min_value: f64::NEG_INFINITY,
                max_value: f64::INFINITY,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence,
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

    fn scan_without_stats_with_predicate(arena: &mut ScalarArena) -> OptExpr {
        let (scan, _) = scan_with_stats("unknown_table", 1, "k", f64::NAN, arena);
        let Operator::LogicalScan(mut scan_op) = scan.op else {
            unreachable!("scan_with_stats returns a scan");
        };
        let predicate = crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_typed("k")),
                op: BinOp::Eq,
                right: Box::new(crate::sql::analysis::TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(7)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        scan_op.predicates.push(intern_typed(arena, &predicate));
        OptExpr::leaf(Operator::LogicalScan(scan_op))
    }

    fn make_push_plan(scan: OptExpr, arena: &mut ScalarArena) -> PushPlan {
        let gb_id = intern_typed(arena, &col_ref_typed("k"));
        PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![gb_id],
            partial_extra_groupby: vec![],
            partial_aggregates: vec![],
        }
    }

    #[test]
    fn low_cardinality_pushes() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_stats("t", 10_000, "k", 10.0, &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(&plan, &arena, &stats));
    }

    #[test]
    fn high_cardinality_rejects() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_stats("t", 10_000, "k", 10_000.0, &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(!should_push(&plan, &arena, &stats));
    }

    #[test]
    fn estimated_upper_bound_ndv_falls_back_to_row_threshold() {
        let mut arena = ScalarArena::new();
        let (scan, mut stats) = scan_with_stats("t", 90_000, "k", 90_000.0, &mut arena);
        stats
            .get_mut("t")
            .unwrap()
            .column_stats
            .get_mut("k")
            .unwrap()
            .confidence = Confidence::Estimated;
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(&plan, &arena, &stats));
    }

    #[test]
    fn unknown_ndv_pushes_above_threshold() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_stats("t", 20_000, "k", f64::NAN, &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(&plan, &arena, &stats));
    }

    #[test]
    fn unknown_ndv_pushes_at_threshold() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_stats("t", 10_000, "k", f64::NAN, &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(&plan, &arena, &stats));
    }

    #[test]
    fn unknown_ndv_rejects_below_threshold() {
        let mut arena = ScalarArena::new();
        let (scan, stats) = scan_with_stats("t", 500, "k", f64::NAN, &mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(!should_push(&plan, &arena, &stats));
    }

    #[test]
    fn fallback_scan_with_predicate_uses_main_optimizer_row_estimate() {
        let mut arena = ScalarArena::new();
        let scan = scan_without_stats_with_predicate(&mut arena);
        let plan = make_push_plan(scan, &mut arena);
        assert!(should_push(&plan, &arena, &HashMap::new()));
    }
}
