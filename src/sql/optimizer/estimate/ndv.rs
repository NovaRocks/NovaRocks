use std::collections::HashMap;

use crate::sql::analysis::TypedExpr;
use crate::sql::optimizer::statistics::{
    ColumnStatistic, Confidence, UNKNOWN_GROUP_BY_CORRELATION,
};

use super::arith::sat_mul;
use super::selectivity::extract_column_name;

const DEFAULT_EXPR_NDV: f64 = 10.0;
const DEFAULT_JOIN_KEY_NDV: f64 = 40.0;

/// Get the NDV for an expression from column statistics.
pub(crate) fn get_expr_ndv(
    expr: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    real_expr_ndv(expr, column_stats)
        .map(|(ndv, _)| ndv)
        .unwrap_or(DEFAULT_EXPR_NDV)
}

pub(crate) fn get_join_key_ndv_with_confidence(
    expr: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> (f64, Confidence) {
    real_expr_ndv(expr, column_stats).unwrap_or((DEFAULT_JOIN_KEY_NDV, Confidence::Fallback))
}

fn real_expr_ndv(
    expr: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> Option<(f64, Confidence)> {
    // A column is only useful for cardinality if it carries a real NDV (> 1).
    // ColumnStatistic::unknown() (propagated for no-stats / managed-lake tables)
    // reports distinct_values_count = 1.0; treating that as a true NDV would make
    // join-key estimation divide left*right by ~1 and explode joins to near
    // cross-products. Mirror the `> 1.0` guard estimate_eq_selectivity uses and
    // fall back to the default NDV for unknown/degenerate columns.
    if let Some(name) = extract_column_name(expr)
        && let Some(cs) = column_stats.get(&name.to_lowercase())
        && cs.distinct_values_count > 1.0
    {
        return Some((cs.distinct_values_count, cs.confidence));
    }
    None
}

/// A column's NDV can never exceed the number of surviving rows.
///
/// Invalid row counts or NDVs collapse to the conservative minimum NDV of 1.0.
pub(crate) fn cap_ndv_at_rows(ndv: f64, rows: f64) -> f64 {
    if !rows.is_finite() || rows <= 0.0 {
        return 1.0;
    }

    let ndv = if !ndv.is_finite() || ndv < 1.0 {
        1.0
    } else {
        ndv
    };
    ndv.min(rows).max(1.0)
}

/// Estimate grouped-aggregate output rows from group-key NDVs. Uses a damped
/// product (so many keys don't explode) capped at child_rows * correlation.
pub(crate) fn agg_group_rows(group_key_ndvs: &[f64], child_rows: f64) -> f64 {
    if group_key_ndvs.is_empty() {
        return 1.0;
    }

    let combined_ndv: f64 = {
        let mut sorted: Vec<f64> = group_key_ndvs
            .iter()
            .copied()
            .map(|n| if n.is_finite() { n.max(1.0) } else { 1.0 })
            .collect();
        sorted.sort_by(|a, b| b.partial_cmp(a).unwrap());
        let mut product = 1.0;
        let mut exp = 1.0;
        for ndv in sorted {
            product = sat_mul(product, ndv.powf(exp)).0;
            exp *= 0.5;
        }
        product
    };
    let capped = if child_rows.is_finite() && child_rows > 0.0 {
        sat_mul(child_rows, UNKNOWN_GROUP_BY_CORRELATION).0
    } else {
        1.0
    };
    combined_ndv.min(capped).max(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::DataType;

    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT;
    use crate::sql::optimizer::statistics::ColumnStatistic;

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    #[test]
    fn get_expr_ndv_ignores_unknown_ndv() {
        // OQ-3 propagates ColumnStatistic::unknown() (distinct_values_count = 1.0)
        // for no-stats / managed-lake tables. get_expr_ndv must treat that as
        // "no information" and return the generic expression default.
        let mut column_stats: HashMap<String, ColumnStatistic> = HashMap::new();
        column_stats.insert("unknown_col".to_string(), ColumnStatistic::unknown());
        assert_eq!(column_stats["unknown_col"].distinct_values_count, 1.0);
        let unknown_expr = col_ref("unknown_col");
        assert_eq!(get_expr_ndv(&unknown_expr, &column_stats), DEFAULT_EXPR_NDV);

        // A degenerate ndv of exactly 1.0 (not via unknown()) is also ignored.
        column_stats.insert(
            "degenerate_col".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 1.0,
                ..Default::default()
            },
        );
        let degenerate_expr = col_ref("degenerate_col");
        assert_eq!(
            get_expr_ndv(&degenerate_expr, &column_stats),
            DEFAULT_EXPR_NDV
        );

        // A real NDV (> 1) is still used verbatim.
        column_stats.insert(
            "real_col".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 50.0,
                ..Default::default()
            },
        );
        let real_expr = col_ref("real_col");
        assert_eq!(get_expr_ndv(&real_expr, &column_stats), 50.0);

        // An unknown column reference (absent from the map) also defaults.
        let missing_expr = col_ref("missing_col");
        assert_eq!(get_expr_ndv(&missing_expr, &column_stats), DEFAULT_EXPR_NDV);
    }

    #[test]
    fn join_key_ndv_uses_wider_fallback() {
        let mut column_stats: HashMap<String, ColumnStatistic> = HashMap::new();
        column_stats.insert("unknown_col".to_string(), ColumnStatistic::unknown());

        let unknown_expr = col_ref("unknown_col");
        let (ndv, confidence) = get_join_key_ndv_with_confidence(&unknown_expr, &column_stats);
        assert_eq!(ndv, DEFAULT_JOIN_KEY_NDV);
        assert_eq!(confidence, Confidence::Fallback);

        column_stats.insert(
            "real_col".to_string(),
            ColumnStatistic {
                distinct_values_count: 50.0,
                confidence: Confidence::Exact,
                ..Default::default()
            },
        );
        let real_expr = col_ref("real_col");
        let (ndv, confidence) = get_join_key_ndv_with_confidence(&real_expr, &column_stats);
        assert_eq!(ndv, 50.0);
        assert_eq!(confidence, Confidence::Exact);
    }

    #[test]
    fn filter_ndv_capped_at_output_rows() {
        // NDV cannot exceed surviving rows.
        assert_eq!(cap_ndv_at_rows(1000.0, 50.0), 50.0);
        assert_eq!(cap_ndv_at_rows(30.0, 50.0), 30.0);
    }

    #[test]
    fn join_output_ndv_capped_at_output_rows() {
        assert_eq!(cap_ndv_at_rows(1e6, 8.0), 8.0);
    }

    #[test]
    fn filter_ndv_cap_handles_invalid_inputs_conservatively() {
        assert_eq!(cap_ndv_at_rows(1000.0, 0.0), 1.0);
        assert_eq!(cap_ndv_at_rows(1000.0, f64::NAN), 1.0);
        assert_eq!(cap_ndv_at_rows(1000.0, f64::INFINITY), 1.0);
        assert_eq!(cap_ndv_at_rows(f64::NAN, 50.0), 1.0);
        assert_eq!(cap_ndv_at_rows(f64::INFINITY, 50.0), 1.0);
        assert_eq!(cap_ndv_at_rows(0.5, 50.0), 1.0);
    }

    #[test]
    fn agg_group_rows_uses_damped_product_when_cap_does_not_bind() {
        let expected = 100.0 * 100.0_f64.sqrt() * 100.0_f64.powf(0.25);
        let rows = agg_group_rows(&[100.0, 100.0, 100.0], 1_000_000.0);
        assert!((rows - expected).abs() < 0.000_001);
        assert!(rows < 100.0 * 100.0 * 100.0);
    }

    #[test]
    fn agg_group_rows_weights_larger_ndvs_first() {
        let expected = 10_000.0 * 100.0_f64.sqrt() * 10.0_f64.powf(0.25);
        let rows = agg_group_rows(&[10.0, 10_000.0, 100.0], 1_000_000.0);
        assert!((rows - expected).abs() < 0.000_001);
    }

    #[test]
    fn agg_group_rows_damped_and_capped() {
        let rows = agg_group_rows(&[1_000_000.0, 1_000_000.0], 10_000.0);
        assert_eq!(rows, 10_000.0 * 0.75);
        assert!(rows > 1.0);
    }

    #[test]
    fn agg_group_rows_handles_invalid_inputs_conservatively() {
        assert_eq!(agg_group_rows(&[10.0, 10.0], f64::NAN), 1.0);
        assert_eq!(agg_group_rows(&[10.0, 10.0], f64::INFINITY), 1.0);
        assert_eq!(agg_group_rows(&[f64::INFINITY, 25.0], 1_000.0), 25.0);
    }

    #[test]
    fn agg_group_rows_caps_huge_inputs_at_max_row_count() {
        let rows = agg_group_rows(&[f64::MAX, f64::MAX, f64::MAX], f64::MAX);
        assert!(rows.is_finite());
        assert!(rows <= MAX_ROW_COUNT);
        assert_eq!(rows, MAX_ROW_COUNT);
    }
}
