use std::collections::HashMap;

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence};

use super::arith::sat_mul;
use super::selectivity::extract_column_id;

const DEFAULT_EXPR_NDV: f64 = 10.0;
const DEFAULT_JOIN_KEY_NDV: f64 = 40.0;

/// Get the NDV for an expression from column statistics.
pub(crate) fn get_expr_ndv(
    expr: &TypedExpr,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> f64 {
    real_expr_ndv(expr, column_stats)
        .map(|(ndv, _)| ndv)
        .unwrap_or(DEFAULT_EXPR_NDV)
}

pub(crate) fn get_join_key_ndv_with_confidence(
    expr: &TypedExpr,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> (f64, Confidence) {
    real_expr_ndv(expr, column_stats).unwrap_or((DEFAULT_JOIN_KEY_NDV, Confidence::Fallback))
}

fn real_expr_ndv(
    expr: &TypedExpr,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> Option<(f64, Confidence)> {
    // A column is only useful for cardinality if it carries a real NDV (> 1).
    // ColumnStatistic::unknown() (propagated for no-stats / managed-lake tables)
    // reports distinct_values_count = 1.0; treating that as a true NDV would make
    // join-key estimation divide left*right by ~1 and explode joins to near
    // cross-products. Mirror the `> 1.0` guard estimate_eq_selectivity uses and
    // fall back to the default NDV for unknown/degenerate columns.
    if let Some(column_id) = extract_column_id(expr)
        && let Some(cs) = column_stats.get(&column_id)
        && cs.distinct_values_count > 1.0
    {
        let confidence = if cs.confidence == Confidence::Fallback {
            // Fallback column NDV here still came from table metadata
            // (currently sqrt(non_null) * 10), which is materially different
            // from having no key statistics and using DEFAULT_JOIN_KEY_NDV.
            // Join cardinality treats true defaulted NDVs conservatively; keep
            // heuristic column NDVs usable as estimated inputs.
            Confidence::Estimated
        } else {
            cs.confidence
        };
        return Some((cs.distinct_values_count, confidence));
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
/// product (so many keys don't explode) capped at child_rows (one row per group).
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
    // The output has one row per distinct group, so the hard upper bound is the
    // input row count, NOT input * correlation. Capping at child_rows * 0.75 would
    // double-discount a two-phase aggregate's GLOBAL phase (whose input is the
    // already-reduced LOCAL output), making the estimate stage-DEPENDENT. child_rows
    // is the stage-idempotent cap. The 0.75 UNKNOWN_GROUP_BY_CORRELATION belongs to
    // multi-column NDV combination / unknown-NDV fallback, not to this row-count cap.
    let capped = if child_rows.is_finite() && child_rows > 0.0 {
        child_rows
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

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "unknown_col" => ColumnId::new_for_test(1),
            "degenerate_col" => ColumnId::new_for_test(2),
            "real_col" => ColumnId::new_for_test(3),
            "missing_col" => ColumnId::new_for_test(4),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
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
        let mut column_stats: HashMap<ColumnId, ColumnStatistic> = HashMap::new();
        column_stats.insert(test_col_id("unknown_col"), ColumnStatistic::unknown());
        assert_eq!(
            column_stats[&test_col_id("unknown_col")].distinct_values_count,
            1.0
        );
        let unknown_expr = col_ref("unknown_col");
        assert_eq!(get_expr_ndv(&unknown_expr, &column_stats), DEFAULT_EXPR_NDV);

        // A degenerate ndv of exactly 1.0 (not via unknown()) is also ignored.
        column_stats.insert(
            test_col_id("degenerate_col"),
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
            test_col_id("real_col"),
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
        let mut column_stats: HashMap<ColumnId, ColumnStatistic> = HashMap::new();
        column_stats.insert(test_col_id("unknown_col"), ColumnStatistic::unknown());

        let unknown_expr = col_ref("unknown_col");
        let (ndv, confidence) = get_join_key_ndv_with_confidence(&unknown_expr, &column_stats);
        assert_eq!(ndv, DEFAULT_JOIN_KEY_NDV);
        assert_eq!(confidence, Confidence::Fallback);

        column_stats.insert(
            test_col_id("real_col"),
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
    fn join_key_ndv_treats_heuristic_column_stats_as_estimated() {
        let mut column_stats: HashMap<ColumnId, ColumnStatistic> = HashMap::new();
        column_stats.insert(
            test_col_id("real_col"),
            ColumnStatistic {
                min_value: 1.0,
                max_value: 10_000.0,
                distinct_values_count: 1_000.0,
                confidence: Confidence::Fallback,
                ..Default::default()
            },
        );

        let expr = col_ref("real_col");
        let (ndv, confidence) = get_join_key_ndv_with_confidence(&expr, &column_stats);

        assert_eq!(ndv, 1_000.0);
        assert_eq!(confidence, Confidence::Estimated);
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
        assert_eq!(rows, 10_000.0);
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

    #[test]
    fn agg_group_rows_is_stage_idempotent_when_cap_binds() {
        // High NDV (1000) over few rows (200): combined_ndv (1000) > input_rows (200),
        // so the row-count cap BINDS (the case the old child_rows*0.75 cap got wrong).
        // A two-phase aggregate must equal a single-phase one: re-aggregating an
        // already-grouped input must NOT shrink it further (stage-idempotent).
        let ndvs = [1000.0];
        let input = 200.0;

        let single = agg_group_rows(&ndvs, input);
        let local = agg_group_rows(&ndvs, input);
        let global = agg_group_rows(&ndvs, local); // GLOBAL agg runs over LOCAL's output

        assert_eq!(
            single, global,
            "two-phase agg (global over local) must equal single-phase: stage-idempotent"
        );
        assert_eq!(
            single, input,
            "row-count cap must bind at input_rows, not input*0.75"
        );
    }
}
