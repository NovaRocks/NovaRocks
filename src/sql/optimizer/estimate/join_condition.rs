use std::collections::HashMap;

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence, PREDICATE_UNKNOWN_FILTER};

use super::arith::damped_conjunction;
use super::ndv::get_join_key_ndv_with_confidence;
use super::selectivity::{estimate_selectivity, extract_column_name};

const UNKNOWN_JOIN_RESIDUAL_EQ_FILTER: f64 = 0.5;

#[derive(Default)]
pub(crate) struct JoinConditionEstimate {
    pub eq_key_ndvs: Vec<(f64, f64, Confidence)>,
    pub eq_key_pairs: Vec<(String, String)>,
    pub residual_selectivity: Option<(f64, Confidence)>,
}

pub(crate) fn estimate_join_condition(
    condition: Option<&TypedExpr>,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
) -> JoinConditionEstimate {
    let Some(condition) = condition else {
        return JoinConditionEstimate::default();
    };

    let mut estimate = JoinConditionEstimate::default();
    let mut residuals = Vec::new();
    collect_join_conjuncts(
        condition,
        left_stats,
        right_stats,
        &mut estimate,
        &mut residuals,
    );

    if !residuals.is_empty() {
        let combined_stats = combined_column_statistics(left_stats, right_stats);
        let selectivities: Vec<_> = residuals
            .iter()
            .map(|expr| estimate_join_residual_selectivity(expr, &combined_stats))
            .collect();
        estimate.residual_selectivity =
            Some((damped_conjunction(&selectivities), Confidence::Estimated));
    }

    estimate
}

fn collect_join_conjuncts<'a>(
    expr: &'a TypedExpr,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
    estimate: &mut JoinConditionEstimate,
    residuals: &mut Vec<&'a TypedExpr>,
) {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_join_conjuncts(left, left_stats, right_stats, estimate, residuals);
            collect_join_conjuncts(right, left_stats, right_stats, estimate, residuals);
        }
        ExprKind::Nested(inner) => {
            collect_join_conjuncts(inner, left_stats, right_stats, estimate, residuals);
        }
        _ => {
            if !try_collect_equi_key(expr, left_stats, right_stats, estimate) {
                residuals.push(expr);
            }
        }
    }
}

fn try_collect_equi_key(
    expr: &TypedExpr,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
    estimate: &mut JoinConditionEstimate,
) -> bool {
    let ExprKind::BinaryOp {
        left,
        op: BinOp::Eq | BinOp::EqForNull,
        right,
    } = &expr.kind
    else {
        return false;
    };

    let Some(left_name) = lower_column_name(left) else {
        return false;
    };
    let Some(right_name) = lower_column_name(right) else {
        return false;
    };

    let forward = left_stats.contains_key(&left_name) && right_stats.contains_key(&right_name);
    let reverse = left_stats.contains_key(&right_name) && right_stats.contains_key(&left_name);
    let (left_expr, right_expr, left_key, right_key) = match (forward, reverse) {
        (true, false) => (left.as_ref(), right.as_ref(), left_name, right_name),
        (false, true) => (right.as_ref(), left.as_ref(), right_name, left_name),
        (true, true) if left_name == right_name => {
            let (left_ndv, left_confidence) = get_join_key_ndv_with_confidence(left, left_stats);
            let (right_ndv, right_confidence) =
                get_join_key_ndv_with_confidence(right, right_stats);
            estimate.eq_key_ndvs.push((
                left_ndv,
                right_ndv,
                left_confidence.combine(right_confidence),
            ));
            return true;
        }
        _ => return false,
    };

    estimate.eq_key_pairs.push((left_key, right_key));
    let (left_ndv, left_confidence) = get_join_key_ndv_with_confidence(left_expr, left_stats);
    let (right_ndv, right_confidence) = get_join_key_ndv_with_confidence(right_expr, right_stats);
    estimate.eq_key_ndvs.push((
        left_ndv,
        right_ndv,
        left_confidence.combine(right_confidence),
    ));
    true
}

fn estimate_join_residual_selectivity(
    expr: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    let selectivity = estimate_selectivity(expr, column_stats);
    if (selectivity - PREDICATE_UNKNOWN_FILTER).abs() < f64::EPSILON
        && is_unknown_column_literal_eq(expr, column_stats)
    {
        UNKNOWN_JOIN_RESIDUAL_EQ_FILTER
    } else {
        selectivity
    }
}

fn is_unknown_column_literal_eq(
    expr: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> bool {
    let ExprKind::BinaryOp {
        left,
        op: BinOp::Eq | BinOp::EqForNull,
        right,
    } = &expr.kind
    else {
        return false;
    };

    let Some(column_name) = extract_column_name(left).or_else(|| extract_column_name(right)) else {
        return false;
    };
    if !(is_literal_like(left) || is_literal_like(right)) {
        return false;
    }
    column_stats
        .get(&column_name.to_lowercase())
        .map_or(true, |cs| cs.distinct_values_count <= 1.0)
}

fn is_literal_like(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::Literal(LiteralValue::Null)
        | ExprKind::Literal(LiteralValue::Bool(_))
        | ExprKind::Literal(LiteralValue::Int(_))
        | ExprKind::Literal(LiteralValue::LargeInt(_))
        | ExprKind::Literal(LiteralValue::Float(_))
        | ExprKind::Literal(LiteralValue::Decimal(_))
        | ExprKind::Literal(LiteralValue::String(_)) => true,
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => is_literal_like(expr),
        _ => false,
    }
}

fn lower_column_name(expr: &TypedExpr) -> Option<String> {
    extract_column_name(expr).map(|name| name.to_lowercase())
}

fn combined_column_statistics(
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
) -> HashMap<String, ColumnStatistic> {
    let mut combined = left_stats.clone();
    combined.extend(right_stats.clone());
    combined
}
