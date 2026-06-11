use std::collections::HashMap;

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence, PREDICATE_UNKNOWN_FILTER};

use super::arith::damped_conjunction;
use super::ndv::get_join_key_ndv_with_confidence;
use super::selectivity::{estimate_selectivity, extract_column_id};

const UNKNOWN_JOIN_RESIDUAL_EQ_FILTER: f64 = 0.5;

#[derive(Default)]
pub(crate) struct JoinConditionEstimate {
    pub eq_key_ndvs: Vec<(f64, f64, Confidence)>,
    pub eq_key_pairs: Vec<(ColumnId, ColumnId)>,
    pub residual_selectivity: Option<(f64, Confidence)>,
}

pub(crate) fn estimate_join_condition(
    condition: Option<&TypedExpr>,
    left_stats: &HashMap<ColumnId, ColumnStatistic>,
    right_stats: &HashMap<ColumnId, ColumnStatistic>,
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
    left_stats: &HashMap<ColumnId, ColumnStatistic>,
    right_stats: &HashMap<ColumnId, ColumnStatistic>,
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
    left_stats: &HashMap<ColumnId, ColumnStatistic>,
    right_stats: &HashMap<ColumnId, ColumnStatistic>,
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

    let Some(left_id) = extract_column_id(left) else {
        return false;
    };
    let Some(right_id) = extract_column_id(right) else {
        return false;
    };

    let forward = left_stats.contains_key(&left_id) && right_stats.contains_key(&right_id);
    let reverse = left_stats.contains_key(&right_id) && right_stats.contains_key(&left_id);
    let (left_expr, right_expr, left_key, right_key) = match (forward, reverse) {
        (true, false) => (left.as_ref(), right.as_ref(), left_id, right_id),
        (false, true) => (right.as_ref(), left.as_ref(), right_id, left_id),
        (true, true) if left_id == right_id => {
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
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
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
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> bool {
    let ExprKind::BinaryOp {
        left,
        op: BinOp::Eq | BinOp::EqForNull,
        right,
    } = &expr.kind
    else {
        return false;
    };

    let Some(column_id) = extract_column_id(left).or_else(|| extract_column_id(right)) else {
        return false;
    };
    if !(is_literal_like(left) || is_literal_like(right)) {
        return false;
    }
    column_stats
        .get(&column_id)
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
        | ExprKind::Literal(LiteralValue::String(_))
        | ExprKind::Literal(LiteralValue::Binary(_)) => true,
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => is_literal_like(expr),
        _ => false,
    }
}

fn combined_column_statistics(
    left_stats: &HashMap<ColumnId, ColumnStatistic>,
    right_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> HashMap<ColumnId, ColumnStatistic> {
    let mut combined = left_stats.clone();
    combined.extend(right_stats.clone());
    combined
}
