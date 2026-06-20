//! Predicate selectivity estimation kernel.

use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::common::{BinOp, LiteralValue, UnOp};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::statistics::{
    ColumnStatistic, Confidence, IN_PREDICATE_DEFAULT_FILTER, IS_NULL_FILTER,
    PREDICATE_UNKNOWN_FILTER,
};

use super::arith::{damped_conjunction, sat_mul};

/// Estimate selectivity of a predicate expression (0.0..1.0).
pub(crate) fn estimate_selectivity(
    arena: &ScalarArena,
    expr: ScalarId,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> f64 {
    match arena.node(expr) {
        ScalarNode::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let mut conjuncts = Vec::new();
                flatten_and(arena, expr, &mut conjuncts);
                let sels: Vec<f64> = conjuncts
                    .iter()
                    .map(|c| estimate_selectivity(arena, *c, column_stats))
                    .collect();
                damped_conjunction(&sels)
            }
            BinOp::Or => {
                let l = estimate_selectivity(arena, *left, column_stats);
                let r = estimate_selectivity(arena, *right, column_stats);
                l + r - l * r
            }
            BinOp::Eq | BinOp::EqForNull => {
                estimate_eq_selectivity(arena, *left, *right, column_stats)
            }
            BinOp::Ne => 1.0 - estimate_eq_selectivity(arena, *left, *right, column_stats),
            BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                estimate_range_selectivity(arena, *left, *right, *op, column_stats)
            }
            _ => PREDICATE_UNKNOWN_FILTER,
        },
        ScalarNode::IsNull { negated, child } => {
            let col_id = extract_column_id(arena, *child);
            let null_frac = col_id
                .and_then(|column_id| column_stats.get(&column_id))
                .map(|cs| {
                    if cs.nulls_fraction > 0.0 {
                        cs.nulls_fraction
                    } else {
                        IS_NULL_FILTER
                    }
                })
                .unwrap_or(IS_NULL_FILTER);
            if *negated { 1.0 - null_frac } else { null_frac }
        }
        ScalarNode::InList {
            child,
            list,
            negated,
        } => {
            let col_id = extract_column_id(arena, *child);
            let ndv = col_id
                .and_then(|column_id| column_stats.get(&column_id))
                .and_then(trusted_distinct_values_count);

            let sel = if let Some(ndv) = ndv {
                (list.len() as f64 / ndv).min(1.0)
            } else {
                IN_PREDICATE_DEFAULT_FILTER
            };
            if *negated { 1.0 - sel } else { sel }
        }
        ScalarNode::Between {
            negated,
            child,
            low,
            high,
        } => {
            // a BETWEEN low AND high  ==  a >= low AND a <= high
            // Keep BETWEEN as a direct range-bound product; generic AND
            // conjunctions use damped_conjunction to avoid collapse.
            let ge = estimate_range_selectivity(arena, *child, *low, BinOp::Ge, column_stats);
            let le = estimate_range_selectivity(arena, *child, *high, BinOp::Le, column_stats);
            let sel = ge * le;
            if *negated { 1.0 - sel } else { sel }
        }
        ScalarNode::Like { negated, .. } => {
            let sel = PREDICATE_UNKNOWN_FILTER;
            if *negated { 1.0 - sel } else { sel }
        }
        ScalarNode::UnaryOp {
            op: UnOp::Not,
            child,
        } => 1.0 - estimate_selectivity(arena, *child, column_stats),
        ScalarNode::IsTruthValue { negated, .. } => {
            // IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
            let base = 0.5;
            if *negated { 1.0 - base } else { base }
        }
        ScalarNode::Nested(inner) => estimate_selectivity(arena, *inner, column_stats),
        _ => PREDICATE_UNKNOWN_FILTER,
    }
}

/// Apply a filter selectivity to a child row count. Valid non-empty inputs
/// floor at 1.0 when the selectivity would collapse the row count below one.
/// Invalid inputs return bounded fallback rows so bad stats stay observable.
pub(crate) fn apply_filter(
    child_rows: f64,
    child_conf: Confidence,
    selectivity: f64,
) -> (f64, Confidence) {
    let valid_child_rows = child_rows.is_finite() && child_rows >= 0.0;
    let valid_selectivity = selectivity.is_finite() && (0.0..=1.0).contains(&selectivity);
    let bounded_selectivity = if selectivity.is_nan() {
        0.0
    } else {
        selectivity.clamp(0.0, 1.0)
    };
    let (raw, saturated) = sat_mul(child_rows, bounded_selectivity);
    if !valid_child_rows || !valid_selectivity || saturated {
        return (raw, Confidence::Fallback);
    }

    if raw < 1.0 && child_rows >= 1.0 {
        (1.0, Confidence::Fallback)
    } else {
        (raw.max(0.0), Confidence::derive(&[child_conf], false))
    }
}

/// Flatten a left/right-nested AND tree into its leaf conjuncts.
fn flatten_and(arena: &ScalarArena, expr: ScalarId, out: &mut Vec<ScalarId>) {
    if let ScalarNode::BinaryOp {
        op: BinOp::And,
        left,
        right,
    } = arena.node(expr)
    {
        flatten_and(arena, *left, out);
        flatten_and(arena, *right, out);
    } else if let ScalarNode::Nested(inner) = arena.node(expr) {
        flatten_and(arena, *inner, out);
    } else {
        out.push(expr);
    }
}

fn estimate_eq_selectivity(
    arena: &ScalarArena,
    left: ScalarId,
    right: ScalarId,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> f64 {
    // col = literal: prefer trusted NDV, then finite min/max for discrete
    // numeric domains when NDV is only a fallback heuristic.
    if let Some((column_id, column_expr, literal_expr)) =
        extract_column_literal_pair(arena, left, right)
        && let Some(cs) = column_stats.get(&column_id)
    {
        if let Some(ndv) = trusted_distinct_values_count(cs) {
            return 1.0 / ndv;
        }
        if let Some(selectivity) =
            discrete_domain_equality_selectivity(arena, column_expr, literal_expr, cs)
        {
            return selectivity;
        }
    }
    PREDICATE_UNKNOWN_FILTER
}

fn extract_column_literal_pair(
    arena: &ScalarArena,
    left: ScalarId,
    right: ScalarId,
) -> Option<(ColumnId, ScalarId, ScalarId)> {
    if let Some(column_id) = extract_column_id(arena, left)
        && extract_literal_f64(arena, right).is_some()
    {
        return Some((column_id, left, right));
    }
    if let Some(column_id) = extract_column_id(arena, right)
        && extract_literal_f64(arena, left).is_some()
    {
        return Some((column_id, right, left));
    }
    None
}

fn trusted_distinct_values_count(stat: &ColumnStatistic) -> Option<f64> {
    if stat.confidence > Confidence::Fallback
        && stat.distinct_values_count.is_finite()
        && stat.distinct_values_count > 1.0
    {
        Some(stat.distinct_values_count)
    } else {
        None
    }
}

fn discrete_domain_equality_selectivity(
    arena: &ScalarArena,
    column_expr: ScalarId,
    literal_expr: ScalarId,
    stat: &ColumnStatistic,
) -> Option<f64> {
    if !is_discrete_numeric_domain(arena.data_type(column_expr)) {
        return None;
    }
    let min = stat.min_value;
    let max = stat.max_value;
    if !min.is_finite() || !max.is_finite() || max < min {
        return None;
    }
    let value = extract_literal_f64(arena, literal_expr)?;
    if value < min || value > max {
        return Some(0.0);
    }
    let domain_width = (max.floor() - min.ceil() + 1.0).max(1.0);
    Some((1.0 / domain_width).clamp(0.0, 1.0))
}

fn is_discrete_numeric_domain(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
    )
}

fn estimate_range_selectivity(
    arena: &ScalarArena,
    left: ScalarId,
    right: ScalarId,
    op: BinOp,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> f64 {
    // Try to use min/max range if available.
    let col_id = extract_column_id(arena, left);
    let literal_val = extract_literal_f64(arena, right);

    if let (Some(column_id), Some(val)) = (col_id, literal_val)
        && let Some(cs) = column_stats.get(&column_id)
    {
        let min = cs.min_value;
        let max = cs.max_value;
        if min.is_finite() && max.is_finite() && max > min {
            let range = max - min;
            return match op {
                BinOp::Lt => ((val - min) / range).clamp(0.01, 0.99),
                BinOp::Le => ((val - min + 1.0) / range).clamp(0.01, 0.99),
                BinOp::Gt => ((max - val) / range).clamp(0.01, 0.99),
                BinOp::Ge => ((max - val + 1.0) / range).clamp(0.01, 0.99),
                _ => 0.5,
            };
        }
    }
    0.5 // default for range predicates
}

fn extract_literal_f64(arena: &ScalarArena, expr: ScalarId) -> Option<f64> {
    match arena.node(expr) {
        ScalarNode::Literal(lit) => match &lit.0 {
            LiteralValue::Int(v) => Some(*v as f64),
            LiteralValue::LargeInt(v) => Some(*v as f64),
            LiteralValue::Float(v) => Some(*v),
            LiteralValue::Decimal(s) => s.parse::<f64>().ok(),
            _ => None,
        },
        ScalarNode::Cast { child, .. } => extract_literal_f64(arena, *child),
        ScalarNode::Nested(inner) => extract_literal_f64(arena, *inner),
        _ => None,
    }
}

/// Extract ColumnId from a simple column reference expression.
pub(crate) fn extract_column_id(arena: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
        ScalarNode::Cast { child, .. } => extract_column_id(arena, *child),
        ScalarNode::Nested(inner) => extract_column_id(arena, *inner),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::{ScalarArena, intern_typed};
    use crate::sql::optimizer::statistics::Confidence;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    fn col(name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn and(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn nested(expr: TypedExpr) -> TypedExpr {
        let data_type = expr.data_type.clone();
        let nullable = expr.nullable;
        TypedExpr {
            kind: ExprKind::Nested(Box::new(expr)),
            data_type,
            nullable,
        }
    }

    fn unknown_eq(index: usize) -> TypedExpr {
        let name = format!("c{index}");
        eq(col(&name, index as u32 + 1), int_lit(index as i64))
    }

    fn and_of_unknown_eq(n: usize) -> TypedExpr {
        let mut predicates = (0..n).map(unknown_eq);
        let first = predicates
            .next()
            .expect("and_of_unknown_eq requires at least one predicate");
        predicates.fold(first, and)
    }

    fn assert_finite_non_negative(rows: f64) {
        assert!(rows.is_finite(), "row count must be finite: {rows}");
        assert!(rows >= 0.0, "row count must be non-negative: {rows}");
    }

    fn estimate_typed(expr: &TypedExpr, column_stats: &HashMap<ColumnId, ColumnStatistic>) -> f64 {
        let mut arena = ScalarArena::new();
        let id = intern_typed(&mut arena, expr);
        estimate_selectivity(&arena, id, column_stats)
    }

    #[test]
    fn tiny_selectivity_floors_and_downgrades() {
        let (rows, conf) = apply_filter(1000.0, Confidence::Exact, 1e-6);
        assert_eq!(rows, 1.0);
        assert_eq!(conf, Confidence::Fallback);
    }

    #[test]
    fn valid_non_floor_filters_derive_confidence() {
        let (rows, conf) = apply_filter(1000.0, Confidence::Exact, 0.5);
        assert_eq!(rows, 500.0);
        assert_eq!(conf, Confidence::Estimated);

        let (fallback_rows, fallback_conf) = apply_filter(1000.0, Confidence::Fallback, 0.5);
        assert_eq!(fallback_rows, 500.0);
        assert_eq!(fallback_conf, Confidence::Fallback);
    }

    #[test]
    fn invalid_filter_inputs_return_finite_fallback_rows() {
        let cases = [
            (1000.0, f64::NAN, 0.0),
            (1000.0, f64::INFINITY, 1000.0),
            (f64::NAN, 0.5, 0.0),
            (-10.0, 0.5, 0.0),
            (1000.0, -0.5, 0.0),
        ];

        for (child_rows, selectivity, expected_rows) in cases {
            let (rows, conf) = apply_filter(child_rows, Confidence::Exact, selectivity);
            assert_finite_non_negative(rows);
            assert_eq!(
                rows, expected_rows,
                "child_rows={child_rows}, selectivity={selectivity}"
            );
            assert_eq!(
                conf,
                Confidence::Fallback,
                "child_rows={child_rows}, selectivity={selectivity}"
            );
        }
    }

    #[test]
    fn fallback_ndv_does_not_drive_equality_selectivity() {
        let mut stats = HashMap::new();
        stats.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                distinct_values_count: 10_000.0,
                confidence: Confidence::Fallback,
                ..ColumnStatistic::unknown()
            },
        );
        let predicate = eq(col("c", 1), int_lit(7));

        assert_eq!(estimate_typed(&predicate, &stats), PREDICATE_UNKNOWN_FILTER);
    }

    #[test]
    fn numeric_bounds_drive_equality_when_ndv_is_fallback() {
        let mut stats = HashMap::new();
        stats.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 1900.0,
                max_value: 2100.0,
                distinct_values_count: 10_000.0,
                confidence: Confidence::Fallback,
                ..ColumnStatistic::unknown()
            },
        );
        let predicate = eq(col("d_year", 1), int_lit(1999));

        let selectivity = estimate_typed(&predicate, &stats);

        assert!(
            (selectivity - (1.0 / 201.0)).abs() < 1e-12,
            "expected inclusive numeric-domain selectivity, got {selectivity}"
        );
    }

    #[test]
    fn fallback_ndv_does_not_drive_in_list_selectivity() {
        let mut stats = HashMap::new();
        stats.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                distinct_values_count: 10_000.0,
                confidence: Confidence::Fallback,
                ..ColumnStatistic::unknown()
            },
        );
        let predicate = TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(col("c", 1)),
                list: vec![int_lit(1), int_lit(2), int_lit(3)],
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: true,
        };

        assert_eq!(
            estimate_typed(&predicate, &stats),
            IN_PREDICATE_DEFAULT_FILTER
        );
    }

    #[test]
    fn and_chain_does_not_collapse() {
        // Construct a=? AND b=? AND c=? AND d=? AND e=? with no column stats:
        // each equality falls back to 0.25.
        let preds = and_of_unknown_eq(5);
        let sel = estimate_typed(&preds, &HashMap::new());
        assert!(sel > 0.01, "5x0.25 AND must not collapse to ~0.001: {sel}");
        assert!(sel <= 0.25, "must not exceed strongest conjunct");
    }

    #[test]
    fn nested_and_matches_flat_and_chain() {
        let p1 = unknown_eq(1);
        let p2 = unknown_eq(2);
        let p3 = unknown_eq(3);

        let grouped = and(nested(and(p1.clone(), p2.clone())), p3.clone());
        let flat = and(and(p1, p2), p3);

        let grouped_sel = estimate_typed(&grouped, &HashMap::new());
        let flat_sel = estimate_typed(&flat, &HashMap::new());
        assert!(
            (grouped_sel - flat_sel).abs() < 1e-12,
            "nested AND selectivity {grouped_sel} must match flat AND {flat_sel}"
        );
    }
}
