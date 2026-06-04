use crate::sql::analysis::JoinKind;
use crate::sql::optimizer::estimate::arith::{MAX_ROW_COUNT, damped_conjunction, sat_add, sat_mul};
use crate::sql::optimizer::statistics::{
    ANTI_JOIN_SELECTIVITY, Confidence, PREDICATE_UNKNOWN_FILTER, SEMI_JOIN_SELECTIVITY,
    UNKNOWN_GROUP_BY_CORRELATION,
};

pub struct JoinCardInput {
    pub left: (f64, Confidence),
    pub right: (f64, Confidence),
    pub kind: JoinKind,
    pub eq_key_ndvs: Vec<(f64, f64, Confidence)>,
    pub non_equi_selectivity: Option<(f64, Confidence)>,
}

pub fn estimate_join_cardinality(input: &JoinCardInput) -> (f64, Confidence) {
    let (left_rows, left_saturated) = row_count(input.left.0);
    let (right_rows, right_saturated) = row_count(input.right.0);
    let input_saturated = left_saturated || right_saturated;

    let mut confidence_inputs = vec![input.left.1, input.right.1];
    let (rows, saturated, used_default_or_invalid) = match input.kind {
        JoinKind::Cross => {
            let (rows, saturated) = sat_mul(left_rows, right_rows);
            (rows, saturated, false)
        }
        JoinKind::Inner => {
            let (rows, saturated, used_default_or_invalid) =
                inner_rows(left_rows, right_rows, input, &mut confidence_inputs);
            (rows.max(1.0), saturated, used_default_or_invalid)
        }
        JoinKind::LeftOuter => {
            let (inner, saturated, used_default_or_invalid) =
                inner_rows(left_rows, right_rows, input, &mut confidence_inputs);
            (inner.max(left_rows), saturated, used_default_or_invalid)
        }
        JoinKind::RightOuter => {
            let (inner, saturated, used_default_or_invalid) =
                inner_rows(left_rows, right_rows, input, &mut confidence_inputs);
            (inner.max(right_rows), saturated, used_default_or_invalid)
        }
        JoinKind::FullOuter => {
            let (inner, saturated, used_default_or_invalid) =
                inner_rows(left_rows, right_rows, input, &mut confidence_inputs);
            (
                inner.max(left_rows).max(right_rows),
                saturated,
                used_default_or_invalid,
            )
        }
        JoinKind::LeftSemi => {
            let (selectivity, used_default_or_invalid) =
                semi_selectivity(input, right_rows, &mut confidence_inputs);
            let (rows, saturated) = bounded_side_rows(left_rows, selectivity);
            (rows, saturated, used_default_or_invalid)
        }
        JoinKind::RightSemi => {
            let (selectivity, used_default_or_invalid) =
                semi_selectivity(input, left_rows, &mut confidence_inputs);
            let (rows, saturated) = bounded_side_rows(right_rows, selectivity);
            (rows, saturated, used_default_or_invalid)
        }
        JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
            let (rows, saturated) = bounded_side_rows(left_rows, ANTI_JOIN_SELECTIVITY);
            (rows, saturated, true)
        }
        JoinKind::RightAnti => {
            let (rows, saturated) = bounded_side_rows(right_rows, ANTI_JOIN_SELECTIVITY);
            (rows, saturated, true)
        }
    };

    if input_saturated || saturated || rows >= MAX_ROW_COUNT {
        return (MAX_ROW_COUNT.min(rows), Confidence::Fallback);
    }

    let combined_input_conf = confidence_inputs
        .into_iter()
        .reduce(Confidence::combine)
        .unwrap_or(Confidence::Estimated);
    (
        rows,
        Confidence::derive(&[combined_input_conf], used_default_or_invalid),
    )
}

pub(crate) fn union_all_rows(inputs: &[f64]) -> (f64, bool) {
    let mut rows = 0.0;
    let mut saturated_or_defaulted = false;
    for &input in inputs {
        let (input_rows, input_defaulted) = normalize_set_op_input_rows(input);
        saturated_or_defaulted |= input_defaulted;
        let Some(input_rows) = input_rows else {
            continue;
        };
        let (next, next_saturated) = sat_add(rows, input_rows);
        rows = next;
        saturated_or_defaulted |= next_saturated;
    }
    (rows, saturated_or_defaulted)
}

pub(crate) fn union_distinct_rows(inputs: &[f64]) -> (f64, bool) {
    let (union_rows, union_saturated) = union_all_rows(inputs);
    let (rows, saturated) = sat_mul(union_rows, UNKNOWN_GROUP_BY_CORRELATION);
    (rows, union_saturated || saturated)
}

pub(crate) fn intersect_rows(inputs: &[f64]) -> (f64, bool) {
    let Some(min_rows) = inputs
        .iter()
        .copied()
        .filter(|rows| !rows.is_nan())
        .reduce(f64::min)
    else {
        return (1.0, true);
    };
    let (rows, saturated) = sat_mul(min_rows, 0.5);
    finite_positive_rows(rows, saturated)
}

pub(crate) fn except_rows(inputs: &[f64]) -> (f64, bool) {
    let Some(&first_rows) = inputs.first() else {
        return (1.0, true);
    };
    let (rows, saturated) = sat_mul(first_rows, 0.5);
    finite_positive_rows(rows, saturated)
}

fn finite_positive_rows(rows: f64, saturated_or_defaulted: bool) -> (f64, bool) {
    if !rows.is_finite() || rows < 1.0 {
        (1.0, true)
    } else {
        (rows, saturated_or_defaulted)
    }
}

fn normalize_set_op_input_rows(rows: f64) -> (Option<f64>, bool) {
    if rows.is_finite() && rows >= 0.0 {
        (Some(rows), false)
    } else {
        (None, true)
    }
}

fn row_count(rows: f64) -> (f64, bool) {
    if rows.is_nan() {
        return (1.0, true);
    }
    sat_mul(rows.max(1.0), 1.0)
}

fn inner_rows(
    left_rows: f64,
    right_rows: f64,
    input: &JoinCardInput,
    confidence_inputs: &mut Vec<Confidence>,
) -> (f64, bool, bool) {
    let key_selectivity = if input.eq_key_ndvs.is_empty() {
        1.0
    } else {
        let mut selectivities = Vec::with_capacity(input.eq_key_ndvs.len());
        let mut used_default_or_invalid = false;
        let mut fallback_key_count = 0;
        for &(left_ndv, right_ndv, confidence) in &input.eq_key_ndvs {
            confidence_inputs.push(confidence);
            let (denominator, invalid_ndv) = ndv_denominator(left_ndv, right_ndv);
            used_default_or_invalid |= invalid_ndv;
            let selectivity = if confidence == Confidence::Fallback {
                fallback_key_count += 1;
                if fallback_key_count > 1 {
                    PREDICATE_UNKNOWN_FILTER
                } else {
                    1.0 / denominator
                }
            } else {
                1.0 / denominator
            };
            selectivities.push(selectivity);
        }
        return inner_rows_with_key_selectivity(
            left_rows,
            right_rows,
            input,
            confidence_inputs,
            damped_conjunction(&selectivities),
            used_default_or_invalid,
        );
    };

    inner_rows_with_key_selectivity(
        left_rows,
        right_rows,
        input,
        confidence_inputs,
        key_selectivity,
        false,
    )
}

fn inner_rows_with_key_selectivity(
    left_rows: f64,
    right_rows: f64,
    input: &JoinCardInput,
    confidence_inputs: &mut Vec<Confidence>,
    key_selectivity: f64,
    mut used_default_or_invalid: bool,
) -> (f64, bool, bool) {
    let non_equi = non_equi_selectivity(input, confidence_inputs);
    used_default_or_invalid |= non_equi.1;
    let (product, product_saturated) = sat_mul(left_rows, right_rows);
    let (rows, selectivity_saturated) = sat_mul(product, key_selectivity * non_equi.0);
    (
        rows.max(1.0),
        product_saturated || selectivity_saturated,
        used_default_or_invalid,
    )
}

fn ndv_denominator(left_ndv: f64, right_ndv: f64) -> (f64, bool) {
    let left = valid_ndv(left_ndv);
    let right = valid_ndv(right_ndv);
    let denominator = match (left, right) {
        (Some(left), Some(right)) => left.max(right),
        (Some(ndv), None) | (None, Some(ndv)) => ndv,
        (None, None) => 1.0,
    };
    (denominator, left.is_none() || right.is_none())
}

fn valid_ndv(ndv: f64) -> Option<f64> {
    if ndv.is_finite() && ndv > 0.0 {
        Some(ndv.max(1.0))
    } else {
        None
    }
}

fn non_equi_selectivity(
    input: &JoinCardInput,
    confidence_inputs: &mut Vec<Confidence>,
) -> (f64, bool) {
    if let Some((selectivity, confidence)) = input.non_equi_selectivity {
        confidence_inputs.push(confidence);
        clamp_selectivity(selectivity)
    } else {
        (1.0, false)
    }
}

fn semi_selectivity(
    input: &JoinCardInput,
    matching_side_rows: f64,
    confidence_inputs: &mut Vec<Confidence>,
) -> (f64, bool) {
    let mut selectivities = Vec::new();
    let mut key_selectivities = Vec::with_capacity(input.eq_key_ndvs.len());
    let mut used_default_or_invalid = false;
    let mut used_fallback_key_ndv = false;

    for &(left_ndv, right_ndv, confidence) in &input.eq_key_ndvs {
        confidence_inputs.push(confidence);
        used_fallback_key_ndv |= confidence == Confidence::Fallback;
        let (denominator, invalid_ndv) = ndv_denominator(left_ndv, right_ndv);
        used_default_or_invalid |= invalid_ndv;
        key_selectivities.push(1.0 / denominator);
    }

    if !key_selectivities.is_empty() {
        let key_selectivity = damped_conjunction(&key_selectivities);
        let (match_probability, saturated) = sat_mul(matching_side_rows, key_selectivity);
        used_default_or_invalid |= saturated;
        let upper_bound = if used_fallback_key_ndv {
            used_default_or_invalid = true;
            PREDICATE_UNKNOWN_FILTER
        } else {
            1.0
        };
        selectivities.push(match_probability.clamp(0.0, upper_bound));
    }

    if let Some((selectivity, confidence)) = input.non_equi_selectivity {
        confidence_inputs.push(confidence);
        let (selectivity, invalid_selectivity) = clamp_selectivity(selectivity);
        used_default_or_invalid |= invalid_selectivity;
        selectivities.push(selectivity);
    }

    if selectivities.is_empty() {
        return (SEMI_JOIN_SELECTIVITY, true);
    }

    (damped_conjunction(&selectivities), used_default_or_invalid)
}

fn bounded_side_rows(side_rows: f64, selectivity: f64) -> (f64, bool) {
    let (selectivity, invalid_selectivity) = clamp_selectivity(selectivity);
    let (rows, saturated) = sat_mul(side_rows, selectivity);
    (rows.clamp(1.0, side_rows), saturated || invalid_selectivity)
}

fn clamp_selectivity(selectivity: f64) -> (f64, bool) {
    if selectivity.is_finite() {
        (
            selectivity.clamp(0.0, 1.0),
            !(0.0..=1.0).contains(&selectivity),
        )
    } else {
        (1.0, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::JoinKind;
    use crate::sql::optimizer::statistics::Confidence;

    fn inp(kind: JoinKind, l: f64, r: f64, keys: Vec<(f64, f64)>) -> JoinCardInput {
        JoinCardInput {
            left: (l, Confidence::Estimated),
            right: (r, Confidence::Estimated),
            kind,
            eq_key_ndvs: keys
                .into_iter()
                .map(|(a, b)| (a, b, Confidence::Estimated))
                .collect(),
            non_equi_selectivity: None,
        }
    }

    #[test]
    fn single_key_inner_matches_containment() {
        let (rows, _) =
            estimate_join_cardinality(&inp(JoinKind::Inner, 1000.0, 800.0, vec![(100.0, 50.0)]));
        assert!((rows - 8000.0).abs() < 1.0, "got {rows}");
    }

    #[test]
    fn single_key_inner_equals_legacy_containment_formula() {
        // Single-key inner joins must stay equivalent to the legacy containment formula.
        for &(left, right, ndv) in &[(1000.0, 500.0, 50.0), (10.0, 8.0, 10.0), (1e6, 1e3, 1e4)] {
            let (rows, _) =
                estimate_join_cardinality(&inp(JoinKind::Inner, left, right, vec![(ndv, ndv)]));
            let expected = (left * right / ndv)
                .min(crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT)
                .max(1.0);
            assert!(
                (rows - expected).abs() <= expected * 1e-9 + 1.0,
                "left={left} right={right} ndv={ndv}: {rows} vs {expected}"
            );
        }
    }

    #[test]
    fn multikey_inner_does_not_collapse_or_inflate() {
        let (rows, _) = estimate_join_cardinality(&inp(
            JoinKind::Inner,
            1000.0,
            1000.0,
            vec![(100.0, 100.0), (100.0, 100.0)],
        ));
        assert!(
            rows < 10000.0 && rows > 1.0,
            "multikey should reduce below single-key but not collapse: {rows}"
        );
        assert!((rows - 1000.0).abs() < 50.0, "got {rows}");
    }

    #[test]
    fn multikey_inner_softens_additional_fallback_keys() {
        let input = JoinCardInput {
            left: (81_000.0, Confidence::Estimated),
            right: (81_000.0, Confidence::Estimated),
            kind: JoinKind::Inner,
            eq_key_ndvs: vec![
                (40.0, 40.0, Confidence::Fallback),
                (40.0, 40.0, Confidence::Fallback),
            ],
            non_equi_selectivity: None,
        };
        let (rows, conf) = estimate_join_cardinality(&input);
        assert_eq!(conf, Confidence::Fallback);
        assert!((rows - 82_012_500.0).abs() < 1.0, "got {rows}");
    }

    #[test]
    fn outer_join_at_least_preserved_side() {
        let (rows, _) =
            estimate_join_cardinality(&inp(JoinKind::LeftOuter, 5000.0, 10.0, vec![(1e6, 1e6)]));
        assert!(rows >= 5000.0, "left outer must keep >= left rows: {rows}");
    }

    #[test]
    fn cross_join_saturates_with_fallback() {
        let (rows, conf) = estimate_join_cardinality(&inp(JoinKind::Cross, 1e9, 1e9, vec![]));
        assert_eq!(rows, crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT);
        assert_eq!(conf, Confidence::Fallback);
    }

    #[test]
    fn inner_join_reports_fallback_when_intermediate_product_saturates() {
        let (rows, conf) =
            estimate_join_cardinality(&inp(JoinKind::Inner, 1e12, 1e12, vec![(1e12, 1e12)]));
        assert!(rows < crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT);
        assert_eq!(conf, Confidence::Fallback);
    }

    #[test]
    fn default_semi_and_anti_selectivity_are_fallback() {
        let exact_input = JoinCardInput {
            left: (1000.0, Confidence::Exact),
            right: (50.0, Confidence::Exact),
            kind: JoinKind::LeftSemi,
            eq_key_ndvs: vec![],
            non_equi_selectivity: None,
        };
        let (_, semi_conf) = estimate_join_cardinality(&exact_input);
        assert_eq!(semi_conf, Confidence::Fallback);

        let anti_input = JoinCardInput {
            kind: JoinKind::LeftAnti,
            ..exact_input
        };
        let (_, anti_conf) = estimate_join_cardinality(&anti_input);
        assert_eq!(anti_conf, Confidence::Fallback);
    }

    #[test]
    fn semi_join_uses_key_ndv_for_match_selectivity() {
        let (left_rows, left_conf) = estimate_join_cardinality(&inp(
            JoinKind::LeftSemi,
            1000.0,
            10.0,
            vec![(1000.0, 1000.0)],
        ));
        assert_eq!(left_conf, Confidence::Estimated);
        assert!((left_rows - 10.0).abs() < 1e-9, "got {left_rows}");

        let (right_rows, right_conf) = estimate_join_cardinality(&inp(
            JoinKind::RightSemi,
            10.0,
            1000.0,
            vec![(1000.0, 1000.0)],
        ));
        assert_eq!(right_conf, Confidence::Estimated);
        assert!((right_rows - 10.0).abs() < 1e-9, "got {right_rows}");
    }

    #[test]
    fn multikey_semi_join_applies_matching_rows_once() {
        let (rows, conf) = estimate_join_cardinality(&inp(
            JoinKind::LeftSemi,
            1000.0,
            100.0,
            vec![(1000.0, 1000.0), (1000.0, 1000.0)],
        ));

        let key_selectivity =
            crate::sql::optimizer::estimate::arith::damped_conjunction(&[0.001, 0.001]);
        let expected = 1000.0 * (100.0 * key_selectivity);
        assert_eq!(conf, Confidence::Estimated);
        assert!(
            (rows - expected).abs() < 1e-9,
            "got {rows}, expected {expected}"
        );
    }

    #[test]
    fn invalid_ndv_and_selectivity_degrade_confidence() {
        let invalid_ndv_input = JoinCardInput {
            left: (1000.0, Confidence::Exact),
            right: (1000.0, Confidence::Exact),
            kind: JoinKind::Inner,
            eq_key_ndvs: vec![(f64::NAN, -1.0, Confidence::Exact)],
            non_equi_selectivity: None,
        };
        let (ndv_rows, ndv_conf) = estimate_join_cardinality(&invalid_ndv_input);
        assert!(ndv_rows.is_finite());
        assert_eq!(ndv_conf, Confidence::Fallback);

        let invalid_selectivity_input = JoinCardInput {
            eq_key_ndvs: vec![],
            non_equi_selectivity: Some((f64::INFINITY, Confidence::Exact)),
            ..invalid_ndv_input
        };
        let (sel_rows, sel_conf) = estimate_join_cardinality(&invalid_selectivity_input);
        assert!(sel_rows.is_finite());
        assert_eq!(sel_conf, Confidence::Fallback);
    }

    #[test]
    fn semi_and_anti_bounded_by_left() {
        let (semi, _) =
            estimate_join_cardinality(&inp(JoinKind::LeftSemi, 1000.0, 50.0, vec![(10.0, 10.0)]));
        assert!(semi <= 1000.0 && semi >= 1.0);
        let (anti, _) =
            estimate_join_cardinality(&inp(JoinKind::LeftAnti, 1000.0, 50.0, vec![(10.0, 10.0)]));
        assert!(anti <= 1000.0 && anti >= 1.0);
    }

    #[test]
    fn union_all_rows_saturates_huge_inputs() {
        let (rows, saturated) = union_all_rows(&[9.0e14, 9.0e14]);

        assert_eq!(rows, crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT);
        assert!(saturated);
    }

    #[test]
    fn union_all_rows_preserves_valid_rows_across_invalid_inputs() {
        let (rows, saturated_or_defaulted) =
            union_all_rows(&[100.0, f64::NAN, f64::INFINITY, -25.0, 50.0]);

        assert_eq!(rows, 150.0);
        assert!(saturated_or_defaulted);
    }

    #[test]
    fn union_distinct_rows_applies_unknown_group_correlation() {
        let (rows, saturated) = union_distinct_rows(&[100.0, 300.0]);

        assert_eq!(rows, 300.0);
        assert!(!saturated);
    }

    #[test]
    fn intersect_and_except_rows_are_finite_for_normal_inputs() {
        let (intersect, intersect_defaulted) = intersect_rows(&[1_000.0, 200.0, 500.0]);
        let (except, except_defaulted) = except_rows(&[1_000.0, 200.0, 500.0]);

        assert_eq!(intersect, 100.0);
        assert!(!intersect_defaulted);
        assert_eq!(except, 500.0);
        assert!(!except_defaulted);
    }
}
