use arrow::datatypes::DataType;

use crate::types::predicate::{is_integer, is_largeint};

/// Determine the result type for a Decimal binary arithmetic operation,
/// taking the operator into account (multiply/divide need different scale rules).
pub(crate) fn decimal_arithmetic_result_type(p1: u8, s1: i8, p2: u8, s2: i8, op: &str) -> DataType {
    let (precision, scale) = match op {
        "mul" | "*" => {
            // multiply: scale = s1+s2, precision = p1+p2
            let s = s1 + s2;
            let p = (p1 as i8 + p2 as i8).min(38);
            (p as u8, s)
        }
        "div" | "/" => {
            // StarRocks divide rule:
            // if lhsScale <= 6:  returnScale = lhsScale + 6
            // if lhsScale <= 12: returnScale = 12
            // else:              returnScale = lhsScale
            // precision = 38 (always max)
            let s = if s1 <= 6 {
                s1 + 6
            } else if s1 <= 12 {
                12
            } else {
                s1
            };
            (38_u8, s)
        }
        _ => {
            // add/sub/mod: scale = max(s1,s2), precision = max(p1-s1, p2-s2)+scale+1
            let s = s1.max(s2);
            let p = ((p1 as i8 - s1).max(p2 as i8 - s2) + s + 1).min(38);
            (p as u8, s)
        }
    };
    DataType::Decimal128(precision, scale)
}

/// Canonical decimal output type for the decimal-preserving aggregates — the
/// single source of truth (pillar P2) shared by the analyzer, the standalone
/// codegen, and the runtime agg spec builders, so the same logical aggregate
/// slot carries an identical descriptor in every fragment by construction.
///
/// Returns `None` for non-decimal inputs and for aggregates that do not
/// canonicalize a decimal result, so callers keep their existing non-decimal
/// arms. `Decimal256` is intentionally out of scope: the analyzer canonicalizes
/// only `Decimal128`, and callers retain their own `Decimal256` arms.
pub(crate) fn canonical_agg_decimal_type(agg_name: &str, input: &DataType) -> Option<DataType> {
    let scale = match input {
        DataType::Decimal128(_, s) => *s,
        _ => return None,
    };
    let out_scale = match agg_name {
        "sum" | "multi_distinct_sum" => scale,
        "avg" => avg_decimal_scale(scale),
        _ => return None,
    };
    Some(DataType::Decimal128(38, out_scale))
}

/// AVG over a decimal is computed as `sum / count`; the StarRocks division scale
/// rule sets the result scale from the input scale.
fn avg_decimal_scale(scale: i8) -> i8 {
    if scale <= 6 {
        scale + 6
    } else if scale <= 12 {
        12
    } else {
        scale
    }
}

/// Determine the result type for binary arithmetic operations (default: add/sub rules).
#[allow(dead_code)] // used by legacy ExprCompiler methods, keeping for type-system completeness
pub(crate) fn arithmetic_result_type(left: &DataType, right: &DataType) -> DataType {
    arithmetic_result_type_with_op(left, right, "add")
}

/// Determine the result type for binary arithmetic operations with a specific operator.
pub(crate) fn arithmetic_result_type_with_op(
    left: &DataType,
    right: &DataType,
    op: &str,
) -> DataType {
    // StarRocks behavior: integer / integer → DOUBLE (not integer).
    let is_div = op == "div";
    let both_integral = is_integer(left) && is_integer(right);
    if is_div && both_integral {
        return DataType::Float64;
    }

    let left_largeint = crate::common::largeint::is_largeint_data_type(left);
    let right_largeint = crate::common::largeint::is_largeint_data_type(right);
    let left_integral = left_largeint
        || matches!(
            left,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        );
    let right_integral = right_largeint
        || matches!(
            right,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        );
    if (left_largeint || right_largeint) && left_integral && right_integral {
        return DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH);
    }

    match (left, right) {
        (l, r) if (is_largeint(l) && is_integer(r)) || (is_integer(l) && is_largeint(r)) => {
            DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH)
        }
        // Decimal + Decimal -> Decimal (op-specific precision/scale)
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            decimal_arithmetic_result_type(*p1, *s1, *p2, *s2, op)
        }
        // Decimal (left) op Integer (right) -> Decimal
        (
            DataType::Decimal128(p, s),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
        ) => decimal_arithmetic_result_type(*p, *s, 19, 0, op),
        // Integer (left) op Decimal (right) -> Decimal
        (
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
            DataType::Decimal128(p, s),
        ) => decimal_arithmetic_result_type(19, 0, *p, *s, op),
        // Decimal + Float -> Float64 (StarRocks FE: both sides promote to Double)
        (DataType::Decimal128(_, _), DataType::Float64 | DataType::Float32)
        | (DataType::Float64 | DataType::Float32, DataType::Decimal128(_, _)) => DataType::Float64,
        // Existing rules
        (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
        (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float64,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int64,
        (DataType::Int16, _) | (_, DataType::Int16) => DataType::Int32,
        (DataType::Int8, _) | (_, DataType::Int8) => DataType::Int16,
        _ => DataType::Float64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn decimal_times_float_returns_float64() {
        let result =
            arithmetic_result_type_with_op(&DataType::Decimal128(7, 2), &DataType::Float64, "mul");
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn float_plus_decimal_returns_float64() {
        let result =
            arithmetic_result_type_with_op(&DataType::Float64, &DataType::Decimal128(18, 6), "add");
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn decimal_div_float32_returns_float64() {
        let result =
            arithmetic_result_type_with_op(&DataType::Decimal128(10, 4), &DataType::Float32, "div");
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn decimal_times_decimal_unchanged() {
        let result = arithmetic_result_type_with_op(
            &DataType::Decimal128(7, 2),
            &DataType::Decimal128(10, 4),
            "mul",
        );
        assert_eq!(result, DataType::Decimal128(17, 6));
    }

    #[test]
    fn decimal_plus_int_unchanged() {
        let result =
            arithmetic_result_type_with_op(&DataType::Decimal128(7, 2), &DataType::Int32, "add");
        assert_eq!(result, DataType::Decimal128(22, 2));
    }

    #[test]
    fn largeint_plus_integer_returns_largeint() {
        let largeint = DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH);
        let result = arithmetic_result_type_with_op(&DataType::Int64, &largeint, "add");
        assert_eq!(result, largeint);
    }

    #[test]
    fn canonical_agg_decimal_sum_widens_precision_keeps_scale() {
        assert_eq!(
            canonical_agg_decimal_type("sum", &DataType::Decimal128(20, 2)),
            Some(DataType::Decimal128(38, 2))
        );
        assert_eq!(
            canonical_agg_decimal_type("multi_distinct_sum", &DataType::Decimal128(20, 2)),
            Some(DataType::Decimal128(38, 2))
        );
    }

    #[test]
    fn canonical_agg_decimal_avg_applies_division_scale_rule() {
        // s <= 6  => s + 6
        assert_eq!(
            canonical_agg_decimal_type("avg", &DataType::Decimal128(10, 3)),
            Some(DataType::Decimal128(38, 9))
        );
        // s <= 12 => 12
        assert_eq!(
            canonical_agg_decimal_type("avg", &DataType::Decimal128(20, 10)),
            Some(DataType::Decimal128(38, 12))
        );
        // else    => s
        assert_eq!(
            canonical_agg_decimal_type("avg", &DataType::Decimal128(38, 13)),
            Some(DataType::Decimal128(38, 13))
        );
    }

    #[test]
    fn canonical_agg_decimal_none_for_non_decimal_or_other_agg() {
        assert_eq!(canonical_agg_decimal_type("sum", &DataType::Int64), None);
        assert_eq!(
            canonical_agg_decimal_type("min", &DataType::Decimal128(10, 2)),
            None
        );
        // Decimal256 is out of scope for canonicalization.
        assert_eq!(
            canonical_agg_decimal_type("sum", &DataType::Decimal256(40, 2)),
            None
        );
    }
}
