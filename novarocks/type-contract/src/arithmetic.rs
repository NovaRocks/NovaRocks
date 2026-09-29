// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_schema::DataType;

use crate::{LARGEINT_BYTE_WIDTH, is_largeint_data_type};

/// Closed binary arithmetic operation set whose result rules are frozen by
/// this contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// Computes the Decimal128 result of one binary arithmetic operation.
///
/// Multiplication adds scales and precisions; division follows the frozen
/// StarRocks-compatible scale bands; add/subtract/modulo align scales and add
/// one carry digit. Precision is capped at the Decimal128 contract limit.
pub fn decimal_arithmetic_result_type(
    p1: u8,
    s1: i8,
    p2: u8,
    s2: i8,
    op: ArithmeticOperator,
) -> Option<DataType> {
    if !(1..=38).contains(&p1) || !(1..=38).contains(&p2) || s1 > p1 as i8 || s2 > p2 as i8 {
        return None;
    }
    let (precision, scale) = match op {
        ArithmeticOperator::Multiply => (
            u8::try_from((i16::from(p1) + i16::from(p2)).min(38)).ok()?,
            i16::from(s1) + i16::from(s2),
        ),
        ArithmeticOperator::Divide => {
            let scale = if s1 <= 6 {
                i16::from(s1) + 6
            } else if s1 <= 12 {
                12
            } else {
                i16::from(s1)
            };
            (38, scale)
        }
        ArithmeticOperator::Add | ArithmeticOperator::Subtract | ArithmeticOperator::Modulo => {
            let scale = i16::from(s1.max(s2));
            let precision =
                ((i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2)) + scale + 1)
                    .min(38);
            (u8::try_from(precision).ok()?, scale)
        }
    };
    let scale = i8::try_from(scale).ok()?;
    if precision == 0 || scale > precision as i8 {
        return None;
    }
    Some(DataType::Decimal128(precision, scale))
}

/// Whether a supported Decimal128 multiplication exceeds its precision
/// domain while retaining a representable scale. A SQL owner may select the
/// Float64 arithmetic domain before planning when its session policy enables
/// promotion. No runtime value or overflow result participates in this rule.
///
/// Integral operands use the same precision 19 as the frozen arithmetic rules.
/// Unsupported pairs, including Decimal256, remain outside that existing domain.
pub fn decimal_multiplication_requires_float64(left: &DataType, right: &DataType) -> bool {
    let (p1, s1, p2, s2) = match (left, right) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => (*p1, *s1, *p2, *s2),
        (DataType::Decimal128(p, s), other) if is_integer(other) => (*p, *s, 19, 0),
        (other, DataType::Decimal128(p, s)) if is_integer(other) => (19, 0, *p, *s),
        _ => return false,
    };
    // Reuse the existing closed type rule so invalid metadata or an excessive
    // result scale cannot be admitted by changing the arithmetic domain.
    decimal_arithmetic_result_type(p1, s1, p2, s2, ArithmeticOperator::Multiply).is_some()
        && u16::from(p1) + u16::from(p2) > 38
}

/// Canonical output for decimal-preserving aggregates.
///
/// `None` means the aggregate or input is outside this closed rule and the
/// caller must use its separately bound result type. Decimal256 is deliberately
/// excluded because this rule freezes only the existing Decimal128 behavior.
pub fn canonical_agg_decimal_type(aggregate: &str, input: &DataType) -> Option<DataType> {
    let DataType::Decimal128(_, scale) = input else {
        return None;
    };
    let result_scale = match aggregate {
        "sum" | "multi_distinct_sum" => *scale,
        "avg" if *scale <= 6 => *scale + 6,
        "avg" if *scale <= 12 => 12,
        "avg" => *scale,
        _ => return None,
    };
    Some(DataType::Decimal128(38, result_scale))
}

/// Computes the default add/subtract result type.
pub fn arithmetic_result_type(left: &DataType, right: &DataType) -> Option<DataType> {
    arithmetic_result_type_with_op(left, right, ArithmeticOperator::Add)
}

/// Computes the exact result type for a bound binary arithmetic operator.
///
/// `None` means the input pair is outside the frozen arithmetic domain. The
/// caller must reject it or introduce an explicit cast before constructing a
/// physical expression.
pub fn arithmetic_result_type_with_op(
    left: &DataType,
    right: &DataType,
    op: ArithmeticOperator,
) -> Option<DataType> {
    // This separately frozen pair preserves both exact input carriers.
    if let Some((precision, scale)) = mixed_decimal_largeint(left, right) {
        if !matches!(op, ArithmeticOperator::Add | ArithmeticOperator::Subtract)
            || precision == 0
            || precision > 76
            || scale > precision as i8
        {
            return None;
        }
        let scale_out = i16::from(scale.max(0));
        let precision_out = (i16::from(precision) - i16::from(scale)).max(39) + scale_out + 1;
        if precision_out > 76 {
            return None;
        }
        return Some(DataType::Decimal256(
            u8::try_from(precision_out).ok()?,
            i8::try_from(scale_out).ok()?,
        ));
    }
    if !is_supported_arithmetic_type(left) || !is_supported_arithmetic_type(right) {
        return None;
    }
    let left_largeint = is_largeint_data_type(left);
    let right_largeint = is_largeint_data_type(right);
    let left_integral = left_largeint || is_integer(left);
    let right_integral = right_largeint || is_integer(right);
    if op == ArithmeticOperator::Divide && left_integral && right_integral {
        return Some(DataType::Float64);
    }
    if (left_largeint || right_largeint) && left_integral && right_integral {
        return Some(DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH));
    }

    Some(match (left, right) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            return decimal_arithmetic_result_type(*p1, *s1, *p2, *s2, op);
        }
        (
            DataType::Decimal128(p, s),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
        ) => return decimal_arithmetic_result_type(*p, *s, 19, 0, op),
        (
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
            DataType::Decimal128(p, s),
        ) => return decimal_arithmetic_result_type(19, 0, *p, *s, op),
        (DataType::Decimal128(_, _), DataType::Float64 | DataType::Float32)
        | (DataType::Float64 | DataType::Float32, DataType::Decimal128(_, _)) => DataType::Float64,
        (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
        (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float64,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int64,
        (DataType::Int16, _) | (_, DataType::Int16) => DataType::Int32,
        (DataType::Int8, _) | (_, DataType::Int8) => DataType::Int16,
        _ => return None,
    })
}

fn mixed_decimal_largeint(left: &DataType, right: &DataType) -> Option<(u8, i8)> {
    let decimal = if is_largeint_data_type(left) {
        right
    } else if is_largeint_data_type(right) {
        left
    } else {
        return None;
    };
    match decimal {
        DataType::Decimal128(precision, scale) if *precision <= 38 => Some((*precision, *scale)),
        DataType::Decimal256(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn is_supported_arithmetic_type(data_type: &DataType) -> bool {
    is_integer(data_type)
        || is_largeint_data_type(data_type)
        || matches!(
            data_type,
            DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _)
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_decimal_largeint_add_sub_have_exact_bounded_width() {
        let largeint = DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH);
        for (decimal, expected) in [
            (
                DataType::Decimal128(38, 15),
                Some(DataType::Decimal256(55, 15)),
            ),
            (
                DataType::Decimal128(38, 36),
                Some(DataType::Decimal256(76, 36)),
            ),
            (DataType::Decimal128(38, 37), None),
            (
                DataType::Decimal128(38, -36),
                Some(DataType::Decimal256(75, 0)),
            ),
            (DataType::Decimal128(38, -38), None),
            (
                DataType::Decimal256(55, 15),
                Some(DataType::Decimal256(56, 15)),
            ),
            (DataType::Decimal256(76, 0), None),
        ] {
            for op in [ArithmeticOperator::Add, ArithmeticOperator::Subtract] {
                assert_eq!(
                    arithmetic_result_type_with_op(&decimal, &largeint, op),
                    expected
                );
                assert_eq!(
                    arithmetic_result_type_with_op(&largeint, &decimal, op),
                    expected
                );
            }
            for op in [
                ArithmeticOperator::Multiply,
                ArithmeticOperator::Divide,
                ArithmeticOperator::Modulo,
            ] {
                assert_eq!(
                    arithmetic_result_type_with_op(&decimal, &largeint, op),
                    None
                );
                assert_eq!(
                    arithmetic_result_type_with_op(&largeint, &decimal, op),
                    None
                );
            }
        }
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Decimal256(55, 15),
                &DataType::Int64,
                ArithmeticOperator::Add
            ),
            None
        );
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Decimal256(55, 15),
                &DataType::Decimal256(55, 15),
                ArithmeticOperator::Add
            ),
            None
        );
    }

    #[test]
    fn decimal_promotion_is_a_precision_decision_with_the_existing_scale_boundary() {
        for (left, right, expected) in [
            (
                DataType::Decimal128(18, 9),
                DataType::Decimal128(18, 9),
                false,
            ),
            (
                DataType::Decimal128(30, 10),
                DataType::Decimal128(18, 9),
                true,
            ),
            (
                DataType::Decimal128(38, 15),
                DataType::Decimal128(38, 15),
                true,
            ),
            (
                DataType::Decimal128(38, 19),
                DataType::Decimal128(20, 19),
                true,
            ),
            (
                DataType::Decimal128(38, 20),
                DataType::Decimal128(20, 19),
                false,
            ),
            (DataType::Decimal128(20, 0), DataType::Int32, true),
            (DataType::Int64, DataType::Decimal128(20, 0), true),
            (DataType::Decimal128(19, 0), DataType::Int8, false),
            (DataType::Decimal128(30, 10), DataType::Float64, false),
            (
                DataType::Decimal256(60, 10),
                DataType::Decimal128(30, 10),
                false,
            ),
            (
                DataType::Decimal128(30, 31),
                DataType::Decimal128(18, 0),
                false,
            ),
        ] {
            assert_eq!(
                decimal_multiplication_requires_float64(&left, &right),
                expected,
                "unexpected promotion eligibility for {left:?} * {right:?}"
            );
        }
        // The false setting keeps the preexisting checked Decimal128 contract.
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Decimal128(30, 10),
                &DataType::Decimal128(18, 9),
                ArithmeticOperator::Multiply
            ),
            Some(DataType::Decimal128(38, 19))
        );
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Decimal128(38, 20),
                &DataType::Decimal128(20, 19),
                ArithmeticOperator::Multiply
            ),
            None
        );
    }

    #[test]
    fn decimal_times_float_returns_float64() {
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Decimal128(7, 2),
                &DataType::Float64,
                ArithmeticOperator::Multiply,
            ),
            Some(DataType::Float64)
        );
    }

    #[test]
    fn float_plus_decimal_returns_float64() {
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Float64,
                &DataType::Decimal128(18, 6),
                ArithmeticOperator::Add,
            ),
            Some(DataType::Float64)
        );
    }

    #[test]
    fn decimal_div_float32_returns_float64() {
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Decimal128(10, 4),
                &DataType::Float32,
                ArithmeticOperator::Divide,
            ),
            Some(DataType::Float64)
        );
    }

    #[test]
    fn decimal_times_decimal_uses_multiplication_scale() {
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Decimal128(7, 2),
                &DataType::Decimal128(10, 4),
                ArithmeticOperator::Multiply,
            ),
            Some(DataType::Decimal128(17, 6))
        );
    }

    #[test]
    fn decimal_plus_integer_uses_decimal_rule() {
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Decimal128(7, 2),
                &DataType::Int32,
                ArithmeticOperator::Add,
            ),
            Some(DataType::Decimal128(22, 2))
        );
    }

    #[test]
    fn largeint_plus_integer_returns_largeint() {
        assert_eq!(
            arithmetic_result_type_with_op(
                &DataType::Int64,
                &DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH),
                ArithmeticOperator::Add,
            ),
            Some(DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH))
        );
    }

    #[test]
    fn largeint_division_uses_fractional_result_domain() {
        let largeint = DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH);
        assert_eq!(
            arithmetic_result_type_with_op(&largeint, &DataType::Int64, ArithmeticOperator::Divide,),
            Some(DataType::Float64),
        );
        assert_eq!(
            arithmetic_result_type_with_op(&largeint, &largeint, ArithmeticOperator::Divide,),
            Some(DataType::Float64),
        );
    }

    #[test]
    fn arithmetic_rule_rejects_unfrozen_types() {
        for data_type in [
            DataType::UInt64,
            DataType::Float16,
            DataType::Decimal32(7, 2),
            DataType::Decimal64(10, 2),
            DataType::Decimal256(60, 10),
        ] {
            assert_eq!(
                arithmetic_result_type_with_op(&data_type, &data_type, ArithmeticOperator::Add,),
                None,
                "{data_type:?}",
            );
        }
    }

    #[test]
    fn canonical_decimal_sum_widens_precision_and_keeps_scale() {
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
    fn canonical_decimal_avg_uses_all_scale_bands() {
        assert_eq!(
            canonical_agg_decimal_type("avg", &DataType::Decimal128(10, 3)),
            Some(DataType::Decimal128(38, 9))
        );
        assert_eq!(
            canonical_agg_decimal_type("avg", &DataType::Decimal128(20, 10)),
            Some(DataType::Decimal128(38, 12))
        );
        assert_eq!(
            canonical_agg_decimal_type("avg", &DataType::Decimal128(38, 13)),
            Some(DataType::Decimal128(38, 13))
        );
    }

    #[test]
    fn canonical_decimal_rule_is_closed() {
        assert_eq!(canonical_agg_decimal_type("sum", &DataType::Int64), None);
        assert_eq!(
            canonical_agg_decimal_type("min", &DataType::Decimal128(10, 2)),
            None
        );
        assert_eq!(
            canonical_agg_decimal_type("sum", &DataType::Decimal256(40, 2)),
            None
        );
    }
}
