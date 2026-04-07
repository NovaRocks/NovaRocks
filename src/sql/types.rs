use arrow::datatypes::DataType;

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

/// Determine the result type for binary arithmetic operations (default: add/sub rules).
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
    let both_integral = matches!(
        left,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) && matches!(
        right,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    );
    if is_div && both_integral {
        return DataType::Float64;
    }

    match (left, right) {
        // Decimal + Decimal -> Decimal (op-specific precision/scale)
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            decimal_arithmetic_result_type(*p1, *s1 as i8, *p2, *s2 as i8, op)
        }
        // Decimal (left) op Integer (right) -> Decimal
        (
            DataType::Decimal128(p, s),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
        ) => decimal_arithmetic_result_type(*p, *s as i8, 19, 0, op),
        // Integer (left) op Decimal (right) -> Decimal
        (
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
            DataType::Decimal128(p, s),
        ) => decimal_arithmetic_result_type(19, 0, *p, *s as i8, op),
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

/// Determine the wider type for unifying two types (comparisons, CASE, UNION, etc.).
pub(crate) fn wider_type(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return a.clone();
    }
    match (a, b) {
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),
        // Decimal + Decimal -> wider Decimal
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            let scale = (*s1).max(*s2);
            let precision =
                ((*p1 as i8 - *s1 as i8).max(*p2 as i8 - *s2 as i8) + scale as i8).min(38) as u8;
            DataType::Decimal128(precision, scale)
        }
        // Decimal + Integer -> Decimal
        (
            DataType::Decimal128(_, _),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
        )
        | (
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
            DataType::Decimal128(_, _),
        ) => {
            let (p, s) = match (a, b) {
                (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => (*p, *s),
                _ => unreachable!(),
            };
            DataType::Decimal128(p, s)
        }
        // Decimal + Float -> Float64 (StarRocks FE: promote to Double)
        (DataType::Decimal128(_, _), DataType::Float64 | DataType::Float32)
        | (DataType::Float64 | DataType::Float32, DataType::Decimal128(_, _)) => DataType::Float64,
        // Decimal + other -> Decimal
        (DataType::Decimal128(_, _), _) | (_, DataType::Decimal128(_, _)) => {
            let (p, s) = match (a, b) {
                (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => (*p, *s),
                _ => unreachable!(),
            };
            DataType::Decimal128(p, s)
        }
        (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
        (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float64,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int64,
        (DataType::Utf8, _) | (_, DataType::Utf8) => DataType::Utf8,
        (DataType::LargeUtf8, _) | (_, DataType::LargeUtf8) => DataType::Utf8,
        _ => a.clone(),
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
    fn wider_type_decimal_vs_float64_returns_float64() {
        let result = wider_type(&DataType::Decimal128(7, 2), &DataType::Float64);
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn wider_type_float32_vs_decimal_returns_float64() {
        let result = wider_type(&DataType::Float32, &DataType::Decimal128(18, 6));
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
}
