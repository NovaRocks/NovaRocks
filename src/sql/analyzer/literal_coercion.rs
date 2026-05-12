// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! StarRocks-aligned literal coercion at analyzer level.
//!
//! When a comparison / IN / BETWEEN has `column op literal` where the column
//! is a typed reference (DATETIME, DATE, DECIMAL, INT family) and the literal
//! is a STRING, the literal must be coerced to the column's type *before*
//! comparison. Mirrors StarRocks' `LiteralExprFactory.create(value, columnType)`.
//!
//! For DATETIME with microsecond scale, this preserves up to 6 fractional
//! digits; longer fractions error rather than silently truncate (matching
//! StarRocks "Datetime literal is invalid").

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, TypedExpr};

/// Returns `true` if `expr` is a column reference (resolved column ref).
/// Used to recognize "column-side" of a comparison.
pub(crate) fn is_column_ref(expr: &TypedExpr) -> bool {
    matches!(expr.kind, ExprKind::ColumnRef { .. })
}

/// Returns `true` if `data_type` is one we want to coerce string literals into.
pub(crate) fn is_coercible_target(data_type: &DataType) -> bool {
    use arrow::datatypes::TimeUnit;
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Timestamp(TimeUnit::Second, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Decimal128(_, _)
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
    )
}

/// If `right` is a string-typed literal and `left` is a column ref of a
/// coercible target type, return `right` coerced to `left`'s type.
/// Otherwise return `right` unchanged.
///
/// Caller is `analyze_binary_op` for `=/!=/<...>=`, `analyze_in_list`,
/// `analyze_between`.
pub(crate) fn coerce_literal_for_comparison(left: &TypedExpr, right: TypedExpr) -> TypedExpr {
    if !is_column_ref(left) {
        return right;
    }
    if !is_coercible_target(&left.data_type) {
        return right;
    }
    if !matches!(right.data_type, DataType::Utf8 | DataType::LargeUtf8) {
        return right;
    }
    // Reuse the existing coercion that already handles STRING → DATE / TIMESTAMP.
    // Decimal128 / Int* coercion still produces a Cast expression that the
    // evaluator handles at runtime; analyzer-level we just attach the cast.
    super::resolve_expr::coerce_to_target_type(right, &left.data_type)
}

#[cfg(test)]
mod coercion_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use arrow::datatypes::TimeUnit;

    fn column(ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: "c".to_string(),
            },
            data_type: ty,
            nullable: false,
        }
    }

    fn string_lit(s: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(s.to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    #[test]
    fn coerces_string_literal_to_datetime_microsecond() {
        let left = column(DataType::Timestamp(TimeUnit::Microsecond, None));
        let right = string_lit("2020-01-01 00:00:00.012");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert!(matches!(
            coerced.data_type,
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
        assert!(matches!(coerced.kind, ExprKind::Cast { .. }));
    }

    #[test]
    fn coerces_string_literal_to_date32() {
        let left = column(DataType::Date32);
        let right = string_lit("2020-01-01");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert_eq!(coerced.data_type, DataType::Date32);
    }

    #[test]
    fn does_not_coerce_when_left_is_not_column_ref() {
        // expr-vs-literal: skip coercion to avoid surprising arithmetic results.
        let left = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(5)),
            data_type: DataType::Int32,
            nullable: false,
        };
        let right = string_lit("foo");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert_eq!(coerced.data_type, DataType::Utf8);
    }

    #[test]
    fn does_not_coerce_when_right_already_typed() {
        let left = column(DataType::Timestamp(TimeUnit::Microsecond, None));
        let right = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1_672_531_200_000_000)),
            data_type: DataType::Timestamp(TimeUnit::Microsecond, None),
            nullable: false,
        };
        let coerced = coerce_literal_for_comparison(&left, right);
        assert!(matches!(
            coerced.kind,
            ExprKind::Literal(LiteralValue::Int(_))
        ));
    }

    #[test]
    fn does_not_coerce_for_non_coercible_target_types() {
        let left = column(DataType::Boolean);
        let right = string_lit("true");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert_eq!(coerced.data_type, DataType::Utf8);
    }
}
